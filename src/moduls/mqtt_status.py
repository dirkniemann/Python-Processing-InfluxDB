"""Best-effort MQTT status reporting for Home Assistant."""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Mapping, Optional

try:
    import paho.mqtt.client as mqtt
except ImportError:  # MQTT is optional when disabled in the stage config.
    mqtt = None

logger = logging.getLogger(__name__)

MAX_TEXT_LENGTH = 240
MAX_EXAMPLES = 5
SAFE_TOPIC_PART = re.compile(r"[^A-Za-z0-9_.-]+")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def sanitize_text(value: Any, limit: int = MAX_TEXT_LENGTH) -> str:
    text = str(value or "")
    text = " ".join(text.replace("\r", " ").replace("\n", " ").split())
    text = re.sub(r"(?i)(token|password|passwd|secret|authorization)\s*[=:]\s*\S+", r"\1=[REDACTED]", text)
    return text[:limit]


def topic_part(value: str) -> str:
    result = SAFE_TOPIC_PART.sub("-", value.strip()).strip(".-")
    return result or "processing"


@dataclass(frozen=True)
class MQTTConfig:
    enabled: bool
    host: str = "localhost"
    port: int = 1883
    tls: bool = False
    username_env: str = "MQTT_USERNAME"
    password_env: str = "MQTT_PASSWORD"
    client_id: str = "python-processing"
    topic_prefix: str = "python-processing"
    discovery_prefix: str = "homeassistant"
    qos: int = 1
    keepalive: int = 30
    connect_timeout_seconds: float = 5.0
    publish_timeout_seconds: float = 5.0
    retries: int = 2
    heartbeat_interval_seconds: int = 60
    state_file: str = "logs/mqtt_status.json"

    @classmethod
    def from_mapping(cls, values: Optional[Mapping[str, Any]], stage: str) -> "MQTTConfig":
        raw = dict(values or {})
        config = cls(
            enabled=bool(raw.get("enabled", stage == "prod")),
            host=str(raw.get("host", "localhost")),
            port=int(raw.get("port", 1883)),
            tls=bool(raw.get("tls", False)),
            username_env=str(raw.get("username_env", "MQTT_USERNAME")),
            password_env=str(raw.get("password_env", "MQTT_PASSWORD")),
            client_id=str(raw.get("client_id", f"python-processing-{stage}")),
            topic_prefix=str(raw.get("topic_prefix", "python-processing")),
            discovery_prefix=str(raw.get("discovery_prefix", "homeassistant")),
            qos=int(raw.get("qos", 1)),
            keepalive=int(raw.get("keepalive", 30)),
            connect_timeout_seconds=float(raw.get("connect_timeout_seconds", 5)),
            publish_timeout_seconds=float(raw.get("publish_timeout_seconds", 5)),
            retries=int(raw.get("retries", 2)),
            heartbeat_interval_seconds=int(raw.get("heartbeat_interval_seconds", 60)),
            state_file=str(raw.get("state_file", f"logs/mqtt_status_{stage}.json")),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if not isinstance(self.enabled, bool) or not isinstance(self.tls, bool):
            raise ValueError("mqtt.enabled and mqtt.tls must be boolean")
        if not self.host.strip():
            raise ValueError("mqtt.host must not be empty")
        if not 1 <= self.port <= 65535:
            raise ValueError("mqtt.port must be between 1 and 65535")
        if not 0 <= self.qos <= 2:
            raise ValueError("mqtt.qos must be between 0 and 2")
        if self.keepalive <= 0 or self.connect_timeout_seconds <= 0 or self.publish_timeout_seconds <= 0:
            raise ValueError("mqtt timeouts and keepalive must be positive")
        if self.retries < 0 or self.heartbeat_interval_seconds <= 0:
            raise ValueError("mqtt retries must be non-negative and heartbeat interval positive")
        for label, value in (("client_id", self.client_id), ("topic_prefix", self.topic_prefix), ("discovery_prefix", self.discovery_prefix)):
            if not value.strip():
                raise ValueError(f"mqtt.{label} must not be empty")

    @property
    def username(self) -> Optional[str]:
        return os.getenv(self.username_env) or None

    @property
    def password(self) -> Optional[str]:
        return os.getenv(self.password_env) or None


@dataclass
class RunDiagnostics:
    warning_count: int = 0
    warning_components: set[str] = field(default_factory=set)
    warning_examples: list[str] = field(default_factory=list)

    def add_warning(self, component: str, message: str) -> None:
        self.warning_count += 1
        self.warning_components.add(sanitize_text(component, 80))
        example = sanitize_text(message)
        if example and example not in self.warning_examples and len(self.warning_examples) < MAX_EXAMPLES:
            self.warning_examples.append(example)


class MQTTStatusPublisher:
    """Publishes compact retained run status; MQTT failures never escape publish calls."""

    ENTITY_DEFINITIONS = (
        ("sensor", "processing_last_run_status", "Last run status", None),
        ("binary_sensor", "processing_running", "Processing running", "running"),
        ("binary_sensor", "processing_failure_latched", "Processing failure latched", "problem"),
        ("sensor", "processing_last_success", "Last successful processing run", "timestamp"),
        ("sensor", "processing_last_runtime", "Last processing runtime", "duration"),
        ("sensor", "processing_last_processed_date", "Last processed date", None),
    )

    def __init__(self, config: MQTTConfig, stage: str, client_factory: Optional[Callable[..., Any]] = None):
        self.config = config
        self.stage = topic_part(stage)
        self.node = topic_part(f"{config.topic_prefix}-{stage}")
        self.client_factory = client_factory
        self.client: Any = None
        self.connected = False
        self.state = self._load_state()

    @property
    def availability_topic(self) -> str:
        return f"{self.config.topic_prefix}/{self.stage}/availability"

    @property
    def state_topic(self) -> str:
        return f"{self.config.topic_prefix}/{self.stage}/state"

    def connect(self) -> bool:
        if not self.config.enabled:
            return False
        if mqtt is None and self.client_factory is None:
            logger.warning("MQTT enabled but paho-mqtt is not installed")
            return False
        factory = self.client_factory or mqtt.Client
        for attempt in range(self.config.retries + 1):
            try:
                self.client = factory(client_id=self.config.client_id, protocol=mqtt.MQTTv311 if mqtt else None)
                if self.config.username:
                    self.client.username_pw_set(self.config.username, self.config.password)
                if self.config.tls:
                    self.client.tls_set()
                self.client.will_set(self.availability_topic, payload="offline", qos=self.config.qos, retain=True)
                self.client.connect(self.config.host, self.config.port, self.config.keepalive)
                loop_start = getattr(self.client, "loop_start", None)
                if loop_start:
                    loop_start()
                self.connected = True
                self.publish(self.availability_topic, "online", retain=True)
                self.publish_discovery()
                return True
            except Exception as exc:
                logger.warning("MQTT connection attempt %d/%d failed: %s", attempt + 1, self.config.retries + 1, sanitize_text(exc))
                self.connected = False
                self.client = None
        return False

    def publish(self, topic: str, payload: Any, *, retain: bool = True) -> bool:
        if not self.connected or self.client is None:
            return False
        try:
            value = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
            result = self.client.publish(topic, value, qos=self.config.qos, retain=retain)
            wait_for_publish = getattr(result, "wait_for_publish", None)
            if wait_for_publish:
                wait_for_publish(timeout=self.config.publish_timeout_seconds)
            return True
        except Exception as exc:
            logger.warning("MQTT publish failed for %s: %s", topic, sanitize_text(exc))
            return False

    def publish_discovery(self) -> None:
        device = {
            "identifiers": [f"python-processing-{self.stage}"],
            "name": f"Python Processing {self.stage}",
            "manufacturer": "Python Processing",
        }
        for component, object_id, name, device_class in self.ENTITY_DEFINITIONS:
            state_topic = f"{self.state_topic}/{object_id}"
            payload: Dict[str, Any] = {
                "name": name,
                "unique_id": f"python_processing_{self.stage}_{object_id}",
                "state_topic": state_topic,
                "availability_topic": self.availability_topic,
                "payload_available": "online",
                "payload_not_available": "offline",
                "device": device,
            }
            if object_id == "processing_last_run_status":
                payload["json_attributes_topic"] = f"{self.state_topic}/last_run_attributes"
            if object_id == "processing_last_runtime":
                payload["unit_of_measurement"] = "s"
            if component == "binary_sensor":
                payload.update({"payload_on": "ON", "payload_off": "OFF"})
            if device_class:
                payload["device_class"] = device_class
            discovery_topic = f"{self.config.discovery_prefix}/{component}/{self.node}/{object_id}/config"
            self.publish(discovery_topic, payload, retain=True)

    def publish_running(self, started_at: datetime) -> None:
        self.publish(f"{self.state_topic}/processing_running", "ON")
        self.publish(f"{self.state_topic}/run_state", {"run_state": "RUNNING", "started_at": iso_utc(started_at)})

    def publish_heartbeat(self, timestamp: Optional[datetime] = None) -> None:
        self.publish(f"{self.state_topic}/heartbeat", iso_utc(timestamp or utc_now()))

    def publish_result(
        self,
        *,
        status: str,
        started_at: datetime,
        finished_at: datetime,
        days_processed: int,
        last_processed_date: Optional[date],
        diagnostics: RunDiagnostics,
        error: Optional[BaseException] = None,
    ) -> None:
        if status not in {"SUCCESS", "SUCCESS_WITH_WARNINGS", "FAILED"}:
            raise ValueError(f"Unsupported MQTT run status: {status}")
        duration = max(0, (finished_at - started_at).total_seconds())
        failure = sanitize_text(error) if error else None
        result = {
            "status": status,
            "started_at": iso_utc(started_at),
            "finished_at": iso_utc(finished_at),
            "runtime_seconds": duration,
            "processed_days": int(days_processed),
            "processed_date": last_processed_date.isoformat() if last_processed_date else None,
            "warning_count": diagnostics.warning_count,
            "warning_components": sorted(diagnostics.warning_components),
            "warning_examples": diagnostics.warning_examples,
        }
        if failure:
            result["error"] = failure
        self.state["last_run"] = result
        if status == "FAILED":
            self.state["failure_latched"] = True
            self.state["last_failure"] = {"timestamp": iso_utc(finished_at), "error": failure or "run failed"}
        else:
            self.state["failure_latched"] = False
            self.state["last_success"] = iso_utc(finished_at)
        self._save_state()
        self.publish(f"{self.state_topic}/processing_running", "OFF")
        self.publish(f"{self.state_topic}/processing_failure_latched", "ON" if self.state["failure_latched"] else "OFF")
        self.publish(f"{self.state_topic}/processing_last_run_status", status)
        self.publish(f"{self.state_topic}/processing_last_success", self.state.get("last_success", ""))
        self.publish(f"{self.state_topic}/processing_last_runtime", duration)
        self.publish(f"{self.state_topic}/processing_last_processed_date", result["processed_date"] or "")
        self.publish(f"{self.state_topic}/last_run_attributes", result)
        self.publish(f"{self.state_topic}/run_state", {"run_state": "IDLE", "finished_at": result["finished_at"]})

    def disconnect(self) -> None:
        if not self.client:
            return
        try:
            self.publish(self.availability_topic, "offline", retain=True)
            loop_stop = getattr(self.client, "loop_stop", None)
            if loop_stop:
                loop_stop()
            self.client.disconnect()
        except Exception as exc:
            logger.warning("MQTT disconnect failed: %s", sanitize_text(exc))
        finally:
            self.connected = False
            self.client = None

    def cleanup_discovery(self) -> None:
        if not self.connected:
            return
        for component, object_id, _, _ in self.ENTITY_DEFINITIONS:
            topic = f"{self.config.discovery_prefix}/{component}/{self.node}/{object_id}/config"
            self.publish(topic, "", retain=True)

    def _load_state(self) -> Dict[str, Any]:
        path = Path(self.config.state_file)
        try:
            with path.open(encoding="utf-8") as handle:
                value = json.load(handle)
            return value if isinstance(value, dict) else {}
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return {}

    def _save_state(self) -> None:
        path = Path(self.config.state_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self.state, handle, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_name, path)
        finally:
            if os.path.exists(temporary_name):
                os.unlink(temporary_name)
