"""Best-effort MQTT status reporting for Home Assistant."""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, Iterable, Mapping, Optional

try:
    import paho.mqtt.client as mqtt
except ImportError:  # MQTT is optional when disabled in the stage config.
    mqtt = None

logger = logging.getLogger(__name__)

SAFE_TOPIC_PART = re.compile(r"[^A-Za-z0-9_.-]+")


def iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def sanitize_text(value: Any, limit: int = 240) -> str:
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
        if self.retries < 0:
            raise ValueError("mqtt retries must be non-negative")
        for label, value in (("client_id", self.client_id), ("topic_prefix", self.topic_prefix), ("discovery_prefix", self.discovery_prefix)):
            if not value.strip():
                raise ValueError(f"mqtt.{label} must not be empty")

    @property
    def username(self) -> Optional[str]:
        return os.getenv(self.username_env) or None

    @property
    def password(self) -> Optional[str]:
        return os.getenv(self.password_env) or None


class MQTTStatusPublisher:
    """Publishes compact retained run status; MQTT failures never escape publish calls."""

    ENTITY_DEFINITIONS = (
        ("sensor", "processing_last_run_status", "Letzter Lauf", None),
        ("sensor", "processing_run_state", "Laufzustand", None),
        ("sensor", "processing_last_success", "Letzter erfolgreicher Lauf", "timestamp"),
        ("sensor", "processing_last_runtime", "Laufzeit letzter Lauf", "duration"),
        ("sensor", "processing_last_processed_date", "Letzter Verarbeitungstag", "date"),
        ("sensor", "processing_last_run_diagnostic", "Diagnose letzter Lauf", None),
    )
    LEGACY_ENTITIES = (
        ("binary_sensor", "processing_running"),
        ("binary_sensor", "processing_failure_latched"),
        ("sensor", "processing_running"),
        ("sensor", "processing_last_error"),
    )

    def __init__(self, config: MQTTConfig, stage: str, client_factory: Optional[Callable[..., Any]] = None):
        self.config = config
        self.stage = topic_part(stage)
        self.node = topic_part(f"{config.topic_prefix}-{stage}")
        self.client_factory = client_factory
        self.client: Any = None
        self.connected = False

    @property
    def state_topic(self) -> str:
        return f"{self.config.topic_prefix}/{self.stage}/state"

    @property
    def status_topic(self) -> str:
        return f"{self.state_topic}/processing_last_run_status"

    def entity_topic(self, object_id: str) -> str:
        return f"{self.state_topic}/{object_id}"

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
                connected_event = threading.Event()
                connection_result = {"accepted": False}

                def on_connect(_client, _userdata, _flags, reason_code, _properties=None):
                    connection_result["accepted"] = reason_code == 0
                    connected_event.set()

                self.client.on_connect = on_connect
                if self.config.username:
                    self.client.username_pw_set(self.config.username, self.config.password)
                if self.config.tls:
                    self.client.tls_set()
                # If the process dies unexpectedly, Home Assistant should show
                # the last run as failed. A clean disconnect does not publish
                # the Will, so the retained result remains unchanged.
                self.client.will_set(self.status_topic, payload="ERROR", qos=self.config.qos, retain=True)
                self.client.connect(self.config.host, self.config.port, self.config.keepalive)
                loop_start = getattr(self.client, "loop_start", None)
                if loop_start:
                    loop_start()
                if self.client_factory is None:
                    if not connected_event.wait(self.config.connect_timeout_seconds):
                        raise TimeoutError("MQTT broker did not acknowledge the connection in time")
                    if not connection_result["accepted"]:
                        raise ConnectionError("MQTT broker rejected the connection")
                elif connected_event.is_set() and not connection_result["accepted"]:
                    raise ConnectionError("MQTT broker rejected the connection")
                self.connected = True
                self.publish_discovery()
                return True
            except Exception as exc:
                logger.warning("MQTT connection attempt %d/%d failed: %s", attempt + 1, self.config.retries + 1, sanitize_text(exc))
                failed_client = self.client
                if failed_client is not None:
                    try:
                        loop_stop = getattr(failed_client, "loop_stop", None)
                        if loop_stop:
                            loop_stop()
                        disconnect = getattr(failed_client, "disconnect", None)
                        if disconnect:
                            disconnect()
                    except Exception:
                        pass
                self.connected = False
                self.client = None
        return False

    def publish(self, topic: str, payload: Any, *, retain: bool = True) -> bool:
        if not self.connected or self.client is None:
            return False
        is_connected = getattr(self.client, "is_connected", None)
        if is_connected is not None and not is_connected():
            self.connected = False
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
        for component, object_id, _, _ in self.ENTITY_DEFINITIONS:
            payload = self._discovery_payload(object_id)
            discovery_topic = f"{self.config.discovery_prefix}/{component}/{self.node}/{object_id}/config"
            self.publish(discovery_topic, payload, retain=True)
        # Remove discovery records from the former multi-entity dashboard so
        # Home Assistant does not keep showing duplicate or diagnostic rows.
        for component, object_id in self.LEGACY_ENTITIES:
            discovery_topic = f"{self.config.discovery_prefix}/{component}/{self.node}/{object_id}/config"
            self.publish(discovery_topic, "", retain=True)

    def _discovery_payload(self, object_id: str) -> Dict[str, Any]:
        definition = next(item for item in self.ENTITY_DEFINITIONS if item[1] == object_id)
        component, _, name, device_class = definition
        payload: Dict[str, Any] = {
            "name": name,
            "unique_id": f"python_processing_{self.stage}_{object_id}",
            "default_entity_id": f"{component}.python_processing_{self.stage}_{object_id}",
            "state_topic": self.entity_topic(object_id),
            "device": {
                "identifiers": [f"python-processing-{self.stage}"],
                "name": "Python Processing",
                "manufacturer": "Python Processing",
            },
        }
        if device_class:
            payload["device_class"] = device_class
        if object_id == "processing_last_runtime":
            payload["unit_of_measurement"] = "s"
        return payload

    def publish_running(self, started_at: datetime) -> None:
        self.publish(self.entity_topic("processing_run_state"), "RUNNING")

    def publish_result(
        self,
        *,
        status: str,
        started_at: datetime,
        finished_at: datetime,
        days_processed: int,
        last_processed_date: Optional[date],
        warning_count: int = 0,
        warning_components: Optional[Iterable[str]] = None,
        warning_examples: Optional[Iterable[str]] = None,
        error: Optional[BaseException] = None,
        error_step: Optional[str] = None,
    ) -> None:
        if status not in {"SUCCESS", "SUCCESS_WITH_WARNINGS", "ERROR"}:
            raise ValueError(f"Unsupported MQTT run status: {status}")
        duration = max(0.0, (finished_at - started_at).total_seconds())
        examples = [sanitize_text(item, 180) for item in (warning_examples or [])]
        examples = [item for item in examples if item][:5]
        components = sorted({sanitize_text(item, 80) for item in (warning_components or [])})
        if status == "ERROR":
            details = sanitize_text(error) if error else "Unbekannter Fehler"
            diagnostic = f"{error_step}: " if error_step else ""
            if error:
                diagnostic += f"{type(error).__name__}: "
            diagnostic += details
        elif warning_count:
            diagnostic = f"{warning_count} Warnung(en)"
            if components:
                diagnostic += f" in {', '.join(components[:3])}"
            if examples:
                diagnostic += ": " + " | ".join(examples)
            if examples and warning_count > len(examples):
                diagnostic += f" (und {warning_count - len(examples)} weitere)"
        else:
            diagnostic = "Keine Warnungen oder Fehler im letzten Lauf."

        self.publish(self.status_topic, status)
        self.publish(self.entity_topic("processing_run_state"), "IDLE")
        self.publish(self.entity_topic("processing_last_runtime"), duration)
        self.publish(self.entity_topic("processing_last_run_diagnostic"), diagnostic)
        if status != "ERROR":
            self.publish(self.entity_topic("processing_last_success"), iso_utc(finished_at))
        if last_processed_date is not None:
            self.publish(
                self.entity_topic("processing_last_processed_date"),
                last_processed_date.isoformat(),
            )

    def disconnect(self) -> None:
        if not self.client:
            return
        try:
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
        for component, object_id in self.LEGACY_ENTITIES:
            topic = f"{self.config.discovery_prefix}/{component}/{self.node}/{object_id}/config"
            self.publish(topic, "", retain=True)
