import json
from datetime import date, datetime, timezone

from moduls.mqtt_status import MQTTConfig, MQTTStatusPublisher, RunDiagnostics, sanitize_text


class PublishResult:
    def wait_for_publish(self, timeout=None):
        return None


class FakeClient:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.published = []
        self.will = None
        self.connected = False

    def username_pw_set(self, username, password):
        self.credentials = (username, password)

    def will_set(self, topic, payload, qos, retain):
        self.will = (topic, payload, qos, retain)

    def connect(self, host, port, keepalive):
        self.connected = True
        self.connection = (host, port, keepalive)

    def loop_start(self):
        return None

    def loop_stop(self):
        return None

    def publish(self, topic, payload, qos, retain):
        self.published.append((topic, payload, qos, retain))
        return PublishResult()

    def disconnect(self):
        self.connected = False


def make_config(tmp_path):
    return MQTTConfig.from_mapping(
        {
            "enabled": True,
            "state_file": str(tmp_path / "mqtt-status.json"),
        },
        "test",
    )


def test_mqtt_publishes_discovery_availability_and_run_status(tmp_path):
    fake = FakeClient()
    publisher = MQTTStatusPublisher(make_config(tmp_path), "prod", client_factory=lambda **kwargs: fake)

    assert publisher.connect() is True
    assert fake.will == ("python-processing/prod/availability", "offline", 1, True)
    discovery_topics = [topic for topic, *_ in fake.published if topic.endswith("/config")]
    assert len(discovery_topics) == 6
    assert all("python-processing-prod" in topic for topic in discovery_topics)

    diagnostics = RunDiagnostics()
    diagnostics.add_warning("processor", "token=secret\nsecond line")
    started = datetime(2026, 9, 27, 4, tzinfo=timezone.utc)
    publisher.publish_running(started)
    publisher.publish_result(
        status="SUCCESS_WITH_WARNINGS",
        started_at=started,
        finished_at=datetime(2026, 9, 27, 4, 0, 3, tzinfo=timezone.utc),
        days_processed=2,
        last_processed_date=date(2026, 9, 26),
        diagnostics=diagnostics,
    )

    payloads = [str(payload) for _, payload, *_ in fake.published]
    assert "SUCCESS_WITH_WARNINGS" in payloads
    assert "ON" in payloads
    assert "OFF" in payloads
    assert "secret" not in " ".join(payloads)
    assert publisher.state["failure_latched"] is False

    publisher.disconnect()
    assert fake.published[-1][0] == "python-processing/prod/availability"
    assert fake.published[-1][1] == "offline"


def test_failure_latch_survives_restart_and_clears_on_success(tmp_path):
    config = make_config(tmp_path)
    fake = FakeClient()
    publisher = MQTTStatusPublisher(config, "prod", client_factory=lambda **kwargs: fake)
    publisher.connect()
    publisher.publish_result(
        status="FAILED",
        started_at=datetime(2026, 9, 27, 4, tzinfo=timezone.utc),
        finished_at=datetime(2026, 9, 27, 4, 0, 1, tzinfo=timezone.utc),
        days_processed=0,
        last_processed_date=None,
        diagnostics=RunDiagnostics(),
        error=RuntimeError("password=hidden"),
    )

    restarted = MQTTStatusPublisher(config, "prod", client_factory=lambda **kwargs: fake)
    assert restarted.state["failure_latched"] is True
    assert "hidden" not in json.dumps(restarted.state)

    restarted.publish_result(
        status="SUCCESS",
        started_at=datetime(2026, 9, 28, 4, tzinfo=timezone.utc),
        finished_at=datetime(2026, 9, 28, 4, 0, 1, tzinfo=timezone.utc),
        days_processed=0,
        last_processed_date=None,
        diagnostics=RunDiagnostics(),
    )
    assert restarted.state["failure_latched"] is False


def test_sanitize_text_is_single_line_and_bounded():
    value = sanitize_text("password=secret\n" + "x" * 500)
    assert "secret" not in value
    assert "\n" not in value
    assert len(value) == 240
