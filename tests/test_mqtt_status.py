import json
from datetime import date, datetime, timezone

from moduls.mqtt_status import MQTTConfig, MQTTStatusPublisher, sanitize_text


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
        if hasattr(self, "on_connect"):
            self.on_connect(self, None, {}, 0)
        return None

    def loop_stop(self):
        return None

    def publish(self, topic, payload, qos, retain):
        self.published.append((topic, payload, qos, retain))
        return PublishResult()

    def disconnect(self):
        self.connected = False

    def is_connected(self):
        return self.connected


class RejectingClient(FakeClient):
    def loop_start(self):
        if hasattr(self, "on_connect"):
            self.on_connect(self, None, {}, 5)


def make_config(tmp_path):
    return MQTTConfig.from_mapping(
        {
            "enabled": True,
        },
        "test",
    )


def test_mqtt_publishes_discovery_availability_and_run_status(tmp_path):
    fake = FakeClient()
    publisher = MQTTStatusPublisher(make_config(tmp_path), "prod", client_factory=lambda **kwargs: fake)

    assert publisher.connect() is True
    assert fake.will == (
        "python-processing/prod/state/processing_last_run_status",
        "Fehler",
        1,
        True,
    )
    discovery_records = [
        (topic, payload)
        for topic, payload, *_ in fake.published
        if topic.endswith("/config") and payload
    ]
    assert len(discovery_records) == len(publisher.ENTITY_DEFINITIONS)
    discovery_payloads = [json.loads(payload) for _, payload in discovery_records]
    discovered_ids = {payload["default_entity_id"] for payload in discovery_payloads}
    assert "sensor.python_processing_prod_processing_last_run_status" in discovered_ids
    assert "sensor.python_processing_prod_processing_run_state" in discovered_ids
    assert "sensor.python_processing_prod_processing_last_run_diagnostic" in discovered_ids
    assert "sensor.python_processing_prod_processing_last_runtime" in discovered_ids
    assert "sensor.python_processing_prod_processing_last_success" in discovered_ids
    assert "sensor.python_processing_prod_processing_last_processed_date" in discovered_ids
    assert all(
        payload["availability_topic"] == "python-processing/prod/availability"
        and payload["payload_available"] == "online"
        and payload["payload_not_available"] == "offline"
        for payload in discovery_payloads
    )
    status_discovery = next(
        payload for payload in discovery_payloads
        if payload["default_entity_id"] == "sensor.python_processing_prod_processing_last_run_status"
    )
    run_state_discovery = next(
        payload for payload in discovery_payloads
        if payload["default_entity_id"] == "sensor.python_processing_prod_processing_run_state"
    )
    assert status_discovery["device_class"] == "enum"
    assert status_discovery["options"] == ["Erfolgreich", "Erfolgreich mit Warnungen", "Fehler"]
    assert run_state_discovery["device_class"] == "enum"
    assert run_state_discovery["options"] == ["Läuft", "Leerlauf"]
    assert (
        "python-processing/prod/availability",
        "online",
        1,
        True,
    ) in fake.published
    cleared_legacy = [
        (topic, payload)
        for topic, payload, *_ in fake.published
        if topic.endswith("/config") and payload == ""
    ]
    assert len(cleared_legacy) == len(publisher.LEGACY_ENTITIES)

    started = datetime(2026, 9, 27, 4, tzinfo=timezone.utc)
    publisher.publish_running(started)
    publisher.publish_result(
        status="SUCCESS_WITH_WARNINGS",
        started_at=started,
        finished_at=datetime(2026, 9, 27, 4, 0, 3, tzinfo=timezone.utc),
        days_processed=2,
        last_processed_date=date(2026, 9, 26),
        warning_count=2,
        warning_components=["processor"],
        warning_examples=["Messreihe fehlt", "token=secret"],
    )

    payloads = [str(payload) for _, payload, *_ in fake.published]
    assert "Erfolgreich mit Warnungen" in payloads
    assert "Läuft" in payloads
    assert "Leerlauf" in payloads
    diagnostic_payloads = [
        payload
        for topic, payload, *_ in fake.published
        if topic.endswith("/processing_last_run_diagnostic")
    ]
    assert "Messreihe fehlt" in diagnostic_payloads[-1]
    assert "secret" not in " ".join(payloads)
    status_payloads = [
        payload
        for topic, payload, *_ in fake.published
        if topic == publisher.status_topic
    ]
    assert status_payloads == ["Erfolgreich mit Warnungen"]
    assert ("python-processing/prod/state/processing_last_runtime", "3.0", 1, True) in fake.published
    assert ("python-processing/prod/state/processing_last_processed_date", "2026-09-26", 1, True) in fake.published
    assert any(
        topic.endswith("/processing_last_success") and payload == "2026-09-27T04:00:03Z"
        for topic, payload, *_ in fake.published
    )

    publisher.disconnect()
    assert fake.connected is False
    assert not any(
        topic == "python-processing/prod/availability" and payload == "offline"
        for topic, payload, *_ in fake.published
    )


def test_last_run_result_survives_restart_and_is_replaced_by_next_result(tmp_path):
    config = make_config(tmp_path)
    fake = FakeClient()
    publisher = MQTTStatusPublisher(config, "prod", client_factory=lambda **kwargs: fake)
    publisher.connect()
    started = datetime(2026, 9, 27, 4, tzinfo=timezone.utc)
    publisher.publish_result(
        status="ERROR",
        started_at=started,
        finished_at=datetime(2026, 9, 27, 4, 0, 1, tzinfo=timezone.utc),
        days_processed=0,
        last_processed_date=None,
        error=RuntimeError("InfluxDB connection failed"),
        error_step="InfluxDB connection",
    )

    restarted = MQTTStatusPublisher(config, "prod", client_factory=lambda **kwargs: fake)
    assert restarted.connect() is True
    restarted.publish_result(
        status="SUCCESS",
        started_at=started,
        finished_at=datetime(2026, 9, 27, 4, 0, 2, tzinfo=timezone.utc),
        days_processed=0,
        last_processed_date=None,
    )
    status_payloads = [
        payload
        for topic, payload, *_ in fake.published
        if topic == restarted.status_topic
    ]
    assert status_payloads == ["Fehler", "Erfolgreich"]
    diagnostic_payloads = [
        payload
        for topic, payload, *_ in fake.published
        if topic.endswith("/processing_last_run_diagnostic")
    ]
    assert "InfluxDB connection failed" in diagnostic_payloads[0]
    assert diagnostic_payloads[-1] == "Keine Warnungen oder Fehler im letzten Lauf."


def test_unexpected_disconnect_lwt_reports_failed_last_run(tmp_path):
    fake = FakeClient()
    publisher = MQTTStatusPublisher(make_config(tmp_path), "prod", client_factory=lambda **kwargs: fake)

    assert publisher.connect() is True
    topic, payload, qos, retain = fake.will
    assert topic == publisher.status_topic
    assert payload == "Fehler"
    assert qos == 1
    assert retain is True


def test_keyboard_interrupt_is_published_as_error_diagnostic(tmp_path):
    fake = FakeClient()
    publisher = MQTTStatusPublisher(make_config(tmp_path), "prod", client_factory=lambda **kwargs: fake)
    assert publisher.connect() is True
    started = datetime(2026, 9, 27, 4, tzinfo=timezone.utc)

    publisher.publish_result(
        status="ERROR",
        started_at=started,
        finished_at=datetime(2026, 9, 27, 4, 0, 1, tzinfo=timezone.utc),
        days_processed=0,
        last_processed_date=None,
        error=KeyboardInterrupt(),
        error_step="data processing",
    )

    assert (publisher.status_topic, "Fehler", 1, True) in fake.published
    diagnostic = next(
        payload
        for topic, payload, *_ in fake.published
        if topic.endswith("/processing_last_run_diagnostic")
    )
    assert diagnostic == "data processing: KeyboardInterrupt: Lauf manuell abgebrochen"


def test_sanitize_text_is_single_line_and_bounded():
    value = sanitize_text("password=secret\n" + "x" * 500)
    assert "secret" not in value
    assert "\n" not in value
    assert len(value) == 240


def test_rejected_connection_does_not_publish_status(tmp_path):
    fake = RejectingClient()
    publisher = MQTTStatusPublisher(make_config(tmp_path), "prod", client_factory=lambda **kwargs: fake)

    assert publisher.connect() is False
    assert fake.published == []
