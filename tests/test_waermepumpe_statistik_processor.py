import pytest
import importlib
from datetime import datetime, timedelta, timezone


@pytest.fixture(scope="module")
def processor_module():
    return importlib.import_module("moduls.processing.waermepumpe_statistik_processor")


@pytest.mark.parametrize(
    ("grid_kwh", "pump_energy", "expected_grid"),
    [
        (5.0, {"wp1": 3.0, "wp2": 0.0}, {"wp1": 3.0, "wp2": 0.0}),
        (0.5, {"wp1": 4.0, "wp2": 0.0}, {"wp1": 0.5, "wp2": 0.0}),
        (2.0, {"wp1": 1.5, "wp2": 1.5}, {"wp1": 1.0, "wp2": 1.0}),
        (5.0, {"wp1": 1.5, "wp2": 1.5}, {"wp1": 1.5, "wp2": 1.5}),
        (5.0, {"wp1": 0.0, "wp2": 2.0}, {"wp1": 0.0, "wp2": 2.0}),
        (5.0, {"wp1": 0.0, "wp2": 0.0}, {"wp1": 0.0, "wp2": 0.0}),
    ],
)
def test_allocate_common_grid_budget_is_shared_and_capped(
    grid_kwh, pump_energy, expected_grid, processor_module
):
    result = processor_module.allocate_common_grid_budget(pump_energy, grid_kwh)

    assert result == pytest.approx(expected_grid)
    assert sum(result.values()) <= max(grid_kwh, 0.0)
    for entity, energy in pump_energy.items():
        assert 0.0 <= result[entity] <= energy


def test_allocate_common_grid_budget_is_proportional(processor_module):
    result = processor_module.allocate_common_grid_budget({"wp1": 1.0, "wp2": 3.0}, 2.0)

    assert result == pytest.approx({"wp1": 0.5, "wp2": 1.5})


@pytest.mark.parametrize(
    ("previous", "current", "expected"),
    [(1.25, 2.0, 0.75), (2.0, 0.5, 0.0), (1.0, 1.0, 0.0)],
)
def test_counter_resets_do_not_create_negative_energy(
    previous, current, expected, processor_module
):
    assert processor_module.calculate_positive_counter_delta(previous, current) == pytest.approx(expected)


def test_change_only_state_is_carried_forward_and_post_run_is_included(processor_module):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    stop = start + timedelta(minutes=30)
    events = [
        {"time": start - timedelta(minutes=10), "value": 1},
        {"time": start + timedelta(minutes=10), "value": 0},
    ]

    active_seconds = processor_module.WaermepumpeStatistikProcessor._active_seconds(
        events, start, stop, post_run_minutes=5
    )

    assert active_seconds == pytest.approx(15 * 60)


def test_unknown_change_only_state_does_not_invent_activity(processor_module):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    stop = start + timedelta(minutes=30)

    active_seconds = processor_module.WaermepumpeStatistikProcessor._active_seconds(
        [], start, stop
    )

    assert active_seconds == 0.0


def test_active_state_before_window_is_carried_to_window_end(processor_module):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    stop = start + timedelta(minutes=30)
    events = [{"time": start - timedelta(minutes=10), "value": 1}]

    active_seconds = processor_module.WaermepumpeStatistikProcessor._active_seconds(
        events, start, stop
    )

    assert active_seconds == pytest.approx(30 * 60)


def test_count_start_events_counts_transitions_not_measurement_intervals(processor_module):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    stop = start + timedelta(hours=1)
    events = [
        {"time": start - timedelta(minutes=5), "value": 0},
        {"time": start + timedelta(minutes=1), "value": 1},
        {"time": start + timedelta(minutes=10), "value": 1},
        {"time": start + timedelta(minutes=20), "value": 0},
        {"time": start + timedelta(minutes=30), "value": 1},
    ]

    starts = processor_module.WaermepumpeStatistikProcessor._count_start_events(
        events, start, stop
    )

    assert starts == 2


def test_process_day_writes_pv_grid_and_daily_totals(processor_module):
    day = datetime(2026, 9, 20).date()
    day_start = datetime(2026, 9, 19, 22, tzinfo=timezone.utc)
    hour = timedelta(hours=1)
    roles = {
        "heat_pump_1_counter": {"entity_id": "pump1", "field": "value"},
        "heat_pump_2_counter": {"entity_id": "pump2", "field": "value"},
        "grid_power": {"entity_id": "grid", "field": "value", "measurement": "W"},
        "compressor_1": {"entity_id": "comp1", "field": "value", "measurement": "state"},
        "compressor_2": {"entity_id": "comp2", "field": "value", "measurement": "state"},
    }
    data = {
        "pump1": [
            {"time": day_start, "value": 0.0},
            {"time": day_start + hour, "value": 0.1},
            {"time": day_start + 2 * hour, "value": 10.4},
        ],
        "pump2": [
            {"time": day_start, "value": 0.0},
            {"time": day_start + hour, "value": 0.1},
            {"time": day_start + 2 * hour, "value": 12.8},
        ],
        "grid": [{"time": day_start, "value": 5500.0}],
        "comp1": [
            {"time": day_start, "value": 0},
            {"time": day_start + hour, "value": 1},
            {"time": day_start + 2 * hour, "value": 0},
        ],
        "comp2": [{"time": day_start, "value": 0}],
    }

    class Handler:
        def __init__(self):
            self.writes = []

        def get_data(self, **kwargs):
            return list(data[kwargs["entity_id"]])

        def write_datapoint(self, **kwargs):
            self.writes.append(kwargs)

    processor = processor_module.WaermepumpeStatistikProcessor(
        influx_handler=Handler(),
        input_bucket="input",
        output_bucket="output",
        version="v2",
        entities=[],
        first_data_day=day,
        output_measurement="stats",
        output_entity_id="total",
        sensor_roles=roles,
        source_version="v1",
        emit_daily_summary=True,
    )

    processor._process_day(day, "v1")

    fields = [write["field"] for write in processor.influx_handler.writes]
    assert "pv_contribution" in fields
    assert "grid_import" in fields
    assert fields.count("daily_pv") == 3
    assert fields.count("daily_grid_import") == 3
    daily_values = {
        write["entity_id"]: write["value"]
        for write in processor.influx_handler.writes
        if write["field"] == "daily_pv"
    }
    assert daily_values["total"] == pytest.approx(
        daily_values["pump1"] + daily_values["pump2"]
    )