import logging
import importlib
from datetime import datetime
from pathlib import Path


def test_get_logger_returns_logger(tmp_path, monkeypatch):
    module = importlib.import_module("moduls.logger_setup")
    importlib.reload(module)

    # redirect log dir to temp to avoid polluting real logs
    monkeypatch.setattr(module.LoggerSetup, "DEFAULT_LOG_DIR", tmp_path)

    logger = module.get_logger(stage="dev", name="test")
    assert isinstance(logger, logging.Logger)
    # ensure file created
    files = list(tmp_path.glob("*.log"))
    assert files, "Log file should be created"


def test_logger_cleanup_removes_old_files(tmp_path, monkeypatch):
    module = importlib.import_module("moduls.logger_setup")
    importlib.reload(module)
    monkeypatch.setattr(module.LoggerSetup, "DEFAULT_LOG_DIR", tmp_path)

    old_file = tmp_path / "20220101_000000.log"
    old_file.write_text("old")
    logger = module.get_logger(stage="dev", name="cleanup")
    assert not old_file.exists()
    assert logging.getLogger().handlers


def test_write_run_summary_appends_one_sanitized_line(tmp_path):
    module = importlib.import_module("moduls.logger_setup")
    summary_file = tmp_path / "runs.log"

    module.write_run_summary(
        started_at=datetime(2026, 9, 27, 4, 0, 0),
        finished_at=datetime(2026, 9, 27, 4, 0, 3),
        stage="prod",
        status="ERROR",
        days=0,
        step="data processing",
        error_type="RuntimeError",
        error_message="token\nwas not included",
        summary_file=summary_file,
    )

    lines = summary_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert "| ERROR |" in lines[0]
    assert "Fehler in data_processing (RuntimeError): token_was_not_included" in lines[0]
    assert "Dauer: 3 s" in lines[0]
    assert "\n" not in lines[0]


def test_run_warning_collector_counts_and_limits_examples():
    module = importlib.import_module("moduls.logger_setup")
    collector = module.RunWarningCollector(max_examples=1, max_message_length=12)
    record = logging.LogRecord("processor", logging.WARNING, __file__, 1, "warning\nmessage", (), None)

    collector.emit(record)
    collector.emit(record)

    assert collector.warning_count == 2
    assert collector.warning_components == {"processor"}
    assert collector.warning_examples == ["warning_mess"]


def test_write_run_summary_includes_warning_count(tmp_path):
    module = importlib.import_module("moduls.logger_setup")
    summary_file = tmp_path / "runs.log"

    module.write_run_summary(
        started_at=datetime(2026, 9, 27, 4, 0, 0),
        finished_at=datetime(2026, 9, 27, 4, 0, 3),
        stage="prod",
        status="SUCCESS_WITH_WARNINGS",
        days=2,
        warning_count=3,
        summary_file=summary_file,
    )

    assert "SUCCESS_WITH_WARNINGS" in summary_file.read_text(encoding="utf-8")
    assert "Warnungen: 3" in summary_file.read_text(encoding="utf-8")
