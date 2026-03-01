import logging
import importlib
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
