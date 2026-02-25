"""
Pytest configuration and shared fixtures for test suite.
"""
import json
import pytest
import sys
from pathlib import Path

# Add src directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


@pytest.fixture
def sample_config():
    """Provide a sample configuration dictionary."""
    return {
        "database": "test_database",
        "host": "localhost",
        "port": 8086,
        "bucket": "test_bucket",
        "org": "test_org"
    }


@pytest.fixture
def temp_config_file(tmp_path, sample_config):
    """Create a temporary configuration file."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_file = config_dir / "test.json"
    config_file.write_text(json.dumps(sample_config))
    return config_file


@pytest.fixture
def past_date():
    """Provide a valid past date string."""
    return "2024-01-15"


@pytest.fixture
def multiple_past_dates():
    """Provide multiple valid past date strings."""
    return ["2024-01-15", "2024-01-16", "2024-01-17"]
