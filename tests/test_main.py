"""
Unit tests for main.py module.
Tests include argument parsing, date validation, and configuration loading.
"""
import json
import pytest
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, mock_open
from src.main import parse_arguments, validate_dates, load_configuration


class TestValidateDates:
    """Test suite for validate_dates function."""
    
    def test_validate_dates_yesterday(self):
        """Test that 'yesterday' returns yesterday's date."""
        result = validate_dates("yesterday")
        expected_date = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
        assert result == [expected_date], f"Expected [{expected_date}], got {result}"
    
    def test_validate_dates_all(self):
        """Test that 'all' returns ['all']."""
        result = validate_dates("all")
        assert result == ["all"], f"Expected ['all'], got {result}"
    
    def test_validate_dates_single_valid_date(self):
        """Test validation of a single valid past date."""
        test_date = "2024-01-15"
        result = validate_dates(test_date)
        assert result == [test_date], f"Expected [{test_date}], got {result}"
    
    def test_validate_dates_multiple_valid_dates(self):
        """Test validation of multiple comma-separated valid dates."""
        test_dates = "2024-01-15, 2024-01-16, 2024-01-17"
        result = validate_dates(test_dates)
        expected = ["2024-01-15", "2024-01-16", "2024-01-17"]
        assert result == expected, f"Expected {expected}, got {result}"
    
    def test_validate_dates_rejects_today(self):
        """Test that today's date raises ValueError."""
        today = datetime.now().date().strftime("%Y-%m-%d")
        with pytest.raises(ValueError, match="is today or in the future"):
            validate_dates(today)
    
    def test_validate_dates_rejects_future(self):
        """Test that future dates raise ValueError."""
        future_date = (datetime.now().date() + timedelta(days=10)).strftime("%Y-%m-%d")
        with pytest.raises(ValueError, match="is today or in the future"):
            validate_dates(future_date)
    
    def test_validate_dates_invalid_format(self):
        """Test that invalid date format raises ValueError."""
        invalid_dates = [
            "2024-13-01",  # Invalid month
            "2024-01-32",  # Invalid day
            "01-01-2024",  # Wrong format
            "not-a-date"   # Completely invalid
        ]
        for invalid_date in invalid_dates:
            with pytest.raises(ValueError, match="Invalid date format"):
                validate_dates(invalid_date)


class TestLoadConfiguration:
    """Test suite for load_configuration function."""
    
    def test_load_configuration_dev(self, tmp_path):
        """Test loading development configuration."""
        # Create a temporary config directory and file
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        config_file = config_dir / "dev.json"
        
        test_config = {
            "database": "test_db",
            "host": "localhost",
            "port": 8086
        }
        config_file.write_text(json.dumps(test_config))
        
        # Patch the Path to point to our temp directory
        with patch('src.main.Path') as mock_path:
            mock_path.return_value.parent.parent = tmp_path
            result = load_configuration("dev")
            assert result == test_config, f"Expected {test_config}, got {result}"
    
    def test_load_configuration_file_not_found(self):
        """Test that missing configuration file raises FileNotFoundError."""
        with patch('src.main.Path') as mock_path:
            mock_path.return_value.parent.parent = Path("/nonexistent")
            with pytest.raises(FileNotFoundError, match="Configuration file not found"):
                load_configuration("dev")
    
    def test_load_configuration_invalid_json(self, tmp_path):
        """Test that invalid JSON raises JSONDecodeError."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        config_file = config_dir / "test.json"
        config_file.write_text("{ invalid json }")
        
        with patch('src.main.Path') as mock_path:
            mock_path.return_value.parent.parent = tmp_path
            with pytest.raises(json.JSONDecodeError):
                load_configuration("test")


class TestParseArguments:
    """Test suite for parse_arguments function."""
    
    def test_parse_arguments_defaults(self):
        """Test default argument values."""
        test_args = ["main.py"]
        with patch.object(sys, 'argv', test_args):
            args = parse_arguments()
            assert args.stage == "dev", f"Expected stage 'dev', got {args.stage}"
            assert args.dates == "yesterday", f"Expected dates 'yesterday', got {args.dates}"
    
    def test_parse_arguments_custom_stage(self):
        """Test parsing custom stage argument."""
        stages = ["dev", "test", "prod"]
        for stage in stages:
            test_args = ["main.py", "--stage", stage]
            with patch.object(sys, 'argv', test_args):
                args = parse_arguments()
                assert args.stage == stage, f"Expected stage '{stage}', got {args.stage}"
    
    def test_parse_arguments_custom_dates(self):
        """Test parsing custom dates argument."""
        test_dates = "2024-01-01,2024-01-02"
        test_args = ["main.py", "--dates", test_dates]
        with patch.object(sys, 'argv', test_args):
            args = parse_arguments()
            assert args.dates == test_dates, f"Expected dates '{test_dates}', got {args.dates}"
    
    def test_parse_arguments_all_options(self):
        """Test parsing all arguments together."""
        test_args = ["main.py", "--stage", "prod", "--dates", "all"]
        with patch.object(sys, 'argv', test_args):
            args = parse_arguments()
            assert args.stage == "prod", f"Expected stage 'prod', got {args.stage}"
            assert args.dates == "all", f"Expected dates 'all', got {args.dates}"
    
    def test_parse_arguments_invalid_stage(self):
        """Test that invalid stage raises SystemExit."""
        test_args = ["main.py", "--stage", "invalid"]
        with patch.object(sys, 'argv', test_args):
            with pytest.raises(SystemExit):
                parse_arguments()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
