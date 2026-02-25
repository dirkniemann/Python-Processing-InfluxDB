import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List
from dotenv import load_dotenv

#!/usr/bin/env python3
"""
Main entry point for InfluxDB Home Assistant data analysis script.
Handles argument parsing, configuration loading, and environment setup.
"""


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments with sensible defaults.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="InfluxDB Home Assistant data analysis tool"
    )
    
    # Stage argument (dev, test, prod)
    parser.add_argument(
        "--stage",
        type=str,
        choices=["dev", "test", "prod"],
        default="dev",
        help="Execution stage (default: dev)"
    )
    
    # Dates argument (single date, multiple dates, or 'all')
    parser.add_argument(
        "--dates",
        type=str,
        default="yesterday",
        help="Dates to process: 'yesterday' (default), 'all', or comma-separated dates (YYYY-MM-DD)"
    )
    
    return parser.parse_args()


def validate_dates(dates_input: str) -> List[str]:
    """
    Validate and process date input.
    
    Args:
        dates_input: User input for dates
        
    Returns:
        List of validated date strings (YYYY-MM-DD format)
        
    Raises:
        ValueError: If today's date is included or invalid format
    """
    today = datetime.now().date()
    
    if dates_input.lower() == "yesterday":
        target_date = today - timedelta(days=1)
        return [target_date.strftime("%Y-%m-%d")]
    
    if dates_input.lower() == "all":
        return ["all"]
    
    # Parse comma-separated dates
    date_list = []
    for date_str in dates_input.split(","):
        date_str = date_str.strip()
        try:
            parsed_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            
            if parsed_date >= today:
                raise ValueError(f"Date {date_str} is today or in the future. Only past dates are allowed.")
            
            date_list.append(date_str)
        except ValueError as e:
            raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD format. Error: {e}")
    
    return date_list


def load_configuration(stage: str) -> dict:
    """
    Load configuration based on the specified stage.
    
    Args:
        stage: Execution stage (dev, test, prod)
        
    Returns:
        Configuration dictionary
    """
    config_path = Path(__file__).parent.parent / "config" / f"{stage}.json"
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        return json.load(f)


def setup_environment(stage: str) -> None:
    """
    Load environment variables from .env file.
    
    Args:
        stage: Execution stage (dev, test, prod)
    """
    env_file = Path(__file__).parent.parent / ".env"
    load_dotenv(env_file)


def main() -> int:
    """
    Main application entry point.
    
    Returns:
        Exit code
    """
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Setup environment
        setup_environment(args.stage)
        
        # Load configuration
        config = load_configuration(args.stage)

        # Validate and process dates
        dates = validate_dates(args.dates)
        
        print(f"Stage: {args.stage}")
        print(f"Dates: {dates}")
        print(f"Config loaded: {config}")
        
        # TODO: Add main application logic here
        
        return 0
        
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
