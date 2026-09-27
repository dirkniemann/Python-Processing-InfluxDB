#!/usr/bin/env python3
"""
Main entry point for InfluxDB Home Assistant data analysis script.
Handles argument parsing, configuration loading, and environment setup.
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List
from dotenv import load_dotenv

from moduls.logger_setup import get_logger, write_run_summary
from moduls.influxdb_handler import InfluxDBHandler
from moduls.processing.HomeAssistant_processing import HomeAssistantProcessor

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
    
    # Log level argument
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=None,
        help="Logging level (default: DEBUG for dev/test, WARNING for prod)"
    )
    
    # Custom log file argument
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Custom log file path (default: logs/app_{stage}_{timestamp}.log)"
    )
    
    return parser.parse_args()


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
    start_time = datetime.now()
    finished_time = start_time
    stage = "unknown"
    logger = None
    influx_handler = None
    days_processed = 0
    step = "argument parsing"
    error = None
    exit_code = 0

    try:
        args = parse_arguments()
        stage = args.stage

        step = "logger setup"
        log_level = getattr(logging, args.log_level.upper(), None) if args.log_level else None
        logger = get_logger(
            stage=args.stage,
            log_level=log_level,
            log_file=args.log_file,
            name=__name__,
        )
        logger.info("Application started")
        logger.info(f"Stage: {args.stage}")

        step = "environment setup"
        setup_environment(args.stage)
        logger.debug("Environment variables loaded")

        step = "configuration loading"
        config = load_configuration(args.stage)
        logger.debug("Configuration loaded successfully")

        step = "InfluxDB connection"
        influx_handler = InfluxDBHandler()
        if not influx_handler.connect():
            raise RuntimeError("Could not connect to InfluxDB")
        logger.info("InfluxDB handler connected successfully")

        step = "first data day lookup"
        first_data_day = influx_handler.get_first_data_day(
            bucket=config["processing"]["input_bucket"],
        )
        if first_data_day is None:
            raise RuntimeError("No data available in input bucket; aborting run")

        step = "processor initialization"
        ha_processor = HomeAssistantProcessor(
            influx_handler=influx_handler,
            processing_config=config["processing"],
            first_data_day=first_data_day,
        )

        step = "data processing"
        days_processed = ha_processor.process_data()
        step = "completed"
    except KeyboardInterrupt as exc:
        error = exc
        exit_code = 130
        if logger:
            logger.error(f"Run interrupted during {step}", exc_info=True)
        else:
            print(f"Run interrupted during {step}", file=sys.stderr)
    except Exception as exc:
        error = exc
        exit_code = 1
        if logger:
            logger.error(f"Run failed during {step}: {exc}", exc_info=True)
        else:
            print(f"Run failed during {step}: {exc}", file=sys.stderr)
    finally:
        if influx_handler:
            try:
                step = "InfluxDB disconnect"
                influx_handler.disconnect()
            except Exception as exc:
                if error is None:
                    error = exc
                if logger:
                    logger.error(f"Run failed during disconnect: {exc}", exc_info=True)

        finished_time = datetime.now()
        status = "ERROR" if error else "SUCCESS"
        try:
            write_run_summary(
                started_at=start_time,
                finished_at=finished_time,
                stage=stage,
                status=status,
                days=days_processed,
                step=None if status == "SUCCESS" else step,
                error_type=None if error is None else type(error).__name__,
                error_message=(
                    None
                    if error is None
                    else str(error) or "Lauf manuell abgebrochen"
                ),
            )
        except Exception as summary_error:
            message = f"Unable to write run summary: {summary_error}"
            if logger:
                logger.error(message, exc_info=True)
            else:
                print(message, file=sys.stderr)
            error = error or summary_error
            exit_code = exit_code or 1

    if error:
        return exit_code or 1

    if logger:
        logger.info(f"Application completed successfully. Duration: {finished_time - start_time}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
