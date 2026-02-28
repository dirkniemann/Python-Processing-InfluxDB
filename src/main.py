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

from moduls.logger_setup import get_logger
from moduls.influxdb_handler import InfluxDBHandler
from moduls.HomeAssistant_processing import HomeAssistantProcessor

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
    try:
        start_time = datetime.now()
        # Parse arguments
        args = parse_arguments()
        
        # Convert log level string to logging constant
        log_level = None
        if args.log_level:
            log_level = getattr(logging, args.log_level.upper(), None)
        
        # Setup logger
        logger = get_logger(
            stage=args.stage,
            log_level=log_level,
            log_file=args.log_file,
            name=__name__
        )
        
        logger.info("Application started")
        logger.info(f"Stage: {args.stage}")
        
        # Setup environment
        setup_environment(args.stage)
        logger.debug("Environment variables loaded")
        
        # Load configuration
        config = load_configuration(args.stage)
        logger.debug("Configuration loaded successfully")
        logger.debug(f"Config: {config}")
        
        # Initialize InfluxDB handler
        logger.debug("Initializing InfluxDB handler...")
        try:
            influx_handler = InfluxDBHandler()
            if influx_handler.connect():
                logger.debug("InfluxDB handler connected successfully")
                
                # Initialize HomeAssistant processor
                try:
                    first_data_day = influx_handler.get_first_data_day(
                        bucket=config["processing"]["input_bucket"],
                    )
                    ha_processor = HomeAssistantProcessor(
                        influx_handler=influx_handler,
                        processing_config=config["processing"],
                        first_data_day=first_data_day
                    )
                    logger.debug("HomeAssistant processor initialized successfully")
                    
                    # Process data
                    ha_processor.process_data()

                except (KeyError, ValueError) as e:
                    logger.error(f"Failed to initialize processor: {e}", exc_info=True)
                
                influx_handler.disconnect()
            else:
                logger.warning("Could not connect to InfluxDB")
        except (ImportError, ValueError) as e:
            logger.error(f"InfluxDB handler initialization failed: {e}")

        duration = datetime.now() - start_time
        logger.info(f"Application completed successfully. Duration: {duration}")
        
        return 0
        
    except ValueError as e:
        if 'logger' in locals():
            logger.error(f"Value Error: {e}", exc_info=True)
        else:
            print(f"Error: {e}", file=sys.stderr)
        return 1
    except FileNotFoundError as e:
        if 'logger' in locals():
            logger.error(f"File not found: {e}", exc_info=True)
        else:
            print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        if 'logger' in locals():
            logger.error(f"Unexpected error: {e}", exc_info=True)
        else:
            print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
