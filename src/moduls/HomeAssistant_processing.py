import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from moduls.influxdb_handler import InfluxDBHandler
from datetime import timedelta

logger = logging.getLogger(__name__)


class HomeAssistantProcessor:
    """
    Process Home Assistant data from InfluxDB.
    Loads data from input bucket, processes it, and writes to output bucket.
    """
    
    def __init__(
        self, 
        influx_handler: InfluxDBHandler,
        processing_config: Dict[str, Any],
        first_data_day: datetime
    ):
        """
        Initialize the Home Assistant data processor.
        
        Args:
            influx_handler: Connected InfluxDB handler instance
            processing_config: Processing configuration containing input_bucket and output_bucket
            first_data_day: The first day with data in the input bucket, used for processing logic
            
        Raises:
            ValueError: If required configuration keys are missing
        """
        self.influx_handler = influx_handler
        
        # Validate configuration
        required_keys = ["input_bucket", "output_bucket", "version", "entities_to_process"]
        missing_keys = [key for key in required_keys if key not in processing_config]
        if missing_keys:
            raise ValueError(f"Missing required config keys: {', '.join(missing_keys)}")
        
        self.input_bucket = processing_config["input_bucket"]
        self.output_bucket = processing_config["output_bucket"]
        self.version = processing_config["version"]
        self.first_data_day = first_data_day

        entities_to_process = processing_config.get("entities_to_process", {})
        if not isinstance(entities_to_process, dict):
            raise ValueError("'entities_to_process' must be a dictionary")

        daily_aggregate_entities = entities_to_process.get("daily_aggregate", [])
        if not isinstance(daily_aggregate_entities, list):
            raise ValueError("'entities_to_process.daily_aggregate' must be a list")

        self.daily_aggregate_entities: List[str] = [
            entity for entity in daily_aggregate_entities if isinstance(entity, str) and entity.strip()
        ]

        logger.debug(
            f"HomeAssistant processor initialized - Input: {self.input_bucket}, "
            f"Output: {self.output_bucket}, Version: {self.version}, "
            f"Daily entities: {len(self.daily_aggregate_entities)}"
        )
    def process_daily_aggregates(self) -> None:
        """
        Process daily aggregate entities by calculating daily sums and writing to output bucket.
        """

        for entity_id in self.daily_aggregate_entities:
            last_data_day = self.influx_handler.get_last_data_day(
                bucket=self.output_bucket,
                entity_id=entity_id,
                version=self.version
            )

            if not last_data_day:
                logger.debug("No existing data in output bucket, starting from first data day")
                last_data_day = self.first_data_day - timedelta(days=1)  # Start processing from the day before the first data day

            days_to_process = []
            current_day = last_data_day + timedelta(days=1)
            today = datetime.now().date()
            yesterday = today - timedelta(days=1)

            while current_day.date() <= yesterday:
                days_to_process.append(current_day)
                current_day += timedelta(days=1)

            if not days_to_process:
                logger.debug("No days to process")
                return
            
            for day in days_to_process:
                day_data = self.influx_handler.get_last_datapoint(
                    start_time=day,
                    bucket=self.input_bucket,
                    entity_id=entity_id,
                    field="value"
                )
                if day_data is not None:
                    self.influx_handler.write_datapoint(
                        bucket=self.output_bucket,
                        entity_id=entity_id,
                        version = self.version,
                        field="daily_sum",
                        unit ="kWh",
                        value=day_data,
                        version=self.version,
                        timestamp=day
                    )
                    logger.info(f"Processed daily aggregate for {entity_id} on {day.date()}: {day_data}")
                else:
                    logger.warning(f"No data found for {entity_id} on {day.date()}, skipping")         
    def process_data(self) -> None:
        """
        Main method to process data from input bucket and write to output bucket.
        This is a placeholder for the actual processing logic.
        """
        logger.debug("Starting data processing...")
        self.process_daily_aggregates()
        logger.debug("Data processing completed.")

