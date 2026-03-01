import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from moduls.influxdb_handler import InfluxDBHandler
from moduls.influxdb_handler import LOCAL_TZ
logger = logging.getLogger(__name__)


class EntityProcessor:
    """Base class for entity processing strategies."""
    
    def __init__(
        self,
        influx_handler: InfluxDBHandler,
        input_bucket: str,
        output_bucket: str,
        version: str,
        entities: List[str],
        first_data_day: datetime
    ):
        self.influx_handler = influx_handler
        self.input_bucket = input_bucket
        self.output_bucket = output_bucket
        self.version = version
        self.entities = entities
        self.first_data_day = first_data_day
        
    def process(self) -> None:
        """Process entities according to the strategy."""
        raise NotImplementedError("Subclasses must implement process()")


class DailyAggregateProcessor(EntityProcessor):
    """Processor for daily aggregate entities."""
    
    def process(self) -> None:
        """
        Process daily aggregate entities by calculating daily sums and writing to output bucket.
        """
        logger.info(f"Processing {len(self.entities)} daily aggregate entities (version: {self.version})")
        
        for entity_id in self.entities:
            self._process_entity(entity_id)
            logger.info(f"Finished processing daily aggregate for {entity_id}")
        logger.info("Completed processing all daily aggregate entities")
            
    def _process_entity(self, entity_id: str) -> None:
        """Process a single entity for daily aggregates."""
        last_data_day = self.influx_handler.get_last_data_day(
            bucket=self.output_bucket,
            entity_id=entity_id,
            version=self.version
        )

        if not last_data_day:
            logger.debug(f"No existing data for {entity_id} in output bucket, starting from first data day")
            last_data_day = self.first_data_day - timedelta(days=1)

        days_to_process = self._get_days_to_process(last_data_day)
        
        if not days_to_process:
            logger.debug(f"No days to process for {entity_id}")
            return
        
        logger.debug(f"Processing {len(days_to_process)} days for {entity_id}")
        
        for day in days_to_process:
            self._process_day(entity_id, day)
            
    def _get_days_to_process(self, last_data_day: datetime) -> List[datetime]:
        """
        Get list of days that need to be processed.
        Each day is properly localized to handle DST changes.
        """
        days_to_process = []
        current_day = last_data_day + timedelta(days=1)
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)

        while current_day.date() <= yesterday:

            if current_day.tzinfo is not None:
                naive_day = current_day.replace(tzinfo=None)
            else:
                naive_day = current_day
            
            localized_day = LOCAL_TZ.localize(naive_day)
            days_to_process.append(localized_day)
            
            logger.debug(f"Added day to process: {localized_day} (tzinfo: {localized_day.tzinfo})")
            
            current_day += timedelta(days=1)
            
        return days_to_process
    
    def _process_day(self, entity_id: str, day: datetime) -> None:
        """Process a single day for an entity."""

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
                version=self.version,
                field="daily_sum",
                unit="kWh",
                value=day_data["value"],
                timestamp=day
            )
            logger.debug(f"Processed daily aggregate for {entity_id} on {day.date()}: {day_data}")
        else:
            logger.warning(f"No data found for {entity_id} on {day.date()}, skipping")


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
        required_keys = ["input_bucket", "output_bucket", "entities_to_process"]
        missing_keys = [key for key in required_keys if key not in processing_config]
        if missing_keys:
            raise ValueError(f"Missing required config keys: {', '.join(missing_keys)}")
        
        self.input_bucket = processing_config["input_bucket"]
        self.output_bucket = processing_config["output_bucket"]
        self.first_data_day = first_data_day
        
        # Initialize processors for different entity types
        self.processors: List[EntityProcessor] = []
        entities_to_process = processing_config.get("entities_to_process", {})
        
        if not isinstance(entities_to_process, dict):
            raise ValueError("'entities_to_process' must be a dictionary")
        
        # Initialize daily aggregate processor if configured
        if "daily_aggregate" in entities_to_process:
            self._init_daily_aggregate_processor(entities_to_process["daily_aggregate"])
        
        logger.debug(
            f"HomeAssistant processor initialized - Input: {self.input_bucket}, "
            f"Output: {self.output_bucket}, Processors: {len(self.processors)}"
        )
    
    def _init_daily_aggregate_processor(self, config: Dict[str, Any]) -> None:
        """Initialize the daily aggregate processor from config."""
        if not isinstance(config, dict):
            raise ValueError("'daily_aggregate' config must be a dictionary")
        
        version = config.get("version")
        if not version:
            raise ValueError("'daily_aggregate' config must include 'version'")
        
        entities = config.get("entities", [])
        if not isinstance(entities, list):
            raise ValueError("'daily_aggregate.entities' must be a list")
        
        # Filter valid entities
        valid_entities = [
            entity for entity in entities 
            if isinstance(entity, str) and entity.strip()
        ]
        
        if valid_entities:
            processor = DailyAggregateProcessor(
                influx_handler=self.influx_handler,
                input_bucket=self.input_bucket,
                output_bucket=self.output_bucket,
                version=version,
                entities=valid_entities,
                first_data_day=self.first_data_day
            )
            self.processors.append(processor)
            logger.debug(f"Initialized DailyAggregateProcessor with {len(valid_entities)} entities (version: {version})")
        else:
            logger.warning("No valid entities found for daily_aggregate processor")
                            
    def process_data(self) -> None:
        """
        Main method to process data from input bucket and write to output bucket.
        Executes all configured processors.
        """
        logger.info("Starting data processing...")
        
        for processor in self.processors:
            try:
                processor.process()
            except Exception as e:
                logger.error(f"Error in processor {processor.__class__.__name__}: {e}", exc_info=True)
        
        logger.info("Data processing completed.")