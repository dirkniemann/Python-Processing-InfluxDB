import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from moduls.influxdb_handler import InfluxDBHandler
from moduls.influxdb_handler import LOCAL_TZ
from moduls.processing.daily_aggregate_processor import DailyAggregateProcessor
from moduls.processing.waermepumpe_statistik_processor import WaermepumpeStatistikProcessor
from moduls.processing.fix_waermepumpe_stromverbrauch_processor import FixWaermepumpeStromverbrauchProcessor

logger = logging.getLogger(__name__)

def get_days_to_process(last_data_day: datetime) -> List[datetime]:
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
        current_day += timedelta(days=1)
        
    return days_to_process

class EntityProcessor:
    """Base class for entity processing strategies."""
    
    def __init__(
        self,
        influx_handler: InfluxDBHandler,
        input_bucket: str,
        output_bucket: str,
        version: str,
        entities: List[str],
        first_data_day: datetime.date,
        output_measurement: Optional[str] = None,
        output_entity_id: Optional[str] = None
    ):
        self.influx_handler = influx_handler
        self.input_bucket = input_bucket
        self.output_bucket = output_bucket
        self.version = version
        self.entities = entities
        self.first_data_day = first_data_day
        self.output_measurement = output_measurement
        self.output_entity_id = output_entity_id
        
    def process(self) -> None:
        """Process entities according to the strategy."""
        raise NotImplementedError("Subclasses must implement process()")

class HomeAssistantProcessor:
    """
    Process Home Assistant data from InfluxDB.
    Loads data from input bucket, processes it, and writes to output bucket.
    """
    
    def __init__(
        self, 
        influx_handler: InfluxDBHandler,
        processing_config: Dict[str, Any],
        first_data_day: datetime.date
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
        
        if "fix_waermepumpe_stromverbrauch" in entities_to_process:
            self._init_fix_waermepumpe_stromverbrauch_processor(entities_to_process["fix_waermepumpe_stromverbrauch"])

        if "daily_aggregate" in entities_to_process:
            self._init_daily_aggregate_processor(entities_to_process["daily_aggregate"])
        
        if "Waermepumpe_statistik" in entities_to_process:
            self._init_waermepumpe_statistik_processor(entities_to_process["Waermepumpe_statistik"])

        logger.debug(
            f"HomeAssistant processor initialized - Input: {self.input_bucket}, "
            f"Output: {self.output_bucket}, Processors: {len(self.processors)}"
        )
    
    def _init_fix_waermepumpe_stromverbrauch_processor(self, config: Dict[str, Any]) -> None:
        """Initialize the fix_waermepumpe_stromverbrauch processor from config."""

        if not isinstance(config, dict):
            raise ValueError("'fix_waermepumpe_stromverbrauch' config must be a dictionary")
        
        version = config.get("version")
        if not version:
            raise ValueError("'fix_waermepumpe_stromverbrauch' config must include 'version'")
        
        entities = config.get("entities", [])
        if not isinstance(entities, list):
            raise ValueError("'fix_waermepumpe_stromverbrauch.entities' must be a list")
        
        # Filter valid entities
        valid_entities = [
            entity for entity in entities 
            if isinstance(entity, str) and entity.strip()
        ]

        output_measurement = config.get("output_measurement")
        if not output_measurement or not isinstance(output_measurement, str):
            raise ValueError("'fix_waermepumpe_stromverbrauch' config must include a valid 'output_measurement'")

        if valid_entities:
            processor = FixWaermepumpeStromverbrauchProcessor(
                influx_handler=self.influx_handler,
                input_bucket=self.input_bucket,
                output_bucket=self.output_bucket,
                version=version,
                entities=valid_entities,
                first_data_day=self.first_data_day,
                output_measurement=output_measurement,
            )
            self.processors.append(processor)

        else:
            logger.warning("No valid entities found for fix_waermepumpe_stromverbrauch processor")

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
        
        output_measurement = config.get("output_measurement")
        if output_measurement and not isinstance(output_measurement, str):
            raise ValueError("'daily_aggregate.output_measurement' must be a string if provided")
        
        output_entity_id = config.get("output_entity_id")
        if not output_entity_id or not isinstance(output_entity_id, str):
            raise ValueError("'daily_aggregate' config must include a valid 'output_entity_id'")

        if valid_entities:
            processor = DailyAggregateProcessor(
                influx_handler=self.influx_handler,
                input_bucket=self.input_bucket,
                output_bucket=self.output_bucket,
                version=version,
                entities=valid_entities,
                first_data_day=self.first_data_day,
                output_measurement=output_measurement,
                output_entity_id=output_entity_id
            )
            self.processors.append(processor)
            logger.debug(f"Initialized DailyAggregateProcessor with {len(valid_entities)} entities (version: {version})")
        else:
            logger.warning("No valid entities found for daily_aggregate processor")

    def _init_waermepumpe_statistik_processor(self, config: Dict[str, Any]) -> None:
        """Initialize the Waermepumpe_statistik processor from config."""

        if not isinstance(config, dict):
            raise ValueError("'Waermepumpe_statistik' config must be a dictionary")
        
        version = config.get("version")
        if not version:
            raise ValueError("'Waermepumpe_statistik' config must include 'version'")
        
        entities = config.get("entities", [])
        if not isinstance(entities, list):
            raise ValueError("'Waermepumpe_statistik.entities' must be a list")
        
        # Filter valid entities
        valid_entities = [
            entity for entity in entities 
            if isinstance(entity, str) and entity.strip()
        ]

        output_measurement = config.get("output_measurement")
        if not output_measurement or not isinstance(output_measurement, str):
            raise ValueError("'Waermepumpe_statistik' config must include a valid 'output_measurement'")
        
        output_entity_id = config.get("output_entity_id")
        if not output_entity_id or not isinstance(output_entity_id, str):
            raise ValueError("'Waermepumpe_statistik' config must include a valid 'output_entity_id'")

        if valid_entities:
            processor = WaermepumpeStatistikProcessor(
                influx_handler=self.influx_handler,
                input_bucket=self.input_bucket,
                output_bucket=self.output_bucket,
                version=version,
                entities=valid_entities,
                first_data_day=self.first_data_day,
                output_measurement=output_measurement,
                output_entity_id=output_entity_id
            )
            self.processors.append(processor)
            logger.debug(f"Initialized WaermepumpeStatistikProcessor with {len(valid_entities)} entities (version: {version})")
        else:
            logger.warning("No valid entities found for Waermepumpe_statistik processor")

    def process_data(self) -> None:
        """
        Main method to process data from input bucket and write to output bucket.
        Executes all configured processors.
        """
        logger.info("Starting data processing...")
        
        for processor in self.processors:
            try:
                logger.info(f"Running processor: {processor.__class__.__name__}")
                processor.process()
            except Exception as e:
                logger.error(f"Error in processor {processor.__class__.__name__}: {e}", exc_info=True)
                raise RuntimeError(f"Processing failed in {processor.__class__.__name__}") from e
        
        logger.info("Data processing completed.")