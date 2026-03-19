from datetime import datetime, timedelta
from typing import List, Optional
from moduls.influxdb_handler import InfluxDBHandler, LOCAL_TZ
import logging

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
        first_data_day: datetime,
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