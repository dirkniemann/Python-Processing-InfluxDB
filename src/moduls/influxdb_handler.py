import logging
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import pytz

logger = logging.getLogger(__name__)

# Timezone configuration
LOCAL_TZ = pytz.timezone("Europe/Berlin")
UTC_TZ = pytz.UTC


def local_to_utc(local_dt: datetime) -> datetime:
    """
    Convert a naive local datetime (Europe/Berlin) to UTC-aware datetime.
    
    Args:
        local_dt: Naive datetime in local timezone (Europe/Berlin)
        
    Returns:
        UTC-aware datetime object
    """
    if local_dt.tzinfo is not None:
        raise ValueError("Expected naive datetime, got timezone-aware")
    # Localize to Berlin timezone, then convert to UTC
    local_aware = LOCAL_TZ.localize(local_dt)
    return local_aware.astimezone(UTC_TZ)


def utc_to_local(utc_dt: datetime) -> datetime:
    """
    Convert UTC-aware datetime to local datetime (Europe/Berlin).
    
    Args:
        utc_dt: UTC-aware datetime object
        
    Returns:
        datetime aware of local timezone (Europe/Berlin)
    """
    if utc_dt.tzinfo is None:
        raise ValueError("Expected timezone-aware datetime, got naive")
    # Convert to Berlin timezone
    local_aware = utc_dt.astimezone(LOCAL_TZ)
    return local_aware


class InfluxDBHandler:
    """
    Handler for InfluxDB connections and operations.
    Manages connection initialization, error handling, and data operations.
    """
    
    def __init__(self, url: Optional[str] = None, token: Optional[str] = None, org: Optional[str] = None):
        """
        Initialize InfluxDB handler.
        
        Credentials are loaded from environment variables if not provided as arguments:
        - INFLUX_URL: InfluxDB server URL (e.g., http://influxdb:8086)
        - INFLUX_TOKEN: API token for authentication
        - INFLUX_ORG: Organization ID
        
        Args:
            url: InfluxDB server URL
            token: API token for authentication
            org: Organization ID/name
            
        Raises:
            ImportError: If influxdb-client is not installed
            ValueError: If required credentials are missing
        """        
        # Load from environment if not provided
        self.url = url or os.getenv("INFLUX_URL")
        self.token = token or os.getenv("INFLUX_TOKEN")
        self.org = org or os.getenv("INFLUX_ORG")
        
        # Validate credentials
        if not all([self.url, self.token, self.org]):
            missing = []
            if not self.url:
                missing.append("INFLUX_URL")
            if not self.token:
                missing.append("INFLUX_TOKEN")
            if not self.org:
                missing.append("INFLUX_ORG")
            raise ValueError(
                f"Missing required InfluxDB credentials: {', '.join(missing)}. "
                f"Set them as environment variables or pass as arguments."
            )
        
        self.client: Optional[InfluxDBClient] = None
        logger.debug(f"InfluxDB handler initialized with URL: {self.url}, Org: {self.org}")
    
    def connect(self) -> bool:
        """
        Establish connection to InfluxDB.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
            # Test connection by fetching health
            health = self.client.health()
            logger.debug(f"Successfully connected to InfluxDB: {health.message}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to InfluxDB: {e}", exc_info=True)
            return False
    
    def disconnect(self) -> None:
        """Close InfluxDB connection."""
        if self.client:
            try:
                self.client.close()
                logger.debug("InfluxDB connection closed")
            except Exception as e:
                logger.error(f"Error closing InfluxDB connection: {e}", exc_info=True)
    
    def get_client(self) -> Optional[InfluxDBClient]:
        """
        Get the InfluxDB client instance.
        
        Returns:
            InfluxDBClient instance or None if not connected
        """
        if not self.client:
            logger.warning("InfluxDB client not initialized. Call connect() first.")
        return self.client
    
    def get_data(
        self,
        start_time: datetime,
        bucket: str,
        entity_id: str,
        stop_time: Optional[datetime] = None,
        field: str = "value"
    ) -> List[Dict[str, Any]]:
        """
        Retrieve all data points for a specific time range, entity, and field.
        
        Args:
            start_time: Start datetime for the query. If stop_time is not provided,
                       the whole day of this datetime will be queried (00:00 to 00:00 next day)
            bucket: Bucket name to query from
            entity_id: Entity ID to filter by
            stop_time: Optional stop datetime. If None, queries the whole day of start_time
            field: Field name to filter by (default: "value")
            
        Returns:
            List of dictionaries with timestamp and value, empty list on error
            
        Example:
            # Query whole day (any time in the day works)
            data = handler.get_data(datetime(2026, 2, 28, 14, 30), 'HomeAssistant', 'sensor.power')
            
            # Query specific time range
            start = datetime(2026, 2, 28, 8, 0, 0)
            stop = datetime(2026, 2, 28, 18, 0, 0)
            data = handler.get_data(start, 'HomeAssistant', 'sensor.power', stop_time=stop)
        """
        if not self.client:
            logger.error("Cannot query data: Client not connected")
            return []
        
        try:
            # If stop_time is not provided, query the whole day of start_time
            if stop_time is None:
                actual_start = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
                actual_stop = actual_start + timedelta(days=1)
                logger.debug(f"No stop_time provided, querying whole day: {actual_start.date()}")
            else:
                actual_start = start_time
                actual_stop = stop_time
            
            # Convert local datetimes to UTC for Flux query
            utc_start = local_to_utc(actual_start)
            utc_stop = local_to_utc(actual_stop)
            
            logger.debug(f"Querying {bucket} for {entity_id} from {actual_start} to {actual_stop} (local time)")
            logger.debug(f"UTC range: {utc_start} to {utc_stop}")
            
            # Build Flux query with UTC timestamps
            query = f'''
            from(bucket: "{bucket}")
                |> range(start: {utc_start.isoformat()}, stop: {utc_stop.isoformat()})
                |> filter(fn: (r) => r["entity_id"] == "{entity_id}")
                |> filter(fn: (r) => r["_field"] == "{field}")
                |> keep(columns: ["_time", "_value"])
            '''
            
            logger.debug(f"Executing query:\n{query}")
            
            # Execute query
            query_api = self.client.query_api()
            tables = query_api.query(query, org=self.org)
            
            # Process results
            results = []
            for table in tables:
                for record in table.records:
                    utc_time = record.get_time()
                    local_time = utc_to_local(utc_time)
                    results.append({
                        "time": local_time,
                        "value": record.get_value()
                    })
            
            logger.debug(f"Retrieved {len(results)} data points for {entity_id} from {actual_start} to {actual_stop}")
            return results
            
        except Exception as e:
            logger.error(f"Error querying data: {e}", exc_info=True)
            return []
        
    def get_last_datapoint(
        self,
        start_time: datetime,
        bucket: str,
        entity_id: str,
        stop_time: Optional[datetime] = None,
        field: str = "value"
    ) -> List[Dict[str, Any]]:
        """
        Retrieve the last data point for a specific time range, entity, and field.
        
        Args:
            start_time: Start datetime for the query. If stop_time is not provided,
                       the whole day of this datetime will be queried (00:00 to 00:00 next day)
            bucket: Bucket name to query from
            entity_id: Entity ID to filter by
            stop_time: Optional stop datetime. If None, queries the whole day of start_time
            field: Field name to filter by (default: "value")
            
        Returns:
            last data point as a dictionary with timestamp and value, or None if no data found
            
        Example:
            # Query whole day (any time in the day works)
            data = handler.get_data(datetime(2026, 2, 28, 14, 30), 'HomeAssistant', 'sensor.power')
            
            # Query specific time range
            start = datetime(2026, 2, 28, 8, 0, 0)
            stop = datetime(2026, 2, 28, 18, 0, 0)
            data = handler.get_data(start, 'HomeAssistant', 'sensor.power', stop_time=stop)
        """
        if not self.client:
            logger.error("Cannot query data: Client not connected")
            return []
        
        try:
            # If stop_time is not provided, query the whole day of start_time
            if stop_time is None:
                actual_start = start_time.replace(hour=0, minute=0, second=0, microsecond=0)

                if actual_start.tzinfo is not None:
                    naive_start_time = actual_start.replace(tzinfo=None)

                else:
                    naive_start_time = actual_start

                actual_start = LOCAL_TZ.localize(naive_start_time)

                actual_stop = actual_start + timedelta(days=1)
                if actual_stop.tzinfo is not None:
                    naive_stop_time = actual_stop.replace(tzinfo=None)

                else:
                    naive_stop_time = actual_stop

                actual_stop = LOCAL_TZ.localize(naive_stop_time)

                logger.debug(f"No stop_time provided, querying whole day: {actual_start.date()}")
            else:
                actual_start = start_time
                actual_stop = stop_time
            
            logger.debug(f"Querying {bucket} for {entity_id} from {actual_start} to {actual_stop} (local time)")
            logger.debug(f"UTC range: {actual_start} to {actual_stop}")
            
            # Build Flux query
            query = f'''
            from(bucket: "{bucket}")
                |> range(start: {actual_start.isoformat()}, stop: {actual_stop.isoformat()})
                |> filter(fn: (r) => r["entity_id"] == "{entity_id}")
                |> filter(fn: (r) => r["_field"] == "{field}")
                |> keep(columns: ["_time", "_value"])
                |> max()
                |> limit(n: 1)
            '''
            
            logger.debug(f"Executing query:\n{query}")
            
            # Execute query
            query_api = self.client.query_api()
            tables = query_api.query(query, org=self.org)
            
            # Process results
            for table in tables:
                for record in table.records:
                    utc_time = record.get_time()
                    local_time = utc_to_local(utc_time)
                    last_data = {
                        "time": local_time,
                        "value": record.get_value()
                    }
                    logger.debug(f"Retrieved last data point for {entity_id}: {last_data}")
                    return last_data
            
            logger.debug(f"No data found for {entity_id}")
            return None
        except Exception as e:
            logger.error(f"Error querying data: {e}", exc_info=True)
            return []

    def get_first_data_day(
        self,
        bucket: str,
    ) -> Optional[datetime]:
        """
        Get the first day with data points in a bucket.
        
        Args:
            bucket: Bucket to check for first data point
            bucket: Raw data bucket to check if no processed data exists
            
        Returns:
            Datetime of the first day with data, or None if no data found
        """
        if not self.client:
            logger.error("Cannot query data: Client not connected")
            return None
        
        try:
            # No data in processed bucket, check raw data bucket for first data point
            logger.info(f"No data found in {bucket}, checking {bucket} for first data point")
            
            query_first = f'''
            from(bucket: "{bucket}")
            |> range(start: 0)
            |> sort(columns: ["_time"])
            |> first()
            '''
            query_api = self.client.query_api()
            logger.debug(f"Query:\n{query_first}")
            tables = query_api.query(query_first, org=self.org)
            
            for table in tables:
                for record in table.records:
                    first_time = record.get_time()
                    local_first_time = utc_to_local(first_time)
                    local_first_time = local_first_time.replace(hour=23, minute=59, second=59, microsecond=0)
                    logger.info(f"Found first data point in {bucket}: {local_first_time.date()}")
                    return local_first_time
            
            logger.warning(f"No data found in {bucket}")
            return None
    
        except Exception as e:
            logger.error(f"Error querying first data day: {e}", exc_info=True)
            return None
    
    def get_last_data_day(
        self,
        bucket: str,
        version: str,
        scenario: Optional[str] = None,
        entity_id: Optional[str] = None
    ) -> Optional[datetime]:
        """
        Get the last day with data points for a specific version tag.
        
        Args:
            bucket: Bucket to check for last data point
            version: Version tag to filter by
            scenario: Optional scenario tag to filter by
            
        Returns:
            Datetime of the last day with data, or None if no data found
            
        Example:
            last_day = handler.get_last_data_day(
                'HomeAssistant_processed',
                'v1',
                scenario='8_modules_2_towers'
                entity_id='sensor.power_consumption'
            )
        """
        if not self.client:
            logger.error("Cannot query data: Client not connected")
            return None
        
        try:
            # Build filter for scenario if provided
            scenario_filter = f'|> filter(fn: (r) => r["scenario"] == "{scenario}")' if scenario else ""
            entity_filter = f'|> filter(fn: (r) => r["entity_id"] == "{entity_id}")' if entity_id else ""
            # Try to get the last data point from processed bucket
            query_last = f'''
            from(bucket: "{bucket}")
                |> range(start: 0)
                |> filter(fn: (r) => r["version"] == "{version}")
                {scenario_filter}
                {entity_filter}
                |> last()
                |> limit(n: 1)
            '''
            
            logger.debug(f"Checking last data point in {bucket}")
            logger.debug(f"Query:\n{query_last}")
            
            query_api = self.client.query_api()
            tables = query_api.query(query_last, org=self.org)
            
            # Check if we got results
            for table in tables:
                for record in table.records:
                    last_time = record.get_time()
                    local_last_time = utc_to_local(last_time)
                    local_last_time = local_last_time.replace(hour=23, minute=59, second=59, microsecond=0)
                    logger.info(f"Found last data point in {bucket}: {local_last_time.date()}")
                    return local_last_time
            return None
        except Exception as e:
            logger.error(f"Error querying last data day: {e}", exc_info=True)
            return None
    def write_datapoint(
        self,
        bucket: str,
        entity_id: str,
        field: str,
        value: Any,
        version: Optional[str] = None,
        scenario: Optional[str] = None,
        unit: Optional[str] = None,
        timestamp: Optional[datetime] = None
    ) -> bool:
        """
        Write a single data point to InfluxDB.
        
        Args:
            bucket: Bucket name to write to
            entity_id: Entity ID to write
            field: Field name to write
            value: Value to write
            version: Optional version tag to add
            scenario: Optional scenario tag to add
            unit: Optional unit tag to add
            timestamp: Optional timestamp for the data point. If None, current time is used

        Returns:
            True if write successful, False otherwise

        Example:
            handler.write_datapoint(
                bucket='HomeAssistant_processed',
                entity_id='sensor.power_consumption',
                field='daily_sum',
                value=123.45,
                version='v1',
                scenario='8_modules_2_towers',
                unit='kWh',
                timestamp=datetime(2026, 2, 28)
            )
        """

        if not self.client:
            logger.error("Cannot write data: Client not connected")
            return False
        
        try:
            write_api = self.client.write_api(write_options=SYNCHRONOUS)
            
            # Use provided timestamp or current time
            write_timestamp = timestamp if timestamp is not None else datetime.now()
            
            # If timestamp is naive, assume it's Berlin time and convert to UTC
            if write_timestamp.tzinfo is None:
                write_timestamp = local_to_utc(write_timestamp)
            else:
                write_timestamp = write_timestamp.astimezone(pytz.UTC)
            
            point = {
                "measurement": "home_assistant",
                "tags": {
                    "entity_id": entity_id,
                    "version": version or "",
                    "scenario": scenario or "",
                    "unit": unit or ""
                },
                "fields": {
                    field: value
                },
                "time": write_timestamp.isoformat()
            }
            logger.debug(f"Writing data point to {bucket}: {point}")
            write_api.write(bucket=bucket, org=self.org, record=point)
            logger.debug("Data point written successfully")
            return True
        except Exception as e:
            logger.error(f"Error writing data point: {e}", exc_info=True)
            return False
        


    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()

