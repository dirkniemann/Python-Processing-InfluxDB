import logging
import os
import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import pytz

logger = logging.getLogger(__name__)

# Timezone configuration
LOCAL_TZ = pytz.timezone("Europe/Berlin")
UTC_TZ = pytz.UTC
BATTERY_SCENARIO_MEASUREMENT = "batterie_szenarien"


def _influx_api_error_message(
    operation: str, bucket: str, org: str, error: Exception
) -> Optional[str]:
    """Return an actionable message for common bucket access failures."""
    status = getattr(error, "status", None)
    permission = "Schreib" if operation == "write" else "Lese"
    operation_name = "Schreiboperation" if operation == "write" else "Abfrage"
    if status == 403:
        return (
            f"Keine {permission}berechtigung für den InfluxDB-Bucket '{bucket}' "
            f"(HTTP 403, Organisation '{org}'). Prüfe, ob der verwendete Token "
            f"Zugriff mit {permission}rechten auf diesen Bucket hat."
        )
    if status == 404:
        return (
            f"Der InfluxDB-Bucket '{bucket}' wurde während der {operation_name} "
            "nicht gefunden (HTTP 404). Prüfe Bucketname und Organisation; "
            "falls der Bucket noch nicht existiert, lege ihn an."
        )
    return None


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
        
        # Allow raising the client timeout via env (ms). Default stays close to library default (~10s)
        self.timeout_ms = int(os.getenv("INFLUX_TIMEOUT_MS", "100000"))

        self.client: Optional[InfluxDBClient] = None
        logger.debug(f"InfluxDB handler initialized with URL: {self.url}, Org: {self.org}")
    
    def connect(self) -> bool:
        """
        Establish connection to InfluxDB.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=self.timeout_ms)
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
        field: str = "value",
        measurement: Optional[str] = None,
        version: Optional[str] = None
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
            measurement: Optional measurement name to filter by 
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
            def _to_utc(dt: datetime) -> datetime:
                if dt.tzinfo is None:
                    return local_to_utc(dt)
                return dt.astimezone(UTC_TZ)

            if stop_time is None:
                start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
                actual_start = _to_utc(start_time)
                actual_stop = _to_utc(start_time + timedelta(days=1))
                logger.debug(f"No stop_time provided, querying whole day: {actual_start.date()}")
            else:
                actual_start = _to_utc(start_time)
                actual_stop = _to_utc(stop_time)

            logger.debug(
                f"Querying {bucket} for {entity_id} from {actual_start} to {actual_stop} (UTC)"
            )
            
            measurement_filter = f'|> filter(fn: (r) => r["_measurement"] == "{measurement}")' if measurement else ""
            version_filter = f'|> filter(fn: (r) => r["version"] == "{version}")' if version else ""
            # Build Flux query with UTC timestamps
            query = f'''
            from(bucket: "{bucket}")
                |> range(start: {actual_start.isoformat()}, stop: {actual_stop.isoformat()})
                |> filter(fn: (r) => r["entity_id"] == "{entity_id}")
                |> filter(fn: (r) => r["_field"] == "{field}")
                {measurement_filter}
                {version_filter}
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
                    results.append({
                        "time": utc_time,
                        "value": record.get_value()
                    })

            results.sort(key=lambda r: r["time"])
            
            logger.debug(f"Retrieved {len(results)} data points for {entity_id} from {actual_start} to {actual_stop}")
            return results
            
        except Exception as e:
            message = _influx_api_error_message("read", bucket, self.org, e)
            if message:
                logger.error(message)
                raise RuntimeError(message) from None
            logger.error(f"Error querying data: {e}", exc_info=True)
            raise RuntimeError(f"Error querying data from bucket '{bucket}': {e}") from e
        
    def get_last_datapoint(
        self,
        start_time: datetime,
        bucket: str,
        entity_id: str,
        stop_time: Optional[datetime] = None,
        field: str = "value",
        measurement: Optional[str] = None,
        version: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve the last data point for a specific time range, entity, and field.
        
        Args:
            start_time: Start datetime for the query. If stop_time is not provided,
                       the whole day of this datetime will be queried (00:00 to 00:00 next day)
            bucket: Bucket name to query from
            entity_id: Entity ID to filter by
            stop_time: Optional stop datetime. If None, queries the whole day of start_time
            field: Field name to filter by (default: "value")
            measurement: Optional measurement name to filter by
            version: Optional version name to filter by
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
            return None
        
        try:
            def _to_utc(dt: datetime) -> datetime:
                if dt.tzinfo is None:
                    return local_to_utc(dt)
                return dt.astimezone(UTC_TZ)

            if stop_time is None:
                start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
                actual_start = _to_utc(start_time)
                actual_stop = _to_utc(start_time + timedelta(days=1))
                logger.debug(f"No stop_time provided, querying whole day: {actual_start.date()}")
            else:
                actual_start = _to_utc(start_time)
                actual_stop = _to_utc(stop_time)

            logger.debug(
                f"Querying {bucket} for {entity_id} from {actual_start} to {actual_stop} (UTC)"
            )
            
            measurement_filter = f'|> filter(fn: (r) => r["_measurement"] == "{measurement}")' if measurement else ""
            version_filter = f'|> filter(fn: (r) => r["version"] == "{version}")' if version else ""
            # Build Flux query
            query = f'''
            from(bucket: "{bucket}")
                |> range(start: {actual_start.isoformat()}, stop: {actual_stop.isoformat()})
                |> filter(fn: (r) => r["entity_id"] == "{entity_id}")
                |> filter(fn: (r) => r["_field"] == "{field}")
                {measurement_filter}
                {version_filter}
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
            return None

    def get_latest_datapoint_by_time(
        self,
        start_time: datetime,
        bucket: str,
        entity_id: str,
        stop_time: Optional[datetime] = None,
        field: str = "value",
        measurement: Optional[str] = None,
        version: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return the chronologically latest point in a time range.

        This method is deliberately separate from ``get_last_datapoint``.
        The latter uses ``max()`` because daily counter processing needs the
        numerically largest value; change-only signals need ``last()`` by time.
        """
        if not self.client:
            logger.error("Cannot query data: Client not connected")
            return None

        try:
            def _to_utc(dt: datetime) -> datetime:
                if dt.tzinfo is None:
                    return local_to_utc(dt)
                return dt.astimezone(UTC_TZ)

            if stop_time is None:
                start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
                actual_start = _to_utc(start_time)
                actual_stop = _to_utc(start_time + timedelta(days=1))
            else:
                actual_start = _to_utc(start_time)
                actual_stop = _to_utc(stop_time)

            measurement_filter = (
                f'|> filter(fn: (r) => r["_measurement"] == "{measurement}")'
                if measurement else ""
            )
            version_filter = (
                f'|> filter(fn: (r) => r["version"] == "{version}")'
                if version else ""
            )
            query = f'''
            from(bucket: "{bucket}")
                |> range(start: {actual_start.isoformat()}, stop: {actual_stop.isoformat()})
                |> filter(fn: (r) => r["entity_id"] == "{entity_id}")
                |> filter(fn: (r) => r["_field"] == "{field}")
                {measurement_filter}
                {version_filter}
                |> sort(columns: ["_time"])
                |> last()
                |> limit(n: 1)
            '''

            tables = self.client.query_api().query(query, org=self.org)
            for table in tables:
                for record in table.records:
                    return {
                        "time": utc_to_local(record.get_time()),
                        "value": record.get_value(),
                    }
            return None
        except Exception as e:
            message = _influx_api_error_message("read", bucket, self.org, e)
            if message:
                logger.error(message)
                raise RuntimeError(message) from None
            logger.error(f"Error querying latest data point: {e}", exc_info=True)
            raise RuntimeError(f"Error querying latest data point in bucket '{bucket}': {e}") from e

    def get_scenario_daily_records(
        self,
        bucket: str,
        scenario: str,
        pv_mode: str,
        version: str,
        measurement: str = BATTERY_SCENARIO_MEASUREMENT,
    ) -> List[Dict[str, Any]]:
        """Return one wide daily record per timestamp for restart decisions."""
        if not self.client:
            logger.error("Cannot query data: Client not connected")
            return []

        query = f'''
        from(bucket: "{bucket}")
            |> range(start: 0)
            |> filter(fn: (r) => r["_measurement"] == "{measurement}")
            |> filter(fn: (r) => r["scenario"] == "{scenario}")
            |> filter(fn: (r) => r["pv_mode"] == "{pv_mode}")
            |> filter(fn: (r) => r["version"] == "{version}")
            |> filter(fn: (r) => r["_field"] == "daily_sum" or r["_field"] == "start" or r["_field"] == "end")
        '''
        try:
            records_by_time: Dict[datetime, Dict[str, Any]] = {}
            for table in self.client.query_api().query(query, org=self.org):
                for record in table.records:
                    values = dict(getattr(record, "values", {}) or {})
                    entity = values.get("entity_id")
                    field_name = values.get("_field")
                    timestamp = record.get_time()
                    target_field = self._scenario_daily_field(entity, field_name)
                    if target_field is None:
                        continue
                    daily_record = records_by_time.setdefault(timestamp, {"time": timestamp})
                    daily_record[target_field] = record.get_value()
            return [records_by_time[key] for key in sorted(records_by_time)]
        except Exception as e:
            message = _influx_api_error_message("read", bucket, self.org, e)
            if message:
                logger.error(message)
                raise RuntimeError(message) from None
            logger.error(
                "Error querying daily battery scenario records from bucket '%s' "
                "(scenario '%s', PV mode '%s', version '%s'): %s",
                bucket,
                scenario,
                pv_mode,
                version,
                e,
                exc_info=True,
            )
            raise RuntimeError(
                f"Error querying daily battery scenario records from bucket '{bucket}': {e}"
            ) from e

    def get_scenario_timeseries_records(
        self,
        bucket: str,
        scenario: str,
        pv_mode: str,
        version: str,
        measurement: str = BATTERY_SCENARIO_MEASUREMENT,
    ) -> List[Dict[str, Any]]:
        """Return wide interval records for diagnostic comparison."""
        query = f'''
        from(bucket: "{bucket}")
            |> range(start: 0)
            |> filter(fn: (r) => r["_measurement"] == "{measurement}")
            |> filter(fn: (r) => r["scenario"] == "{scenario}")
            |> filter(fn: (r) => r["pv_mode"] == "{pv_mode}")
            |> filter(fn: (r) => r["version"] == "{version}")
            |> filter(fn: (r) => r["_field"] == "actual")
            |> filter(fn: (r) => r["entity_id"] == "soc_pct" or r["entity_id"] == "pv_to_battery" or r["entity_id"] == "battery_to_load")
        '''
        if not self.client:
            logger.error("Cannot query data: Client not connected")
            return []
        try:
            records_by_time: Dict[datetime, Dict[str, Any]] = {}
            for table in self.client.query_api().query(query, org=self.org):
                for record in table.records:
                    values = dict(getattr(record, "values", {}) or {})
                    entity = values.get("entity_id")
                    target_field = {
                        "soc_pct": "soc_pct",
                        "pv_to_battery": "battery_charge_dc_kw",
                        "battery_to_load": "battery_discharge_dc_kw",
                    }.get(entity)
                    if target_field is None:
                        continue
                    timestamp = record.get_time()
                    interval_record = records_by_time.setdefault(timestamp, {"time": timestamp})
                    interval_record[target_field] = record.get_value()
            return [records_by_time[key] for key in sorted(records_by_time)]
        except Exception as e:
            message = _influx_api_error_message("read", bucket, self.org, e)
            if message:
                logger.error(message)
                raise RuntimeError(message) from None
            logger.error(
                "Error querying battery scenario intervals from bucket '%s' "
                "(scenario '%s', PV mode '%s', version '%s'): %s",
                bucket,
                scenario,
                pv_mode,
                version,
                e,
                exc_info=True,
            )
            raise RuntimeError(
                f"Error querying battery scenario intervals from bucket '{bucket}': {e}"
            ) from e

    @staticmethod
    def _scenario_daily_field(entity: Optional[str], field_name: Optional[str]) -> Optional[str]:
        if field_name == "daily_sum" and entity not in ("soc_pct", "stored_energy"):
            return f"{entity}_kwh" if entity else None
        state_fields = {
            ("soc_pct", "start"): "soc_start_pct",
            ("soc_pct", "end"): "soc_end_pct",
            ("stored_energy", "start"): "stored_energy_start_kwh",
            ("stored_energy", "end"): "stored_energy_end_kwh",
        }
        return state_fields.get((entity, field_name))

    def get_first_data_day(
        self,
        bucket: str,
    ) -> Optional[datetime.date]:
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
                    logger.info(f"Found first data point in {bucket}: {first_time.date()}")
                    return first_time.date()
            
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
        entity_id: Optional[str] = None,
        measurement: Optional[str] = None,
        field: Optional[str] = None
    ) -> Optional[datetime.date]:
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
            measurement_filter = f'|> filter(fn: (r) => r["_measurement"] == "{measurement}")' if measurement else ""
            field_filter = f'|> filter(fn: (r) => r["_field"] == "{field}")' if field else ""
            # Try to get the last data point from processed bucket
            query_last = f'''
            from(bucket: "{bucket}")
                |> range(start: 0)
                |> filter(fn: (r) => r["version"] == "{version}")
                {scenario_filter}
                {entity_filter}
                {measurement_filter}
                {field_filter}
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
                    logger.debug(f"Found last data point in {bucket}: {last_time.date()}")
                    return last_time.date()
            return None
        except Exception as e:
            message = _influx_api_error_message("read", bucket, self.org, e)
            if message:
                logger.error(message)
                raise RuntimeError(message) from None
            logger.error(f"Error querying last data day in bucket '{bucket}': {e}", exc_info=True)
            return None
        
    def write_datapoint(
        self,
        bucket: str,
        entity_id: str,
        value: Any,
        field: Optional[str] = "value",
        version: Optional[str] = None,
        scenario: Optional[str] = None,
        unit: Optional[str] = None,
        timestamp: Optional[datetime] = None,
        measurement: Optional[str] = "home_assistant"
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
            True if write successful

        Raises:
            RuntimeError: If client is not connected or write fails

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
            message = "Cannot write data: Client not connected"
            logger.error(message)
            raise RuntimeError(message)
        
        try:
            write_api = self.client.write_api(write_options=SYNCHRONOUS)
            
            # Use provided timestamp or current time
            write_timestamp = timestamp if timestamp is not None else datetime.now()
            
            # If timestamp is naive, assume it's Berlin time and convert to UTC
            if write_timestamp.tzinfo is None:
                write_timestamp = local_to_utc(write_timestamp)
            
            # Normalize numeric values to float to avoid field type conflicts in InfluxDB
            normalized_value = float(value) if isinstance(value, (int, float)) else value

            point = {
                "measurement": measurement,
                "tags": {
                    "entity_id": entity_id,
                    "version": version or "",
                    "scenario": scenario or "",
                    "unit": unit or ""
                },
                "fields": {
                    field: normalized_value
                },
                "time": write_timestamp.isoformat()
            }
            logger.debug(f"Writing data point to {bucket}: {point}")
            write_api.write(bucket=bucket, org=self.org, record=point)
            logger.debug("Data point written successfully")
            return True
        except Exception as e:
            message = _influx_api_error_message("write", bucket, self.org, e)
            if message:
                logger.error(message)
                raise RuntimeError(message) from None
            logger.error(f"Error writing data point to bucket '{bucket}': {e}", exc_info=True)
            raise RuntimeError(f"Error writing data point to bucket '{bucket}': {e}") from e

    def write_fields_datapoint(
        self,
        bucket: str,
        measurement: str,
        fields: Dict[str, Any],
        tags: Optional[Dict[str, str]] = None,
        timestamp: Optional[datetime] = None,
    ) -> bool:
        """Write several fields at one timestamp with one shared tag set."""
        if not self.client:
            message = "Cannot write data: Client not connected"
            logger.error(message)
            raise RuntimeError(message)
        if not measurement or not fields:
            raise ValueError("measurement and fields must not be empty")

        try:
            write_timestamp = timestamp if timestamp is not None else datetime.now()
            if write_timestamp.tzinfo is None:
                write_timestamp = local_to_utc(write_timestamp)

            normalized_fields = {
                name: float(value) if isinstance(value, (int, float)) else value
                for name, value in fields.items()
            }
            point = {
                "measurement": measurement,
                "tags": dict(tags or {}),
                "fields": normalized_fields,
                "time": write_timestamp.isoformat(),
            }
            write_api = self.client.write_api(write_options=SYNCHRONOUS)
            write_api.write(bucket=bucket, org=self.org, record=point)
            return True
        except Exception as e:
            message = _influx_api_error_message("write", bucket, self.org, e)
            if message:
                logger.error(message)
                raise RuntimeError(message) from None
            logger.error(f"Error writing multi-field data point to bucket '{bucket}': {e}", exc_info=True)
            raise RuntimeError(f"Error writing multi-field data point to bucket '{bucket}': {e}") from e

    def write_fields_datapoints(
        self,
        bucket: str,
        measurement: str,
        datapoints: List[Dict[str, Any]],
        batch_size: int = 5000,
    ) -> int:
        """Write timestamped multi-field points in bounded synchronous batches."""
        if not self.client:
            message = "Cannot write data: Client not connected"
            logger.error(message)
            raise RuntimeError(message)
        if not measurement:
            raise ValueError("measurement must not be empty")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if not datapoints:
            return 0

        try:
            points = []
            for datapoint in datapoints:
                fields = datapoint.get("fields")
                if not fields:
                    raise ValueError("every datapoint must contain fields")
                timestamp = datapoint.get("timestamp")
                write_timestamp = timestamp if timestamp is not None else datetime.now()
                if write_timestamp.tzinfo is None:
                    write_timestamp = local_to_utc(write_timestamp)
                points.append(
                    {
                        "measurement": measurement,
                        "tags": dict(datapoint.get("tags") or {}),
                        "fields": {
                            name: float(value) if isinstance(value, (int, float)) else value
                            for name, value in fields.items()
                        },
                        "time": write_timestamp.isoformat(),
                    }
                )

            write_api = self.client.write_api(write_options=SYNCHRONOUS)
            for offset in range(0, len(points), batch_size):
                write_api.write(
                    bucket=bucket,
                    org=self.org,
                    record=points[offset : offset + batch_size],
                )
            return len(points)
        except Exception as e:
            message = _influx_api_error_message("write", bucket, self.org, e)
            if message:
                logger.error(message)
                raise RuntimeError(message) from None
            logger.error("Error batch-writing points to bucket '%s': %s", bucket, e, exc_info=True)
            raise RuntimeError(f"Error batch-writing points to bucket '{bucket}': {e}") from e
        

    def get_last_version(
        self,
        bucket: str,
        scenario: Optional[str] = None,
        entity_id: Optional[str] = None,
        measurement: Optional[str] = None,
        field: Optional[str] = None
    ) -> Optional[str]:
        """
        Get the latest version tag present in a bucket, optionally filtered by tags.

        Returns the highest semver-like suffix (numeric suffix wins) or ``None`` if
        no version is stored.
        """
        if not self.client:
            logger.error("Cannot query data: Client not connected")
            return None
        
        try:
            scenario_filter = f'|> filter(fn: (r) => r["scenario"] == "{scenario}")' if scenario else ""
            entity_filter = f'|> filter(fn: (r) => r["entity_id"] == "{entity_id}")' if entity_id else ""
            measurement_filter = f'|> filter(fn: (r) => r["_measurement"] == "{measurement}")' if measurement else ""
            field_filter = f'|> filter(fn: (r) => r["_field"] == "{field}")' if field else ""

            query = f'''
            from(bucket: "{bucket}")
                |> range(start: 0)
                
                {scenario_filter}
                {entity_filter}
                {measurement_filter}
                {field_filter}
                |> keep(columns: ["version"])
                |> group()
                |> distinct(column: "version")
            '''
            logger.debug(f"Querying available versions with:\n{query}")
            query_api = self.client.query_api()
            tables = query_api.query(query, org=self.org)
            
            versions = set()
            for table in tables:
                for record in table.records:
                    version = record.get_value()
                    if version:
                        versions.add(version)

            version_list = sorted(versions, key=self._version_sort_key)
            logger.debug(f"Available versions in {bucket}: {version_list}")
            return version_list[-1] if version_list else None
        except Exception as e:
            logger.error(f"Error querying available versions: {e}", exc_info=True)
            raise RuntimeError(f"Error querying available versions: {e}")
        
    @staticmethod
    def _version_sort_key(value: str) -> tuple[int, int, str]:
        """Sort versions by numeric suffix first, then lexicographically."""
        match = re.search(r"(\d+)$", value)
        if match:
            return (0, int(match.group(1)), value)
        return (-1, 0, value)
    
    def __enter__(self):
        """Open connection for use in ``with InfluxDBHandler() as handler`` blocks."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close connection when leaving a context manager scope."""
        self.disconnect()

