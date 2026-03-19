import logging
from datetime import datetime, timedelta, time
from moduls.influxdb_handler import LOCAL_TZ, local_to_utc
from moduls.processing.HomeAssistant_processor import EntityProcessor, get_days_to_process

logger = logging.getLogger(__name__)

class FixWaermepumpeStromverbrauchProcessor(EntityProcessor):
    """Processor to fix heat pump electricity consumption data by überarbeiten der Zeitstempel. Eigentlich sollten die Daten um 00:00 Uhr auf 0 zurückgesetzt werden, das passiert baer nicht sicher. Teilweise kommen nach Mitternacht noch Daten vom Vortag, die dann fälschlicherweise dem neuen Tag zugeordnet werden. Dieser Prozessor soll die Daten korrigieren, indem er die Zeitstempel überarbeitet und die Werte entsprechend anpasst.
    """
    def process(self) -> None:

        last_data_day = self.influx_handler.get_last_data_day(
            bucket=self.output_bucket,
            entity_id=self.entities[0],
            version=self.version,
            field="value",
            measurement=self.output_measurement,
        )

        if not last_data_day:
            logger.debug(
                f"No existing data for {self.entities[0]} in output bucket, starting from first data day"
            )
            last_data_day = self.first_data_day - timedelta(days=1)

        days_to_process = get_days_to_process(last_data_day)

        if not days_to_process:
            logger.info(f"No days to process for {self.entities[0]}")
            return

        logger.info(f"Processing {len(days_to_process)} days.")

        for day in days_to_process:
            self._process_day(day)

    def _process_day(self, day: datetime.date) -> None:
        day_start_time = local_to_utc(datetime.combine(day, time(hour=0, minute=0, second=0, microsecond=0)))
        day_end_time = local_to_utc(datetime.combine(day, time(hour=23, minute=59, second=59, microsecond=0)))
        start_time = local_to_utc(datetime.combine(day - timedelta(days=1), time(hour=20)))
        end_time = local_to_utc(datetime.combine(day + timedelta(days=1), time(hour=4)))

        for entity_id in self.entities:
            correct_data = []
            last_value = 0.0
            first_reset_idx = None
            second_reset_idx = None
            day_data = self.influx_handler.get_data(
                start_time=start_time,
                stop_time=end_time,
                bucket=self.input_bucket,
                entity_id=entity_id,
                field="value",
            )
            if day_data is not None and len(day_data) > 0:
                for idx, record in enumerate(day_data):
                    record_value = record.get("value")
                    record_time = record.get("time")
                    record_value = round(record_value, 3)
                    if record_value >= last_value:
                        last_value = record_value
                    else:
                        # determine if this is the first or second reset based on the time of the record
                        time_diff_first_reset = abs((record_time - day_start_time).total_seconds())
                        time_diff_second_reset = abs(((day_end_time) - record_time).total_seconds())

                        if first_reset_idx is None and time_diff_first_reset <= time_diff_second_reset:
                            first_reset_idx = idx
                            last_value = record_value
                        elif second_reset_idx is None and time_diff_second_reset < time_diff_first_reset:
                            second_reset_idx = idx - 1
                            last_value = record_value
                            break

                if second_reset_idx is None:
                    if day_data[-1].get("time") <= day_end_time and day_data[-1].get("time") > day_start_time:
                        # Couldn find a second reset, but the last record is placed correctly, so we can just use it as correct data
                        second_reset_idx = len(day_data) - 1
                    else:
                        # Coudlnt find a second reset so we assume that the last record is the second reset and adjust the time to the end of the day
                        logger.warning(
                            f"Could not find a second reset for {entity_id} on {day}! "
                            f"The last datapoint is {day_data[-1].get('value')} at {day_data[-1].get('time')}! "
                            "Moving it to the end of the day."
                        )
                        second_reset_idx = len(day_data) - 1

                if first_reset_idx is None:
                    if day_data[0].get("value") == 0.0 and day_data[0].get("time") >= day_start_time and day_data[0].get("time") <= day_end_time:
                        # Couldn find a reset, but the first record is already at 0 and placed correctly, so we can just use it as correct data
                        first_reset_idx = 0
                    elif day_data[0].get("time") >= day_start_time and day_data[0].get("value") > 0.0:
                        # Data is corrupt, add record at midnight and set the idx
                        logger.warning(
                            f"Could not find a first reset for {entity_id} on {day}! "
                            f"The first datapoint is {day_data[0].get('value')} at {day_data[0].get('time')}! "
                            "Adding a reset at midnight."
                        )
                        first_reset_idx = 0
                        day_data.insert(0, {"time": day_start_time, "value": 0.0})
                        second_reset_idx += 1
                    elif day_data[0].get("time") < day_start_time and day_data[0].get("value") == 0.0:
                        # Data is corrupt, but the first record is at 0, so we can just move it to midnight and set the idx
                        logger.warning(
                            f"Could not find a first reset for {entity_id} on {day}! "
                            f"The first datapoint is {day_data[0].get('value')} at {day_data[0].get('time')}! "
                            "Moving it to midnight."
                        )
                        first_reset_idx = 0
                        day_data[0]["time"] = day_start_time
                    else:
                        logger.error(
                            f"Could not find a first reset for {entity_id} on {day}! "
                            f"The first datapoint is {day_data[0].get('value')} at {day_data[0].get('time')}!"
                        )
                        raise ValueError(
                            f"Could not find a first reset for {entity_id} on {day}! "
                            f"The first datapoint is {day_data[0].get('value')} at {day_data[0].get('time')}!"
                        )
                
                first_reset_time = day_data[first_reset_idx].get("time")
                second_reset_time = day_data[second_reset_idx].get("time")
                if first_reset_time == second_reset_time and first_reset_time >= day_start_time and first_reset_time <= day_end_time and len(day_data) <= 1 and day_data[0].get("value") == 0.0:
                    correct_data =  self._fill_with_zeroes(day_start_time, day_end_time)
                elif first_reset_time >= day_start_time and second_reset_time <= day_end_time:
                    correct_data = self._collect_range(day_data, first_reset_idx, second_reset_idx)
                elif first_reset_time < day_start_time and second_reset_time <= day_end_time:
                    correct_data = self._adjust_first_reset(day_start_time, day_data, first_reset_idx, second_reset_idx, day_end_time, day_start_time)
                elif first_reset_time >= day_start_time and second_reset_time >= day_end_time:
                    correct_data = self._adjust_second_reset(day_start_time, day_data, first_reset_idx, second_reset_idx, day_end_time, day_start_time)
                else:
                    correct_data = self._adjust_both_resets(day_start_time, day_data, first_reset_idx, second_reset_idx, day_end_time, day_start_time)
            else:
                logger.warning(f"No data found for {entity_id} on {day}, fill with 0")
                correct_data =  self._fill_with_zeroes(day_start_time, day_end_time)

            correct_data = self._make_monoton(correct_data)
            if correct_data[0].get("time") != day_start_time:
                correct_data.insert(0, {"time": day_start_time, "value": 0.0})
            if correct_data[-1].get("time") != day_end_time:
                correct_data.append({"time": day_end_time, "value": correct_data[-1].get("value")})

            for record in correct_data:
                self.influx_handler.write_datapoint(
                    bucket=self.output_bucket,
                    entity_id=entity_id,
                    version=self.version,
                    field="value",
                    unit="kWh",
                    value=record.get("value"),
                    timestamp=record.get("time"),
                    measurement=self.output_measurement,
                )
                
    def _make_monoton(self, day_data: list[dict]) -> list[dict]:
        last_value = 0.0
        for idx, record in enumerate(day_data):
            record_value = record.get("value")
            if record_value < last_value:
                diff = last_value - record_value
                if diff > 0.25:
                    logger.error(
                        f"Monotonicity violation > 0.25 detected at idx {idx}: value {record_value} time {record.get('time')} (prev {last_value})"
                    )
                    raise ValueError(
                        f"Monotonicity violation > 0.25 detected at idx {idx} for time {record.get('time')}"
                    )
                logger.warning(
                    f"Monotonicity violation detected at idx {idx}: value {record_value} < prev {last_value}. Adjusting to {last_value}."
                )
                day_data[idx] = {"time": day_data[idx].get("time"), "value": last_value}
            else:
                last_value = record_value
        return day_data


    def _fill_with_zeroes(self, day_start_time: datetime, day_end_time: datetime) -> list[dict]:
        correct_data = []
        current_time = day_start_time + timedelta(hours=1)
        while current_time < day_end_time:
            correct_data.append({"time": current_time, "value": 0.0})
            current_time += timedelta(hours=1)
        return correct_data

    def _collect_range(self, day_data: list[dict], start_idx: int, end_idx: int) -> list[dict]:
        return [
            {"time": day_data[idx].get("time"), "value": day_data[idx].get("value")}
            for idx in range(start_idx, end_idx + 1)
        ]

    def _adjust_first_reset(
        self, day: datetime, day_data: list[dict], first_reset_idx: int, second_reset_idx: int, day_end_time: datetime, day_start_time: datetime
    ) -> list[dict]:
        correct_data = []
        last_incorrect_idx = first_reset_idx

        corrected_time = day_start_time
        correct_data.append({"time": corrected_time, "value": day_data[first_reset_idx].get("value")})

        for idx in range(first_reset_idx + 1, second_reset_idx + 1):
            record = day_data[idx]
            if record.get("time") >= day:
                correct_data.append({"time": record.get("time"), "value": record.get("value")})
            else:
                corrected_time = day_start_time
                correct_data.append({"time": corrected_time, "value": record.get("value")})
                last_incorrect_idx = idx

        if last_incorrect_idx != first_reset_idx:
            logger.warning(f"Found {last_incorrect_idx - first_reset_idx} records after the first reset that are placed before midnight! Adjusting their timestamps as well.")
            number_incorrect_records = last_incorrect_idx - first_reset_idx
            next_anchor_time = day_data[last_incorrect_idx + 1].get("time")
            time_diff = next_anchor_time - day
            time_step = time_diff.total_seconds() / (number_incorrect_records + 1)
            for i in range(1, number_incorrect_records + 1):
                corrected_time = day + timedelta(seconds=(i + 1) * time_step)
                correct_data[i] = {"time": corrected_time, "value": correct_data[i].get("value")}

        return correct_data

    def _adjust_second_reset(
        self, day: datetime, day_data: list[dict], first_reset_idx: int, second_reset_idx: int, day_end_time: datetime, day_start_time: datetime
    ) -> list[dict]:
        correct_data = []
        first_incorrect_idx = None

        for idx in range(first_reset_idx, second_reset_idx + 1):
            record = day_data[idx]
            if record.get("time") <= day_end_time:
                correct_data.append({"time": record.get("time"), "value": record.get("value")})
            else:
                corrected_time = day_end_time
                correct_data.append({"time": corrected_time, "value": record.get("value")})
                if first_incorrect_idx is None:
                    first_incorrect_idx = idx

        if first_incorrect_idx is not None and first_incorrect_idx != second_reset_idx:
            logger.warning(f"Found {second_reset_idx - first_incorrect_idx} records before the second reset that are placed after the end of the day! Adjusting their timestamps as well.")
            number_incorrect_records = second_reset_idx - first_incorrect_idx
            prev_time = day_data[first_incorrect_idx - 1].get("time")
            time_step = (day_end_time - prev_time).total_seconds() / (number_incorrect_records + 1)
            first_correct_data_idx = len(correct_data) - number_incorrect_records - 1
            for i in range(number_incorrect_records):
                corrected_time = prev_time + timedelta(seconds=(i + 1) * time_step)
                correct_data[first_correct_data_idx + i] = {
                    "time": corrected_time,
                    "value": correct_data[first_correct_data_idx + i].get("value"),
                }

        return correct_data

    def _adjust_both_resets(
        self, day: datetime, day_data: list[dict], first_reset_idx: int, second_reset_idx: int, day_end_time: datetime, day_start_time: datetime
    ) -> list[dict]:
        # First move the early reset to midnight and normalize the leading records
        first_corrected = self._adjust_first_reset(day, day_data, first_reset_idx, second_reset_idx, day_end_time, day_start_time)
        # Then move the late reset to the end of the day using the normalized list
        return self._adjust_second_reset(day, first_corrected, 0, len(first_corrected) - 1, day_end_time, day_start_time)

