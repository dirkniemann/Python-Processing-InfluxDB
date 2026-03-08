import argparse
import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delete points from InfluxDB with a predicate")
    parser.add_argument("--bucket", default="testing", help="Target bucket")
    parser.add_argument("--measurement", default="Waermepumpe_statistik", help="Measurement to delete")
    parser.add_argument("--entity-id", dest="entity_id", help="Entity ID tag to match")
    parser.add_argument("--version", dest="version", default=None, help="Optional version tag to match")
    parser.add_argument("--start", default="1970-01-01T00:00:00Z", help="Start time (UTC)")
    parser.add_argument("--stop", default="2026-03-08T00:00:00Z", help="Stop time (UTC)")
    parser.add_argument("--all", action="store_true", help="Delete everything in bucket within time range (predicate=true)")
    parser.add_argument("--timeout-ms", type=int, default=None, help="HTTP timeout in milliseconds (overrides env INFLUX_TIMEOUT_MS)")
    return parser.parse_args()


def build_predicate(measurement: str, entity_id: str | None, version: str | None, delete_all: bool) -> str:
    if delete_all:
        return ""
    parts = [f'_measurement="{measurement}"']
    if entity_id:
        parts.append(f'"entity_id"="{entity_id}"')
    if version:
        parts.append(f'"version"="{version}"')
    return " AND ".join(parts)


def main() -> int:
    load_dotenv()

    url = os.getenv("INFLUX_URL")
    token = os.getenv("INFLUX_TOKEN")
    org = os.getenv("INFLUX_ORG")
    env_timeout_ms = os.getenv("INFLUX_TIMEOUT_MS")

    if not all([url, token, org]):
        print("INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG must be set in environment/.env", file=sys.stderr)
        return 1

    args = parse_args()
    timeout_ms = args.timeout_ms or int(env_timeout_ms) if env_timeout_ms else 120000
    predicate = build_predicate(args.measurement, args.entity_id, args.version, args.all)

    print(f"Deleting from bucket={args.bucket} where {predicate or 'true'} between {args.start} and {args.stop} (timeout_ms={timeout_ms})")

    try:
        with InfluxDBClient(url=url, token=token, org=org, timeout=timeout_ms) as client:
            delete_api = client.delete_api()
            delete_api.delete(args.start, args.stop, predicate, bucket=args.bucket, org=org)
        print("Delete request sent successfully")
        return 0
    except Exception as exc:
        print(f"Delete failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())