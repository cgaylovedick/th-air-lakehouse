"""
Bronze ingestion: pull hourly air-quality and weather data for Thai provinces
from the public Open-Meteo APIs and land the raw JSON in the lake.

Each run covers exactly one data interval (one hour), and the object key is
derived from that interval, so re-running or backfilling an hour overwrites
the same file instead of creating duplicates (idempotent load).
"""

from datetime import datetime, timedelta, timezone

import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from common.provinces import PROVINCES
from common.storage import ensure_bucket, get_bucket, get_s3_client, put_json

SOURCES = {
    "air_quality": {
        "url": "https://air-quality-api.open-meteo.com/v1/air-quality",
        "hourly": "pm2_5,pm10,carbon_monoxide,nitrogen_dioxide,ozone,us_aqi",
    },
    "weather": {
        "url": "https://api.open-meteo.com/v1/forecast",
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
    },
}


@dag(
    dag_id="ingest_open_meteo",
    schedule="@hourly",
    start_date=datetime(2026, 11, 18, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["bronze", "open-meteo"],
)
def ingest_open_meteo():
    @task
    def create_bucket() -> str:
        bucket = get_bucket()
        ensure_bucket(get_s3_client(), bucket)
        return bucket

    @task
    def extract_and_load(source: str, bucket: str) -> dict:
        ctx = get_current_context()
        start = ctx["data_interval_start"].in_timezone("UTC")
        hour = start.strftime("%Y-%m-%dT%H:%M")

        cfg = SOURCES[source]
        params = {
            "latitude": ",".join(str(p["lat"]) for p in PROVINCES),
            "longitude": ",".join(str(p["lon"]) for p in PROVINCES),
            "hourly": cfg["hourly"],
            "timezone": "GMT",
            "start_hour": hour,
            "end_hour": hour,
        }
        resp = requests.get(cfg["url"], params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        # Open-Meteo returns a list when several coordinates are requested
        locations = payload if isinstance(payload, list) else [payload]
        if len(locations) != len(PROVINCES):
            raise ValueError(
                f"Expected {len(PROVINCES)} locations, got {len(locations)}"
            )

        record = {
            "source": source,
            "interval_start_utc": start.isoformat(),
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            "provinces": PROVINCES,
            "payload": locations,
        }
        key = (
            f"bronze/open_meteo/{source}/"
            f"dt={start.strftime('%Y-%m-%d')}/hr={start.strftime('%H')}/data.json"
        )
        put_json(get_s3_client(), bucket, key, record)

        print(f"Wrote s3://{bucket}/{key} ({len(locations)} locations)")
        return {"source": source, "key": key, "locations": len(locations)}

    bucket = create_bucket()
    extract_and_load.partial(bucket=bucket).expand(source=list(SOURCES))


ingest_open_meteo()
