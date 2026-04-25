"""
eonet_to_bronze DAG
====================
Fetches natural disaster events from NASA EONET API (no auth required)
and stores JSON files in MinIO under bronze/eonet/.

NASA EONET API docs: https://eonet.gsfc.nasa.gov/docs/v3
No API key required. Free and public.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone, timedelta
import requests, json, os, io, logging
from minio import Minio

log = logging.getLogger(__name__)

default_args = {"owner": "airflow", "retries": 2, "retry_delay": timedelta(minutes=5)}

EONET_BASE = "https://eonet.gsfc.nasa.gov/api/v3"

def fetch_and_store():
    client = Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=False,
    )
    bucket = "bronze"
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    now = datetime.now(timezone.utc)
    prefix = f"eonet/year={now.year}/month={now.month:02d}/day={now.day:02d}"
    total_saved = 0

    # Fetch open events (default, most reliable endpoint)
    for status in ["open", "closed"]:
        params = {
            "limit": 200,
            "days": 60,
            "status": status,
        }
        url = f"{EONET_BASE}/events"
        log.info(f"Calling EONET: {url} status={status}")

        try:
            r = requests.get(url, params=params, timeout=30)
            log.info(f"EONET status={status} → HTTP {r.status_code}")

            if r.status_code == 500:
                log.warning(f"EONET server error for status={status}, skipping this batch")
                continue

            r.raise_for_status()
            events = r.json().get("events", [])
            log.info(f"EONET returned {len(events)} {status} events")

            for event in events:
                event_id = event.get("id", "unknown")
                key = f"{prefix}/{event_id}.json"
                content = json.dumps(event).encode("utf-8")
                client.put_object(
                    bucket, key,
                    io.BytesIO(content), length=len(content),
                    content_type="application/json",
                )
                total_saved += 1

        except requests.exceptions.HTTPError as e:
            log.warning(f"HTTP error fetching EONET {status} events: {e}")
            continue
        except Exception as e:
            log.warning(f"Unexpected error fetching EONET {status} events: {e}")
            continue

    if total_saved == 0:
        raise Exception("EONET: no events saved from either open or closed endpoints — API may be down")

    log.info(f"Saved {total_saved} EONET events to MinIO bronze/eonet/")


with DAG(
    dag_id="eonet_to_bronze",
    default_args=default_args,
    start_date=datetime(2026, 3, 31),
    schedule_interval="@daily",
    catchup=False,
    tags=["bronze", "eonet", "nasa"],
) as dag:
    PythonOperator(task_id="fetch_and_store", python_callable=fetch_and_store)