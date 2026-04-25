from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
import requests, json, os
from minio import Minio

default_args = {"owner": "airflow", "retries": 1}

def fetch_and_store():
    # Simple GET — no date params to avoid timeout
    url = "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()

    features = data.get("features", [])
    if not features:
        print("No features returned from GDACS")
        return

    print(f"GDACS returned {len(features)} features")

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
    saved = 0
    for feature in features:
        props = feature.get("properties", {})
        event_id = props.get("eventid", "unknown")
        episode_id = props.get("episodeid", "0")
        key = f"gdacs/year={now.year}/month={now.month:02d}/day={now.day:02d}/{event_id}_{episode_id}.json"

        payload = json.dumps(feature).encode("utf-8")
        import io
        client.put_object(
            bucket, key, io.BytesIO(payload), length=len(payload),
            content_type="application/json"
        )
        saved += 1

    print(f"Saved {saved} GDACS events to MinIO bronze/gdacs/")

with DAG(
    dag_id="gdacs_to_bronze",
    default_args=default_args,
    start_date=datetime(2026, 3, 31),
    schedule_interval="@hourly",
    catchup=False,
    tags=["bronze", "gdacs"],
) as dag:
    PythonOperator(task_id="fetch_and_store", python_callable=fetch_and_store)