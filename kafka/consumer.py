import json
import os
import time
import io
from datetime import datetime, timezone
from collections import defaultdict
from kafka import KafkaConsumer
from minio import Minio

KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC_EARTHQUAKES", "earthquakes-raw")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS     = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
BUCKET         = os.getenv("MINIO_BUCKET_BRONZE", "bronze")

BATCH_SIZE     = 50    # écrire dans MinIO tous les 50 messages
FLUSH_INTERVAL = 30    # ou toutes les 30 secondes

print(f"[BOOT] Kafka={KAFKA_BROKER} MinIO={MINIO_ENDPOINT}", flush=True)

minio = Minio(MINIO_ENDPOINT, access_key=MINIO_USER,
              secret_key=MINIO_PASS, secure=False)
if not minio.bucket_exists(BUCKET):
    minio.make_bucket(BUCKET)
print(f"[MinIO] Bucket '{BUCKET}' OK", flush=True)

def write_batch(batch_by_day):
    """Écrit un fichier JSONL par jour dans MinIO"""
    for day_key, events in batch_by_day.items():
        year, month, day = day_key
        path = f"earthquakes/year={year}/month={month:02d}/day={day:02d}/batch_{int(time.time())}.jsonl"
        # JSONL = un JSON par ligne — format standard pour les batches
        content = "\n".join(json.dumps(e) for e in events).encode("utf-8")
        minio.put_object(
            BUCKET, path,
            io.BytesIO(content),
            length=len(content),
            content_type="application/x-ndjson"
        )
        print(f"[MinIO] {len(events)} séismes → {path}", flush=True)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="bronze-group-v2",   # nouveau group_id pour relire depuis le début
    enable_auto_commit=True,
    fetch_max_bytes=1048576,
    max_partition_fetch_bytes=1048576,
    max_poll_records=50,
)
print("[Consumer] En ecoute...", flush=True)

batch_by_day = defaultdict(list)
last_flush   = time.time()
total        = 0

for msg in consumer:
    event = msg.value

    # Extraire la date de l'événement (pas la date d'ingestion)
    ts_ms = event.get("properties", {}).get("time", int(time.time() * 1000))
    dt    = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    day_key = (dt.year, dt.month, dt.day)

    batch_by_day[day_key].append(event)
    total += 1

    # Écrire si batch plein OU timeout atteint
    elapsed = time.time() - last_flush
    total_buffered = sum(len(v) for v in batch_by_day.values())