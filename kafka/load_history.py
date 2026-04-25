import requests
import json
import time
import os
import psycopg2
from kafka import KafkaProducer
from dotenv import load_dotenv
import pathlib

if pathlib.Path(".env.local").exists():
    load_dotenv(".env.local", override=True)
else:
    load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC_EARTHQUAKES", "earthquakes-raw")

SOURCES = [
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson",
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_month.geojson",
]

def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB", "disasters"),
        user=os.getenv("POSTGRES_USER", "pipeline"),
        password=os.getenv("POSTGRES_PASSWORD", "pipeline123"),
        port=int(os.getenv("POSTGRES_PORT", 5432))
    )

def load_sent_ids(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT event_id FROM cdc_sent_ids WHERE source='usgs'")
        return set(r[0] for r in cur.fetchall())

def save_sent_id(conn, event_id):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO cdc_sent_ids (event_id, source)
            VALUES (%s, 'usgs') ON CONFLICT DO NOTHING
        """, (event_id,))
    conn.commit()

if __name__ == "__main__":
    conn = get_pg_conn()
    sent_ids = load_sent_ids(conn)
    print(f"IDs déjà connus : {len(sent_ids)}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3
    )

    total_sent = 0
    for url in SOURCES:
        print(f"\nChargement : {url.split('/')[-1]}")
        r = requests.get(url, timeout=30)
        features = r.json()["features"]
        print(f"  {len(features)} événements disponibles")

        sent = 0
        for feature in features:
            event_id = feature["id"]
            if event_id in sent_ids:
                continue
            feature["ingested_at"] = int(time.time() * 1000)
            producer.send(KAFKA_TOPIC, value=feature)
            save_sent_id(conn, event_id)
            sent_ids.add(event_id)
            sent += 1

        producer.flush()
        total_sent += sent
        print(f"  Envoyés : {sent}")

    print(f"\nTotal envoyé : {total_sent} événements")
    print(f"Total connu  : {len(sent_ids)} événements")
    producer.close()
    conn.close()