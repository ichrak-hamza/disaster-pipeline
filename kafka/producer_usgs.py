# kafka/producer_usgs.py — version CDC robuste avec PostgreSQL
import requests
import json
import time
import os
import psycopg2
from kafka import KafkaProducer
from dotenv import load_dotenv

from dotenv import load_dotenv
import pathlib

# Charge .env.local si présent (Windows), sinon .env (Docker)
env_file = pathlib.Path(".env.local")
if env_file.exists():
    load_dotenv(dotenv_path=".env.local", override=True)
else:
    load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC_EARTHQUAKES", "earthquakes-raw")
USGS_URL     = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"

# ── Connexion PostgreSQL pour stocker les IDs ──
def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),   # fallback localhost
        dbname=os.getenv("POSTGRES_DB", "disasters"),
        user=os.getenv("POSTGRES_USER", "pipeline"),
        password=os.getenv("POSTGRES_PASSWORD", "pipeline123"),
        port=int(os.getenv("POSTGRES_PORT", 5432))
    )

def init_cdc_table(conn):
    """Crée la table de tracking CDC si elle n'existe pas"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cdc_sent_ids (
                event_id    VARCHAR(50) PRIMARY KEY,
                sent_at     TIMESTAMP DEFAULT NOW(),
                source      VARCHAR(20) DEFAULT 'usgs'
            )
        """)
    conn.commit()

def load_sent_ids(conn):
    """Charge tous les IDs déjà envoyés depuis PostgreSQL"""
    with conn.cursor() as cur:
        cur.execute("SELECT event_id FROM cdc_sent_ids WHERE source = 'usgs'")
        return set(row[0] for row in cur.fetchall())

def save_sent_id(conn, event_id):
    """Enregistre un nouvel ID comme traité"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO cdc_sent_ids (event_id, source)
            VALUES (%s, 'usgs')
            ON CONFLICT DO NOTHING
        """, (event_id,))
    conn.commit()

# ── Producer Kafka ──
def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3
    )

def fetch_and_produce(producer, conn, sent_ids):
    try:
        response = requests.get(USGS_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        new_count = 0
        for feature in data["features"]:
            event_id = feature["id"]
            if event_id in sent_ids:
                continue

            feature["ingested_at"] = int(time.time() * 1000)
            producer.send(KAFKA_TOPIC, value=feature)
            save_sent_id(conn, event_id)
            sent_ids.add(event_id)
            new_count += 1

        producer.flush()
        print(f"[OK] {new_count} nouveaux | {len(sent_ids)} total connus")

    except Exception as e:
        print(f"[ERREUR] {e}")

if __name__ == "__main__":
    conn     = get_pg_conn()
    init_cdc_table(conn)
    sent_ids = load_sent_ids(conn)
    producer = create_producer()

    print(f"[CDC] {len(sent_ids)} événements déjà connus en base")
    fetch_and_produce(producer, conn, sent_ids)

    producer.close()
    conn.close()