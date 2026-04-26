"""
bronze_to_silver DAG
====================
1. Creates raw tables in PostgreSQL
2. Loads GDACS events from MinIO bronze/gdacs/ (or bronze/earthquakes/)
3. Loads NASA EONET events from MinIO bronze/eonet/
4. Runs dbt (staging views + mart tables)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, json, logging, subprocess, shutil
import psycopg2
from minio import Minio
import requests

log = logging.getLogger(__name__)

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)}

# ─── helpers ──────────────────────────────────────────────────────────────────

def get_minio():
    return Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=False,
    )

def get_pg():
    return psycopg2.connect(
        host="postgres", dbname="disasters",
        user="pipeline", password="pipeline123"
    )

# ─── task 1 : create raw tables ───────────────────────────────────────────────

def create_raw_tables():
    conn = get_pg()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_gdacs (
            event_id            TEXT,
            episode_id          TEXT,
            event_type          TEXT,
            event_name          TEXT,
            alert_level         TEXT,
            from_date           TEXT,
            to_date             TEXT,
            country             TEXT,
            iso3                TEXT,
            latitude            TEXT,
            longitude           TEXT,
            severity_value      TEXT,
            severity_unit       TEXT,
            population_affected TEXT,
            glide               TEXT,
            url                 TEXT,
            ingested_at         TIMESTAMP WITH TIME ZONE DEFAULT now(),
            PRIMARY KEY (event_id, episode_id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_eonet (
            eonet_id        TEXT PRIMARY KEY,
            title           TEXT,
            description     TEXT,
            link            TEXT,
            closed          TEXT,
            status          TEXT,
            category_id     TEXT,
            category_title  TEXT,
            source_id       TEXT,
            source_url      TEXT,
            geometry_type   TEXT,
            latitude        TEXT,
            longitude       TEXT,
            event_date      TEXT,
            ingested_at     TIMESTAMP WITH TIME ZONE DEFAULT now()
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    log.info("Raw tables created/verified.")

# ─── task 2 : load GDACS from MinIO ───────────────────────────────────────────

def load_gdacs_to_postgres():
    client = get_minio()
    conn = get_pg()
    cur = conn.cursor()

    objects = list(client.list_objects("bronze", prefix="gdacs/", recursive=True))
    log.info(f"Found {len(objects)} objects under bronze/gdacs/")
    if len(objects) == 0:
        objects = list(client.list_objects("bronze", prefix="earthquakes/", recursive=True))
        log.info(f"Falling back: found {len(objects)} objects under bronze/earthquakes/")

    inserted = 0
    errors = 0

    def safe_str(val):
        if val is None:
            return None
        if isinstance(val, (dict, list)):
            return json.dumps(val)
        return str(val)

    for obj in objects:
        if not obj.object_name.endswith(".json"):
            continue
        try:
            resp = client.get_object("bronze", obj.object_name)
<<<<<<< HEAD

=======
            raw = resp.read().decode("utf-8")
            lines = [l.strip() for l in raw.strip().split("\n") if l.strip()]
>>>>>>> bb53a841175eb3b562739133a8dc472634ece009
        except Exception as e:
            errors += 1
            log.warning(f"Error reading {obj.object_name}: {e}")
            continue

        for line in lines:
            try:
                data = json.loads(line)

                if "properties" in data:
                    props = data.get("properties", {})
                    geo = data.get("geometry", {})
                    coords = geo.get("coordinates", [None, None])
                else:
                    props = data
                    coords = [data.get("longitude"), data.get("latitude")]

                severity_data = props.get("severitydata", {})
                if not isinstance(severity_data, dict):
                    severity_data = {}
                population_data = props.get("population", {})
                if not isinstance(population_data, dict):
                    population_data = {}

                event_id = str(props.get("eventid", ""))
                episode_id = str(props.get("episodeid", "0"))
                if not event_id:
                    continue

                cur.execute("""
                    INSERT INTO raw_gdacs (
                        event_id, episode_id, event_type, event_name, alert_level,
                        from_date, to_date, country, iso3,
                        latitude, longitude, severity_value, severity_unit,
                        population_affected, glide, url
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (event_id, episode_id) DO UPDATE SET
                        alert_level         = EXCLUDED.alert_level,
                        to_date             = EXCLUDED.to_date,
                        country             = EXCLUDED.country,
                        iso3                = EXCLUDED.iso3,
                        latitude            = EXCLUDED.latitude,
                        longitude           = EXCLUDED.longitude,
                        severity_value      = EXCLUDED.severity_value,
                        severity_unit       = EXCLUDED.severity_unit,
                        population_affected = EXCLUDED.population_affected,
                        ingested_at         = now()
                """, (
                    event_id, episode_id,
                    safe_str(props.get("eventtype")),
                    safe_str(props.get("eventname")),
                    safe_str(props.get("alertlevel")),
                    safe_str(props.get("fromdate")),
                    safe_str(props.get("todate")),
                    safe_str(props.get("country")),
                    safe_str(props.get("iso3")),
                    safe_str(coords[1]) if coords and len(coords) > 1 and coords[1] is not None else None,
                    safe_str(coords[0]) if coords and len(coords) > 0 and coords[0] is not None else None,
                    safe_str(severity_data.get("severity")),
                    safe_str(severity_data.get("severityunit")),
                    safe_str(population_data.get("populationaffected")),
                    safe_str(props.get("glide")),
                    safe_str(props.get("url")),
                ))
                inserted += 1
            except Exception as e:
                errors += 1
                log.warning(f"Error processing line in {obj.object_name}: {e}")
                continue

    conn.commit()
    cur.close()
    conn.close()
    log.info(f"GDACS: upserted {inserted} records, {errors} errors → raw_gdacs")
# ─── task 3 : load NASA EONET from MinIO ──────────────────────────────────────

def load_eonet_to_postgres():
    client = get_minio()
    conn = get_pg()
    cur = conn.cursor()

    objects = list(client.list_objects("bronze", prefix="eonet/", recursive=True))
    log.info(f"Found {len(objects)} EONET objects under bronze/eonet/")

    if len(objects) == 0:
        log.info("No EONET files in MinIO yet — skipping (run eonet_to_bronze DAG first)")
        cur.close()
        conn.close()
        return

    inserted = 0
    errors = 0
    for obj in objects:
        if not obj.object_name.endswith(".json"):
            continue
        try:
            resp = client.get_object("bronze", obj.object_name)
            event = json.loads(resp.read())

            eonet_id = event.get("id", "")
            if not eonet_id:
                continue

            categories = event.get("categories", [])
            cat_id = categories[0].get("id", "") if categories else ""
            cat_title = categories[0].get("title", "") if categories else ""

            sources = event.get("sources", [])
            src_id = sources[0].get("id", "") if sources else ""
            src_url = sources[0].get("url", "") if sources else ""

            geometries = event.get("geometry", [])
            lat = lon = geo_type = event_date = None
            if geometries:
                geo = geometries[-1]
                geo_type = geo.get("type", "")
                event_date = geo.get("date", "")
                coords = geo.get("coordinates", [])
                if geo_type == "Point" and len(coords) >= 2:
                    lon = str(coords[0])
                    lat = str(coords[1])

            cur.execute("""
                INSERT INTO raw_eonet (
                    eonet_id, title, description, link, closed, status,
                    category_id, category_title, source_id, source_url,
                    geometry_type, latitude, longitude, event_date
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (eonet_id) DO UPDATE SET
                    status      = EXCLUDED.status,
                    closed      = EXCLUDED.closed,
                    event_date  = EXCLUDED.event_date,
                    latitude    = EXCLUDED.latitude,
                    longitude   = EXCLUDED.longitude,
                    ingested_at = now()
            """, (
                eonet_id,
                event.get("title"),
                event.get("description"),
                event.get("link"),
                event.get("closed"),
                event.get("status"),
                cat_id, cat_title,
                src_id, src_url,
                geo_type, lat, lon, event_date,
            ))
            inserted += 1
        except Exception as e:
            errors += 1
            log.warning(f"Error processing {obj.object_name}: {e}")
            continue

    conn.commit()
    cur.close()
    conn.close()
    log.info(f"EONET: upserted {inserted} records, {errors} errors → raw_eonet")

# ─── tasks 4+5 : find dbt binary and run it ───────────────────────────────────

def _find_dbt_bin():
    dbt_bin = shutil.which("dbt")
    if dbt_bin:
        return dbt_bin
    candidates = [
        "/home/airflow/.local/bin/dbt",
        "/usr/local/bin/dbt",
        "/usr/bin/dbt",
        "/root/.local/bin/dbt",
    ]
    for c in candidates:
        if os.path.isfile(c) and os.access(c, os.X_OK):
            return c
    raise FileNotFoundError(
        "dbt binary not found. Add 'dbt-postgres==1.7.4' to "
        "_PIP_ADDITIONAL_REQUIREMENTS in docker-compose.yml and restart."
    )

def run_dbt(subcmd: str):
    dbt_bin = _find_dbt_bin()
    log.info(f"dbt binary: {dbt_bin}")
    cmd = [dbt_bin, subcmd,
           "--profiles-dir", "/opt/airflow/dbt",
           "--project-dir",  "/opt/airflow/dbt"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    for line in result.stdout.splitlines():
        log.info(line)
    if result.returncode != 0:
        for line in result.stderr.splitlines():
            log.error(line)
        raise Exception(f"dbt {subcmd} exited with code {result.returncode}")

def dbt_run():
    run_dbt("run")

def dbt_test():
    run_dbt("test")

# ─── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="bronze_to_silver",
    default_args=default_args,
    start_date=datetime(2026, 3, 31),
    schedule_interval="@hourly",
    catchup=False,
    tags=["silver", "dbt"],
) as dag:

    t1 = PythonOperator(task_id="create_raw_tables", python_callable=create_raw_tables)
    t2 = PythonOperator(task_id="load_gdacs",        python_callable=load_gdacs_to_postgres)
    t3 = PythonOperator(task_id="load_eonet",        python_callable=load_eonet_to_postgres)
    t4 = PythonOperator(task_id="dbt_run",           python_callable=dbt_run)
    t5 = PythonOperator(task_id="dbt_test",          python_callable=dbt_test)

    t1 >> [t2, t3] >> t4 >> t5