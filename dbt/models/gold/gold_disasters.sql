-- depends_on: {{ ref('mart_disasters') }}
-- models/gold/gold_disasters.sql
-- Gold layer: unified GDACS + EONET, denormalized for Streamlit

{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['event_date'], 'type': 'btree'},
      {'columns': ['event_type'], 'type': 'btree'},
    ]
  )
}}

WITH gdacs AS (
    SELECT
        disaster_id,
        event_type,
        event_type_label,
        event_name,
        alert_level,
        alert_level_num,
        status,
        country,
        iso3,
        latitude,
        longitude,
        event_date,
        event_end_date,
        severity_value,
        severity_unit,
        population_affected,
        gdacs_url                   AS source_url,
        'GDACS'                     AS source_tag,
        ingested_at
    FROM {{ ref('mart_disasters') }}
),

eonet AS (
    SELECT
        eonet_id                    AS disaster_id,
        category_id                 AS event_type,
        category_title              AS event_type_label,
        title                       AS event_name,
        CASE
            WHEN status = 'open'   THEN 'ONGOING'
            WHEN status = 'closed' THEN 'CLOSED'
            ELSE 'UNKNOWN'
        END                         AS alert_level,
        0                           AS alert_level_num,
        status,
        NULL::TEXT                  AS country,
        NULL::TEXT                  AS iso3,
        latitude,
        longitude,
        event_date,
        NULL::TIMESTAMP WITH TIME ZONE AS event_end_date,
        NULL::NUMERIC               AS severity_value,
        NULL::TEXT                  AS severity_unit,
        NULL::BIGINT                AS population_affected,
        source_url,
        'EONET'                     AS source_tag,
        ingested_at
    FROM {{ ref('mart_eonet') }}
)

SELECT
    disaster_id, event_type, event_type_label, event_name,
    alert_level, alert_level_num, status, country, iso3,
    latitude, longitude, event_date, event_end_date,
    severity_value, severity_unit, population_affected,
    source_url, source_tag, ingested_at,
    CASE WHEN status IN ('ongoing','open') THEN TRUE ELSE FALSE END AS is_active,
    DATE_TRUNC('day',   event_date)::DATE   AS event_day,
    DATE_TRUNC('month', event_date)::DATE   AS event_month,
    EXTRACT(YEAR FROM event_date)::INT      AS event_year
FROM gdacs
UNION ALL
SELECT
    disaster_id, event_type, event_type_label, event_name,
    alert_level, alert_level_num, status, country, iso3,
    latitude, longitude, event_date, event_end_date,
    severity_value, severity_unit, population_affected,
    source_url, source_tag, ingested_at,
    CASE WHEN status IN ('ongoing','open') THEN TRUE ELSE FALSE END AS is_active,
    DATE_TRUNC('day',   event_date)::DATE   AS event_day,
    DATE_TRUNC('month', event_date)::DATE   AS event_month,
    EXTRACT(YEAR FROM event_date)::INT      AS event_year
FROM eonet
ORDER BY event_date DESC NULLS LAST