-- depends_on: {{ ref('stg_gdacs') }}
-- models/marts/mart_disasters.sql
-- Silver mart: GDACS disasters cleaned and typed

{{ config(materialized='table') }}

SELECT
    CONCAT(event_id, '-', episode_id)           AS disaster_id,
    event_type,
    {{ map_event_type('upper(trim(event_type))') }}     AS event_type_label,
    event_name,
    alert_level,
    CASE
        WHEN UPPER(alert_level) = 'RED'    THEN 3
        WHEN UPPER(alert_level) = 'ORANGE' THEN 2
        WHEN UPPER(alert_level) = 'GREEN'  THEN 1
        ELSE 0
    END                                          AS alert_level_num,
    CASE
        WHEN to_date IS NULL THEN 'ongoing'
        ELSE 'past'
    END                                          AS status,
    country,
    iso3,
    latitude,
    longitude,
    from_date::TIMESTAMP WITH TIME ZONE          AS event_date,
    to_date::TIMESTAMP WITH TIME ZONE            AS event_end_date,
    severity_value,
    severity_unit,
    population_affected::BIGINT                  AS population_affected,
    NULL::TEXT                                   AS reliefweb_id,
    source_url                                   AS gdacs_url,
    NULL::TEXT                                   AS reliefweb_url,
    ingested_at

FROM {{ ref('stg_gdacs') }}
WHERE event_id IS NOT NULL