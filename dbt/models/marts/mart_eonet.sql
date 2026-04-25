-- models/marts/mart_eonet.sql
-- =============================================================================
-- SILVER MART — NASA EONET events, cleaned and typed.
-- Reads from raw_eonet (loaded by bronze_to_silver DAG).
-- =============================================================================

{{
  config(materialized='table')
}}

SELECT
    eonet_id,
    title,
    description,
    link                                        AS source_url,
    status,
    closed,
    category_id,
    category_title,
    source_id,
    geometry_type,

    -- Safe numeric cast
    CASE
        WHEN latitude  ~ '^-?[0-9]+(\.[0-9]+)?$' THEN latitude::NUMERIC
        ELSE NULL
    END AS latitude,
    CASE
        WHEN longitude ~ '^-?[0-9]+(\.[0-9]+)?$' THEN longitude::NUMERIC
        ELSE NULL
    END AS longitude,

    -- Safe date cast
    CASE
        WHEN event_date ~ '^\d{4}-\d{2}-\d{2}' THEN event_date::TIMESTAMP WITH TIME ZONE
        ELSE NULL
    END AS event_date,

    ingested_at

FROM {{ source('disasters_raw', 'raw_eonet') }}
WHERE eonet_id IS NOT NULL