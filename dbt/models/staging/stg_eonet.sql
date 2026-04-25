-- models/staging/stg_eonet.sql
-- Cleans and standardizes raw NASA EONET data

{{ config(materialized='view') }}

SELECT
    eonet_id                                            AS event_id,
    title                                               AS event_name,
    status,
    category_id,
    category_title                                      AS event_type_label,
    source_url,
    CASE
        WHEN latitude  ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN latitude::NUMERIC ELSE NULL
    END                                                 AS latitude,
    CASE
        WHEN longitude ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN longitude::NUMERIC ELSE NULL
    END                                                 AS longitude,
    CASE
        WHEN event_date ~ '^\d{4}-\d{2}-\d{2}'
        THEN event_date::TIMESTAMP WITH TIME ZONE
        ELSE NULL
    END                                                 AS event_date,
    ingested_at

FROM {{ source('disasters_raw', 'raw_eonet') }}
WHERE eonet_id IS NOT NULL