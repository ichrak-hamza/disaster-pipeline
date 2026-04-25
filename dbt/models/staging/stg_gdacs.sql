{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with source as (
    select * FROM {{ source('disasters_raw', 'raw_gdacs') }}
),

cleaned as (
    select
        -- identifiers
        event_id::integer                               as event_id,
        episode_id::integer                             as episode_id,
        glide                                           as glide,

        -- event classification
        upper(trim(event_type))                         as event_type,
        {{ map_event_type('upper(trim(event_type))') }} as event_type_label,
        trim(event_name)                                as event_name,
        upper(trim(alert_level))                        as alert_level,
        {{ map_alert_level('upper(trim(alert_level))') }} as alert_level_num,

        -- geography
        trim(country)                                   as country,
        upper(trim(iso3))                               as iso3,
        latitude::double precision                      as latitude,
        longitude::double precision                     as longitude,

        -- dates
        case
            when from_date is not null and from_date != ''
            then from_date::timestamp with time zone
            else null
        end                                             as from_date,
        case
            when to_date is not null and to_date != ''
            then to_date::timestamp with time zone
            else null
        end                                             as to_date,

        -- severity
        case
            when severity_value is not null and severity_value != ''
            then severity_value::double precision
            else null
        end                                             as severity_value,
        trim(severity_unit)                             as severity_unit,

        -- impact
        case
            when population_affected is not null and population_affected != ''
            then population_affected::bigint
            else null
        end                                             as population_affected,

        -- metadata
        url                                             as source_url,
        ingested_at::timestamp with time zone           as ingested_at,
        'gdacs'                                         as source_system

    from source
    where event_id is not null
      and event_type is not null
)

select * from cleaned