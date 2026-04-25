-- depends_on: {{ ref('mart_disasters') }}
{{
  config(
    materialized='table',
    schema='marts'
  )
}}

/*
  Aggregated view per country — used for choropleth map in Streamlit dashboard.
*/

with base as (
    select * from {{ ref('mart_disasters') }}
    where country is not null
      and country != ''
),

agg as (
    select
        country,
        iso3,
        count(*)                                            as total_disasters,
        count(*) filter (where status = 'ongoing')          as ongoing_count,
        count(*) filter (where event_type = 'EQ')           as earthquake_count,
        count(*) filter (where event_type = 'FL')           as flood_count,
        count(*) filter (where event_type = 'TC')           as cyclone_count,
        count(*) filter (where event_type = 'DR')           as drought_count,
        count(*) filter (where event_type = 'VO')           as volcano_count,
        count(*) filter (where event_type = 'WF')           as wildfire_count,
        sum(population_affected)                            as total_population_affected,
        max(alert_level_num)                                as max_alert_level,
        max(event_date)                                     as latest_event_date,
        min(event_date)                                     as earliest_event_date
    from base
    group by country, iso3
)

select * from agg
order by total_disasters desc