-- depends_on: {{ ref('mart_disasters') }}
{{
  config(
    materialized='table',
    schema='marts'
  )
}}

/*
  Daily disaster counts by type — used for time series chart in dashboard.
*/

with base as (
    select * from {{ ref('mart_disasters') }}
    where event_date is not null
),

daily as (
    select
        date_trunc('day', event_date)::date     as event_day,
        event_type,
        event_type_label,
        count(*)                                as disaster_count,
        sum(population_affected)                as population_affected,
        count(*) filter (where alert_level = 'RED')    as red_alerts,
        count(*) filter (where alert_level = 'ORANGE') as orange_alerts,
        count(*) filter (where alert_level = 'GREEN')  as green_alerts
    from base
    group by 1, 2, 3
),

-- Also total per day across all types
daily_total as (
    select
        date_trunc('day', event_date)::date     as event_day,
        'ALL'                                   as event_type,
        'All Disasters'                         as event_type_label,
        count(*)                                as disaster_count,
        sum(population_affected)                as population_affected,
        count(*) filter (where alert_level = 'RED')    as red_alerts,
        count(*) filter (where alert_level = 'ORANGE') as orange_alerts,
        count(*) filter (where alert_level = 'GREEN')  as green_alerts
    from base
    group by 1
)

select * from daily
union all
select * from daily_total
order by event_day desc, event_type