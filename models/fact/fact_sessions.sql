{{ config(materialized = 'view') }}

with base as (

    select
        user_pseudo_id,
        event_date,
        event_timestamp,
        event_name,
        platform,
        stream_id,
        traffic_medium,
        traffic_source,
        traffic_name,
        to_timestamp_ntz(event_timestamp / 1000000) as event_time
    from {{ ref('stg_ga4_payload') }}

),

session_events as (

    select
        *,
        -- Detecta inicio de sesión
        case when event_name = 'session_start' then 1 else 0 end as is_session_start
    from base

),

sessions as (

    select
        user_pseudo_id,
        platform,
        stream_id,
        traffic_medium,
        traffic_source,
        traffic_name,
        min(event_date) as session_date,
        min(event_time) as session_start_time,
        count(*) as total_events,
        max(is_session_start) as has_session_start
    from session_events
    group by
        user_pseudo_id,
        platform,
        stream_id,
        traffic_medium,
        traffic_source,
        traffic_name
)

select * from sessions