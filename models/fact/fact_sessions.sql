{{ config(materialized = 'view') }}

with base as (

    -- Extraer eventos con su info y el ga_session_id
    select
        s.user_pseudo_id,
        s.event_timestamp,
        to_timestamp_ntz(s.event_timestamp / 1000000) as event_time,
        s.event_date,
        s.event_name,
        cast(s.param_value as number) as ga_session_id,

        p.platform,
        p.stream_id,
        p.traffic_medium,
        p.traffic_source,
        p.traffic_name
    from {{ ref('stg_ga4_payload_event_params') }} s
    inner join {{ ref('stg_ga4_payload') }} p
        on s.user_pseudo_id = p.user_pseudo_id
        and s.event_timestamp = p.event_timestamp
    where lower(s.param_key) = 'ga_session_id'

),

session_events as (

    select
        *,
        case when event_name = 'session_start' then 1 else 0 end as is_session_start
    from base

),

sessions as (

    select
        ga_session_id,
        user_pseudo_id,
        platform,
        stream_id,
        traffic_medium,
        traffic_source,
        traffic_name,
        min(event_date) as session_date,
        min(event_time) as session_start_time,
        max(event_time) as session_end_time,
        datediff(second, min(event_time), max(event_time)) as session_duration_seconds,
        count(*) as total_events,
        max(is_session_start) as has_session_start
    from session_events
    group by
        ga_session_id,
        user_pseudo_id,
        platform,
        stream_id,
        traffic_medium,
        traffic_source,
        traffic_name
)

select * from sessions