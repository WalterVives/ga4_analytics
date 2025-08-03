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
        case when event_name = 'session_start' then 1 else 0 end as is_session_start
    from base

),

aggregated as (

    select
        user_pseudo_id,
        platform,
        stream_id,
        traffic_medium,
        traffic_source,
        traffic_name,
        min(event_date) as session_date,
        min(event_time) as session_start_time,
        max(event_time) as session_end_time,
        datediff('second', min(event_time), max(event_time)) as session_duration_seconds,
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
),

with_session_id as (

    select
        a.*,
        s.session_id
    from aggregated a
    left join {{ ref('dim_sessions') }} s
      on a.user_pseudo_id = s.user_pseudo_id
     and a.platform = s.platform
     and a.stream_id = s.stream_id
     and a.traffic_medium = s.traffic_medium
     and a.traffic_source = s.traffic_source
     and a.traffic_name = s.traffic_name
),

with_user as (

    select
        ws.*,
        u.user_id
    from with_session_id ws
    left join {{ ref('dim_users') }} u
        on ws.user_pseudo_id = u.user_pseudo_id
)

select
    session_id,
    user_id,
    session_date,
    session_start_time,
    session_end_time,
    session_duration_seconds,
    total_events,
    has_session_start
from with_user