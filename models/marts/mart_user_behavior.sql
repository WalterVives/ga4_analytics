{{
    config(
        materialized='table'
    )
}}

with sessions as (
    select
        user_id,
        count(distinct session_id) as total_sessions,
        min(session_start_time) as first_session,
        max(session_end_time) as last_session,
        avg(session_duration_seconds) as avg_session_duration
    from {{ ref('fact_sessions') }}
    group by user_id
),

events as (
    select
        user_id,
        count(event_id) as total_events
    from {{ ref('fact_events') }}
    group by user_id
)

select
    u.user_id,
    s.total_sessions,
    e.total_events,
    datediff('day', s.first_session, s.last_session) as days_between_first_last_session,
    s.avg_session_duration
from sessions s
left join events e on s.user_id = e.user_id
left join {{ ref('dim_users') }} u on u.user_id = s.user_id