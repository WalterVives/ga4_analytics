{{ config(materialized = 'view') }}

with base as (

    select
        s.user_pseudo_id,
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

deduplicated as (

    select distinct
        ga_session_id,
        user_pseudo_id,
        platform,
        stream_id,
        traffic_medium,
        traffic_source,
        traffic_name
    from base

)

select
    {{ dbt_utils.generate_surrogate_key(['ga_session_id', 'user_pseudo_id']) }} as session_id,
    *
from deduplicated