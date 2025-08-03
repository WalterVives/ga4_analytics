{{ config(
    materialized = 'view'
) }}

with params as (

    select
        user_pseudo_id,
        cast(event_timestamp as bigint) as event_timestamp,
        cast(event_date as string) as event_date,
        event_name,
        param.value:key::string as param_key,
        coalesce(
            param.value:value.string_value,
            cast(param.value:value.int_value as string),
            cast(param.value:value.float_value as string),
            cast(param.value:value.double_value as string)
        ) as param_value
    from {{ source('raw', 'ga4_payload') }},
         lateral flatten(input => event_params) as param

),

sessions as (

    select
        user_pseudo_id,
        event_date,
        param_value::number as ga_session_id,
        count(distinct event_name) as total_event_types,
        count(*) as total_events,
        min(event_timestamp) as session_start_ts,
        max(event_timestamp) as session_end_ts,
        datediff('second', 
            to_timestamp_ntz(min(event_timestamp) / 1000000), 
            to_timestamp_ntz(max(event_timestamp) / 1000000)
        ) as session_duration_sec
    from params
    where param_key = 'ga_session_id'
    group by 1, 2, 3

)

select * from sessions