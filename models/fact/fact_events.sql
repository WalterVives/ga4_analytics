{{ config(
    materialized = 'view'
) }}

with base as (

    select
        raw:user_pseudo_id::string as user_pseudo_id,
        raw:event_name::string as event_name,
        raw:event_date::string as event_date,
        raw:event_timestamp::number as event_timestamp,
        raw:platform::string as platform,
        raw:stream_id::string as stream_id
    from {{ source('raw', 'ga4_payload') }}

),

events as (

    select
        user_pseudo_id,
        event_name,
        event_date,
        event_timestamp,
        platform,
        stream_id,
        to_timestamp_ntz(event_timestamp / 1000000) as event_time
    from base

)

select * from events