{{
  config(
    materialized='view'
  )
}}

with source as (

    select * from {{ ref('stg_ga4_payload_event_params') }}

),

pivoted as (

    select
        user_pseudo_id,
        event_timestamp,
        event_name,
        MAX(CASE WHEN param_key = 'ga_session_id' THEN param_value END) as ga_session_id,
        MAX(CASE WHEN param_key = 'page_location' THEN param_value END) as page_location,
        MAX(CASE WHEN param_key = 'page_title' THEN param_value END) as page_title,
        MAX(CASE WHEN param_key = 'session_engaged' THEN param_value END) as session_engaged,
        MAX(CASE WHEN param_key = 'ga_session_number' THEN param_value END) as ga_session_number,
        MAX(CASE WHEN param_key = 'value' THEN param_value END) as event_value,
        MAX(CASE WHEN param_key = 'currency' THEN param_value END) as event_currency
        
    from source
    group by 1, 2, 3
)

select * from pivoted