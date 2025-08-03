-- models/fact/fact_events.sql

{{ config(materialized = 'view') }}

with base as (
    select
        raw:user_pseudo_id::string as user_pseudo_id,
        raw:event_name::string as event_name,
        raw:event_date::string as event_date,
        raw:event_timestamp::number as event_timestamp,
        raw:platform::string as platform,
        -- raw:stream_id::string as stream_id,
        raw:device.category::string as device_category,
        raw:device.operating_system::string as device_os,
        raw:device.operating_system_version::string as device_os_version,
        raw:device.language::string as device_language,
        raw:device.mobile_brand_name::string as device_brand,
        raw:device.mobile_model_name::string as device_model,
        raw:device.web_info.browser::string as browser,
        raw:device.web_info.browser_version::string as browser_version
    from {{ source('raw', 'ga4_payload') }}
),

with_device_id as (
    select
        b.*,
        d.device_id,
        to_timestamp_ntz(event_timestamp / 1000000) as event_time
    from base b
    left join {{ ref('dim_device') }} d
        on b.device_category = d.device_category
        and b.device_os = d.device_os
        and b.device_os_version = d.device_os_version
        and b.device_language = d.device_language
        and b.device_brand = d.device_brand
        and b.device_model = d.device_model
        and b.browser = d.browser
        and b.browser_version = d.browser_version
)

select * from with_device_id