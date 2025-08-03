{{ 
    config(
        materialized = 'view'
        )
}}

WITH source AS (
    SELECT 
        *
    FROM {{source('raw', 'ga4_payload')}}
),
flattened AS (
    SELECT
        -- Identificadores
        user_pseudo_id,
        CAST(event_timestamp AS number) AS event_timestamp,
        CAST(event_date AS string) AS event_date,
        event_name,
        platform,
        stream_id,
        -- Device
        device:category::string as device_category,
        device:operating_system::string as device_os,
        device:operating_system_version::string as devce_os_version,
        device:language::string as device_language,
        device:mobile_brand_name::string as device_brand,
        device:mobile_model_name::string as device_model,
        device:web_info.browser::string as browser,
        device:web_info.browser_version::string as browser_version,
        -- Geo
        geo:city::string as geo_city,
        geo:country::string as geo_country,
        geo:region::string as geo_region,
        geo:sub_continent::string as geo_subcontinent,
        -- Traffic
        traffic_source:medium::string as trafic_medium,
        traffic_source:source::string as traffic_source,
        traffic_source:name::string as traffic_name,
        -- LTV
        user_ltv:currency::string as ltv_currency,
        user_ltv:revenue::float as ltv_revenue
    FROM 
        source
        )
SELECT
    *
FROM 
    flattened