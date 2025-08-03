{{ 
    config(
        materialized = 'view'
        )
}}

WITH source AS (

    SELECT *
    FROM {{ source('raw', 'ga4_payload') }}

),

flattened AS (

    SELECT
        -- Identificadores
        RAW:user_pseudo_id::string AS user_pseudo_id,
        CAST(RAW:event_timestamp AS number) AS event_timestamp,
        CAST(RAW:event_date AS string) AS event_date,
        RAW:event_name::string AS event_name,
        RAW:platform::string AS platform,
        RAW:stream_id::number AS stream_id,

        -- Device
        RAW:device.category::string AS device_category,
        RAW:device.operating_system::string AS device_os,
        RAW:device.operating_system_version::string AS device_os_version,
        RAW:device.language::string AS device_language,
        RAW:device.mobile_brand_name::string AS device_brand,
        RAW:device.mobile_model_name::string AS device_model,
        RAW:web_info.browser::string AS browser,
        RAW:web_info.browser_version::string AS browser_version,

        -- Geo
        RAW:geo.city::string AS geo_city,
        RAW:geo.country::string AS geo_country,
        RAW:geo.region::string AS geo_region,
        RAW:geo.sub_continent::string AS geo_subcontinent,

        -- Traffic
        RAW:traffic_source.medium::string AS traffic_medium,
        RAW:traffic_source.source::string AS traffic_source,
        RAW:traffic_source.name::string AS traffic_name,

        -- LTV
        RAW:user_ltv.currency::string AS ltv_currency,
        RAW:user_ltv.revenue::float AS ltv_revenue

    FROM source

)

SELECT * FROM flattened