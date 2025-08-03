{{ 
    config(
        materialized = 'view'
    )
}}

with source as (

    select *
    from {{ source('raw', 'ga4_payload') }}

),

flattened as (

    select
        -- Identificadores
        raw:user_pseudo_id::string as user_pseudo_id,
        cast(raw:event_timestamp as number) as event_timestamp,
        cast(raw:event_date as string) as event_date,
        raw:event_name::string as event_name,
        raw:platform::string as platform,
        raw:stream_id::number as stream_id,

        -- Device (normalizados)
        lower(trim(raw:device.category::string)) as device_category,
        lower(trim(raw:device.operating_system::string)) as device_os,
        lower(trim(raw:device.operating_system_version::string)) as device_os_version,
        lower(trim(raw:device.language::string)) as device_language,
        lower(trim(raw:device.mobile_brand_name::string)) as device_brand,
        lower(trim(raw:device.mobile_model_name::string)) as device_model,
        lower(trim(raw:web_info.browser::string)) as browser,
        lower(trim(raw:web_info.browser_version::string)) as browser_version,

        -- Geo (normalizados)
        lower(trim(raw:geo.city::string)) as geo_city,
        lower(trim(raw:geo.country::string)) as geo_country,
        lower(trim(raw:geo.continent::string)) as geo_continent,
        lower(trim(raw:geo.region::string)) as geo_region,
        lower(trim(raw:geo.sub_continent::string)) as geo_subcontinent,

        -- Traffic Source (normalizados)
        lower(trim(raw:traffic_source.medium::string)) as traffic_medium,
        lower(trim(raw:traffic_source.source::string)) as traffic_source,
        lower(trim(raw:traffic_source.name::string)) as traffic_name,

        -- LTV
        lower(trim(raw:user_ltv.currency::string)) as ltv_currency,
        cast(raw:user_ltv.revenue as float) as ltv_revenue

    from source
)

select * from flattened