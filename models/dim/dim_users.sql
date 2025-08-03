{{ 
    config(
        materialized = 'view'
        )
}}

with source as (

    select * 
    from GA4_ANALYTICS.dev_waltervives.stg_ga4_payload

),

users as (

    select
        user_pseudo_id,

        -- Datos del dispositivo
        device_category,
        device_os,
        device_os_version,
        device_language,
        device_brand,
        device_model,
        browser,
        browser_version,

        -- Ubicación geográfica
        geo_city,
        geo_country,
        geo_continent,
        geo_region,
        geo_subcontinent,

        -- Fuente de tráfico
        traffic_medium,
        traffic_source,
        traffic_name,

        -- Lifetime value (LTV)
        ltv_currency,
        ltv_revenue,

        -- Primer día visto
        min(event_date) as first_seen_date,

        -- Último día visto
        max(event_date) as last_seen_date

    from source
    group by
        user_pseudo_id,
        device_category,
        device_os,
        device_os_version,
        device_language,
        device_brand,
        device_model,
        browser,
        browser_version,
        geo_city,
        geo_country,
        geo_continent,
        geo_region,
        geo_subcontinent,
        traffic_medium,
        traffic_source,
        traffic_name,
        ltv_currency,
        ltv_revenue
)

select * from users