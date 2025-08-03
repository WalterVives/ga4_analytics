{{ config(materialized = 'view') }}

with base as (

    select
        user_pseudo_id,
        event_name,
        event_date,
        event_timestamp,
        platform,

        -- Device
        device_category,
        device_os,
        device_os_version,
        device_language,
        device_brand,
        device_model,
        browser,
        browser_version,

        -- Geo
        geo_city,
        geo_country,
        geo_continent,
        geo_region,
        geo_subcontinent,

        -- Surrogate Keys
        {{ dbt_utils.generate_surrogate_key([
            'device_category',
            'device_os',
            'device_os_version',
            'device_language',
            'device_brand',
            'device_model',
            'browser',
            'browser_version'
        ]) }} as _device_id,

        {{ dbt_utils.generate_surrogate_key([
            'geo_city',
            'geo_country',
            'geo_region',
            'geo_subcontinent',
            'geo_continent'
        ]) }} as _geo_id

    from {{ ref('stg_ga4_payload') }}

),

with_device as (

    select
        b.*,
        d.device_id
    from base b
    left join {{ ref('dim_device') }} d
        on b._device_id = d.device_id

),

with_geo as (

    select
        wd.*,
        g.geo_id
    from with_device wd
    left join {{ ref('dim_geo') }} g
        on wd._geo_id = g.geo_id

),

final as (

    select
        user_pseudo_id,
        event_name,
        event_date,
        to_timestamp_ntz(event_timestamp / 1000000) as event_time,
        platform,
        device_id,
        geo_id
    from with_geo

)

select 
    user_pseudo_id,
    device_id,
    geo_id,
    event_name,
    platform
    event_date,
    event_time
from
    final