{{ config(materialized = 'view') }}

with base as (

    select
        user_pseudo_id,
        event_name,
        event_date,
        event_timestamp,
        platform,

        -- Device fields (for surrogate key)
        device_category,
        device_os,
        device_os_version,
        device_language,
        device_brand,
        device_model,
        browser,
        browser_version,

        -- Geo fields
        geo_city,
        geo_country,
        geo_continent,
        geo_region,
        geo_subcontinent,

        -- Surrogate key para device_id
        {{ dbt_utils.generate_surrogate_key([
            'device_category',
            'device_os',
            'device_os_version',
            'device_language',
            'device_brand',
            'device_model',
            'browser',
            'browser_version'
        ]) }} as device_id

    from {{ ref('stg_ga4_payload') }}
),

with_device as (

    select
        b.*,
        d.device_id as validated_device_id
    from base b
    left join {{ ref('dim_device') }} d
        on b.device_id = d.device_id

),

with_geo as (

    select
        wd.*,
        g.geo_id
    from with_device wd
    left join {{ ref('dim_geo') }} g
        on wd.geo_city = g.geo_city
        and wd.geo_country = g.geo_country
        and wd.geo_continent = g.geo_continent
        and wd.geo_region = g.geo_region
        and wd.geo_subcontinent = g.geo_subcontinent

),

final as (

    select
        user_pseudo_id,
        event_name,
        event_date,
        to_timestamp_ntz(event_timestamp / 1000000) as event_time,
        platform,
        validated_device_id as device_id,
        geo_id
    from with_geo

)

select * from final