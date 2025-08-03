{{ config(materialized = 'view') }}

with base as (

    select
        *,
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

with_session as (

    select
        wg.*,
        s.session_id
    from with_geo wg
    left join {{ ref('dim_sessions') }} s
        on wg.user_pseudo_id = s.user_pseudo_id
        and wg.platform = s.platform
        and wg.stream_id = s.stream_id
        and wg.traffic_medium = s.traffic_medium
        and wg.traffic_source = s.traffic_source
        and wg.traffic_name = s.traffic_name

),

with_user as (

    select
        ws.*,
        u.user_id
    from with_session ws
    left join {{ ref('dim_users') }} u
        on ws.user_pseudo_id = u.user_pseudo_id
)

select
    user_id,
    event_name,
    event_date,
    to_timestamp_ntz(event_timestamp / 1000000) as event_time,
    platform,
    device_id,
    geo_id,
    session_id
from with_user