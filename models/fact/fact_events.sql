{{ config(materialized = 'view') }}

with base as (

    select
        sp.user_pseudo_id,
        sp.event_name,
        sp.event_date,
        sp.event_timestamp,
        to_timestamp_ntz(sp.event_timestamp / 1000000) as event_time,
        sp.platform,
        sp.device_category,
        sp.device_os,
        sp.device_os_version,
        sp.device_language,
        sp.device_brand,
        sp.device_model,
        sp.browser,
        sp.browser_version,
        sp.geo_city,
        sp.geo_country,
        sp.geo_continent,
        sp.geo_region,
        sp.geo_subcontinent
    from {{ ref('stg_ga4_payload') }} sp

),

with_ids as (

    select
        b.*,
        d.device_id,
        g.geo_id
    from base b
    left join {{ ref('dim_device') }} d
        on {{ dbt_utils.generate_surrogate_key([
            'b.device_category',
            'b.device_os',
            'b.device_os_version',
            'b.device_language',
            'b.device_brand',
            'b.device_model',
            'b.browser',
            'b.browser_version'
        ]) }} = d.device_id
    left join {{ ref('dim_geo') }} g
        on {{ dbt_utils.generate_surrogate_key([
            'b.geo_city',
            'b.geo_country',
            'b.geo_continent',
            'b.geo_region',
            'b.geo_subcontinent'
        ]) }} = g.geo_id
),

with_session_id as (

    select
        w.*,
        s.session_id
    from with_ids w
    left join {{ ref('dim_sessions') }} s
      on w.user_pseudo_id = s.user_pseudo_id
     and w.platform = s.platform
     and w.stream_id = s.stream_id
     and w.traffic_medium = s.traffic_medium
     and w.traffic_source = s.traffic_source
     and w.traffic_name = s.traffic_name
)

select
    user_pseudo_id,
    event_name,
    event_date,
    event_time,
    platform,
    device_id,
    geo_id,
    session_id
from with_session_id