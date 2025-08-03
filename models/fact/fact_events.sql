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
),

-- Nuevo CTE para unir los parámetros del evento
with_params as (
    select
        wu.*,
        p.ga_session_id,
        p.page_location,
        p.page_title,
        p.session_engaged,
        p.ga_session_number,
        p.event_value,
        p.event_currency
    from with_user wu
    left join {{ ref('stg_ga4_event_params_pivoted') }} p
        on wu.user_pseudo_id = p.user_pseudo_id
        and wu.event_timestamp = p.event_timestamp
        and wu.event_name = p.event_name
)

select
    user_id,
    event_name,
    event_date,
    to_timestamp_ntz(event_timestamp / 1000000) as event_time,
    platform,
    device_id,
    geo_id,
    session_id,
    -- Nuevas columnas de parámetros
    ga_session_id,
    page_location,
    page_title,
    session_engaged,
    ga_session_number,
    event_value,
    event_currency
from with_params