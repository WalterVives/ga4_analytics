{{ config(materialized = 'view') }}

-- No necesitamos stg_payload CTE si lo unimos directamente en la siguiente CTE
-- with stg_payload as (
--     select *
--     from {{ ref('stg_ga4_payload') }}
-- ),

-- 1. CTE para unir los parámetros del evento
with with_params as (
    select
        s.*,
        p.ga_session_id,
        p.page_location,
        p.page_title,
        p.session_engaged,
        p.ga_session_number,
        p.event_value,
        p.event_currency
    from {{ ref('stg_ga4_payload') }} s
    left join {{ ref('stg_ga4_event_params_pivoted') }} p
        on s.user_pseudo_id = p.user_pseudo_id
        and s.event_timestamp = p.event_timestamp
        and s.event_name = p.event_name
),

-- 2. CTE para generar y unir las claves de dimensiones (device, geo, user)
with_dimension_keys as (

    select
        -- Seleccionamos explícitamente las columnas de `with_params` (aliased as p)
        p.user_pseudo_id,
        p.event_name,
        p.event_date,
        p.event_timestamp,
        p.platform,
        -- Atributos de dispositivo para generar la clave
        p.device_category,
        p.device_os,
        p.device_os_version,
        p.device_language,
        p.device_brand,
        p.device_model,
        p.browser,
        p.browser_version,
        -- Atributos de geo para generar la clave
        p.geo_city,
        p.geo_country,
        p.geo_region,
        p.geo_subcontinent,
        p.geo_continent,
        p.ga_session_id,
        p.page_location,
        p.page_title,
        p.session_engaged,
        p.ga_session_number,
        p.event_value,
        p.event_currency,
        
        -- Claves de dimensiones unidas
        d.device_id,
        g.geo_id,
        u.user_id,

        -- Clave de sesión
        {{ dbt_utils.generate_surrogate_key(['p.ga_session_id', 'p.user_pseudo_id']) }} as session_id

    from with_params p
    left join {{ ref('dim_device') }} d
        on {{ dbt_utils.generate_surrogate_key([
            'p.device_category',
            'p.device_os',
            'p.device_os_version',
            'p.device_language',
            'p.device_brand',
            'p.device_model',
            'p.browser',
            'p.browser_version'
        ]) }} = d.device_id
    left join {{ ref('dim_geo') }} g
        on {{ dbt_utils.generate_surrogate_key([
            'p.geo_city',
            'p.geo_country',
            'p.geo_region',
            'p.geo_subcontinent',
            'p.geo_continent'
        ]) }} = g.geo_id
    left join {{ ref('dim_users') }} u
        on p.user_pseudo_id = u.user_pseudo_id
)

select
    {{ dbt_utils.generate_surrogate_key(['event_name', 'event_timestamp', 'user_pseudo_id']) }} as event_id,
    user_id,
    event_name,
    event_date,
    to_timestamp_ntz(event_timestamp / 1000000) as event_time,
    platform,
    device_id,
    geo_id,
    session_id,
    ga_session_id,
    page_location,
    page_title,
    session_engaged,
    ga_session_number,
    event_value,
    event_currency
from with_dimension_keys