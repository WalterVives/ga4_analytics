{{ config(materialized = 'view') }}

-- 1. CTE para limpiar y seleccionar eventos de la tabla de staging
with stg_payload as (
    select
        user_pseudo_id,
        cast(RAW:event_timestamp as number) as event_timestamp,
        cast(RAW:event_date as string) as event_date,
        -- Generamos la clave de dispositivo
        {{ dbt_utils.generate_surrogate_key([
            'device.category', 'device.operating_system', 'device.operating_system_version', 'device.language', 
            'device.mobile_brand_name', 'device.mobile_model_name', 'web_info.browser', 'web_info.browser_version'
        ]) }} as _device_id,
        -- Extraemos los parámetros de sesión clave
        MAX(CASE WHEN param.value:key::string = 'ga_session_id' THEN CAST(param.value:value.int_value AS number) END) AS ga_session_id,
        MAX(CASE WHEN param.value:key::string = 'session_start' THEN 1 ELSE 0 END) AS is_session_start,
        raw:platform::string as platform,
        raw:stream_id::number as stream_id,
        raw:traffic_source.medium::string as traffic_medium,
        raw:traffic_source.source::string as traffic_source,
        raw:traffic_source.name::string as traffic_name
    from {{ source('raw', 'ga4_payload') }},
    LATERAL FLATTEN(input => RAW:event_params) AS param
    group by 1, 2, 3, 4, 11, 12, 13, 14
),

-- 2. CTE para agregar los datos a nivel de sesión
aggregated_sessions as (
    select
        user_pseudo_id,
        ga_session_id,
        _device_id,
        platform,
        stream_id,
        traffic_medium,
        traffic_source,
        traffic_name,
        min(event_date) as session_date,
        to_timestamp_ntz(min(event_timestamp) / 1000000) as session_start_time,
        to_timestamp_ntz(max(event_timestamp) / 1000000) as session_end_time,
        datediff('second', min(to_timestamp_ntz(event_timestamp / 1000000)), max(to_timestamp_ntz(event_timestamp / 1000000))) as session_duration_seconds,
        count(event_timestamp) as total_events,
        max(is_session_start) as has_session_start
    from stg_payload
    where ga_session_id is not null
    group by 1, 2, 3, 4, 5, 6, 7, 8
),

-- 3. CTE para unir con las dimensiones de usuario y dispositivo
with_dimensions as (
    select
        s.*,
        u.user_id,
        d.device_id
    from aggregated_sessions s
    left join {{ ref('dim_users') }} u
        on s.user_pseudo_id = u.user_pseudo_id
    left join {{ ref('dim_device') }} d
        on s._device_id = d.device_id
)

-- 4. Selección final de las columnas
select
    -- Usamos el ga_session_id como la clave primaria
    {{ dbt_utils.generate_surrogate_key(['ga_session_id', 'user_pseudo_id']) }} as session_id,
    ga_session_id,
    user_id,
    device_id,
    platform,
    stream_id,
    traffic_medium,
    traffic_source,
    traffic_name,
    session_date,
    session_start_time,
    session_end_time,
    session_duration_seconds,
    total_events,
    has_session_start
from with_dimensions