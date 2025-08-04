{{ 
    config(
        materialized = 'incremental',
        unique_key = 'user_id',
        on_schema_change = 'fail'
    ) 
}}

with latest_ltv as (
    -- 1. Seleccionamos el registro de LTV más reciente para cada user_pseudo_id de la tabla completa.
    select
        user_pseudo_id,
        ltv_currency,
        coalesce(ltv_revenue, 0) as ltv_revenue,
        event_timestamp
    from {{ ref('stg_ga4_payload') }}
    qualify row_number() over (partition by user_pseudo_id order by event_timestamp desc) = 1
),

{% if is_incremental() %}

-- 2. CTE incremental: si estamos en una ejecución incremental, unimos con la tabla existente
--    para identificar qué usuarios ya existen y cuáles son nuevos.
ltv_to_insert as (
    select
        l.user_pseudo_id,
        l.ltv_currency,
        l.ltv_revenue
    from latest_ltv l
    -- Unimos con la tabla de destino usando la clave user_id
    left join {{ ref('dim_users') }} u on l.user_pseudo_id = u.user_pseudo_id
    left join {{ this }} t on u.user_id = t.user_id
    where t.user_id is null -- Seleccionamos solo los usuarios que no existen en la tabla de destino
),

ltv_source as (
    select * from ltv_to_insert
),

{% else %}

-- 3. CTE inicial: si es la primera ejecución, usamos todos los datos de latest_ltv
ltv_source as (
    select
        user_pseudo_id,
        ltv_currency,
        ltv_revenue
    from latest_ltv
),

{% endif %}

final as (
    -- 4. Unimos con dim_users para obtener la clave user_id.
    select
        u.user_id,
        l.ltv_currency,
        l.ltv_revenue,
        current_timestamp() as last_updated_at
    from ltv_source l
    left join {{ ref('dim_users') }} u
        on l.user_pseudo_id = u.user_pseudo_id
)

select 
    -- 5. Seleccionamos la clave única final y las métricas
    user_id,
    ltv_currency,
    ltv_revenue,
    last_updated_at
from final