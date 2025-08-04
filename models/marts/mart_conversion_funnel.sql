{{
    config(
        materialized = 'table'
    )
}}

with sessions as (
    select
        user_id,
        count(*) as total_sessions,

        -- Consideramos sesiones de más de 10s como engaged
        sum(case when session_duration_seconds > 10 then 1 else 0 end) as engaged_sessions

    from {{ ref('fact_sessions') }}
    group by user_id
),

events as (
    select
        user_id,
        count(*) as total_events,

        -- Eventos clave del funnel
        sum(case when event_name = 'add_to_cart' then 1 else 0 end) as add_to_cart_events,
        sum(case when event_name = 'begin_checkout' then 1 else 0 end) as checkout_events,
        sum(case when event_name = 'purchase' then 1 else 0 end) as purchase_events

    from {{ ref('fact_events') }}
    group by user_id
),

funnel as (
    select
        s.user_id,
        s.total_sessions,
        s.engaged_sessions,
        e.total_events,
        e.add_to_cart_events,
        e.checkout_events,
        e.purchase_events,

        -- Tasas del embudo
        case 
            when s.total_sessions = 0 then 0
            else round(e.add_to_cart_events::float / s.total_sessions, 4)
        end as add_to_cart_rate,

        case 
            when s.total_sessions = 0 then 0
            else round(e.checkout_events::float / s.total_sessions, 4)
        end as checkout_rate,

        case 
            when s.total_sessions = 0 then 0
            else round(e.purchase_events::float / s.total_sessions, 4)
        end as purchase_rate

    from sessions s
    left join events e on s.user_id = e.user_id
)

select
    u.user_id,
    f.total_sessions,
    f.engaged_sessions,
    f.total_events,
    f.add_to_cart_events,
    f.checkout_events,
    f.purchase_events,
    f.add_to_cart_rate,
    f.checkout_rate,
    f.purchase_rate
from funnel f
left join {{ ref('dim_users') }} u on f.user_id = u.user_id