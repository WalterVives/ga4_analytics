{{ 
    config(
        materialized = 'table'
    )
}}

with latest_session_per_user as (

    select *
    from (
        select 
            user_id,
            traffic_medium,
            traffic_source,
            traffic_name,
            session_start_time,
            row_number() over (partition by user_id order by session_start_time desc) as rn
        from {{ ref('fact_sessions') }}
    )
    where rn = 1

),

ltv as (

    select 
        user_id,
        ltv_revenue,
        ltv_currency
    from {{ ref('fact_user_ltv') }}

)

select
    s.traffic_source,
    s.traffic_medium,
    s.traffic_name,
    count(distinct s.user_id) as total_users,
    count(distinct case when l.ltv_revenue > 0 then s.user_id end) as users_with_revenue,
    sum(coalesce(l.ltv_revenue, 0)) as total_ltv_revenue,
    avg(coalesce(l.ltv_revenue, 0)) as avg_ltv_per_user,
    max(l.ltv_currency) as ltv_currency  -- Se asume misma moneda por grupo

from latest_session_per_user s
left join ltv l on l.user_id = s.user_id

group by
    s.traffic_source,
    s.traffic_medium,
    s.traffic_name