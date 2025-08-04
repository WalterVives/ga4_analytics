{{ 
    config(
        materialized = 'table'
        ) 
}}

with unique_dates as (

    select distinct
        to_date(event_date, 'YYYYMMDD') as date_day
    from {{ ref('stg_ga4_payload') }}

),

final as (

    select
        date_day,
        year(date_day) as year,
        quarter(date_day) as quarter,
        month(date_day) as month,
        to_char(date_day, 'Month') as month_name,
        weekofyear(date_day) as week,
        dayofweek(date_day) as day_of_week,
        to_char(date_day, 'Day') as day_name,
        case when dayofweek(date_day) in (1, 7) then true else false end as is_weekend

    from unique_dates

)

select * from final