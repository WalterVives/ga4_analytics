{{ config(
    materialized = 'table'
) }}

with distinct_event_types as (

    select distinct
            event_name
    from {{ ref('stg_ga4_payload') }}

),

with_descriptions as (

    select
        event_name,
        
        -- Adding event description (Data enrichment)
        case event_name
            when 'page_view' then 'User viewed a page'
            when 'scroll' then 'User scrolled on a page'
            when 'click' then 'User clicked on an element'
            when 'purchase' then 'User completed a purchase'
            when 'login' then 'User logged in'
            when 'sign_up' then 'User signed up'
            else 'Uncategorized event'
        end as description

    from distinct_event_types

)

select * from with_descriptions