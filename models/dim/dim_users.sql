{{ config(materialized = 'view') }}

with source as (

    select distinct
        user_pseudo_id
        -- ltv_currency,
        -- ltv_revenue
    from {{ ref('stg_ga4_payload') }}

),

with_id as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'user_pseudo_id'
        ]) }} as user_id,
        *
    from source

)

select
    user_id,
    user_pseudo_id
    -- ltv_currency,
    -- ltv_revenue
from with_id