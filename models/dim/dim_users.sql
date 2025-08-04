{{
  config(
    materialized='table'
  )
}}

with source as (

    select distinct
        user_pseudo_id
    from {{ ref('stg_ga4_payload') }}

),

with_id as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'user_pseudo_id'
        ]) }} as user_id,
        user_pseudo_id
    from source

)

select
    user_id,
    user_pseudo_id
from with_id