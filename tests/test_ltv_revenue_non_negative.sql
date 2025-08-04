{{ config(materialized='test') }}

select *
from {{ ref('fact_user_ltv') }}
where ltv_revenue < 0