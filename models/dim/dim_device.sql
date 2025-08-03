{{ 
    config(
        materialized = 'view'
    ) 
}}

with source as (

    select distinct
        device_category,
        device_os,
        device_os_version,
        device_language,
        device_brand,
        device_model,
        browser,
        browser_version
    from {{ ref('stg_ga4_payload') }}

)

select 
    {{ dbt_utils.generate_surrogate_key([
        'device_category',
        'device_os',
        'device_os_version',
        'device_language',
        'device_brand',
        'device_model',
        'browser',
        'browser_version'
    ]) }} as device_id,
    *
from source