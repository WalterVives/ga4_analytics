{{
    config(
        materialized = 'view'
        ) 
}}

with source as (

    select distinct
        geo_city,
        geo_country,
        geo_continent,
        geo_region,
        geo_subcontinent
    from {{ ref('stg_ga4_payload') }} -- GA4_ANALYTICS.dev_waltervives.stg_ga4_payload

),

geo_with_id as (

    select
        {{ dbt_utils.generate_surrogate_key([
                                    'geo_city',
                                    'geo_country',
                                    'geo_region',
                                    'geo_subcontinent',
                                    'geo_continent'
                                    ]) }} as geo_id,

        geo_city,
        geo_country,
        geo_continent,
        geo_region,
        geo_subcontinent

    from source

)

select * from geo_with_id