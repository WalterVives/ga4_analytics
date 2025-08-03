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
    from GA4_ANALYTICS.dev_waltervives.stg_ga4_payload

),

geo_with_id as (

    select
        {{ dbt_utils.surrogate_key([
            'geo_city',
            'geo_country',
            'geo_continent',
            'geo_region',
            'geo_subcontinent'
        ]) }} as geo_id,

        geo_city,
        geo_country,
        geo_continent,
        geo_region,
        geo_subcontinent

    from source

)

select * from geo_with_id