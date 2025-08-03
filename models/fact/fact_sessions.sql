{{ 
    config(
        materialized = 'view'
        ) 
}}

with base as (

    select
        raw:user_pseudo_id::string as user_pseudo_id,
        raw:event_date::string as event_date,
        raw:event_name::string as event_name,
        raw:event_timestamp::number as event_timestamp,
        raw:geo.city::string as geo_city,
        raw:geo.country::string as geo_country,
        raw:geo.continent::string as geo_continent,
        raw:geo.region::string as geo_region,
        raw:geo.sub_continent::string as geo_subcontinent,
        param.value:key::string as param_key,
        coalesce(
            param.value:value.string_value,
            cast(param.value:value.int_value as string),
            cast(param.value:value.float_value as string),
            cast(param.value:value.double_value as string)
        ) as param_value
    from {{ source('raw', 'ga4_payload') }},
         lateral flatten(input => raw:event_params) as param

),

with_geo_id as (

    select
        b.*,
        g.geo_id
    from base b
    left join {{ ref('dim_geo') }} g
        on b.geo_city = g.geo_city
        and b.geo_country = g.geo_country
        and b.geo_continent = g.geo_continent
        and b.geo_region = g.geo_region
        and b.geo_subcontinent = g.geo_subcontinent

),

sessions as (

    select
        user_pseudo_id,
        event_date,
        param_value::number as ga_session_id,
        geo_id,
        count(distinct event_name) as total_event_types,
        count(*) as total_events,
        min(event_timestamp) as session_start_ts,
        max(event_timestamp) as session_end_ts,
        datediff('second', 
            to_timestamp_ntz(min(event_timestamp) / 1000000), 
            to_timestamp_ntz(max(event_timestamp) / 1000000)
        ) as session_duration_sec
    from with_geo_id
    where param_key = 'ga_session_id'
    group by 1, 2, 3, 4

)

select * from sessions