with source as (

    select *
    from {{ source('raw', 'ga4_payload') }}

),

flattened_params as (

    select
        user_pseudo_id,
        cast(event_timestamp as number) as event_timestamp,
        cast(event_date as string) as event_date,
        event_name,
        
        -- Flatten event_params
        param.value:key::string as param_key,
        coalesce(
            param.value:value.string_value as string,
            cast(param.value:value.int_value as string),
            cast(param.value:value.float_value as string),
            cast(param.value:value.double_value as string)
        ) as param_value

    from source,
         lateral flatten(input => event_params) as param

)

select * from flattened_params