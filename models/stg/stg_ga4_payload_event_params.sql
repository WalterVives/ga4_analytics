{{ 
    config(
        materialized = 'view'
        )
}}

WITH source AS (

    SELECT *
    FROM {{ source('raw', 'ga4_payload') }}

),

flattened_params AS (

    SELECT
        RAW:user_pseudo_id::string AS user_pseudo_id,
        CAST(RAW:event_timestamp AS number) AS event_timestamp,
        CAST(RAW:event_date AS string) AS event_date,
        RAW:event_name::string AS event_name,

        -- Flatten de event_params
        param.value:key::string AS param_key,
        COALESCE(
            param.value:value.string_value,
            CAST(param.value:value.int_value AS string),
            CAST(param.value:value.float_value AS string),
            CAST(param.value:value.double_value AS string)
        ) AS param_value

    FROM source,
         LATERAL FLATTEN(input => RAW:event_params) AS param

)

SELECT * FROM flattened_params