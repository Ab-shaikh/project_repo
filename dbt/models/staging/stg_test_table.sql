{{ config(
    materialized='incremental',
    unique_key='id'
) }}

SELECT
    id,
    name,
    amount,
    created_at
FROM {{ source('raw', 'test_table') }}
{% if is_incremental() %}
    WHERE created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}

