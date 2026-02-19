{{ config(
    materialized = 'incremental',
    unique_key = ['timestamp','device_id']
) }}

with base as (
    select
        r.timestamp,
        r.power_plant_id,
        r.device_id,
        r.json_data
    from {{ source('public','raw_transformer') }} r
    {% if is_incremental() %}
      where r.timestamp > (select max(timestamp) from {{ this }})
    {% endif %}
)

select
    timestamp,
    power_plant_id,
    device_id
from base
