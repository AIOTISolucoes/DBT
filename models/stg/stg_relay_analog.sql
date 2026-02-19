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
    from {{ source('public','raw_relay') }} r

    {% if is_incremental() %}
      where r.timestamp > (select max(timestamp) from {{ this }})
    {% endif %}

)

select
    timestamp,
    power_plant_id,
    device_id,

    -- POTÊNCIAS
    (json_data ->> 'active_power')::numeric        as active_power_kw,
    (json_data ->> 'apparent_power')::numeric     as apparent_power_kva,
    (json_data ->> 'reactive_power')::numeric     as reactive_power_kvar,

    -- TENSÕES
    (json_data ->> 'voltage_ab')::numeric          as voltage_ab_v,
    (json_data ->> 'voltage_bc')::numeric          as voltage_bc_v,
    (json_data ->> 'voltage_ca')::numeric          as voltage_ca_v,

    -- CORRENTES
    (json_data ->> 'current_a')::numeric           as current_a_a,
    (json_data ->> 'current_b')::numeric           as current_b_a,
    (json_data ->> 'current_c')::numeric           as current_c_a

from base
