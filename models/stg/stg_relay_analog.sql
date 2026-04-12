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
    inner join public.device d
        on d.id = r.device_id
       and d.is_active is true

    {% if is_incremental() %}
      where r.timestamp > (
        select coalesce(max(timestamp), '1970-01-01'::timestamptz)
        from {{ this }}
      )
    {% endif %}

),

cleaned as (

    select
        timestamp,
        power_plant_id,
        device_id,

        regexp_replace(trim(json_data ->> 'status_relay'), '^-+', '-', '')     as status_relay_txt,

        regexp_replace(trim(json_data ->> 'active_power'), '^-+', '-', '')      as active_power_txt,
        regexp_replace(trim(json_data ->> 'apparent_power'), '^-+', '-', '')    as apparent_power_txt,
        regexp_replace(trim(json_data ->> 'reactive_power'), '^-+', '-', '')    as reactive_power_txt,

        regexp_replace(trim(json_data ->> 'voltage_ab'), '^-+', '-', '')        as voltage_ab_txt,
        regexp_replace(trim(json_data ->> 'voltage_bc'), '^-+', '-', '')        as voltage_bc_txt,
        regexp_replace(trim(json_data ->> 'voltage_ca'), '^-+', '-', '')        as voltage_ca_txt,

        regexp_replace(trim(json_data ->> 'current_a'), '^-+', '-', '')         as current_a_txt,
        regexp_replace(trim(json_data ->> 'current_b'), '^-+', '-', '')         as current_b_txt,
        regexp_replace(trim(json_data ->> 'current_c'), '^-+', '-', '')         as current_c_txt

    from base

)

select
    timestamp,
    power_plant_id,
    device_id,

    -- STATUS
    case
        when nullif(status_relay_txt, '') is null then null
        when status_relay_txt ~ '^-?[0-9]+$' then status_relay_txt::integer
        else null
    end as status_relay,

    -- POTÊNCIAS
    case
        when nullif(active_power_txt, '') is null then null
        when active_power_txt ~ '^-?[0-9]+(\.[0-9]+)?$' then active_power_txt::numeric
        else null
    end as active_power_kw,

    case
        when nullif(apparent_power_txt, '') is null then null
        when apparent_power_txt ~ '^-?[0-9]+(\.[0-9]+)?$' then apparent_power_txt::numeric
        else null
    end as apparent_power_kva,

    case
        when nullif(reactive_power_txt, '') is null then null
        when reactive_power_txt ~ '^-?[0-9]+(\.[0-9]+)?$' then reactive_power_txt::numeric
        else null
    end as reactive_power_kvar,

    -- TENSÕES
    case
        when nullif(voltage_ab_txt, '') is null then null
        when voltage_ab_txt ~ '^-?[0-9]+(\.[0-9]+)?$' then voltage_ab_txt::numeric
        else null
    end as voltage_ab_v,

    case
        when nullif(voltage_bc_txt, '') is null then null
        when voltage_bc_txt ~ '^-?[0-9]+(\.[0-9]+)?$' then voltage_bc_txt::numeric
        else null
    end as voltage_bc_v,

    case
        when nullif(voltage_ca_txt, '') is null then null
        when voltage_ca_txt ~ '^-?[0-9]+(\.[0-9]+)?$' then voltage_ca_txt::numeric
        else null
    end as voltage_ca_v,

    -- CORRENTES
    case
        when nullif(current_a_txt, '') is null then null
        when current_a_txt ~ '^-?[0-9]+(\.[0-9]+)?$' then current_a_txt::numeric
        else null
    end as current_a_a,

    case
        when nullif(current_b_txt, '') is null then null
        when current_b_txt ~ '^-?[0-9]+(\.[0-9]+)?$' then current_b_txt::numeric
        else null
    end as current_b_a,

    case
        when nullif(current_c_txt, '') is null then null
        when current_c_txt ~ '^-?[0-9]+(\.[0-9]+)?$' then current_c_txt::numeric
        else null
    end as current_c_a

from cleaned