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
    from {{ source('public','raw_tracker') }} r
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

parsed as (

    select
        timestamp,
        power_plant_id,
        device_id,

        case
            when trim(json_data ->> 'posat') ~ '^-?\d+(\.\d+)?$'
                then (trim(json_data ->> 'posat'))::numeric
            else null
        end as posicao_atual,

        case
            when trim(json_data ->> 'posal') ~ '^-?\d+(\.\d+)?$'
                then (trim(json_data ->> 'posal'))::numeric
            else null
        end as posicao_alvo

    from base

)

select
    timestamp,
    power_plant_id,
    device_id,
    posicao_atual,
    posicao_alvo,

    (posicao_alvo - posicao_atual) as diferenca_posicao,
    abs(posicao_alvo - posicao_atual) as diferenca_posicao_abs

from parsed