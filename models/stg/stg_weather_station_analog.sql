{{ config(
    materialized = 'incremental',
    unique_key = ['timestamp', 'power_plant_id', 'device_id'],
    on_schema_change = 'sync_all_columns'
) }}

with base as (

    select
        r.timestamp,
        r.power_plant_id,
        r.device_id,
        r.json_data::jsonb as payload
    from {{ source('public','raw_weather_station') }} r
    inner join public.device d
        on d.id = r.device_id
       and d.is_active is true

    {% if is_incremental() %}
    where r.timestamp >= (
        select coalesce(max(timestamp), '1970-01-01'::timestamptz) - interval '1 day'
        from {{ this }}
    )
    {% endif %}

)

select
    b.timestamp,
    b.power_plant_id,
    b.device_id,

    -- Irradiância
    case
        when nullif(btrim(b.payload ->> 'GHI_irradiance'), '') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
            then (btrim(b.payload ->> 'GHI_irradiance'))::numeric
        else null
    end as irradiance_ghi_wm2,

    case
        when nullif(btrim(b.payload ->> 'POA_irradiance'), '') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
            then (btrim(b.payload ->> 'POA_irradiance'))::numeric
        else null
    end as irradiance_poa_wm2,

    case
        when nullif(btrim(b.payload ->> 'POA_REAR_irradiance'), '') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
            then (btrim(b.payload ->> 'POA_REAR_irradiance'))::numeric
        else null
    end as irradiance_poa_rear_wm2,

    -- Acumulados de irradiância
    case
        when nullif(btrim(b.payload ->> 'GHI_irradiance_acc'), '') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
            then (btrim(b.payload ->> 'GHI_irradiance_acc'))::numeric
        else null
    end as irradiance_ghi_acc,

    case
        when nullif(btrim(b.payload ->> 'POA_irradiance_acc'), '') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
            then (btrim(b.payload ->> 'POA_irradiance_acc'))::numeric
        else null
    end as irradiance_poa_acc,

    -- Temperaturas
    case
        when nullif(btrim(b.payload ->> 'temp_ambiente'), '') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
            then (btrim(b.payload ->> 'temp_ambiente'))::numeric
        else null
    end as air_temperature_c,

    case
        when nullif(btrim(b.payload ->> 'temp_modulo'), '') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
            then (btrim(b.payload ->> 'temp_modulo'))::numeric
        else null
    end as module_temperature_c,

    -- Chuva
    case
        when nullif(btrim(b.payload ->> 'sensor_chuva'), '') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
            then (btrim(b.payload ->> 'sensor_chuva'))::numeric
        else null
    end as rain_signal,

    coalesce(
        case
            when nullif(btrim(b.payload ->> 'acumulador_pluv_hour'), '') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
                then (btrim(b.payload ->> 'acumulador_pluv_hour'))::numeric
            else null
        end,
        case
            when nullif(btrim(b.payload ->> 'hourly_accumulated_rain'), '') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
                then (btrim(b.payload ->> 'hourly_accumulated_rain'))::numeric
            else null
        end
    ) as hourly_accumulated_rain_mm,

    case
        when nullif(btrim(b.payload ->> 'acumulador_pluv_acc'), '') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
            then (btrim(b.payload ->> 'acumulador_pluv_acc'))::numeric
        else null
    end as accumulated_rain_mm,

    case
        when nullif(btrim(b.payload ->> 'acumulador_pluv_month'), '') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
            then (btrim(b.payload ->> 'acumulador_pluv_month'))::numeric
        else null
    end as monthly_accumulated_rain_mm,

    -- Vento
    coalesce(
        case
            when nullif(btrim(b.payload ->> 'vel_vento'), '') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
                then (btrim(b.payload ->> 'vel_vento'))::numeric
            else null
        end,
        case
            when nullif(btrim(b.payload ->> 'wind_speed'), '') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
                then (btrim(b.payload ->> 'wind_speed'))::numeric
            else null
        end
    ) as wind_speed,

    case
        when nullif(btrim(b.payload ->> 'wind_direction'), '') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
            then (btrim(b.payload ->> 'wind_direction'))::numeric
        else null
    end as wind_direction,

    -- Outros
    case
        when nullif(btrim(b.payload ->> 'volt_battery'), '') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
            then (btrim(b.payload ->> 'volt_battery'))::numeric
        else null
    end as volt_battery,

    case
        when nullif(btrim(b.payload ->> 'communication_fault'), '') ~ '^[+-]?\d+$'
            then (btrim(b.payload ->> 'communication_fault'))::integer
        else null
    end as communication_fault

from base b