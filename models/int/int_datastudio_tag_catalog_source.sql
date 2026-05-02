{{ config(materialized='view') }}

with plants as (

    select
        id::bigint as power_plant_id,
        customer_id::bigint as customer_id,
        name as plant_name
    from public.power_plant
    where coalesce(is_active, true) = true

),

devices as (

    select
        d.id::bigint as device_id,
        d.power_plant_id::bigint as power_plant_id,
        p.customer_id::bigint as customer_id,
        d.device_type_id::bigint as device_type_id,
        lower(coalesce(dt.name, 'device'))::text as device_type,
        coalesce(
            nullif(trim(d.display_name), ''),
            nullif(trim(d.name), ''),
            ('DEVICE ' || d.id)
        )::text as display_name
    from public.device d
    join plants p
      on p.power_plant_id = d.power_plant_id
    left join public.device_type dt
      on dt.id = d.device_type_id
    where coalesce(d.is_active, true) = true

),

plant_static_tags as (

    select
        customer_id,
        power_plant_id,
        null::text as device_type,
        null::bigint as device_id,
        'PLANT'::text as context,
        point_name,
        pathname,
        description,
        data_kind,
        source,
        unit
    from plants
    cross join (
        values
            ('active_power_kw',          'PLANT.active_power_kw',          'Potência ativa da usina',                 'analog',   'historico',   'kW'),
            ('irradiance_ghi_wm2',       'PLANT.irradiance_ghi_wm2',       'Irradiância GHI',                         'analog',   'historico',   'W/m²'),
            ('irradiance_poa_wm2',       'PLANT.irradiance_poa_wm2',       'Irradiância POA',                         'analog',   'historico',   'W/m²'),
            ('inverter_count_ok',        'PLANT.inverter_count_ok',        'Quantidade de inversores OK',             'discrete', 'historico',   'count'),
            ('inverter_count_fault',     'PLANT.inverter_count_fault',     'Quantidade de inversores em falha',       'discrete', 'historico',   'count'),
            ('inverter_count_null',      'PLANT.inverter_count_null',      'Quantidade de inversores sem leitura',    'discrete', 'historico',   'count'),
            ('active_alarm_count',       'PLANT.active_alarm_count',       'Alarmes ativos da usina',                 'discrete', 'historico',   'count'),
            ('high_alarm_count',         'PLANT.high_alarm_count',         'Alarmes high ativos da usina',            'discrete', 'historico',   'count'),
            ('energy_real_kwh_daily',    'PLANT.energy_real_kwh_daily',    'Energia real diária',                     'analog',   'consolidado', 'kWh'),
            ('irradiance_kwh_m2_daily',  'PLANT.irradiance_kwh_m2_daily',  'Irradiância diária',                      'analog',   'consolidado', 'kWh/m²'),
            ('energy_theoretical_kwh_daily','PLANT.energy_theoretical_kwh_daily','Energia teórica diária',           'analog',   'consolidado', 'kWh'),
            ('performance_ratio_daily',  'PLANT.performance_ratio_daily',  'Performance Ratio diário',                'analog',   'consolidado', '%'),
            ('capacity_dc',              'PLANT.capacity_dc',              'Capacidade DC',                           'analog',   'consolidado', 'kWp'),
            ('energy_kwh_monthly',       'PLANT.energy_kwh_monthly',       'Energia mensal',                          'analog',   'consolidado', 'kWh')
    ) as t(point_name, pathname, description, data_kind, source, unit)

),

recent_data_tags as (

    select distinct on (
        customer_id,
        power_plant_id,
        pathname
    )
        customer_id::bigint,
        power_plant_id::bigint,
        device_type::text,
        device_id::bigint,
        context::text,
        point_name::text,
        pathname::text,
        description::text,
        data_kind::text,
        source::text,
        unit::text
    from {{ ref('mart_datastudio_timeseries') }}
    where pathname is not null
      and ts >= now() - interval '30 days'
    order by
        customer_id,
        power_plant_id,
        pathname,
        ts desc

),

device_alarm_catalog_tags as (

    select distinct
        d.customer_id,
        d.power_plant_id,
        d.device_type,
        d.device_id,
        d.display_name::text as context,
        ('alarm_' || c.code::text)::text as point_name,
        (
            case
                when d.device_type like '%inverter%' then 'INV_'
                when d.device_type like '%relay%' then 'RELAY_'
                when d.device_type like '%meter%' or d.device_type like '%multimeter%' then 'METER_'
                when d.device_type like '%weather%' then 'WEATHER_'
                else 'DEV_'
            end
            || d.device_id::text
            || '.alarm_'
            || c.code::text
        )::text as pathname,
        coalesce(
            nullif(trim(c.description_pt), ''),
            nullif(trim(c.description_en), ''),
            ('Alarme ' || c.code::text)
        )::text as description,
        'discrete'::text as data_kind,
        'historico'::text as source,
        'count'::text as unit
    from devices d
    join public.tb_event_alarm_catalog c
      on c.device_type_id = d.device_type_id
    where coalesce(c.is_active, true) = true
      and c.type_en = 'alarm'
      and c.value = 1

),

final as (

    select * from plant_static_tags

    union all

    select * from recent_data_tags

    union all

    select * from device_alarm_catalog_tags

)

select distinct on (
    customer_id,
    power_plant_id,
    pathname
)
    customer_id,
    power_plant_id,
    device_type,
    device_id,
    context,
    point_name,
    pathname,
    description,
    data_kind,
    source,
    unit
from final
where customer_id is not null
  and power_plant_id is not null
  and pathname is not null
order by
    customer_id,
    power_plant_id,
    pathname,
    case
        when source = 'historico' then 1
        when source = 'consolidado' then 2
        else 9
    end