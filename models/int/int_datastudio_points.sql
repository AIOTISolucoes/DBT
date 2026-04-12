{{ config(
    materialized='view'
) }}

with plant_intraday as (

    select
        di.customer_id,
        di.power_plant_id,
        null::text as device_type,
        null::bigint as device_id,
        'PLANT'::text as context,
        'active_power_kw'::text as point_name,
        'PLANT.active_power_kw'::text as pathname,
        di.ts::timestamptz as ts,
        di.active_power_kw::double precision as value,
        'kW'::text as unit,
        'analog'::text as data_kind,
        'historico'::text as source,
        'Potência ativa da usina'::text as description
    from {{ ref('mart_datastudio_intraday') }} di
    where di.active_power_kw is not null

    union all

    select
        di.customer_id,
        di.power_plant_id,
        null::text,
        null::bigint,
        'PLANT',
        'irradiance_ghi_wm2',
        'PLANT.irradiance_ghi_wm2',
        di.ts::timestamptz,
        di.irradiance_ghi_wm2::double precision,
        'W/m²',
        'analog',
        'historico',
        'Irradiância GHI'
    from {{ ref('mart_datastudio_intraday') }} di
    where di.irradiance_ghi_wm2 is not null

    union all

    select
        di.customer_id,
        di.power_plant_id,
        null::text,
        null::bigint,
        'PLANT',
        'irradiance_poa_wm2',
        'PLANT.irradiance_poa_wm2',
        di.ts::timestamptz,
        di.irradiance_poa_wm2::double precision,
        'W/m²',
        'analog',
        'historico',
        'Irradiância POA'
    from {{ ref('mart_datastudio_intraday') }} di
    where di.irradiance_poa_wm2 is not null

    union all

    select
        di.customer_id,
        di.power_plant_id,
        null::text,
        null::bigint,
        'PLANT',
        'inverter_count_ok',
        'PLANT.inverter_count_ok',
        di.ts::timestamptz,
        di.inverter_count_ok::double precision,
        'count',
        'discrete',
        'historico',
        'Inversores OK (contagem)'
    from {{ ref('mart_datastudio_intraday') }} di
    where di.inverter_count_ok is not null

    union all

    select
        di.customer_id,
        di.power_plant_id,
        null::text,
        null::bigint,
        'PLANT',
        'inverter_count_fault',
        'PLANT.inverter_count_fault',
        di.ts::timestamptz,
        di.inverter_count_fault::double precision,
        'count',
        'discrete',
        'historico',
        'Inversores em falha (contagem)'
    from {{ ref('mart_datastudio_intraday') }} di
    where di.inverter_count_fault is not null

    union all

    select
        di.customer_id,
        di.power_plant_id,
        null::text,
        null::bigint,
        'PLANT',
        'inverter_count_null',
        'PLANT.inverter_count_null',
        di.ts::timestamptz,
        di.inverter_count_null::double precision,
        'count',
        'discrete',
        'historico',
        'Inversores sem dado (contagem)'
    from {{ ref('mart_datastudio_intraday') }} di
    where di.inverter_count_null is not null
),

plant_daily as (

    select
        dd.customer_id,
        dd.power_plant_id,
        null::text as device_type,
        null::bigint as device_id,
        'PLANT'::text as context,
        'energy_real_kwh_daily'::text as point_name,
        'PLANT.energy_real_kwh_daily'::text as pathname,
        dd.date::timestamptz as ts,
        dd.energy_real_kwh::double precision as value,
        'kWh'::text as unit,
        'analog'::text as data_kind,
        'consolidado'::text as source,
        'Energia real diária'::text as description
    from {{ ref('mart_datastudio_daily') }} dd
    where dd.energy_real_kwh is not null

    union all

    select
        dd.customer_id,
        dd.power_plant_id,
        null::text,
        null::bigint,
        'PLANT',
        'irradiance_kwh_m2_daily',
        'PLANT.irradiance_kwh_m2_daily',
        dd.date::timestamptz,
        dd.irradiance_kwh_m2::double precision,
        'kWh/m²',
        'analog',
        'consolidado',
        'Irradiância diária (kWh/m²)'
    from {{ ref('mart_datastudio_daily') }} dd
    where dd.irradiance_kwh_m2 is not null

    union all

    select
        dd.customer_id,
        dd.power_plant_id,
        null::text,
        null::bigint,
        'PLANT',
        'energy_theoretical_kwh_daily',
        'PLANT.energy_theoretical_kwh_daily',
        dd.date::timestamptz,
        dd.energy_theoretical_kwh::double precision,
        'kWh',
        'analog',
        'consolidado',
        'Energia teórica diária'
    from {{ ref('mart_datastudio_daily') }} dd
    where dd.energy_theoretical_kwh is not null

    union all

    select
        dd.customer_id,
        dd.power_plant_id,
        null::text,
        null::bigint,
        'PLANT',
        'performance_ratio_daily',
        'PLANT.performance_ratio_daily',
        dd.date::timestamptz,
        dd.performance_ratio::double precision,
        '%',
        'analog',
        'consolidado',
        'Performance Ratio diário'
    from {{ ref('mart_datastudio_daily') }} dd
    where dd.performance_ratio is not null

    union all

    select
        dd.customer_id,
        dd.power_plant_id,
        null::text,
        null::bigint,
        'PLANT',
        'capacity_dc',
        'PLANT.capacity_dc',
        dd.date::timestamptz,
        dd.capacity_dc::double precision,
        'kWp',
        'analog',
        'consolidado',
        'Capacidade DC (referência)'
    from {{ ref('mart_datastudio_daily') }} dd
    where dd.capacity_dc is not null
),

plant_monthly as (

    select
        dm.customer_id,
        dm.power_plant_id,
        null::text as device_type,
        null::bigint as device_id,
        'PLANT'::text as context,
        'energy_kwh_monthly'::text as point_name,
        'PLANT.energy_kwh_monthly'::text as pathname,
        dm.month::timestamptz as ts,
        dm.energy_kwh::double precision as value,
        'kWh'::text as unit,
        'analog'::text as data_kind,
        'consolidado'::text as source,
        'Energia mensal'::text as description
    from {{ ref('mart_datastudio_monthly') }} dm
    where dm.energy_kwh is not null
),

plant_alarm_points as (

    select
        p.customer_id,
        e.power_plant_id,
        null::text as device_type,
        null::bigint as device_id,
        'PLANT'::text as context,
        'active_alarm_count'::text as point_name,
        'PLANT.active_alarm_count'::text as pathname,
        date_trunc('hour', e."timestamp")::timestamptz as ts,
        sum(
            case
                when e.type_en = 'alarm'
                 and coalesce(e.is_active_event, false) = true
                then 1 else 0
            end
        )::double precision as value,
        'count'::text as unit,
        'discrete'::text as data_kind,
        'historico'::text as source,
        'Alarmes ativos da usina (contagem)'::text as description
    from rt.int_events_alarms e
    join public.power_plant p
      on p.id = e.power_plant_id
    where e.type_en = 'alarm'
    group by
        p.customer_id,
        e.power_plant_id,
        date_trunc('hour', e."timestamp")

    union all

    select
        p.customer_id,
        e.power_plant_id,
        null::text as device_type,
        null::bigint as device_id,
        'PLANT'::text as context,
        'high_alarm_count'::text as point_name,
        'PLANT.high_alarm_count'::text as pathname,
        date_trunc('hour', e."timestamp")::timestamptz as ts,
        sum(
            case
                when e.type_en = 'alarm'
                 and lower(coalesce(e.severity, '')) = 'high'
                 and coalesce(e.is_active_event, false) = true
                then 1 else 0
            end
        )::double precision as value,
        'count'::text as unit,
        'discrete'::text as data_kind,
        'historico'::text as source,
        'Alarmes high ativos da usina (contagem)'::text as description
    from rt.int_events_alarms e
    join public.power_plant p
      on p.id = e.power_plant_id
    where e.type_en = 'alarm'
    group by
        p.customer_id,
        e.power_plant_id,
        date_trunc('hour', e."timestamp")
),

plant_alarm_type_points as (

    select
        p.customer_id,
        e.power_plant_id,
        null::text as device_type,
        null::bigint as device_id,
        'PLANT'::text as context,
        ('alarm_' || e.code::text)::text as point_name,
        ('PLANT.alarm_' || e.code::text)::text as pathname,
        date_trunc('hour', e."timestamp")::timestamptz as ts,
        count(*)::double precision as value,
        'count'::text as unit,
        'discrete'::text as data_kind,
        'historico'::text as source,
        coalesce(
            nullif(trim(e.description_pt), ''),
            nullif(trim(e.description_en), ''),
            ('Alarme ' || e.code::text)
        )::text as description
    from rt.int_events_alarms e
    join public.power_plant p
      on p.id = e.power_plant_id
    where e.type_en = 'alarm'
      and e.code is not null
    group by
        p.customer_id,
        e.power_plant_id,
        e.code,
        coalesce(
            nullif(trim(e.description_pt), ''),
            nullif(trim(e.description_en), ''),
            ('Alarme ' || e.code::text)
        ),
        date_trunc('hour', e."timestamp")
),

inverter_device_map as (
    select
        d.id as device_id,
        d.power_plant_id,
        coalesce(nullif(trim(d.display_name), ''), d.name, ('INVERSOR ' || d.id)) as display_name
    from public.device d
    join public.device_type dt
      on dt.id = d.device_type_id
    where lower(coalesce(dt.name, '')) = 'inverter'
),

meter_device_map as (
    select
        d.id as device_id,
        d.power_plant_id,
        coalesce(nullif(trim(d.display_name), ''), d.name, ('MULTIMETER ' || d.id)) as display_name
    from public.device d
    join public.device_type dt
      on dt.id = d.device_type_id
    where lower(coalesce(dt.name, '')) like '%meter%'
       or lower(coalesce(dt.name, '')) like '%multimeter%'
),

inverter_base as (

    select
        pp.customer_id,
        ia.power_plant_id,
        pp.name as power_plant_name,
        ia.device_id,
        d.name as device_name,
        coalesce(dt.name, 'inverter')::text as device_type,
        coalesce(
            idm.display_name,
            ('INVERSOR ' || dense_rank() over (
                partition by ia.power_plant_id
                order by ia.device_id
            ))::text
        ) as context,
        ia.timestamp::timestamptz as ts,
        ia.active_power_kw,
        ia.power_reactive_kvar,
        ia.apparent_power_kva,
        ia.power_input_kw,
        ia.power_factor,
        ia.daily_active_energy_kwh,
        ia.cumulative_active_energy_kwh,
        ia.power_dc_kw,
        ia.frequency_hz,
        ia.efficiency_pct,
        ia.current_phase_a_a,
        ia.current_phase_b_a,
        ia.current_phase_c_a,
        ia.line_voltage_ab_v,
        ia.line_voltage_bc_v,
        ia.line_voltage_ca_v,
        ia.string_voltage_v,
        ia.temperature_internal_c,
        ia.resistance_insulation_mohm,
        case
            when coalesce(ia.active_power_kw, 0) > 0.1 then true
            else false
        end as is_generating,
        ia.state_operation,
        ia.working_status,
        null::double precision as inverter_status,
        null::double precision as communication_fault,
        null::double precision as communication_fault_code,
        null::boolean as is_communication_ok
    from rt.stg_inverter_analog ia
    join public.power_plant pp
      on pp.id = ia.power_plant_id
    left join public.device d
      on d.id = ia.device_id
    left join public.device_type dt
      on dt.id = d.device_type_id
    left join inverter_device_map idm
      on idm.device_id = ia.device_id
),

inverter_points as (

    select customer_id, power_plant_id, device_type, device_id, context,
           'active_power'::text as point_name,
           ('INV_' || device_id || '.active_power')::text as pathname,
           ts, active_power_kw::double precision as value,
           'kW'::text as unit, 'analog'::text as data_kind, 'historico'::text as source,
           'Potência ativa do inversor'::text as description
    from inverter_base where active_power_kw is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'reactive_power', ('INV_' || device_id || '.reactive_power'),
           ts, power_reactive_kvar::double precision,
           'kvar', 'analog', 'historico',
           'Potência reativa do inversor'
    from inverter_base where power_reactive_kvar is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'apparent_power', ('INV_' || device_id || '.apparent_power'),
           ts, apparent_power_kva::double precision,
           'kVA', 'analog', 'historico',
           'Potência aparente do inversor'
    from inverter_base where apparent_power_kva is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'power_input', ('INV_' || device_id || '.power_input'),
           ts, power_input_kw::double precision,
           'kW', 'analog', 'historico',
           'Potência de entrada do inversor'
    from inverter_base where power_input_kw is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'power_factor', ('INV_' || device_id || '.power_factor'),
           ts, power_factor::double precision,
           'pu', 'analog', 'historico',
           'Fator de potência do inversor'
    from inverter_base where power_factor is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'daily_energy', ('INV_' || device_id || '.daily_energy'),
           ts, daily_active_energy_kwh::double precision,
           'kWh', 'analog', 'historico',
           'Energia diária do inversor'
    from inverter_base where daily_active_energy_kwh is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'cumulative_energy', ('INV_' || device_id || '.cumulative_energy'),
           ts, cumulative_active_energy_kwh::double precision,
           'kWh', 'analog', 'historico',
           'Energia acumulada do inversor'
    from inverter_base where cumulative_active_energy_kwh is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'power_dc', ('INV_' || device_id || '.power_dc'),
           ts, power_dc_kw::double precision,
           'kW', 'analog', 'historico',
           'Potência DC do inversor'
    from inverter_base where power_dc_kw is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'frequency', ('INV_' || device_id || '.frequency'),
           ts, frequency_hz::double precision,
           'Hz', 'analog', 'historico',
           'Frequência do inversor'
    from inverter_base where frequency_hz is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'efficiency', ('INV_' || device_id || '.efficiency'),
           ts, efficiency_pct::double precision,
           '%', 'analog', 'historico',
           'Eficiência do inversor'
    from inverter_base where efficiency_pct is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'current_a', ('INV_' || device_id || '.current_a'),
           ts, current_phase_a_a::double precision,
           'A', 'analog', 'historico',
           'Corrente fase A do inversor'
    from inverter_base where current_phase_a_a is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'current_b', ('INV_' || device_id || '.current_b'),
           ts, current_phase_b_a::double precision,
           'A', 'analog', 'historico',
           'Corrente fase B do inversor'
    from inverter_base where current_phase_b_a is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'current_c', ('INV_' || device_id || '.current_c'),
           ts, current_phase_c_a::double precision,
           'A', 'analog', 'historico',
           'Corrente fase C do inversor'
    from inverter_base where current_phase_c_a is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'voltage_ab', ('INV_' || device_id || '.voltage_ab'),
           ts, line_voltage_ab_v::double precision,
           'V', 'analog', 'historico',
           'Tensão AB do inversor'
    from inverter_base where line_voltage_ab_v is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'voltage_bc', ('INV_' || device_id || '.voltage_bc'),
           ts, line_voltage_bc_v::double precision,
           'V', 'analog', 'historico',
           'Tensão BC do inversor'
    from inverter_base where line_voltage_bc_v is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'voltage_ca', ('INV_' || device_id || '.voltage_ca'),
           ts, line_voltage_ca_v::double precision,
           'V', 'analog', 'historico',
           'Tensão CA do inversor'
    from inverter_base where line_voltage_ca_v is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'string_voltage', ('INV_' || device_id || '.string_voltage'),
           ts, string_voltage_v::double precision,
           'V', 'analog', 'historico',
           'Tensão string do inversor'
    from inverter_base where string_voltage_v is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'temperature_internal', ('INV_' || device_id || '.temperature_internal'),
           ts, temperature_internal_c::double precision,
           '°C', 'analog', 'historico',
           'Temperatura interna do inversor'
    from inverter_base where temperature_internal_c is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'resistance_insulation', ('INV_' || device_id || '.resistance_insulation'),
           ts, resistance_insulation_mohm::double precision,
           'Mohm', 'analog', 'historico',
           'Resistência de isolação do inversor'
    from inverter_base where resistance_insulation_mohm is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'is_generating', ('INV_' || device_id || '.is_generating'),
           ts,
           case
               when lower(trim(coalesce(is_generating::text, '0'))) in ('1', 'true', 't') then 1
               else 0
           end::double precision,
           'bool', 'discrete', 'historico',
           'Inversor gerando'
    from inverter_base where is_generating is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'state_operation', ('INV_' || device_id || '.state_operation'),
           ts, state_operation::double precision,
           'code', 'discrete', 'historico',
           'Código de operação do inversor'
    from inverter_base where state_operation is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'working_status', ('INV_' || device_id || '.working_status'),
           ts, working_status::double precision,
           'code', 'discrete', 'historico',
           'Working status do inversor'
    from inverter_base where working_status is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'inverter_status', ('INV_' || device_id || '.inverter_status'),
           ts, inverter_status::double precision,
           'code', 'discrete', 'historico',
           'Status do inversor'
    from inverter_base where inverter_status is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'communication_fault', ('INV_' || device_id || '.communication_fault'),
           ts, communication_fault::double precision,
           'code', 'discrete', 'historico',
           'Falha de comunicação do inversor'
    from inverter_base where communication_fault is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'communication_fault_code', ('INV_' || device_id || '.communication_fault_code'),
           ts, communication_fault_code::double precision,
           'code', 'discrete', 'historico',
           'Código de falha de comunicação do inversor'
    from inverter_base where communication_fault_code is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'is_communication_ok', ('INV_' || device_id || '.is_communication_ok'),
           ts,
           case
               when lower(trim(coalesce(is_communication_ok::text, '0'))) in ('1', 'true', 't') then 1
               else 0
           end::double precision,
           'bool', 'discrete', 'historico',
           'Comunicação OK do inversor'
    from inverter_base where is_communication_ok is not null
),

inverter_alarm_base as (

    select
        p.customer_id,
        e.power_plant_id,
        e.device_id,
        coalesce(dt.name, 'inverter')::text as device_type,
        coalesce(
            nullif(trim(d.display_name), ''),
            ('INVERSOR ' || dense_rank() over (
                partition by e.power_plant_id
                order by e.device_id
            ))::text
        ) as context,
        date_trunc('hour', e."timestamp")::timestamptz as ts,
        e.type_en,
        lower(coalesce(e.severity, '')) as severity,
        coalesce(e.is_active_event, false) as is_active_event,
        e.code,
        e.description_pt,
        e.description_en
    from rt.int_events_alarms e
    join public.power_plant p
      on p.id = e.power_plant_id
    left join public.device d
      on d.id = e.device_id
    left join public.device_type dt
      on dt.id = d.device_type_id
    where e.type_en = 'alarm'
      and e.device_id is not null
      and lower(coalesce(dt.name, '')) = 'inverter'
),

inverter_alarm_points as (

    select
        customer_id,
        power_plant_id,
        device_type,
        device_id,
        context,
        'active_alarm_count'::text as point_name,
        ('INV_' || device_id || '.active_alarm_count')::text as pathname,
        ts,
        sum(case when is_active_event = true then 1 else 0 end)::double precision as value,
        'count'::text as unit,
        'discrete'::text as data_kind,
        'historico'::text as source,
        'Alarmes ativos do inversor (contagem)'::text as description
    from inverter_alarm_base
    group by customer_id, power_plant_id, device_type, device_id, context, ts

    union all

    select
        customer_id,
        power_plant_id,
        device_type,
        device_id,
        context,
        'high_alarm_count'::text as point_name,
        ('INV_' || device_id || '.high_alarm_count')::text as pathname,
        ts,
        sum(case when is_active_event = true and severity = 'high' then 1 else 0 end)::double precision as value,
        'count'::text as unit,
        'discrete'::text as data_kind,
        'historico'::text as source,
        'Alarmes high ativos do inversor (contagem)'::text as description
    from inverter_alarm_base
    group by customer_id, power_plant_id, device_type, device_id, context, ts
),

inverter_alarm_type_points as (

    select
        customer_id,
        power_plant_id,
        device_type,
        device_id,
        context,
        ('alarm_' || code::text)::text as point_name,
        ('INV_' || device_id || '.alarm_' || code::text)::text as pathname,
        ts,
        count(*)::double precision as value,
        'count'::text as unit,
        'discrete'::text as data_kind,
        'historico'::text as source,
        coalesce(
            nullif(trim(description_pt), ''),
            nullif(trim(description_en), ''),
            ('Alarme ' || code::text)
        )::text as description
    from inverter_alarm_base
    where code is not null
    group by
        customer_id,
        power_plant_id,
        device_type,
        device_id,
        context,
        code,
        coalesce(
            nullif(trim(description_pt), ''),
            nullif(trim(description_en), ''),
            ('Alarme ' || code::text)
        ),
        ts
),

weather_base as (

    select
        pp.customer_id,
        wa.power_plant_id,
        pp.name as power_plant_name,
        wa.device_id,
        d.name as device_name,
        coalesce(dt.name, 'weather_station')::text as device_type,
        'WEATHER 1'::text as context,
        wa.timestamp::timestamptz as ts,
        wa.irradiance_ghi_wm2,
        wa.irradiance_poa_wm2,
        wa.air_temperature_c,
        wa.module_temperature_c,
        wa.rain_signal,
        wa.accumulated_rain_mm,
        wa.hourly_accumulated_rain_mm
    from rt.stg_weather_station_analog wa
    join public.power_plant pp
      on pp.id = wa.power_plant_id
    left join public.device d
      on d.id = wa.device_id
    left join public.device_type dt
      on dt.id = d.device_type_id
),

weather_points as (

    select customer_id, power_plant_id, device_type, device_id, context,
           'irradiance_ghi'::text as point_name,
           ('WEATHER_' || device_id || '.irradiance_ghi')::text as pathname,
           ts, irradiance_ghi_wm2::double precision as value,
           'W/m²'::text as unit, 'analog'::text as data_kind, 'historico'::text as source,
           'Irradiância GHI da estação'::text as description
    from weather_base where irradiance_ghi_wm2 is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'irradiance_poa', ('WEATHER_' || device_id || '.irradiance_poa'),
           ts, irradiance_poa_wm2::double precision,
           'W/m²', 'analog', 'historico',
           'Irradiância POA da estação'
    from weather_base where irradiance_poa_wm2 is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'air_temperature', ('WEATHER_' || device_id || '.air_temperature'),
           ts, air_temperature_c::double precision,
           '°C', 'analog', 'historico',
           'Temperatura do ar'
    from weather_base where air_temperature_c is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'module_temperature', ('WEATHER_' || device_id || '.module_temperature'),
           ts, module_temperature_c::double precision,
           '°C', 'analog', 'historico',
           'Temperatura do módulo'
    from weather_base where module_temperature_c is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'rain_signal', ('WEATHER_' || device_id || '.rain_signal'),
           ts, rain_signal::double precision,
           'bool', 'discrete', 'historico',
           'Sinal de chuva'
    from weather_base where rain_signal is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'accumulated_rain', ('WEATHER_' || device_id || '.accumulated_rain'),
           ts, accumulated_rain_mm::double precision,
           'mm', 'analog', 'historico',
           'Chuva acumulada'
    from weather_base where accumulated_rain_mm is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'hourly_accumulated_rain', ('WEATHER_' || device_id || '.hourly_accumulated_rain'),
           ts, hourly_accumulated_rain_mm::double precision,
           'mm', 'analog', 'historico',
           'Chuva acumulada por hora'
    from weather_base where hourly_accumulated_rain_mm is not null
),

relay_base as (

    select
        pp.customer_id,
        ra.power_plant_id,
        d.name as device_name,
        coalesce(dv.name, 'relay')::text as device_type,
        ra.device_id,
        'RELAY 1'::text as context,
        ra.timestamp::timestamptz as ts,
        ra.active_power_kw,
        ra.apparent_power_kva,
        ra.reactive_power_kvar,
        ra.voltage_ab_v,
        ra.voltage_bc_v,
        ra.voltage_ca_v,
        ra.current_a_a,
        ra.current_b_a,
        ra.current_c_a
    from rt.stg_relay_analog ra
    join public.power_plant pp
      on pp.id = ra.power_plant_id
    left join public.device d
      on d.id = ra.device_id
    left join public.device_type dv
      on dv.id = d.device_type_id
),

relay_points as (

    select customer_id, power_plant_id, device_type, device_id, context,
           'active_power'::text as point_name,
           ('RELAY_' || device_id || '.active_power')::text as pathname,
           ts, active_power_kw::double precision as value,
           'kW'::text as unit, 'analog'::text as data_kind, 'historico'::text as source,
           'Potência ativa do relé'::text as description
    from relay_base where active_power_kw is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'apparent_power', ('RELAY_' || device_id || '.apparent_power'),
           ts, apparent_power_kva::double precision,
           'kVA', 'analog', 'historico',
           'Potência aparente do relé'
    from relay_base where apparent_power_kva is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'reactive_power', ('RELAY_' || device_id || '.reactive_power'),
           ts, reactive_power_kvar::double precision,
           'kvar', 'analog', 'historico',
           'Potência reativa do relé'
    from relay_base where reactive_power_kvar is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'voltage_ab', ('RELAY_' || device_id || '.voltage_ab'),
           ts, voltage_ab_v::double precision,
           'V', 'analog', 'historico',
           'Tensão AB do relé'
    from relay_base where voltage_ab_v is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'voltage_bc', ('RELAY_' || device_id || '.voltage_bc'),
           ts, voltage_bc_v::double precision,
           'V', 'analog', 'historico',
           'Tensão BC do relé'
    from relay_base where voltage_bc_v is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'voltage_ca', ('RELAY_' || device_id || '.voltage_ca'),
           ts, voltage_ca_v::double precision,
           'V', 'analog', 'historico',
           'Tensão CA do relé'
    from relay_base where voltage_ca_v is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'current_a', ('RELAY_' || device_id || '.current_a'),
           ts, current_a_a::double precision,
           'A', 'analog', 'historico',
           'Corrente A do relé'
    from relay_base where current_a_a is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'current_b', ('RELAY_' || device_id || '.current_b'),
           ts, current_b_a::double precision,
           'A', 'analog', 'historico',
           'Corrente B do relé'
    from relay_base where current_b_a is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'current_c', ('RELAY_' || device_id || '.current_c'),
           ts, current_c_a::double precision,
           'A', 'analog', 'historico',
           'Corrente C do relé'
    from relay_base where current_c_a is not null
),

relay_alarm_base as (

    select
        p.customer_id,
        e.power_plant_id,
        e.device_id,
        coalesce(dt.name, 'relay')::text as device_type,
        'RELAY 1'::text as context,
        date_trunc('hour', e."timestamp")::timestamptz as ts,
        e.type_en,
        lower(coalesce(e.severity, '')) as severity,
        coalesce(e.is_active_event, false) as is_active_event,
        e.code,
        e.description_pt,
        e.description_en
    from rt.int_events_alarms e
    join public.power_plant p
      on p.id = e.power_plant_id
    left join public.device d
      on d.id = e.device_id
    left join public.device_type dt
      on dt.id = d.device_type_id
    where e.type_en = 'alarm'
      and e.device_id is not null
      and lower(coalesce(dt.name, '')) like '%relay%'
),

relay_alarm_points as (

    select
        customer_id,
        power_plant_id,
        device_type,
        device_id,
        context,
        'active_alarm_count'::text as point_name,
        ('RELAY_' || device_id || '.active_alarm_count')::text as pathname,
        ts,
        sum(case when is_active_event = true then 1 else 0 end)::double precision as value,
        'count'::text as unit,
        'discrete'::text as data_kind,
        'historico'::text as source,
        'Alarmes ativos do relé (contagem)'::text as description
    from relay_alarm_base
    group by customer_id, power_plant_id, device_type, device_id, context, ts

    union all

    select
        customer_id,
        power_plant_id,
        device_type,
        device_id,
        context,
        'high_alarm_count'::text as point_name,
        ('RELAY_' || device_id || '.high_alarm_count')::text as pathname,
        ts,
        sum(case when is_active_event = true and severity = 'high' then 1 else 0 end)::double precision as value,
        'count'::text as unit,
        'discrete'::text as data_kind,
        'historico'::text as source,
        'Alarmes high ativos do relé (contagem)'::text as description
    from relay_alarm_base
    group by customer_id, power_plant_id, device_type, device_id, context, ts
),

relay_alarm_type_points as (

    select
        customer_id,
        power_plant_id,
        device_type,
        device_id,
        context,
        ('alarm_' || code::text)::text as point_name,
        ('RELAY_' || device_id || '.alarm_' || code::text)::text as pathname,
        ts,
        count(*)::double precision as value,
        'count'::text as unit,
        'discrete'::text as data_kind,
        'historico'::text as source,
        coalesce(
            nullif(trim(description_pt), ''),
            nullif(trim(description_en), ''),
            ('Alarme ' || code::text)
        )::text as description
    from relay_alarm_base
    where code is not null
    group by
        customer_id,
        power_plant_id,
        device_type,
        device_id,
        context,
        code,
        coalesce(
            nullif(trim(description_pt), ''),
            nullif(trim(description_en), ''),
            ('Alarme ' || code::text)
        ),
        ts
),

meter_base as (

    select
        pp.customer_id,
        ma.power_plant_id,
        d.name as device_name,
        coalesce(dv.name, 'multimeter')::text as device_type,
        ma.device_id,
        coalesce(
            mdm.display_name,
            ('MULTIMETER ' || dense_rank() over (
                partition by ma.power_plant_id
                order by ma.device_id
            ))::text
        ) as context,
        ma.timestamp::timestamptz as ts,
        ma.active_power_kw,
        ma.reactive_power_kvar as power_reactive_kvar,
        null::double precision as apparent_power_kva,
        ma.power_factor,
        ma.frequency_hz,
        ma.energy_import_kwh as imported_active_energy_kwh,
        ma.energy_export_kwh as exported_active_energy_kwh,
        null::double precision as imported_reactive_energy_kvarh,
        null::double precision as exported_reactive_energy_kvarh,
        ma.current_a_a as current_phase_a_a,
        ma.current_b_a as current_phase_b_a,
        ma.current_c_a as current_phase_c_a,
        ma.voltage_ab_v as line_voltage_ab_v,
        ma.voltage_bc_v as line_voltage_bc_v,
        ma.voltage_ca_v as line_voltage_ca_v
    from rt.stg_meter_analog ma
    join public.power_plant pp
      on pp.id = ma.power_plant_id
    left join public.device d
      on d.id = ma.device_id
    left join public.device_type dv
      on dv.id = d.device_type_id
    left join meter_device_map mdm
      on mdm.device_id = ma.device_id
),

meter_points as (

    select customer_id, power_plant_id, device_type, device_id, context,
           'active_power'::text as point_name,
           ('METER_' || device_id || '.active_power')::text as pathname,
           ts, active_power_kw::double precision as value,
           'kW'::text as unit, 'analog'::text as data_kind, 'historico'::text as source,
           'Potência ativa do multimedidor'::text as description
    from meter_base where active_power_kw is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'reactive_power', ('METER_' || device_id || '.reactive_power'),
           ts, power_reactive_kvar::double precision,
           'kvar', 'analog', 'historico',
           'Potência reativa do multimedidor'
    from meter_base where power_reactive_kvar is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'apparent_power', ('METER_' || device_id || '.apparent_power'),
           ts, apparent_power_kva::double precision,
           'kVA', 'analog', 'historico',
           'Potência aparente do multimedidor'
    from meter_base where apparent_power_kva is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'power_factor', ('METER_' || device_id || '.power_factor'),
           ts, power_factor::double precision,
           'pu', 'analog', 'historico',
           'Fator de potência do multimedidor'
    from meter_base where power_factor is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'frequency', ('METER_' || device_id || '.frequency'),
           ts, frequency_hz::double precision,
           'Hz', 'analog', 'historico',
           'Frequência do multimedidor'
    from meter_base where frequency_hz is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'imported_active_energy', ('METER_' || device_id || '.imported_active_energy'),
           ts, imported_active_energy_kwh::double precision,
           'kWh', 'analog', 'historico',
           'Energia ativa importada do multimedidor'
    from meter_base where imported_active_energy_kwh is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'exported_active_energy', ('METER_' || device_id || '.exported_active_energy'),
           ts, exported_active_energy_kwh::double precision,
           'kWh', 'analog', 'historico',
           'Energia ativa exportada do multimedidor'
    from meter_base where exported_active_energy_kwh is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'imported_reactive_energy', ('METER_' || device_id || '.imported_reactive_energy'),
           ts, imported_reactive_energy_kvarh::double precision,
           'kvarh', 'analog', 'historico',
           'Energia reativa importada do multimedidor'
    from meter_base where imported_reactive_energy_kvarh is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'exported_reactive_energy', ('METER_' || device_id || '.exported_reactive_energy'),
           ts, exported_reactive_energy_kvarh::double precision,
           'kvarh', 'analog', 'historico',
           'Energia reativa exportada do multimedidor'
    from meter_base where exported_reactive_energy_kvarh is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'current_a', ('METER_' || device_id || '.current_a'),
           ts, current_phase_a_a::double precision,
           'A', 'analog', 'historico',
           'Corrente fase A do multimedidor'
    from meter_base where current_phase_a_a is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'current_b', ('METER_' || device_id || '.current_b'),
           ts, current_phase_b_a::double precision,
           'A', 'analog', 'historico',
           'Corrente fase B do multimedidor'
    from meter_base where current_phase_b_a is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'current_c', ('METER_' || device_id || '.current_c'),
           ts, current_phase_c_a::double precision,
           'A', 'analog', 'historico',
           'Corrente fase C do multimedidor'
    from meter_base where current_phase_c_a is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'voltage_ab', ('METER_' || device_id || '.voltage_ab'),
           ts, line_voltage_ab_v::double precision,
           'V', 'analog', 'historico',
           'Tensão AB do multimedidor'
    from meter_base where line_voltage_ab_v is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'voltage_bc', ('METER_' || device_id || '.voltage_bc'),
           ts, line_voltage_bc_v::double precision,
           'V', 'analog', 'historico',
           'Tensão BC do multimedidor'
    from meter_base where line_voltage_bc_v is not null

    union all
    select customer_id, power_plant_id, device_type, device_id, context,
           'voltage_ca', ('METER_' || device_id || '.voltage_ca'),
           ts, line_voltage_ca_v::double precision,
           'V', 'analog', 'historico',
           'Tensão CA do multimedidor'
    from meter_base where line_voltage_ca_v is not null
),

meter_alarm_base as (

    select
        p.customer_id,
        e.power_plant_id,
        e.device_id,
        coalesce(dt.name, 'meter')::text as device_type,
        coalesce(
            nullif(trim(d.display_name), ''),
            ('MULTIMETER ' || dense_rank() over (
                partition by e.power_plant_id
                order by e.device_id
            ))::text
        ) as context,
        date_trunc('hour', e."timestamp")::timestamptz as ts,
        e.type_en,
        lower(coalesce(e.severity, '')) as severity,
        coalesce(e.is_active_event, false) as is_active_event,
        e.code,
        e.description_pt,
        e.description_en
    from rt.int_events_alarms e
    join public.power_plant p
      on p.id = e.power_plant_id
    left join public.device d
      on d.id = e.device_id
    left join public.device_type dt
      on dt.id = d.device_type_id
    where e.type_en = 'alarm'
      and e.device_id is not null
      and (
            lower(coalesce(dt.name, '')) like '%meter%'
         or lower(coalesce(dt.name, '')) like '%multimeter%'
      )
),

meter_alarm_points as (

    select
        customer_id,
        power_plant_id,
        device_type,
        device_id,
        context,
        'active_alarm_count'::text as point_name,
        ('METER_' || device_id || '.active_alarm_count')::text as pathname,
        ts,
        sum(case when is_active_event = true then 1 else 0 end)::double precision as value,
        'count'::text as unit,
        'discrete'::text as data_kind,
        'historico'::text as source,
        'Alarmes ativos do multimedidor (contagem)'::text as description
    from meter_alarm_base
    group by customer_id, power_plant_id, device_type, device_id, context, ts

    union all

    select
        customer_id,
        power_plant_id,
        device_type,
        device_id,
        context,
        'high_alarm_count'::text as point_name,
        ('METER_' || device_id || '.high_alarm_count')::text as pathname,
        ts,
        sum(case when is_active_event = true and severity = 'high' then 1 else 0 end)::double precision as value,
        'count'::text as unit,
        'discrete'::text as data_kind,
        'historico'::text as source,
        'Alarmes high ativos do multimedidor (contagem)'::text as description
    from meter_alarm_base
    group by customer_id, power_plant_id, device_type, device_id, context, ts
),

meter_alarm_type_points as (

    select
        customer_id,
        power_plant_id,
        device_type,
        device_id,
        context,
        ('alarm_' || code::text)::text as point_name,
        ('METER_' || device_id || '.alarm_' || code::text)::text as pathname,
        ts,
        count(*)::double precision as value,
        'count'::text as unit,
        'discrete'::text as data_kind,
        'historico'::text as source,
        coalesce(
            nullif(trim(description_pt), ''),
            nullif(trim(description_en), ''),
            ('Alarme ' || code::text)
        )::text as description
    from meter_alarm_base
    where code is not null
    group by
        customer_id,
        power_plant_id,
        device_type,
        device_id,
        context,
        code,
        coalesce(
            nullif(trim(description_pt), ''),
            nullif(trim(description_en), ''),
            ('Alarme ' || code::text)
        ),
        ts
),

final as (
    select * from plant_intraday
    union all
    select * from plant_daily
    union all
    select * from plant_monthly
    union all
    select * from plant_alarm_points
    union all
    select * from plant_alarm_type_points
    union all
    select * from inverter_points
    union all
    select * from inverter_alarm_points
    union all
    select * from inverter_alarm_type_points
    union all
    select * from weather_points
    union all
    select * from relay_points
    union all
    select * from relay_alarm_points
    union all
    select * from relay_alarm_type_points
    union all
    select * from meter_points
    union all
    select * from meter_alarm_points
    union all
    select * from meter_alarm_type_points
)

select *
from final
where value is not null