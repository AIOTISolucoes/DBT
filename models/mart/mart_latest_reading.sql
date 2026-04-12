{{ config(
    materialized = 'table',
    on_schema_change = 'sync_all_columns'
) }}

with all_analog as (

    select
        'inverter'::text as reading_source,
        a.timestamp,
        a.power_plant_id,
        a.device_id,
        to_jsonb(a)
            - 'id'
            - 'timestamp'
            - 'power_plant_id'
            - 'device_id'
            - 'created_at'
            - 'updated_at' as analog_json
    from {{ ref('stg_inverter_analog') }} a

    union all

    select
        'tracker'::text as reading_source,
        a.timestamp,
        a.power_plant_id,
        a.device_id,
        to_jsonb(a)
            - 'id'
            - 'timestamp'
            - 'power_plant_id'
            - 'device_id'
            - 'created_at'
            - 'updated_at' as analog_json
    from {{ ref('stg_tracker_analog') }} a

    union all

    select
        'transformer'::text as reading_source,
        a.timestamp,
        a.power_plant_id,
        a.device_id,
        to_jsonb(a)
            - 'id'
            - 'timestamp'
            - 'power_plant_id'
            - 'device_id'
            - 'created_at'
            - 'updated_at' as analog_json
    from {{ ref('stg_transformer_analog') }} a

    union all

    select
        'meter'::text as reading_source,
        a.timestamp,
        a.power_plant_id,
        a.device_id,
        to_jsonb(a)
            - 'id'
            - 'timestamp'
            - 'power_plant_id'
            - 'device_id'
            - 'created_at'
            - 'updated_at' as analog_json
    from {{ ref('stg_meter_analog') }} a

    union all

    select
        'weather_station'::text as reading_source,
        a.timestamp,
        a.power_plant_id,
        a.device_id,
        to_jsonb(a)
            - 'id'
            - 'timestamp'
            - 'power_plant_id'
            - 'device_id'
            - 'created_at'
            - 'updated_at' as analog_json
    from {{ ref('stg_weather_station_analog') }} a

),

ranked as (

    select
        a.*,
        row_number() over (
            partition by a.reading_source, a.device_id
            order by a.timestamp desc
        ) as rn
    from all_analog a

),

latest_only as (

    select
        reading_source,
        timestamp,
        power_plant_id,
        device_id,
        analog_json
    from ranked
    where rn = 1

),

enriched as (

    select
        l.reading_source,
        l.timestamp,
        l.power_plant_id,
        pp.name::text as power_plant_name,
        l.device_id,
        d.name::text as device_name,
        d.device_type_id,
        dt.name::text as device_type_name,
        l.analog_json
    from latest_only l
    left join public.device d
        on l.device_id = d.id
    left join public.device_type dt
        on d.device_type_id = dt.id
    left join public.power_plant pp
        on l.power_plant_id = pp.id

),

exploded as (

    select
        e.timestamp,
        e.power_plant_id,
        e.power_plant_name,
        e.device_id,
        e.device_name,
        e.device_type_id,
        e.device_type_name,
        e.reading_source,
        kv.key as point_name,
        case
            when jsonb_typeof(kv.value) = 'number'
                then (kv.value #>> '{}')::numeric
            when jsonb_typeof(kv.value) = 'string'
                 and btrim(kv.value #>> '{}') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
                then btrim(kv.value #>> '{}')::numeric
            else null
        end as point_value
    from enriched e
    cross join lateral jsonb_each(e.analog_json) as kv(key, value)
    where
        jsonb_typeof(kv.value) = 'number'
        or (
            jsonb_typeof(kv.value) = 'string'
            and btrim(kv.value #>> '{}') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
        )

)

select *
from exploded