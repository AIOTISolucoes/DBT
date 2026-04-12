{{ config(
    materialized = 'incremental',
    unique_key = 'event_row_id',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'append_new_columns',
    full_refresh = false
) }}

with source_events as (

    select
        e.event_source,
        e.timestamp,
        e.power_plant_id,
        pp.name::text as power_plant_name,
        e.device_id,
        d.name::text as device_name,
        d.device_type_id,
        dt.name::text as device_type_name,
        e.discrete_data_json::jsonb as discrete_data_json
    from {{ ref('int_all_events') }} e
    left join public.device d
        on e.device_id = d.id
    left join public.device_type dt
        on d.device_type_id = dt.id
    left join public.power_plant pp
        on e.power_plant_id = pp.id

    {% if is_incremental() %}
      where e.timestamp > (
        select coalesce(max(timestamp), '1970-01-01'::timestamptz)
        from {{ this }}
      )
    {% endif %}

),

exploded as (

    select
        s.event_source,
        s.timestamp,
        s.power_plant_id,
        s.power_plant_name,
        s.device_id,
        s.device_name,
        s.device_type_id,
        s.device_type_name,
        kv.key as raw_key,
        kv.value as raw_value,
        kv.key as code,
        case
            when kv.key = 'communication_fault'
                 and btrim(kv.value) ~ '^-?[0-9]+$'
                then btrim(kv.value)::int
            when btrim(lower(kv.value)) in ('true', '1', '1.0', 'on', 'active')
                then 1
            when btrim(lower(kv.value)) in ('false', '0', '0.0', 'off', 'inactive')
                then 0
            when btrim(kv.value) ~ '^-?[0-9]+(\.[0-9]+)?$'
                then (btrim(kv.value))::numeric::int
            else null
        end as value,
        kv.key::text as point_name,
        s.device_name::text as equipment_name,
        false as is_seed
    from source_events s
    cross join lateral jsonb_each_text(s.discrete_data_json) as kv

),

historical_seed as (

    {% if is_incremental() %}
    select distinct on (
        event_source,
        power_plant_id,
        device_id,
        code
    )
        event_source,
        timestamp,
        power_plant_id,
        power_plant_name,
        device_id,
        device_name,
        device_type_id,
        device_type_name,
        raw_key,
        raw_value,
        code,
        value,
        point_name,
        equipment_name,
        true as is_seed
    from {{ this }}
    order by
        event_source,
        power_plant_id,
        device_id,
        code,
        timestamp desc
    {% else %}
    select
        null::text        as event_source,
        null::timestamptz as timestamp,
        null::int         as power_plant_id,
        null::text        as power_plant_name,
        null::int         as device_id,
        null::text        as device_name,
        null::int         as device_type_id,
        null::text        as device_type_name,
        null::text        as raw_key,
        null::text        as raw_value,
        null::text        as code,
        null::int         as value,
        null::text        as point_name,
        null::text        as equipment_name,
        true              as is_seed
    where false
    {% endif %}

),

compare_base as (

    select * from exploded
    union all
    select * from historical_seed

),

with_prev as (

    select
        *,
        lag(value) over (
            partition by event_source, power_plant_id, device_id, code
            order by timestamp, is_seed desc
        ) as prev_value
    from compare_base

),

only_code_changes as (

    select
        event_source,
        timestamp,
        power_plant_id,
        power_plant_name,
        device_id,
        device_name,
        device_type_id,
        device_type_name,
        raw_key,
        raw_value,
        code,
        value,
        point_name,
        equipment_name
    from with_prev
    where
        is_seed = false
        and prev_value is distinct from value

),

catalog_join as (

    select
        e.event_source,
        e.timestamp,
        e.power_plant_id,
        e.power_plant_name,
        e.device_id,
        e.device_name,
        e.device_type_id,
        e.device_type_name,
        e.code,
        e.value,
        e.raw_key,
        e.raw_value,
        c.description_pt,
        c.description_en,
        c.status_description_pt,
        c.status_description_en,
        c.type_pt,
        c.type_en,
        c.severity,
        e.point_name,
        e.equipment_name,
        case
            when c.id is not null then true
            else false
        end as catalog_found,
        case
            when c.status_description_en = 'active' then true
            when c.status_description_en = 'inactive' then false
            else null
        end as is_active_event
    from only_code_changes e
    left join public.tb_event_alarm_catalog c
        on e.device_type_id = c.device_type_id
       and e.code = c.code
       and e.value = c.value
       and c.is_active = true

),

base_final as (

    select
        md5(
            coalesce(event_source, '') || '|' ||
            coalesce(timestamp::text, '') || '|' ||
            coalesce(device_id::text, '') || '|' ||
            coalesce(code, '') || '|' ||
            coalesce(value::text, '')
        ) as event_row_id,
        event_source,
        timestamp,
        power_plant_id,
        power_plant_name,
        device_id,
        device_name,
        device_type_id,
        device_type_name,
        code,
        value,
        raw_key,
        raw_value,
        description_pt,
        description_en,
        status_description_pt,
        status_description_en,
        type_pt,
        type_en,
        severity,
        point_name,
        equipment_name,
        is_active_event,
        catalog_found
    from catalog_join

),

existing_ack as (

    {% if is_incremental() %}
    select
        t.event_row_id,
        t.acknowledged,
        t.acknowledged_at,
        t.acknowledged_by,
        t.acknowledgment_note
    from {{ this }} t
    {% else %}
    select
        null::text as event_row_id,
        null::boolean as acknowledged,
        null::timestamptz as acknowledged_at,
        null::text as acknowledged_by,
        null::text as acknowledgment_note
    where false
    {% endif %}

),

final as (

    select
        b.event_row_id,
        b.event_source,
        b.timestamp,
        b.power_plant_id,
        b.power_plant_name,
        b.device_id,
        b.device_name,
        b.device_type_id,
        b.device_type_name,
        b.code,
        b.value,
        b.raw_key,
        b.raw_value,
        b.description_pt,
        b.description_en,
        b.status_description_pt,
        b.status_description_en,
        b.type_pt,
        b.type_en,
        b.severity,
        b.point_name,
        b.equipment_name,
        b.is_active_event,
        b.catalog_found,
        coalesce(a.acknowledged, false) as acknowledged,
        a.acknowledged_at,
        a.acknowledged_by,
        a.acknowledgment_note
    from base_final b
    left join existing_ack a
        on b.event_row_id = a.event_row_id

)

select *
from final