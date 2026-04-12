{{ config(
    materialized='table',
    post_hook=[
      "create index if not exists ix_ds_hist_15m_cust_plant_path_ts on {{ this }} (customer_id, power_plant_id, pathname, ts)"
    ]
) }}

with base as (

    select
        customer_id,
        power_plant_id,

        (
          date_trunc('hour', ts)
          + floor(date_part('minute', ts) / 15) * interval '15 minute'
        )::timestamptz as ts,

        pathname,
        unit,
        data_kind,
        source,
        context,
        description,
        value::double precision as value
    from {{ ref('mart_datastudio_timeseries') }}
    where source = 'historico'
      and value is not null

),

base_with_capacity as (

    select
        b.*,
        coalesce(p.capacity_ac, 0)::double precision as capacity_ac
    from base b
    left join public.power_plant p
      on p.id = b.power_plant_id

),

clean as (

    select *
    from base_with_capacity
    where
        value is not null
        and (
            pathname <> 'PLANT.active_power_kw'
            or (
                value >= 0
                and (
                    capacity_ac <= 0
                    or value <= capacity_ac * 1.3
                )
            )
        )

)

select
    customer_id,
    power_plant_id,
    ts,
    pathname,
    unit,
    data_kind,
    source,
    context,
    description,

    avg(value)::double precision as avg_value,
    sum(value)::double precision as sum_value,
    max(value)::double precision as max_value,
    min(value)::double precision as min_value,
    count(*)::int as count_points

from clean
group by
    customer_id,
    power_plant_id,
    ts,
    pathname,
    unit,
    data_kind,
    source,
    context,
    description