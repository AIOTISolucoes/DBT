{{ config(
    materialized='table',
    post_hook=[
      "create index if not exists ix_ds_daily_cust_plant_date on {{ this }} (customer_id, power_plant_id, date)"
    ]
) }}

select
    f.customer_id,
    f.power_plant_id,
    f.date_day as date,

    -- métricas do Data Studio (mantendo o contrato antigo)
    f.generation_daily_kwh::double precision as energy_real_kwh,
    f.irradiation_daily_kwh_m2::double precision as irradiance_kwh_m2,

    case
        when f.pr_daily_pct is not null
         and f.pr_daily_pct > 0
         and f.generation_daily_kwh is not null
        then (f.generation_daily_kwh / (f.pr_daily_pct / 100.0))::double precision
        else null::double precision
    end as energy_theoretical_kwh,

    f.pr_daily_pct::double precision as performance_ratio,

    -- referência
    f.rated_power_kwp::double precision as capacity_dc

from rt.fct_power_plant_metrics_daily f