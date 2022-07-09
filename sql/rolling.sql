with p as (
    select
        period as "time",
        -- array_agg(forecast_lead order by period desc, forecast_lead asc) forecast_lead,
        array_agg(export_price order by period desc, forecast_lead asc) export_price,
        array_agg(usage_price order by period desc, forecast_lead asc) usage_price
    from amber_forecast_rolling
    where period >= now() - interval '1 day'
    group by
        period
    ORDER BY
        period
),
pri as (
    select
        time,
        usage_price[1] as usage_price1,
        usage_price[2] as usage_price2,
        usage_price[3] as usage_price3,
        export_price[1] as export_price1,
        export_price[2] as export_price2,
        export_price[3] as export_price3
    from p
)
select
    pri.time,
    m30.usage_price,
    pri.usage_price1,
    pri.usage_price2,
    pri.usage_price3,
    m30.usage_price - pri.usage_price1 usage_delta,
    m30.export_price,
    pri.export_price1,
    pri.export_price2,
    pri.export_price3,
    m30.export_price - pri.export_price1 export_delta
from pri
left join amber_actual_30min m30 on pri.time = m30.period
order by pri.time


-- last n forecasts
-- with p as (
--     select
--         period as "time",
--         -- array_agg(forecast_lead order by period desc, forecast_lead asc) forecast_lead,
--         array_agg(export_price order by period desc, forecast_lead asc) export_price,
--         array_agg(usage_price order by period desc, forecast_lead asc) usage_price
--     from amber_forecast_rolling
--     where period >= now() - interval '1 hour'
--     group by
--         period
--     ORDER BY
--         period
-- )
-- select
--     time,
--     usage_price[1] as usage_price1,
--     usage_price[2] as usage_price2,
--     usage_price[3] as usage_price3,
--     export_price[1] as export_price1,
--     export_price[2] as export_price2,
--     export_price[3] as export_price3
-- from p
-- order by p.time



-- with periods as (
--     SELECT
--         distinct period
--     FROM amber_forecast_rolling
--     WHERE
--     period > now()
-- )
-- select
--     periods.period,
--     array_agg(afr.forecast_lead)
-- from periods
-- inner join amber_forecast_rolling afr on afr.period = periods.period
-- group by
--     periods.period
-- ORDER BY
--     periods.period

-- SELECT
--   period AS "time",
--   event_time,
--   forecast_lead,
--   lag(usage_price, 1) over (order by forecastedat, forecast_lead) usage_30,
--   lag(usage_price, 2) over (order by forecastedat, forecast_lead) usage_60,
--   lag(usage_price, 3) over (order by forecastedat, forecast_lead) usage_90,
--   lag(usage_price, 4) over (order by forecastedat, forecast_lead) usage_120,
--   lag(export_price, 1) over (order by forecastedat, forecast_lead) export_30,
--   lag(export_price, 2) over (order by forecastedat, forecast_lead) export_60,
--   lag(export_price, 3) over (order by forecastedat, forecast_lead) export_90,
--   lag(export_price, 4) over (order by forecastedat, forecast_lead) export_120
-- FROM amber_forecast_rolling
-- WHERE
--   period between (now() - interval '3 hours') and (now() + interval '3 hours')
--   and forecast_lead between 0 and 3600
-- ORDER BY
--     event_time,
--     forecast_lead


-- SELECT
--   period AS "time",
--   usage_price,
--   lag(usage_price, 1) over (order by forecastedat) usage_30,
--   lag(usage_price, 2) over (order by forecastedat) usage_60,
--   lag(usage_price, 3) over (order by forecastedat) usage_90,
--   lag(usage_price, 4) over (order by forecastedat) usage_120,
--   export_price,
--   lag(export_price, 1) over (order by forecastedat) export_30,
--   lag(export_price, 2) over (order by forecastedat) export_60,
--   lag(export_price, 3) over (order by forecastedat) export_90,
--   lag(export_price, 4) over (order by forecastedat) export_120
-- FROM amber_forecast_rolling
-- WHERE
--   period between (now() - interval '3 hours') and (now() + interval '3 hours')
-- ORDER BY
--     forecastedat,
--     forecast_lead

-- grafana prototype
-- SELECT
--   period AS "time",
--   export_price,
--   usage_price
-- FROM amber_forecast_rolling
-- WHERE
--   forecast_lead = 3600
--   and
--   period > (now() - interval '30 days')
--   ORDER BY 1

-- select
--     *
-- from
--     amber_forecast_rolling
-- limit 10;
