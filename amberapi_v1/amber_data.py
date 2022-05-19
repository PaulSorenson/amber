#!/usr/bin/env python3

import logging
from typing import Dict, Tuple

import pandas as pd

from .datacomposer import FieldMap, TableSpec, compose_create, compose_insert

logging.basicConfig()
log = logging.getLogger(__file__)
log.setLevel(logging.INFO)

DB = "timeseries"
TABLE_RAW_ACTUAL_30MIN = "amber_actual_30min"
TABLE_RAW_ACTUAL_5MIN = "amber_actual_5min"
TABLE_RAW_FORECAST = "amber_forecast"
TABLE_RAW_FORECAST_ROLLING = "amber_forecast_rolling"
# provided by the scheduler
TIME_FIELD = "event_time"


actual_30min_fields = [
    FieldMap(TIME_FIELD, "TIMESTAMPTZ NOT NULL"),
    FieldMap("semiScheduledGeneration", "DOUBLE PRECISION"),
    FieldMap("operationalDemand", "DOUBLE PRECISION"),
    FieldMap("rooftopSolar", "DOUBLE PRECISION"),
    FieldMap("createdAt", "TIMESTAMPTZ"),
    FieldMap("wholesaleKWHPrice", "DOUBLE PRECISION"),
    FieldMap("region"),
    FieldMap("period", "TIMESTAMPTZ NOT NULL"),
    FieldMap("renewablesPercentage", "DOUBLE PRECISION"),
    FieldMap("percentileRank", "DOUBLE PRECISION"),
    FieldMap("postcode"),
    # calculated values
    FieldMap("usage_price", "DOUBLE PRECISION"),
    FieldMap("export_price", "DOUBLE PRECISION"),
]


actual_5min_fields = [
    FieldMap(TIME_FIELD, "TIMESTAMPTZ NOT NULL", keep=True),
    FieldMap("operationalDemand", "DOUBLE PRECISION"),
    FieldMap("rooftopSolar", "DOUBLE PRECISION"),
    FieldMap("wholesaleKWHPrice", "DOUBLE PRECISION"),
    FieldMap("region"),
    FieldMap("period", "TIMESTAMPTZ NOT NULL"),
    FieldMap("renewablesPercentage", "DOUBLE PRECISION"),
    FieldMap("percentileRank", "DOUBLE PRECISION"),
    FieldMap("latestPeriod", "TIMESTAMPTZ", keep=False),
    FieldMap("usage", "DOUBLE PRECISION", keep=False),
    # calculated values
    FieldMap("postcode"),
    FieldMap("usage_price", "DOUBLE PRECISION"),
    FieldMap("export_price", "DOUBLE PRECISION"),
]


forecast_fields = [
    # inserted by the sceduler
    FieldMap(TIME_FIELD, "TIMESTAMPTZ NOT NULL", keep=True),
    FieldMap("semiScheduledGeneration", "DOUBLE PRECISION"),
    FieldMap("operationalDemand", "DOUBLE PRECISION"),
    FieldMap("rooftopSolar", "DOUBLE PRECISION"),
    FieldMap("createdAt", "TIMESTAMPTZ"),
    FieldMap("wholesaleKWHPrice", "DOUBLE PRECISION"),
    FieldMap("region"),
    FieldMap("period", "TIMESTAMPTZ NOT NULL"),
    FieldMap("renewablesPercentage", "DOUBLE PRECISION"),
    FieldMap("periodSource"),
    FieldMap("percentileRank", "DOUBLE PRECISION"),
    # these two fields are only populated for forecasts (not actuals)
    FieldMap("forecastedAt", "TIMESTAMPTZ"),
    FieldMap("wholesaleKWHPriceRange", "DOUBLE PRECISION[]"),  # (min, max)
    FieldMap("postcode"),
    FieldMap("usage_price", "DOUBLE PRECISION"),
    FieldMap("export_price", "DOUBLE PRECISION"),
]


forecast_rolling_fields = [
    # inserted by the sceduler, not used as primary key in rolling forecast.
    FieldMap(TIME_FIELD, "TIMESTAMPTZ NOT NULL", keep=True),
    FieldMap("semiScheduledGeneration", "DOUBLE PRECISION"),
    FieldMap("operationalDemand", "DOUBLE PRECISION"),
    FieldMap("rooftopSolar", "DOUBLE PRECISION"),
    FieldMap("createdAt", "TIMESTAMPTZ"),
    FieldMap("wholesaleKWHPrice", "DOUBLE PRECISION"),
    FieldMap("region"),
    FieldMap("period", "TIMESTAMPTZ NOT NULL", comment="30 minute period for forecast"),
    FieldMap("renewablesPercentage", "DOUBLE PRECISION"),
    FieldMap("periodSource"),
    FieldMap("percentileRank", "DOUBLE PRECISION"),
    # these two fields are only populated for forecasts (not actuals)
    FieldMap(
        "forecastedAt",
        "TIMESTAMPTZ",
        comment="30 minute period when forecast was made",
    ),
    FieldMap("wholesaleKWHPriceRange", "DOUBLE PRECISION[]", comment="[min, max]"),
    FieldMap("postcode"),
    FieldMap("usage_price", "DOUBLE PRECISION"),
    FieldMap("export_price", "DOUBLE PRECISION"),
    FieldMap(
        "forecast_lead",
        "INTEGER",
        comment="lead time in seconds for forecast"
        " derived from period and forecastedAt",
    ),
]


def map_to_array(m: Dict[str, str]) -> Tuple[float, float]:
    try:
        t = (float(m["min"]), float(m["max"]))
        return t
    except Exception:
        log.exception("json encode failed")
        return (0, 0)


def calculate_actual_30min(spec: TableSpec, df: pd.DataFrame) -> pd.DataFrame:
    of = df.loc[
        (df.periodType == "ACTUAL") & (df.periodSource == "30MIN"),
        df.columns.intersection(spec.get_column_names()),
    ]
    return of.iloc[[-1]]


def calculate_actual_5min(spec: TableSpec, df: pd.DataFrame) -> pd.DataFrame:
    of = df.loc[
        (df.periodType == "ACTUAL") & (df.periodSource == "5MIN"),
        df.columns.intersection(spec.get_column_names()),
    ]
    # this sometimes has no rows
    return of


def calculate_forecast(spec: TableSpec, df: pd.DataFrame) -> pd.DataFrame:
    # table_columns = ad.table_specs[ad.TABLE_RAW_FORECAST].get_column_names()
    of = df.loc[
        df.periodType == "FORECAST",
        df.columns.intersection(spec.get_column_names()),
    ]
    if "wholesaleKWHPriceRange" in of.columns:
        of["wholesaleKWHPriceRange"] = of.wholesaleKWHPriceRange.map(map_to_array)
    return of.iloc[[0]]


def calculate_forecast_rolling(spec: TableSpec, df: pd.DataFrame) -> pd.DataFrame:
    of = df.loc[
        df.periodType == "FORECAST",
        df.columns.intersection(spec.get_column_names()),
    ]
    of["forecast_lead"] = (of.period - of.forecastedAt).dt.seconds
    if "wholesaleKWHPriceRange" in of.columns:
        of["wholesaleKWHPriceRange"] = of.wholesaleKWHPriceRange.map(map_to_array)
    return of


# https://docs.timescale.com/latest/api#create_hypertable

DEFAULT_HYPERTABLE = (
    "SELECT create_hypertable('{table_name}', '{primary_key}', "
    "if_not_exists => TRUE);"
)

table_specs = [
    TableSpec(
        field_map=actual_30min_fields,
        table_name=TABLE_RAW_ACTUAL_30MIN,
        primary_key=TIME_FIELD,
        calculate=calculate_actual_30min,
        extra_sql=[DEFAULT_HYPERTABLE],
    ),
    TableSpec(
        field_map=actual_5min_fields,
        table_name=TABLE_RAW_ACTUAL_5MIN,
        primary_key=TIME_FIELD,
        calculate=calculate_actual_5min,
        extra_sql=[DEFAULT_HYPERTABLE],
    ),
    TableSpec(
        field_map=forecast_fields,
        table_name=TABLE_RAW_FORECAST,
        primary_key=TIME_FIELD,
        calculate=calculate_forecast,
        extra_sql=[DEFAULT_HYPERTABLE],
    ),
    TableSpec(
        field_map=forecast_rolling_fields,
        table_name=TABLE_RAW_FORECAST_ROLLING,
        primary_key="period, forecast_lead",
        calculate=calculate_forecast_rolling,
        extra_sql=[
            (
                "SELECT create_hypertable('{table_name}', 'period', "
                "if_not_exists => TRUE);"
                # "partitioning_column => 'forecast_lead', number_partitions => 1);"
            ),
        ],
    ),
]


if __name__ == "__main__":

    create_sql = compose_create(
        table_name=TABLE_RAW_ACTUAL_30MIN,
        fields=actual_30min_fields,
        primary_key=TIME_FIELD,
    )
    print(create_sql)

    import pandas as pd

    filename = "../data/amber.pickle"
    df = pd.read_pickle(filename)
    names = [str(c) for c in df.columns]
    print(names)
    insert_sql = compose_insert(
        field_names=names,
        table_name=TABLE_RAW_ACTUAL_30MIN,
        xclause="ON CONFLICT DO UPDATE",
    )
    print(insert_sql)
