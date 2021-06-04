#!/usr/bin/env python3

from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Sequence

logging.basicConfig()
log = logging.getLogger(__file__)
log.setLevel(logging.INFO)


def null_convert(input: Any) -> Any:
    return input


@dataclass
class FieldMap:
    """meta data for sql"""

    in_name: str
    data_type: Optional[str] = "VARCHAR"
    column_name: Optional[str] = None
    primary_key: Optional[bool] = False
    keep: Optional[bool] = True
    default: Optional[str] = None
    convert: Optional[Callable[[Any], Any]] = null_convert
    comment: Optional[str] = None


def get_in_names(fm: Sequence[FieldMap]) -> Sequence[str]:
    """return names in incoming data"""
    return [f.in_name for f in fm if f.keep]


def get_column_names(fm: Sequence[FieldMap]) -> Sequence[str]:
    """return column names in incoming data"""
    return [f.column_name if f.column_name else f.in_name for f in fm if f.keep]


@dataclass
class TableSpec:
    field_map: Sequence[FieldMap]
    table_name: str
    primary_key: Optional[str] = None
    calculate: Optional[Callable[[TableSpec, Any], Any]] = None
    extra_sql: Optional[List[str]] = None

    def do_calculate(self, timeseries: Any) -> Any:
        if self.calculate:
            try:
                return self.calculate(self, timeseries)
            except Exception:
                log.exception("calculation exception: %s", self.table_name)
                # log.error("calculate: %s", timeseries)

    def get_in_names(self):
        return get_in_names(self.field_map)

    def get_column_names(self):
        return get_column_names(self.field_map)


def compose_create(
    table_name: str,
    fields: List[FieldMap],
    primary_key: Optional[str] = None,
    extra_sql: Optional[List[str]] = None,
) -> str:
    clauses = []
    fdesc = ",\n".join(
        [
            f"{f.column_name if f.column_name else f.in_name} {f.data_type} "
            f"{f.default if f.default is not None else ''}".strip()
            for f in fields
            if f.keep
        ]
    )
    clauses.append(f"CREATE TABLE IF NOT EXISTS {table_name} (\n{fdesc}")
    if primary_key:
        clauses.append(f", PRIMARY KEY({primary_key})")
    clauses.append(");")
    if extra_sql:
        try:
            for xsql in extra_sql:
                clauses.append(
                    xsql.format(table_name=table_name, primary_key=primary_key)
                )
        except Exception:
            print("xsql %s table_name %s primary_key %s", xsql, table_name, primary_key)
            raise
    sql = "\n".join(clauses)
    return sql


def compose_insert(
    field_names: Sequence, table_name: str, xclause: Optional[str] = None
) -> str:
    """compose parameterized insert SQL

    Args:
        field_names (Sequence): database table field names
        table_name (str): database table name.

    Returns:
        str: insert SQL.
    """
    fields = ", ".join(field_names)
    placeholders = ", ".join([f"${i+1}" for i in range(len(field_names))])
    sql = f"INSERT INTO {table_name} ({fields}) values ({placeholders})"
    if xclause:
        sql = f"{sql}\n{xclause}"
    return sql
