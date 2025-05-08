from __future__ import annotations

import os
from typing import Iterable, List, Tuple

import pandas as pd
from snowflake.connector.errors import ProgrammingError

from src.snowflake_utils import get_connector, SnowflakeConfig

# Try to import write_pandas only if we might use it
try:
    from snowflake.connector.pandas_tools import write_pandas  # type: ignore
except Exception:
    write_pandas = None


def _q(ident: str) -> str:
    # simple identifier quote; good enough for normal names
    return f'"{ident.replace(chr(34), chr(34)*2)}"'


def _fqtn(database: str, schema: str, table: str) -> str:
    return f'{_q(database)}.{_q(schema)}.{_q(table)}'


def _ensure_table(conn, fqtn: str, columns: Iterable[str]) -> None:
    # Create table if not exists; map everything to VARCHAR for the demo
    cols_sql = ", ".join(f'{_q(c)} VARCHAR' for c in columns)
    sql = f"CREATE TABLE IF NOT EXISTS {fqtn} ({cols_sql})"
    with conn.cursor() as cur:
        cur.execute(sql)


def _iter_batches(rows: List[Tuple], batch_size: int):
    for i in range(0, len(rows), batch_size):
