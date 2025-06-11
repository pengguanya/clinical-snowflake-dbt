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
        yield rows[i : i + batch_size]


def _insert_rows_without_stage(conn, df: pd.DataFrame, fqtn: str, batch_size: int = 1000) -> Tuple[bool, int]:
    # Convert NaN -> None so DB-API sends NULLs
    df2 = df.where(pd.notnull(df), None)

    cols = list(df2.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO {fqtn} ({', '.join(_q(c) for c in cols)}) VALUES ({placeholders})"

    rows = [tuple(rec) for rec in df2.itertuples(index=False, name=None)]
    inserted = 0
    with conn.cursor() as cur:
        for batch in _iter_batches(rows, batch_size):
            cur.executemany(sql, batch)
            inserted += len(batch)
    # autocommit is typically True, but be explicit
    try:
        conn.commit()
    except Exception:
        pass
    return True, inserted


def load_dataframe(df: pd.DataFrame, table: str, schema: str):
    cfg = SnowflakeConfig()
    conn = get_connector()  # token/password already handled inside
    try:
        fqtn = _fqtn(cfg.database, schema, table)

        # Always ensure table exists (VARCHAR columns for simplicity)
        _ensure_table(conn, fqtn, df.columns)

        use_bulk = os.getenv("SNOWFLAKE_BULK_STAGE", "true").lower() not in ("0", "false", "no")

