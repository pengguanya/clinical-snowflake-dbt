import os
import snowflake.connector
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

@dataclass
class SnowflakeConfig:
    account: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
    user: str = os.getenv("SNOWFLAKE_USER", "")
    role: str = os.getenv("SNOWFLAKE_ROLE", "")
    warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE", "")
    database: str = os.getenv("SNOWFLAKE_DATABASE", "")
    schema_raw: str = os.getenv("SNOWFLAKE_SCHEMA_RAW", "")
    password: str = os.getenv("SNOWFLAKE_PASSWORD", "")
    host: str = os.getenv("SNOWFLAKE_HOST", "")
    auth_method: str = os.getenv("SNOWFLAKE_AUTH_METHOD", "password")  # password|externalbrowser|oauth

def _force_session_context(conn, cfg: SnowflakeConfig) -> None:
    """Always set role/warehouse/database explicitly and fail early if warehouse can't be set."""
    with conn.cursor() as cur:
        # Role (some OAuth tokens may block USE ROLE; ignore failures gracefully)
        if cfg.role:
            try:
                cur.execute(f"USE ROLE {cfg.role}")
            except snowflake.connector.errors.ProgrammingError:
                # Keep the token's default role
                pass

        if not cfg.warehouse or not cfg.warehouse.strip():
            raise RuntimeError(
                "SNOWFLAKE_WAREHOUSE is empty; set it to a warehouse your active role can USE."
            )

        # Force-select the warehouse; error here is better than a late 000606
        try:
            cur.execute(f"USE WAREHOUSE {cfg.warehouse}")
        except snowflake.connector.errors.ProgrammingError as e:
            raise RuntimeError(
                f"Failed to USE WAREHOUSE {cfg.warehouse}. "
                "Name may be wrong or your role lacks USAGE. Under your active role, try:\n"
                f"  SHOW WAREHOUSES LIKE '{cfg.warehouse}';\n"
                "and ask for: GRANT USAGE ON WAREHOUSE <WH> TO ROLE <ROLE>."
            ) from e

        if cfg.database:
            cur.execute(f"USE DATABASE {cfg.database}")

def get_connector(token: str | None = None):
    cfg = SnowflakeConfig()
    params = {
        "account": cfg.account,
        "user": cfg.user,
        "role": cfg.role or None,
        "warehouse": None,   # let _force_session_context do it explicitly
        "database": cfg.database or None,
    }
    if cfg.host:
        params["host"] = cfg.host

    am = (cfg.auth_method or "password").lower()
    if am == "oauth":
        tok = token or os.getenv("SNOWFLAKE_OAUTH_TOKEN")
        if not tok:
            raise RuntimeError("SNOWFLAKE_AUTH_METHOD=oauth but no token provided and SNOWFLAKE_OAUTH_TOKEN is empty.")
        params["authenticator"] = "oauth"
        params["token"] = tok
    elif am == "externalbrowser":
        params["authenticator"] = "externalbrowser"
    else:
        params["password"] = cfg.password

    conn = snowflake.connector.connect(**{k: v for k, v in params.items() if v is not None})
    _force_session_context(conn, cfg)
    return conn

def execute_sql(sql: str):
    conn = get_connector()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            try:
                return cur.fetchall()
            except Exception:
                return None
    finally:
        conn.close()

