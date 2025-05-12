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
