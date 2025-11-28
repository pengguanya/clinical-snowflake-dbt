from snowflake.snowpark import Session
from src.snowflake_utils import SnowflakeConfig

def filter_dm_by_studyid(studyid: str):
    cfg = SnowflakeConfig()
    conn_params = {
        "account": cfg.account,
        "user": cfg.user,
        "password": cfg.password,
        "role": cfg.role,
        "warehouse": cfg.warehouse,
        "database": cfg.database,
        "schema": cfg.schema_raw
    }
    session = Session.builder.configs(conn_params).create()
    df = session.table(f'{cfg.database}.{cfg.schema_raw}.DM').filter(f"STUDYID = '{studyid}'")
    return df.collect()
