from src.snowflake_utils import get_connector

def search_tables(pattern: str, schema='RAW'):
    sql = f"""
    select table_schema, table_name
    from information_schema.tables
    where table_schema = '{schema}' and table_name ilike '%{pattern}%'
    order by table_name
