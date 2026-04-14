# src/utils/config.py
import clickhouse_connect
import os

def get_env_var(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing environment variable: {name}")
    return value

# Clickhouse Client Configuration
def setup_clickhouse_client(username, password, host):
    if not all([username, password, host]):
        raise ValueError("Missing ClickHouse environment variables- username/ password/ host.")
    return clickhouse_connect.get_client(
        host=host,
        user=username,
        password=password,
        secure=True
    )

def create_table_if_not_exists(client, table_name, schema, engine= "MergeTree()"):
    """
    Create a ClickHouse table if it doesn't exist.
    
    Args:
        client: ClickHouse client instance
        table_name: Name of the table to create
        schema: Dictionary mapping column names to their definitions
        engine: Storage engine (default: MergeTree)
    
    Returns:
        bool: True if table was created, False if it already existed
    """
    
    try:
        client.command(f"DROP TABLE IF EXISTS {table_name} SYNC")
    except Exception as e:
        print(f"Warning: Could not drop existing table: {e}")
        raise
    
    # Build column definitions
    columns = ",\n    ".join([f"{col} {defn}" for col, defn in schema.items()])
    
    # Build and execute CREATE TABLE statement
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns}
        ) ENGINE = {engine}
        PRIMARY KEY (id);
    """
    
    try:
        client.command(create_sql)
        print(f"Table '{table_name}' created successfully.")

    except Exception as e:
        print(f"Error creating table '{table_name}': {e}")
        raise