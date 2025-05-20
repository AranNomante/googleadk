"""ClickHouse client module providing database connection and query functionality."""

import clickhouse_connect
from typing import List, Dict, Any, Optional
from .config import CLICKHOUSE


class ClickHouseClient:
    """Singleton class for managing ClickHouse client connection."""

    _instance = None

    @classmethod
    def get_instance(cls):
        """Returns the singleton ClickHouse client instance, creating it if necessary."""
        if cls._instance is None:
            cls._instance = clickhouse_connect.get_client(
                host=CLICKHOUSE["host"],
                port=CLICKHOUSE["port"],
                user=CLICKHOUSE["user"],
                password=CLICKHOUSE["password"],
                database=CLICKHOUSE["database"],
                secure=True,
            )
        return cls._instance


def execute_clickhouse_query(
    sql: str, params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Execute a SQL query against ClickHouse and return the results in a structured format.

    Args:
        sql: The SQL query to execute
        params: Optional parameters for the query

    Returns:
        Dict containing:
        - success: bool indicating if query executed successfully
        - results: List of result rows as dictionaries
        - error: Error message if query failed
        - columns: List of column names
    """
    try:
        client = ClickHouseClient.get_instance()
        result = client.query(sql, params or {})

        if result:
            columns = [desc[0] for desc in result.column_names]
            rows = [dict(zip(columns, row)) for row in result.result_rows]
            return {"success": True, "results": rows, "columns": columns, "error": None}
        return {"success": True, "results": [], "columns": [], "error": None}
    except Exception as e:
        return {"success": False, "results": [], "columns": [], "error": str(e)}


def get_schema_info() -> Dict[str, Any]:
    """
    Get the current database schema information.

    Returns:
        Dict containing schema information in a structured format
    """
    schema_query = """
    SELECT 
        table,
        name,
        type
    FROM system.columns 
    WHERE database = currentDatabase()
    ORDER BY table, position
    """
    return execute_clickhouse_query(schema_query)


def get_table_stats() -> Dict[str, Any]:
    """
    Get statistics about tables in the current database.

    Returns:
        Dict containing table statistics in a structured format
    """
    table_stats_query = """
    SELECT 
        name as table,
        total_rows as row_count
    FROM system.tables 
    WHERE database = currentDatabase()
    """
    return execute_clickhouse_query(table_stats_query)
