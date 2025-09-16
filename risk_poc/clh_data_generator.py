"""ClickHouse data generator module."""

from typing import List, Optional

import clickhouse_connect
from clickhouse_connect.driver import Client
from rich.console import Console
from rich.table import Table

console = Console()


def connect_to_clickhouse(
    host: str = "localhost",
    port: int = 8123,
    database: str = "risk_poc",
    user: str = "default",
    password: str = "",
) -> Client:
    """Connect to ClickHouse server.

    Args:
        host: ClickHouse server host
        port: ClickHouse server port
        database: Database name
        user: Username for authentication
        password: Password for authentication

    Returns:
        ClickHouse client connection

    Raises:
        Exception: If connection fails
    """
    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            database=database,
            username=user,
            password=password,
        )
        console.print(f"[green]Successfully connected to ClickHouse at {host}:{port}[/green]")
        return client
    except Exception as e:
        console.print(f"[red]Failed to connect to ClickHouse: {e}[/red]")
        raise


def list_tables(client: Client, database: Optional[str] = None) -> list[str]:
    """List all tables in the specified database.

    Args:
        client: ClickHouse client connection
        database: Database name (uses current database if None)

    Returns:
        List of table names
    """
    try:
        if database:
            query = f"SHOW TABLES FROM {database}"
        else:
            query = "SHOW TABLES"

        result = client.query(query)
        tables = [row[0] for row in result.result_rows]
        return tables
    except Exception as e:
        console.print(f"[red]Error listing tables: {e}[/red]")
        raise


def print_tables_formatted(tables: list[str], database: str = "risk_poc") -> None:
    """Print tables in a formatted table.

    Args:
        tables: List of table names
        database: Database name
    """
    if not tables:
        console.print(f"[yellow]No tables found in database '{database}'[/yellow]")
        return

    table = Table(title=f"Tables in database '{database}'")
    table.add_column("Table Name", style="cyan", no_wrap=True)

    for table_name in sorted(tables):
        table.add_row(table_name)

    console.print(table)
    console.print(f"\n[green]Total tables: {len(tables)}[/green]")


def test_connection() -> None:
    """Test connection to ClickHouse and list tables."""
    try:
        client = connect_to_clickhouse()
        tables = list_tables(client)
        print_tables_formatted(tables)
    except Exception as e:
        console.print(f"[red]Connection test failed: {e}[/red]")
        raise
