# type: ignore[attr-defined]
from typing import Optional

import typer
from rich.console import Console

from . import version
from .clh_data_generator import (
    connect_to_clickhouse,
    list_tables,
    print_tables_formatted,
)

app = typer.Typer(
    name="risk_poc",
    help="ClickHouse risk evaluation data generator",
    add_completion=False,
)
console = Console()


def version_callback(print_version: bool) -> None:
    """Print the version of the package."""
    if print_version:
        console.print(f"[yellow]risk_poc[/] version: [bold blue]{version}[/]")
        raise typer.Exit()


@app.command()
def list_tables_cmd(
    host: str = typer.Option("localhost", help="ClickHouse server host"),
    port: int = typer.Option(8123, help="ClickHouse server port"),
    database: str = typer.Option("risk_poc", help="Database name"),
    user: str = typer.Option("default", help="Username for authentication"),
    password: str = typer.Option("", help="Password for authentication"),
) -> None:
    """Connect to ClickHouse and list all tables in the database."""
    try:
        client = connect_to_clickhouse(host, port, database, user, password)
        tables = list_tables(client, database)
        print_tables_formatted(tables, database)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(code=1)


@app.command()
def test_connection(
    host: str = typer.Option("localhost", help="ClickHouse server host"),
    port: int = typer.Option(8123, help="ClickHouse server port"),
    database: str = typer.Option("risk_poc", help="Database name"),
    user: str = typer.Option("default", help="Username for authentication"),
    password: str = typer.Option("", help="Password for authentication"),
) -> None:
    """Test connection to ClickHouse server."""
    try:
        client = connect_to_clickhouse(host, port, database, user, password)
        console.print(f"[green]✓ Connection successful![/green]")
        console.print(f"[cyan]Server info: {client.query('SELECT version()').result_rows[0][0]}[/cyan]")
    except Exception as e:
        console.print(f"[red]✗ Connection failed: {e}[/red]")
        raise typer.Exit(code=1)


@app.callback()
def main(
    version: bool = typer.Option(
        None,
        "-v",
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Print version and exit.",
    ),
) -> None:
    """ClickHouse risk evaluation data generator."""
    pass


if __name__ == "__main__":
    app()
