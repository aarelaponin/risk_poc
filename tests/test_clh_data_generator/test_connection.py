"""Tests for ClickHouse connection functions."""
from unittest.mock import MagicMock, Mock, patch

import pytest

from risk_poc.clh_data_generator import (
    connect_to_clickhouse,
    list_tables,
    print_tables_formatted,
)


@patch("risk_poc.clh_data_generator.clickhouse_connect.get_client")
def test_connect_to_clickhouse_success(mock_get_client):
    """Test successful connection to ClickHouse."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    result = connect_to_clickhouse(
        host="localhost",
        port=8123,
        database="risk_poc",
        user="default",
        password="",
    )

    assert result == mock_client
    mock_get_client.assert_called_once_with(
        host="localhost",
        port=8123,
        database="risk_poc",
        username="default",
        password="",
    )


@patch("risk_poc.clh_data_generator.clickhouse_connect.get_client")
def test_connect_to_clickhouse_failure(mock_get_client):
    """Test failed connection to ClickHouse."""
    mock_get_client.side_effect = Exception("Connection refused")

    with pytest.raises(Exception) as exc_info:
        connect_to_clickhouse()

    assert "Connection refused" in str(exc_info.value)


def test_list_tables_success():
    """Test listing tables successfully."""
    mock_client = Mock()
    mock_result = Mock()
    mock_result.result_rows = [["table1"], ["table2"], ["table3"]]
    mock_client.query.return_value = mock_result

    result = list_tables(mock_client)

    assert result == ["table1", "table2", "table3"]
    mock_client.query.assert_called_once_with("SHOW TABLES")


def test_list_tables_with_database():
    """Test listing tables from specific database."""
    mock_client = Mock()
    mock_result = Mock()
    mock_result.result_rows = [["table1"], ["table2"]]
    mock_client.query.return_value = mock_result

    result = list_tables(mock_client, "test_db")

    assert result == ["table1", "table2"]
    mock_client.query.assert_called_once_with("SHOW TABLES FROM test_db")


def test_list_tables_error():
    """Test error handling when listing tables fails."""
    mock_client = Mock()
    mock_client.query.side_effect = Exception("Query failed")

    with pytest.raises(Exception) as exc_info:
        list_tables(mock_client)

    assert "Query failed" in str(exc_info.value)


@patch("risk_poc.clh_data_generator.console")
def test_print_tables_formatted_with_tables(mock_console):
    """Test printing formatted table with tables."""
    tables = ["users", "transactions", "logs"]
    print_tables_formatted(tables, "risk_poc")

    # Check that console.print was called
    assert mock_console.print.called


@patch("risk_poc.clh_data_generator.console")
def test_print_tables_formatted_empty(mock_console):
    """Test printing formatted table with no tables."""
    tables = []
    print_tables_formatted(tables, "risk_poc")

    # Check that warning message was printed
    mock_console.print.assert_called_with(
        "[yellow]No tables found in database 'risk_poc'[/yellow]"
    )
