
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from fetch_data import fetch_intraday, fetch_symbol_data, store_to_duckdb
import duckdb

# Test successful API response
@patch("fetch_data.requests.get")
def test_fetch_intraday_success(mock_get):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "Time Series (5min)": {
            "2024-01-01 09:30:00": {
                "1. open": "100",
                "2. high": "101",
                "3. low": "99",
                "4. close": "100.5",
                "5. volume": "10000"
            }
        }
    }

    df = fetch_intraday("AAPL", "5min", "")
    assert not df.empty
    assert "timestamp" in df.columns
    assert "1. open" in df.columns

# Test retry logic on API failure
@patch("fetch_data.requests.get")
def test_fetch_intraday_retry_on_failure(mock_get):
    fail = MagicMock()
    fail.status_code = 503
    fail.raise_for_status.side_effect = Exception("Service Unavailable")

    success = MagicMock()
    success.status_code = 200
    success.json.return_value = {
        "Time Series (5min)": {
            "2024-01-01 09:30:00": {
                "1. open": "100",
                "2. high": "101",
                "3. low": "99",
                "4. close": "100.5",
                "5. volume": "10000"
            }
        }
    }

    mock_get.side_effect = [fail, success]

    df = fetch_intraday("AAPL", "5min", "", retries=2)
    assert not df.empty
    assert "timestamp" in df.columns

# Test DuckDB insert and deduplication
def test_store_to_duckdb_insert_and_dedup(tmp_path):
    db_file = tmp_path / "test.duckdb"
    df = pd.DataFrame({
        "timestamp": pd.to_datetime(["2024-01-01 09:30:00", "2024-01-01 09:35:00"]),
        "open": [100, 101],
        "high": [101, 102],
        "low": [99, 100],
        "close": [100.5, 101.5],
        "volume": [10000, 11000]
    })

    inserted = store_to_duckdb(df.copy(), str(db_file))
    assert inserted == 2

    # Try inserting the same data again
    inserted_again = store_to_duckdb(df.copy(), str(db_file))
    assert inserted_again == 0

# Integration test: fetch_symbol_data orchestrates calls
@patch("fetch_data.fetch_intraday")
@patch("fetch_data.store_to_duckdb")
def test_fetch_symbol_data_flow(mock_store, mock_fetch):
    mock_fetch.return_value = pd.DataFrame({
        "timestamp": pd.to_datetime(["2024-01-01 09:30:00"]),
        "open": [100], "high": [101], "low": [99],
        "close": [100.5], "volume": [10000]
    })

    fetch_symbol_data("AAPL", ["2024-01"], "5min", ":memory:")
    mock_fetch.assert_called_once()
    mock_store.assert_called_once()
