
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from fetch_data import fetch_intraday, fetch_symbol_data, store_to_duckdb
import duckdb

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
    assert "symbol" in df.columns
    assert df.loc[0, "symbol"] == "AAPL"

@patch("fetch_data.requests.get")
def test_fetch_intraday_retry_on_failure(mock_get):
    # Simulate failure, then success
    fail_response = MagicMock()
    fail_response.status_code = 503
    fail_response.raise_for_status.side_effect = Exception("Service Unavailable")

    success_response = MagicMock()
    success_response.status_code = 200
    success_response.json.return_value = {
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

    mock_get.side_effect = [fail_response, success_response]

    df = fetch_intraday("AAPL", "5min", "", retries=2)
    assert not df.empty

def test_store_to_duckdb_inserts_and_skips_duplicates(tmp_path):
    db_path = tmp_path / "test.duckdb"
    df = pd.DataFrame({
        "timestamp": pd.to_datetime(["2024-01-01 09:30:00", "2024-01-01 09:35:00"]),
        "open": [100, 101],
        "high": [101, 102],
        "low": [99, 100],
        "close": [100.5, 101.5],
        "volume": [10000, 11000],
        "symbol": ["AAPL", "AAPL"]
    })

    # First insert
    new_count = store_to_duckdb(df.copy(), str(db_path))
    assert new_count == 2

    # Duplicate insert
    new_count = store_to_duckdb(df.copy(), str(db_path))
    assert new_count == 0

@patch("fetch_data.fetch_intraday")
@patch("fetch_data.store_to_duckdb")
def test_fetch_symbol_data_calls(mock_store, mock_fetch):
    mock_fetch.return_value = pd.DataFrame({
        "timestamp": pd.to_datetime(["2024-01-01 09:30:00"]),
        "open": [100], "high": [101], "low": [99],
        "close": [100.5], "volume": [10000], "symbol": ["AAPL"]
    })

    fetch_symbol_data("AAPL", ["2024-01"], "5min", ":memory:")
    assert mock_fetch.called
    assert mock_store.called
