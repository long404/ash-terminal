import argparse
import requests
import duckdb
import pandas as pd
from datetime import datetime, timedelta
import time
from config import BASE_URL, ALPHA_API_KEY, SYMBOLS, DATABASE_DIR, TABLE_NAME

def debug_and_exit(print_obj):
    print(print_obj)
    raise SystemExit()

def update_all_symbols():
    for symbol in SYMBOLS:
        data = fetch_symbol_data(symbol)
        # Add parsing and insertion logic here
        print(f"Fetched data for {symbol}")

# TODO: merge update_db with this
def fetch_intraday(symbol, interval, month):
    print(f"Fetch for: {symbol}, {interval}, {month}")
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": interval,
        "adjusted": "true",
        "extended_hours": "true",
        "month": month,
        "outputsize": "full",
        "datatype": "json",
        "apikey": ALPHA_API_KEY
    }
    
    # Fetch the data
    response = requests.get(BASE_URL, params=params)
    data = response.json()

    # Extract time series data
    # The actual time series key varies: "Time Series (1min)", "Time Series (5min)", etc.
    time_series_key = next((k for k in data.keys() if "Time Series" in k), None)
    if not time_series_key:
        raise Exception(f"Error fetching data: {data.get('Note') or data.get('Error Message') or data}")

    # Convert to DataFrame
    df = pd.DataFrame.from_dict(data[time_series_key], orient="index")
    df.index.name = "timestamp"
    df = df.rename(columns=lambda x: x.split('. ')[1])  # Clean up column names ("1. open" → "open")

    # Convert data types
    df = df.reset_index()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col])
    df["volume"] = pd.to_numeric(df["volume"], downcast="integer")
    print("============================================")
    print(df)
    return df

def store_to_duckdb(df, db_file):
    try:
        con = duckdb.connect(db_file)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                timestamp TIMESTAMP UNIQUE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT
            )
        """)
        con.register("df_temp", df)
        con.execute(f"INSERT INTO {TABLE_NAME} SELECT * FROM df_temp")
        con.close()
    except Exception as e:
        print(f"Storing data went wrong: {e}")

def get_slices_for_year(year):
    slices = []
    months = ["{:02}".format(m) for m in range(1, 13)]
    for i, month in enumerate(months):
        slices.append(f"{year}-{month}")
    #print(f"Slices for year {year}: ")
    #print(slices)
    return slices

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download intraday data per ticker and store in separate DuckDB files.")
    parser.add_argument("symbol", help="Ticker symbol, e.g. TQQQ")
    parser.add_argument("year", help="Year to fetch data for (e.g., 2024)")
    parser.add_argument("--interval", default="1min", help="Interval (e.g., 1min, 5min, 15min)")
    args = parser.parse_args()

    symbol = args.symbol.upper()
    dates = get_slices_for_year(args.year)
    db_file = f"{DATABASE_DIR}/{symbol}_intraday.duckdb"

    for date in dates:
        print(f"Fetching {symbol} {date}...")
        try:
            df = fetch_intraday(symbol, args.interval, date)
        except Exception as e:
            print(f"ERR: Failed to fetch data for {symbol} {date}!")
            continue
        if not df.empty:
            print(df)
            #df["timestamp"] = pd.to_datetime(df["time"])
            #df = df.drop(columns=["time"])
            store_to_duckdb(df, db_file)
            print(f"Stored {len(df)} records for {date}")
        else:
            debug_and_exit(f"Failed to fetch: {symbol}, {date}")
        time.sleep(1)