import argparse
import requests
import duckdb
import pandas as pd
from datetime import datetime, timedelta
import time
import config

def debug_and_exit(print_obj):
    if config.DEBUG == 1:
        print(f"404: {print_obj}")
        raise SystemExit()

def debug(print_obj):
    if config.DEBUG == 1:
        print(f"404: {print_obj}")

def fetch_symbol_data(symbol, dates, interval, db_file):
    if len(dates) == 0:
        # fetch the last 100 datapoints
        debug(f"Fetching LATEST {symbol} data...")
        try:
            df = fetch_intraday(symbol, interval, "")
        except Exception as e:
            print(f"ERR: Failed to fetch LATEST 100 datapoints for {symbol}!")
            return
        if not df.empty:
            debug(df)
            #df["timestamp"] = pd.to_datetime(df["time"])
            #df = df.drop(columns=["time"])
            store_to_duckdb(df, db_file)
            debug(f"Stored LATEST {len(df)} records")
        else:
            debug_and_exit(f"Failed to fetch LATEST {symbol}")
        time.sleep(1)
        return

    # fetch data for partucular month
    for date in dates:
        try:
            df = fetch_intraday(symbol, interval, date)
        except Exception as e:
            print(f"ERR: Failed to fetch data for {symbol} {date}!")
            continue
        if not df.empty:
            print(df)
            #df["timestamp"] = pd.to_datetime(df["time"])
            #df = df.drop(columns=["time"])
            store_to_duckdb(df, db_file)
            debug(f"Stored {len(df)} records for {date}")
        else:
            debug_and_exit(f"Failed to fetch: {symbol}, {date}")
        time.sleep(1)


def update_all_symbols():
    for symbol in config.SYMBOLS:
        data = fetch_symbol_data(symbol)
        #TODO Add parsing and insertion logic here
        print(f"Fetched data for {symbol}")

# Leave the month empty to get the latest 100 data points (e.g. minutes)
def fetch_intraday(symbol, interval, month):
    print(f"Fetch for: {symbol}, {interval}, {month}")
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": interval,
        "adjusted": "true", # default is true 
        "extended_hours": "true", # default is true
        "month": month,  # YYYY-MM. If left EMPTY, the default is the last 100 datapoints (based on interval)
        "entitlement": "delayed", # depends on the pay plan, if left empty data is from the previous day!
        "outputsize": "full", # 'compact' (defualt) returns last 100 data points, 'full' returns last 30 days
        "datatype": "json", # 'json (default) or 'csv'
        "apikey": config.ALPHA_API_KEY
    }
    
    # Fetch the data
    response = requests.get(config.BASE_URL, params=params)
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
    except Exception as e:
        debug_and_exit(f"Failed to open the db [{db_file}]")

    try:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {config.TABLE_NAME} (
                timestamp TIMESTAMP UNIQUE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT
            )
        """)
    except Exception as e:
        debug(f"Table {config.TABLE_NAME} already exists")

    try:
        # query the latest existing timestamp
        last = f"(SELECT timestamp FROM {config.TABLE_NAME} ORDER BY timestamp DESC LIMIT 1)"
        con.register("df_temp", df)
        con.execute(f"INSERT INTO {config.TABLE_NAME} SELECT * FROM df_temp WHERE timestamp > {last}")
        con.close()
    except Exception as e:
        print(f"Storing data went wrong: {e}")

def get_dates_for_year(year):
    slices = []
    months = ["{:02}".format(m) for m in range(1, 13)]
    for i, month in enumerate(months):
        slices.append(f"{year}-{month}")
    #print(f"Slices for year {year}: ")
    #print(slices)
    return slices

# Validate the year string is in the format 'YYYY' and is a valid year between 1950 and "now"
def validate_year(str):
    err = f"'year' must be a 4 digit year between 1950 and {datetime.now().year}, e.g. '2023'."
    if len(str) != 4:
        raise ValueError(err)
    year = int(str)
    # validate
    if year < 1950 or year > datetime.now().year:
        raise ValueError(err)

# Validate the month string is in the format 'YYYY-MM' and is a valid year between 1950 and "now" and a valid month
def validate_year_month(str):
    err = f"'month' must be in the format 'yyyy-mm' and be a valid month between 1950-01 and {datetime.now().year}-{datetime.now().strftime('%m')}, e.g. '2023-07'."
    if len(str) != 7 or str[4] != '-':
        raise ValueError(err)
    month = int(str[5:])
    year = int(str[:4])
    if year < 1950 or year > datetime.now().year or month > 12 or month < 1 or (year == datetime.now().year and month > datetime.now().month):
        raise ValueError(err)
    
def parse_config():
    parser = argparse.ArgumentParser(description="Download intraday data per symbol and store in the database.")
    parser.add_argument("--symbol", help="Ticker symbol (e.g. AMZN), if empty will get the data for all symbols in the config.")
    parser.add_argument("--year", help="Year to fetch data for (e.g., 2024)")
    parser.add_argument("--month", help="Month to fetch data for (e.g., 2024-02)")
    parser.add_argument("--date", help="Day to fetch data for (e.g., 2024-02-23)")
    parser.add_argument("--interval", default="1min", help="Interval (e.g., 1min, 5min, 15min)")
    return parser.parse_args()
    
if __name__ == "__main__":
    args = parse_config()

    dates = []
    if args.year:
        validate_year(args.year)    
        # generate a list of yyyy-mm values as the AlphaVantage API uses months to extract historical data
        dates = get_dates_for_year(args.year)
    elif args.month:
        validate_year_month(args.month)
        # single month "list"
        dates = [args.month]
    elif args.date:
        debug_and_exit("NOT IMPLEMENTED YET!")    
    
    debug(f"dates {dates}")
    
    if not args.symbol:
        # fetch the data for the symbols defined in the config
        print(f"Fetching config symbols: {config.SYMBOLS}!")
        for symbol in config.SYMBOLS:
            db_file = f"{config.DATABASE_DIR}/{symbol}_intraday.duckdb"
            fetch_symbol_data(symbol, dates, args.interval, db_file)
    else:
        symbol = args.symbol.upper()
        db_file = f"{config.DATABASE_DIR}/{symbol}_intraday.duckdb"
        print(f"Fetching data for symbol: {symbol}!")
        fetch_symbol_data(symbol, dates, args.interval, db_file)
