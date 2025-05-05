import argparse
import requests
import duckdb
import pandas as pd
from datetime import datetime, timedelta
import time
import config
import sys
import logging

# setup logging
log = logging.getLogger(f"ash-terminal")
log_level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
log_format = "%(asctime)s [%(levelname)s] [%(name)s] [%(funcName)s] %(message)s"
if config.LOG_TO_FILE:
    logging.basicConfig(
        level=log_level,
        format=log_format,
        filename=config.LOG_FILE_PATH,
        filemode='a'
    )
else:
    logging.basicConfig(
        level=log_level,
        format=log_format
    )

def logcritical_and_exit(err):
    log.critical(err)
    sys.exit(-1)

def fetch_symbol_data(symbol, dates, interval, db_file, latest='compact'):
    if len(dates) == 0:
        # fetch the latest datapoints
        last_x = "30 days" if latest == 'full' else "100 datapoints"
        log.info(f"Fetching LATEST {last_x} for {symbol}...")
        try:
            df = fetch_intraday(symbol, interval, month="", outputsize=latest)
        except Exception as e:
            log.error(f"Failed to fetch LATEST 100 datapoints for {symbol}: {e}", exc_info=True)
            return
        if not df.empty:
            new = store_to_duckdb(df, db_file)
            log.info(f"Stored LATEST {new} new records")
        else:
            log.warning(f"Got an empty result for the LATEST 100 datapoints for {symbol}")
        time.sleep(1)
        return

    # fetch data for partucular month
    for date in dates:
        try:
            df = fetch_intraday(symbol, interval, date)
        except Exception as e:
            log.error(f"Failed to fetch data for {symbol} {date}: {e}", exc_info=True)
            continue
        if not df.empty:
            new = store_to_duckdb(df, db_file)
            log.info(f"Stored {new} new records for {date}")
        else:
            log.warning(f"Failed to fetch: {symbol} for {date}! Moving on with other dates...")
        time.sleep(1)

# Fetch intraday data for a specific ticker.
# symbol: the symbol of the ticker we are fetching data for
# interval: the interval of the intraday data points ('1min', '5min', '15min', '30min', '60min')
# month: If not empty (''), speficies a specific month ('YYYY-MM') for which to get the data
# outputsize: if month is not set (''), outputsize defines if we are to get the most recent 30 days ('full') or the last 100 datapoints ('compact')
# retries: defines how many times to retry the API request if it fails or times out
# backoff: prevents overthrottling by extending the sleeps between retries (if calls to the API fail). The sleep is (backoff ** retry) seconds
def fetch_intraday(symbol, interval, month, outputsize='compact', retries=3, backoff=2):
    log.info(f"Fetch intraday data for: {symbol}, {interval}, {month}")
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": interval,
        "adjusted": "true", # default is true 
        "extended_hours": "true", # default is true
        "month": month,  # YYYY-MM. If left EMPTY, the default is the last 100 datapoints (based on interval)
        "entitlement": "delayed", # depends on the pay plan, if left empty data is from the previous day!
        "outputsize": outputsize, # 'compact' (defualt) returns last 100 data points, 'full' returns the last 30 days
        "datatype": "json", # data format is either 'json' (default) or 'csv'
        "apikey": config.ALPHA_API_KEY
    }
    
    #retry the API call with backoffs
    for attempt in range(retries):
        try:
            # Fetch the data
            response = requests.get(config.BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Extract time series data
            # The actual time series key varies: "Time Series (1min)", "Time Series (5min)", etc.
            time_series_key = next((k for k in data.keys() if "Time Series" in k), None)
            if not time_series_key:
                err = f"Error fetching data: {data.get('Note') or data.get('Error Message') or data}"
                log.debug(err)
                raise ValueError(err)

            # Convert to DataFrame
            df = pd.DataFrame.from_dict(data[time_series_key], orient="index")
            df.index.name = "timestamp"
            df = df.rename(columns=lambda x: x.split('. ')[1])  # Clean up column names ("1. open" -> "open")

            # Convert data types
            df = df.reset_index()
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            for col in ["open", "high", "low", "close"]:
                df[col] = pd.to_numeric(df[col])
            df["volume"] = pd.to_numeric(df["volume"], downcast="integer")
            log.debug(f"\n{df}")
            return df
        except Exception as e:
                log.warning(f"Attempt {attempt + 1}/{retries} failed for {symbol}: {e}", exc_info=True)
                time.sleep(backoff ** attempt)
    logcritical_and_exit(f"All retries failed for {symbol} ({month})")

# Stores a pandas dataframe with OHLCV data to the DB of a spefific ticker
# df: data frame with the OHLCV data
# dc_file: the database in which to store the data 
def store_to_duckdb(df, db_file):
    try:
        con = duckdb.connect(db_file)
    except Exception as e:
        logcritical_and_exit(f"Failed to open the db [{db_file}]: {e}")

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
        log.debug(f"Table {config.TABLE_NAME} already exists")

    try:
        # query the latest existing timestamp
        last = f"(SELECT timestamp FROM {config.TABLE_NAME} ORDER BY timestamp DESC LIMIT 1)"
        con.register("df_temp", df) # treat the dataframe as a sql db
        new_records = con.execute(f"SELECT COUNT(*) FROM df_temp WHERE timestamp > {last}").fetchall()[0][0]
        log.debug(f"New records in fetched data: {new_records}")
        if new_records > 0:
            con.execute(f"INSERT INTO {config.TABLE_NAME} SELECT * FROM df_temp WHERE timestamp > {last}")
        con.close()
        return new_records
    except Exception as e:
        log.error(f"Storing DB data went wrong: {e}", exc_info=True)

# return a list of months ('YYYY-MM') for which to fetch the data
def get_dates_for_year(year):
    slices = []
    months = ["{:02}".format(m) for m in range(1, 13)]
    for i, month in enumerate(months):
        slices.append(f"{year}-{month}")
    log.debug(f"Slices for year {year}: {slices}")
    return slices

# Validate the year string is in the format 'YYYY' and is a valid year between 1950 and "now"
def validate_year(str):
    err = f"'year' must be a 4 digit year between 1950 and {datetime.now().year}, e.g. '2023'."
    year = int(str)
    if len(str) != 4 or year < 1950 or year > datetime.now().year:
        log.error(err)
        raise ValueError(err)

# Validate the month string is in the format 'YYYY-MM' and is a valid year between 1950 and "now" and a valid month
def validate_year_month(str):
    err = f"'month' must be in the format 'YYYY-MM' and be a valid month between 1950-01 and {datetime.now().year}-{datetime.now().strftime('%m')}, e.g. '2023-07'."
    if len(str) != 7 or str[4] != '-':
        log.error(err)
        raise ValueError(err)
    month = int(str[5:])
    year = int(str[:4])
    if year < 1950 or year > datetime.now().year or month > 12 or month < 1 or (year == datetime.now().year and month > datetime.now().month):
        log.error(err)
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
        # generate a list of YYYY-MM values as the AlphaVantage API uses months to extract historical data
        dates = get_dates_for_year(args.year)
    elif args.month:
        validate_year_month(args.month)
        # fetch data for a specific/single month
        dates = [args.month]
    elif args.date:
        # fetch data for a specific day
        logcritical_and_exit("Fetching specific day data is NOT IMPLEMENTED YET!")    
    
    log.debug(f"Dates: {dates}")
    
    if not args.symbol:
        # fetch the data for the symbols defined in the config
        log.info(f"Fetching config symbols: {config.SYMBOLS}!")
        for symbol in config.SYMBOLS:
            db_file = f"{config.DATABASE_DIR}/{symbol}_intraday.duckdb"
            fetch_symbol_data(symbol, dates, args.interval, db_file)
    else:
        symbol = args.symbol.upper()
        db_file = f"{config.DATABASE_DIR}/{symbol}_intraday.duckdb"
        log.info(f"Fetching data for symbol: {symbol}!")
        fetch_symbol_data(symbol, dates, args.interval, db_file)
