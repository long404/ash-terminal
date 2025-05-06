from flask import Flask, request, jsonify, abort, g
import db_access
from datetime import datetime
import time
import config
import requests
import pandas as pd

app = Flask("ash-backend")

# =========== Helpers ===========
def log_error_and_abort(msg):
    app.logger.error(f"Error: {msg}")
    abort(400, description=msg)

def profile(msg='Ellapsed:'):
    elapsed_ms = (time.perf_counter() - g.start_ts) * 1000
    app.logger.debug(f"{msg} {elapsed_ms:.0f} ms")

@app.before_request
def log_request():
    g.start_ts = time.perf_counter()
    app.logger.info(f"{request.method} {request.path} {request.args}")

@app.after_request
def finish_time(response):
    profile("Done: ")
    return response

@app.route('/favicon.ico')
def favicon():
    return '', 204

@app.route('/api/current')
def get_current_price():

    symbol = request.args.get('symbol')
    app.logger.debug(f"Symbol: {symbol}")
    if not symbol:
        log_error_and_abort("Error: the 'symbol' argument is mandatory!")
    
    app.logger.info(f"Fetch latest price for: {symbol}")
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": "1min",
        "adjusted": "true", # default is true 
        "extended_hours": "true", # default is true
        "month": '',  # get the latest 100 data points
        "entitlement": "delayed", # depends on the pay plan, if left empty data is from the previous day!
        "outputsize": 'compact', # get the latest 100 data points
        "datatype": "json", # data format is either 'json' (default) or 'csv'
        "apikey": config.ALPHA_API_KEY
    }
    
    retries = 3
    backoff = 2
    #retry the API call with backoffs
    for attempt in range(retries):
        try:
            # Fetch the data
            response = requests.get(config.BASE_URL, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()
            profile("After API call:")

            # Extract time series data
            # The actual time series key varies: "Time Series (1min)", "Time Series (5min)", etc.
            time_series_key = next((k for k in data.keys() if "Time Series" in k), None)
            if not time_series_key:
                err = f"Error fetching latest price for {symbol}: {data.get('Note') or data.get('Error Message') or data}"
                log_error_and_abort(err)

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
            app.logger.debug(f"======\n{df.iloc[0]}\n=======")
            
            profile("After data processing call:")
            return jsonify(df.iloc[0].to_dict()) # return just the latest entry
        except Exception as e:
                app.logger.warning(f"Attempt {attempt + 1}/{retries} failed for {symbol}: {e}", exc_info=True)
                time.sleep(backoff ** attempt)
    log_error_and_abort(f"Latest price fetch retries failed for {symbol}")

@app.route('/api/history')
def get_history():

    app.logger.debug("Start...")
    start_ts = time.perf_counter()

    app.logger.debug(f"Symbol: {request.args.get('symbol')}")
    app.logger.debug(f"From date: {request.args.get('from')}")
    app.logger.debug(f"To date: {request.args.get('to')}")

    symbol = request.args.get('symbol')
    from_ts = request.args.get('from')
    to_ts = request.args.get('to')

    if not symbol:
        log_error_and_abort("Error: the 'symbol' argument is mandatory!")
    
    if not to_ts:
        # if not specified, consider it now()
        to_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        app.logger.info(f"Got empty 'to' argument. Using {to_ts}!")

    if not from_ts:
        # if not specified, consider it the start of today
        from_ts = datetime.now().strftime("%Y-%m-%d 00:00:01")
        app.logger.info(f"Got empty 'from' argument. Using {from_ts}!")

    try:
        from_dt = datetime.fromisoformat(from_ts)
        to_dt = datetime.fromisoformat(to_ts)
    except ValueError:
        abort(400, description="Invalid datetime format. Use ISO 8601 (YYYY-MM-DDTHH:MM:SS)")
    
    data = db_access.load_history(symbol, from_ts, to_ts)

    elapsed_ms = (time.perf_counter() - start_ts) * 1000
    app.logger.debug(f"Done... {elapsed_ms:.0f} ms")
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)
