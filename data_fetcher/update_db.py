import duckdb
import os
import requests
from config import API_KEY, SYMBOLS, DATABASE_DIR

def fetch_symbol_data(symbol):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=1min&apikey={API_KEY}&outputsize=full"
    response = requests.get(url)
    return response.json()

def update_all_symbols():
    for symbol in SYMBOLS:
        data = fetch_symbol_data(symbol)
        # Add parsing and insertion logic here
        print(f"Fetched data for {symbol}")

