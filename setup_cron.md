# Setting Up the Cron Job
1. Edit `data_fetcher/config.py` to set your Alpha Vantage API key and symbols.
2. Create a cron job:
    @daily /path/to/python /path/to/financial_terminal/data_fetcher/fetch_data.py
