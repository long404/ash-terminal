import duckdb
import os
import config

def load_history(symbol, from_ts, to_ts):
    db_path = os.path.join(config.DATABASE_DIR, f"{symbol}_intraday.duckdb")
    con = duckdb.connect(db_path)
    query = f"SELECT * FROM {config.TABLE_NAME} WHERE timestamp BETWEEN '{from_ts}' AND '{to_ts}' ORDER BY timestamp;"
    result = con.execute(query).fetchall()
    con.close()
    return result
