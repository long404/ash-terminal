import duckdb
import os
from config import DATABASE_DIR

def load_history(symbol, from_ts, to_ts):
    db_path = os.path.join(DATABASE_DIR, f"{symbol}.duckdb")
    con = duckdb.connect(db_path)
    query = f"SELECT * FROM price_data WHERE timestamp BETWEEN '{from_ts}' AND '{to_ts}' ORDER BY timestamp;"
    result = con.execute(query).fetchall()
    con.close()
    return result
