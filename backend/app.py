from flask import Flask, request, jsonify
import db_access
import logging
import config
import sys

# setup logging
log = logging.getLogger(f"ash-be")
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


# =========== Backend ===========
app = Flask(__name__)

@app.route('/api/history')
def get_history():
    symbol = request.args.get('symbol')
    from_ts = request.args.get('from')
    to_ts = request.args.get('to')
    data = db_access.load_history(symbol, from_ts, to_ts)
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)
