from flask import Flask, request, jsonify
import db_access

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
