from flask import Flask, jsonify, send_from_directory
from engine import build_report

app = Flask(__name__, static_folder="public")


@app.get('/api/report')
def api_report():
    return jsonify(build_report())


@app.get('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=False)
