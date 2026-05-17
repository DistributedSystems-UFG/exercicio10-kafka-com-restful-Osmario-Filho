"""(3) Consumidor + Web Service RESTful (Flask).

- Consome estatisticas do TOPIC_STATS (Kafka) em uma thread em background
  e armazena em SQLite.
- Expoe endpoints HTTP/JSON para clientes consultarem ultima leitura,
  historico, sensores conhecidos e contagem total.
"""
import json
import logging
import sqlite3
import threading

from flask import Flask, jsonify, request
from kafka import KafkaConsumer

import const


# ============================================================
# Camada de persistencia (SQLite)
# ============================================================
class StatsDB:
    def __init__(self, path: str):
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute('''
            CREATE TABLE IF NOT EXISTS temperature_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id TEXT NOT NULL,
                avg_temperature REAL NOT NULL,
                min_temperature REAL NOT NULL,
                max_temperature REAL NOT NULL,
                sample_count INTEGER NOT NULL,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                computed_at TEXT NOT NULL
            )
        ''')
        self._conn.commit()

    def insert(self, s: dict):
        with self._lock:
            self._conn.execute(
                'INSERT INTO temperature_stats '
                '(sensor_id, avg_temperature, min_temperature, max_temperature, '
                ' sample_count, window_start, window_end, computed_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (
                    s['sensor_id'],
                    float(s['avg_temperature']),
                    float(s['min_temperature']),
                    float(s['max_temperature']),
                    int(s['sample_count']),
                    s['window_start'],
                    s['window_end'],
                    s['computed_at'],
                ),
            )
            self._conn.commit()

    def latest(self):
        with self._lock:
            cur = self._conn.execute(
                'SELECT sensor_id, avg_temperature, min_temperature, max_temperature, '
                '       sample_count, window_start, window_end, computed_at '
                'FROM temperature_stats ORDER BY id DESC LIMIT 1'
            )
            return cur.fetchone()

    def latest_by_sensor(self, sensor_id: str):
        with self._lock:
            cur = self._conn.execute(
                'SELECT sensor_id, avg_temperature, min_temperature, max_temperature, '
                '       sample_count, window_start, window_end, computed_at '
                'FROM temperature_stats WHERE sensor_id = ? ORDER BY id DESC LIMIT 1',
                (sensor_id,),
            )
            return cur.fetchone()

    def history(self, sensor_id: str, limit: int):
        with self._lock:
            cur = self._conn.execute(
                'SELECT sensor_id, avg_temperature, min_temperature, max_temperature, '
                '       sample_count, window_start, window_end, computed_at '
                'FROM temperature_stats WHERE sensor_id = ? ORDER BY id DESC LIMIT ?',
                (sensor_id, limit),
            )
            return cur.fetchall()

    def list_sensors(self):
        with self._lock:
            cur = self._conn.execute(
                'SELECT DISTINCT sensor_id FROM temperature_stats ORDER BY sensor_id'
            )
            return [row[0] for row in cur.fetchall()]

    def count(self):
        with self._lock:
            cur = self._conn.execute('SELECT COUNT(*) FROM temperature_stats')
            return int(cur.fetchone()[0])


# ============================================================
# Consumidor Kafka (em thread)
# ============================================================
def kafka_consumer_loop(db: StatsDB):
    consumer = KafkaConsumer(
        const.TOPIC_STATS,
        bootstrap_servers=[const.BROKER_ADDR + ':' + const.BROKER_PORT],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
    )
    print(f'[service:kafka] consumindo {const.TOPIC_STATS}')
    for msg in consumer:
        try:
            db.insert(msg.value)
            print(f'[service:kafka] gravado: {msg.value}')
        except Exception as e:
            print(f'[service:kafka] erro ao gravar: {e}')


# ============================================================
# Servico REST (Flask)
# ============================================================
def _row_to_dict(row):
    if row is None:
        return None
    return {
        'sensor_id': row[0],
        'avg_temperature': row[1],
        'min_temperature': row[2],
        'max_temperature': row[3],
        'sample_count': row[4],
        'window_start': row[5],
        'window_end': row[6],
        'computed_at': row[7],
    }


def build_app(db: StatsDB) -> Flask:
    app = Flask(__name__)

    @app.get('/stats/latest')
    def get_latest_stats():
        row = db.latest()
        if row is None:
            return jsonify({'error': 'no stats available'}), 404
        return jsonify(_row_to_dict(row))

    @app.get('/sensors/<sensor_id>/stats/latest')
    def get_latest_by_sensor(sensor_id):
        row = db.latest_by_sensor(sensor_id)
        if row is None:
            return jsonify({'error': f'sensor "{sensor_id}" not found'}), 404
        return jsonify(_row_to_dict(row))

    @app.get('/sensors/<sensor_id>/history')
    def get_history(sensor_id):
        try:
            limit = int(request.args.get('limit', 50))
        except ValueError:
            return jsonify({'error': 'limit must be an integer'}), 400
        if limit <= 0:
            limit = 50
        rows = db.history(sensor_id, limit)
        return jsonify({'stats': [_row_to_dict(r) for r in rows]})

    @app.get('/sensors')
    def list_sensors():
        return jsonify({'sensor_ids': db.list_sensors()})

    @app.get('/stats/count')
    def count_stats():
        return jsonify({'count': db.count()})

    return app


def serve():
    db = StatsDB(const.DB_PATH)

    # Kafka consumer em thread separada
    t = threading.Thread(target=kafka_consumer_loop, args=(db,), daemon=True)
    t.start()

    app = build_app(db)
    print(f'[service:rest] ouvindo em {const.REST_HOST}:{const.REST_PORT}')
    app.run(host=const.REST_HOST, port=const.REST_PORT, threaded=True, use_reloader=False)


if __name__ == '__main__':
    logging.basicConfig()
    serve()
