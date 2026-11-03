"""Mock ClickHouse HTTP endpoint for the failover drill.

Accepts POSTed SQL, records every INSERT/ALTER DELETE with its window and
query_id in a SQLite file (shared by both service instances), honours
KILL QUERY, and can delay aggregation to simulate long-running SQL.
"""

from __future__ import annotations

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import re
import sqlite3
import threading
import time
from urllib.parse import parse_qs, urlsplit

LOCK = threading.Lock()
KILLED: set[str] = set()


def target(sql: str) -> str:
    match = re.search(r"(?:INSERT\s+INTO|ALTER\s+TABLE)\s+([\w.]+)", sql, re.I)
    return match.group(1) if match else ''


class Handler(BaseHTTPRequestHandler):
    db: str = ''
    delay: float = 0.0

    def log_message(self, *_):
        pass

    def do_POST(self):
        query = parse_qs(urlsplit(self.path).query)
        sql = self.rfile.read(int(self.headers.get('Content-Length', 0))).decode()
        query_id = query.get('query_id', [''])[0]
        kind = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ''
        if kind == 'KILL':
            KILLED.update(re.findall(r"'([^']+)'", sql))
            return self._reply()
        if kind == 'INSERT' and self.delay:
            end = time.monotonic() + self.delay
            while time.monotonic() < end:
                if query_id in KILLED:
                    return self._reply(500, 'Code: 394. Query was cancelled')
                time.sleep(0.05)
        if kind in ('INSERT', 'ALTER'):
            self._record(kind, sql, query, query_id)
        return self._reply()

    def _record(self, kind, sql, query, query_id):
        with LOCK, sqlite3.connect(self.db) as connection:
            connection.execute('''INSERT INTO calls(at, kind, target, start, end, query_id, settings)
                VALUES (?, ?, ?, ?, ?, ?, ?)''', (
                time.time(), kind, target(sql), query.get('param_start_time', [''])[0],
                query.get('param_end_time', [''])[0], query_id,
                json.dumps({k: v[0] for k, v in query.items()
                            if k in ('max_threads', 'readonly', 'load_balancing',
                                     'max_memory_usage', 'max_execution_time')})))

    def _reply(self, code=200, text=''):
        body = (text or json.dumps({'data': [[1]]})).encode()
        self.send_response(code)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--db', required=True)
    parser.add_argument('--delay', type=float, default=0.0)
    args = parser.parse_args()
    with sqlite3.connect(args.db) as connection:
        connection.execute('''CREATE TABLE IF NOT EXISTS calls (at REAL, kind TEXT, target TEXT,
            start TEXT, end TEXT, query_id TEXT, settings TEXT)''')
    Handler.db, Handler.delay = args.db, args.delay
    ThreadingHTTPServer(('127.0.0.1', args.port), Handler).serve_forever()


if __name__ == '__main__':
    main()
