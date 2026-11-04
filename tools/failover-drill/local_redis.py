"""Local Redis for the drill: a real redis-server when available, else fakeredis TCP.

fakeredis needs ``pip install "fakeredis[lua]"`` (Lua runs the fencing scripts).
Both are drill-only tools, not runtime dependencies of the service.

``--ready-file`` is written only after the password is in force. Without it a
client that connects during ``CONFIG SET requirepass`` can have its socket
closed by fakeredis, which looks like a service failure in the drill.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import socket
import subprocess
import time


def wait_port(port: int, timeout: float = 15) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket() as sock:
            if sock.connect_ex(('127.0.0.1', port)) == 0:
                return
        time.sleep(0.05)
    raise SystemExit(f'redis port {port} did not open')


def mark_ready(path: str | None) -> None:
    if path:
        Path(path).write_text('ready')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--password', required=True)
    parser.add_argument('--ready-file')
    args = parser.parse_args()
    server = shutil.which('redis-server')
    if server:
        process = subprocess.Popen([server, '--port', str(args.port), '--save', '',
                                    '--appendonly', 'no', '--requirepass', args.password])
        wait_port(args.port)
        mark_ready(args.ready_file)
        raise SystemExit(process.wait())
    import threading
    import redis
    from fakeredis import TcpFakeServer

    tcp = TcpFakeServer(('127.0.0.1', args.port), server_type='redis')
    thread = threading.Thread(target=tcp.serve_forever, daemon=True)
    thread.start()
    redis.Redis(port=args.port).config_set('requirepass', args.password)
    mark_ready(args.ready_file)
    thread.join()


if __name__ == '__main__':
    main()
