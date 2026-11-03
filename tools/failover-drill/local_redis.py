"""Local Redis for the drill: a real redis-server when available, else fakeredis TCP.

fakeredis needs ``pip install "fakeredis[lua]"`` (Lua runs the fencing scripts).
Both are drill-only tools, not runtime dependencies of the service.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--password', required=True)
    args = parser.parse_args()
    server = shutil.which('redis-server')
    if server:
        raise SystemExit(subprocess.call([server, '--port', str(args.port), '--save', '',
                                          '--appendonly', 'no', '--requirepass', args.password]))
    import threading
    import redis
    from fakeredis import TcpFakeServer

    tcp = TcpFakeServer(('127.0.0.1', args.port), server_type='redis')
    thread = threading.Thread(target=tcp.serve_forever, daemon=True)
    thread.start()
    redis.Redis(port=args.port).config_set('requirepass', args.password)
    thread.join()


if __name__ == '__main__':
    main()
