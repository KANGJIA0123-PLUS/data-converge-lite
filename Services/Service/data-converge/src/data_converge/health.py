"""Local process heartbeat; probes never decrypt credentials or query ClickHouse."""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path


class Heartbeat:
    def __init__(self, directory: Path, interval: float = 5):
        self.path = Path(directory) / 'heartbeat.json'
        self.interval = interval
        self.ready = False
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread = None

    def _write(self):
        with self._lock:
            temporary = self.path.with_suffix('.tmp')
            temporary.write_text(json.dumps({'pid': os.getpid(), 'time': time.time(), 'ready': self.ready}))
            temporary.chmod(0o600)
            os.replace(temporary, self.path)

    def _run(self):
        while not self._stop.wait(self.interval):
            try:
                self._write()
            except OSError:
                # A stale heartbeat makes both probes fail if storage is unavailable.
                return

    def set_ready(self, ready: bool):
        self.ready = ready
        self._write()

    def __enter__(self):
        self._write()
        self._thread = threading.Thread(target=self._run, name='heartbeat', daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        self._thread.join(timeout=self.interval + 1)
        self.path.unlink(missing_ok=True)


def healthcheck(directory: Path, kind: str, max_age: float = 75) -> bool:
    try:
        value = json.loads((directory / 'heartbeat.json').read_text())
        pid, timestamp = value['pid'], value['time']
        if type(pid) is not int or pid <= 0 or type(timestamp) not in (float, int):
            return False
        if not 0 <= time.time() - timestamp <= max_age:
            return False
        os.kill(pid, 0)
        return kind == 'liveness' or value.get('ready') is True
    except (OSError, ValueError, KeyError, TypeError):
        return False
