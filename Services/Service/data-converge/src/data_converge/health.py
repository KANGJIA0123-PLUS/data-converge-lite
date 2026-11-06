"""Local process heartbeat; probes never decrypt credentials or query ClickHouse."""
from __future__ import annotations

import json
import hashlib
import os
import socket
import threading
import time
from pathlib import Path


def heartbeat_directory(directory: Path, config: dict) -> Path:
    """Standby probes must never read the active replica's heartbeat."""
    settings = config.get('leadership', {})
    if not isinstance(settings, dict) or settings.get('enabled') is not True:
        return directory
    identity = (os.getenv('DATA_CONVERGE_INSTANCE_ID') or os.getenv('POD_NAME')
                or os.getenv('HOSTNAME') or socket.gethostname())
    key = hashlib.sha256(identity.encode('utf-8')).hexdigest()[:24]
    return directory.parent / ('.' + directory.name + '.health') / key


class Heartbeat:
    """Write liveness evidence; while serving it also records scheduler progress.

    The file is written by a dedicated thread, so on its own it only proves the
    process exists. ``progress()`` is called by the scheduler loop each round;
    when the loop stops advancing for longer than ``stall_seconds`` the written
    ``progress_at`` ages and the liveness probe fails, letting the platform
    restart a hung writer instead of keeping it alive forever.
    """

    def __init__(self, directory: Path, interval: float = 5):
        self.path = Path(directory) / 'heartbeat.json'
        self.interval = interval
        self.ready = False
        self.readiness_check = None
        self.stall_seconds = None
        self._progress_at = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread = None

    def _write(self):
        with self._lock:
            temporary = self.path.with_suffix('.tmp')
            ready = self.ready and (self.readiness_check is None or self.readiness_check())
            value = {'pid': os.getpid(), 'time': time.time(), 'ready': ready}
            if self._progress_at is not None and self.stall_seconds is not None:
                value.update(progress_at=self._progress_at, stall_seconds=self.stall_seconds)
            temporary.write_text(json.dumps(value))
            temporary.chmod(0o600)
            os.replace(temporary, self.path)

    def _run(self):
        while not self._stop.wait(self.interval):
            try:
                self._write()
            except OSError:
                # A stale heartbeat makes both probes fail if storage is unavailable.
                return

    def watch_progress(self, stall_seconds: float) -> None:
        """Start enforcing scheduler progress; the loop must call progress() each round."""
        self.stall_seconds = stall_seconds
        self._progress_at = time.time()

    def progress(self) -> None:
        self._progress_at = time.time()

    def unwatch_progress(self) -> None:
        """Standby and shutdown phases have no scheduler loop to observe."""
        self._progress_at = None
        self.stall_seconds = None

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
        now = time.time()
        if not 0 <= now - timestamp <= max_age:
            return False
        os.kill(pid, 0)
        if _stalled(value, now):
            return False
        return kind == 'liveness' or value.get('ready') is True
    except (OSError, ValueError, KeyError, TypeError):
        return False


def _stalled(value: dict, now: float) -> bool:
    """True when a serving scheduler has not completed a round within its budget."""
    progress, limit = value.get('progress_at'), value.get('stall_seconds')
    if progress is None or limit is None:
        return False
    if type(progress) not in (float, int) or type(limit) not in (float, int) or limit <= 0:
        return True
    return now - progress > limit
