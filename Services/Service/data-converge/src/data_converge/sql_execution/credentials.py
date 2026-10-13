"""Platform Python XMS decryption and PMS credential selection.

The subprocess protocol carries secrets on private pipes only. SDK output and
exceptions are never attached to user-facing diagnostics or exception chains.
"""

import base64
import json
import math
import os
import re
import selectors
import signal
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path

MAX_SECRET_BYTES = 1024 * 1024
MAX_FRAME_BYTES = 16 + 4 * ((MAX_SECRET_BYTES + 2) // 3)
MAX_STDERR_BYTES = 64 * 1024
FRAME_PREFIX = b"DC_KMS_V1\n"


def _text(value):
    return isinstance(value, str) and bool(value.strip()) and "\0" not in value


def validate_provider(clickhouse, config_directory):
    """Validate provider metadata without reading a file, environment secret or SDK."""
    providers = [key for key in ("password_file", "password_env") if clickhouse.get(key) is not None]
    if "password" in clickhouse or len(providers) != 1:
        raise ValueError("CLICKHOUSE_PROVIDER_INVALID") from None
    provider = providers[0]
    if not _text(clickhouse[provider]):
        raise ValueError("CLICKHOUSE_PROVIDER_INVALID") from None
    if provider == "password_file":
        path = Path(clickhouse[provider])
        clickhouse[provider] = str(path if path.is_absolute() else (config_directory / path).resolve())
    elif not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", clickhouse[provider]):
        raise ValueError("CLICKHOUSE_PROVIDER_INVALID") from None
    timeout = clickhouse.get("kms_timeout_seconds", 30)
    name = clickhouse.get("paas_name", "clickhouse-pms-password")
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 300 or not _text(name):
        raise ValueError("KMS_CONFIGURATION_INVALID") from None
    clickhouse["kms_timeout_seconds"] = timeout
    clickhouse["paas_name"] = name


def _unique_pairs(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate key")
        value[key] = item
    return value


def _invalid_constant(_):
    raise ValueError("invalid JSON constant")


def parse_pms_password(plaintext, username, paas_name="clickhouse-pms-password"):
    """Decode the exporter PMS envelope; never normalize password contents."""
    try:
        if (
            not isinstance(plaintext, str)
            or not 0 < len(plaintext.encode("utf-8")) <= MAX_SECRET_BYTES
        ):
            raise ValueError("invalid plaintext length")
        decoded = base64.b64decode(plaintext, validate=True).decode("utf-8")
        document = json.loads(
            decoded, object_pairs_hook=_unique_pairs, parse_constant=_invalid_constant
        )
        items = document["paasconfig"]
        if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
            raise ValueError("invalid paas list")
        selected = [item for item in items if item.get("name") == paas_name]
        if len(selected) != 1:
            raise ValueError("ambiguous paas selection")
        accounts = selected[0]["info"]
        if not isinstance(accounts, list) or any(
            not isinstance(account, dict) for account in accounts
        ):
            raise ValueError("invalid account list")
        selected = [account for account in accounts if account.get("user") == username]
        if len(selected) != 1:
            raise ValueError("ambiguous account selection")
        password = selected[0]["password"]
        if not isinstance(password, str) or not password or "\0" in password:
            raise ValueError("invalid password")
        return password
    except (ValueError, TypeError, KeyError, RecursionError):
        raise ValueError("KMS_PMS_INVALID") from None


def _stop(process):
    """Kill/reap this helper and its process group on failed exchange (POSIX)."""
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        elif process.poll() is None:
            process.kill()
    except ProcessLookupError:
        pass
    process.wait()


@contextmanager
def _termination_guard():
    """Make default CLI SIGTERM unwind cleanup during startup decryption.

    Respect applications that already installed/ignored a termination handler.
    Python's default SIGINT already raises KeyboardInterrupt and unwinds normally.
    """
    install = (
        threading.current_thread() is threading.main_thread()
        and signal.getsignal(signal.SIGTERM) == signal.SIG_DFL
    )

    def terminate(signum, _frame):
        raise SystemExit(128 + signum)

    if install:
        previous = signal.signal(signal.SIGTERM, terminate)
    try:
        yield
    finally:
        if install:
            signal.signal(signal.SIGTERM, previous)


def _collect(process, payload, timeout):
    deadline = time.monotonic() + timeout
    output, stderr_size, offset = bytearray(), 0, 0
    with selectors.DefaultSelector() as selector:
        for stream, event, name in [
            (process.stdin, selectors.EVENT_WRITE, "stdin"),
            (process.stdout, selectors.EVENT_READ, "stdout"),
            (process.stderr, selectors.EVENT_READ, "stderr"),
        ]:
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, event, name)
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ValueError("KMS_DECRYPT_TIMEOUT")
            for key, _ in selector.select(remaining):
                stream = key.fileobj
                if key.data == "stdin":
                    try:
                        offset += os.write(stream.fileno(), payload[offset : offset + 65536])
                    except BrokenPipeError:
                        offset = len(payload)
                    if offset == len(payload):
                        selector.unregister(stream)
                        stream.close()
                    continue
                chunk = os.read(stream.fileno(), 65536)
                if not chunk:
                    selector.unregister(stream)
                    stream.close()
                    continue
                if key.data == "stdout":
                    output.extend(chunk)
                    if len(output) > MAX_FRAME_BYTES:
                        raise ValueError("KMS_OUTPUT_LIMIT")
                else:
                    stderr_size += len(chunk)
                    if stderr_size > MAX_STDERR_BYTES:
                        raise ValueError("KMS_OUTPUT_LIMIT")
        remaining = deadline - time.monotonic()
        try:
            return_code = process.wait(timeout=max(0, remaining))
        except subprocess.TimeoutExpired:
            raise ValueError("KMS_DECRYPT_TIMEOUT") from None
    if return_code != 0:
        raise ValueError("KMS_DECRYPT_FAILED") from None
    return bytes(output)


def run_decryptor(command, ciphertext, timeout):
    """Run a bounded one-shot decryptor using private pipes on POSIX hosts."""
    if not isinstance(ciphertext, bytes) or not 0 < len(ciphertext) <= MAX_SECRET_BYTES:
        raise ValueError("KMS_SECRET_INVALID")
    if os.name != "posix":
        raise ValueError("KMS_PLATFORM_UNSUPPORTED") from None
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(filter(None, (
        str(Path(__file__).resolve().parents[2]), environment.get("PYTHONPATH"))))
    with _termination_guard():
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
                env=environment,
            )
        except (OSError, ValueError):
            raise ValueError("KMS_DECRYPT_START_FAILED") from None
        try:
            frame = _collect(process, ciphertext, timeout)
        except BaseException:
            _stop(process)
            raise
        finally:
            for stream in (process.stdin, process.stdout, process.stderr):
                stream.close()
    try:
        if not frame.startswith(FRAME_PREFIX) or not frame.endswith(b"\n"):
            raise ValueError("invalid frame")
        payload = base64.b64decode(frame[len(FRAME_PREFIX) : -1], validate=True)
        if not 0 < len(payload) <= MAX_SECRET_BYTES:
            raise ValueError("invalid plaintext length")
        return payload.decode("utf-8")
    except ValueError:
        raise ValueError("KMS_PROTOCOL_INVALID") from None


def load_password(clickhouse):
    """Resolve once at startup; callers retain the returned password in memory."""
    if clickhouse.get("password_env"):
        password = os.environ.get(clickhouse["password_env"])
        if not isinstance(password, str) or not password or "\0" in password:
            raise ValueError("CLICKHOUSE_CREDENTIAL_MISSING") from None
        return password
    try:
        with Path(clickhouse["password_file"]).open("rb") as stream:
            ciphertext = stream.read(MAX_SECRET_BYTES + 1)
    except OSError:
        raise ValueError("KMS_SECRET_UNREADABLE") from None
    command = [sys.executable, "-m", "data_converge.sql_execution.kms_worker"]
    plaintext = run_decryptor(command, ciphertext, clickhouse["kms_timeout_seconds"])
    return parse_pms_password(plaintext, clickhouse["username"], clickhouse["paas_name"])
