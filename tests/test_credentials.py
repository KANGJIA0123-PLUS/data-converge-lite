"""Configuration and isolated SDK credential handling, using synthetic secrets."""

import base64
import json
import os
import sys
from pathlib import Path

import pytest
import yaml

from data_converge.sql_execution.client import load_config

from .paths import SOURCE_ROOT


def write_config(tmp_path, **changes):
    values = dict(host="${TEST_CH_HOST}", port="${TEST_CH_PORT}", database="metrics",
                  username="user", password_file="missing-ciphertext")
    values.update(changes)
    path = tmp_path / "app.yaml"
    path.write_text(yaml.safe_dump({"clickhouse": values}))
    return path


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setenv("TEST_CH_HOST", "https://db.example")
    monkeypatch.setenv("TEST_CH_PORT", "8443")


def envelope(accounts=None, entries=None):
    if entries is None:
        entries = [{"name": "clickhouse-pms-password", "info": accounts or [{"user": "user", "password": " exact password "}]}]
    return base64.b64encode(json.dumps({"paasconfig": entries}).encode()).decode()


def test_offline_validation_expands_connection_without_reading_secret(tmp_path):
    ch = load_config(write_config(tmp_path), resolve_password=False)["clickhouse"]
    assert ch["host"] == "https://db.example" and ch["port"] == 8443
    assert ch["secure"] is True and ch["verify"] is True
    assert "password" not in ch
    assert ch["password_file"] == str(tmp_path / "missing-ciphertext")


@pytest.mark.parametrize("changes", [
    {"host": "${MISSING_CH_HOST}"}, {"port": "${MISSING_CH_PORT}"},
    {"host": "http://user:secret@db.example"}, {"host": "http://db.example/path"},
    {"host": "http://db.example?secret=value"}, {"host": "http://db.example#fragment"},
    {"host": "https://db.example", "secure": False}, {"host": "http://db.example", "secure": True},
    {"host": "ftp://db.example"}, {"host": "db.example\nsecret"},
    {"host": "http://db.example:8123", "port": 8443},
    {"port": "native"}, {"port": 0}, {"port": 65536}, {"port": True},
    {"secure": "false"}, {"verify": "false"},
    {"password_env": "TEST_CH_PASSWORD"}, {"password": "do not accept plaintext"},
    {"password_file": ""}, {"kms_timeout_seconds": 0},
])
def test_rejects_unsafe_or_ambiguous_configuration_without_echoing_values(tmp_path, changes):
    with pytest.raises(ValueError) as caught:
        load_config(write_config(tmp_path, **changes), resolve_password=False)
    assert all(value not in str(caught.value) for value in ("secret", "plaintext", "value", "native"))


def test_bare_host_defaults_to_http(tmp_path):
    ch = load_config(write_config(tmp_path, host="db.example", port=8123), resolve_password=False)["clickhouse"]
    assert ch["secure"] is False


def test_password_environment_read_only_at_runtime(tmp_path, monkeypatch):
    path = write_config(tmp_path, password_file=None, password_env="SYNTHETIC_CH_PASSWORD")
    assert "password" not in load_config(path, resolve_password=False)["clickhouse"]
    with pytest.raises(ValueError):
        load_config(path)
    monkeypatch.setenv("SYNTHETIC_CH_PASSWORD", " leading and trailing \n")
    assert load_config(path)["clickhouse"]["password"] == " leading and trailing \n"


def test_pms_selection_preserves_exact_password():
    from data_converge.sql_execution.credentials import parse_pms_password
    assert parse_pms_password(envelope(), "user") == " exact password "


@pytest.mark.parametrize("plaintext", [
    "not base64 private password", base64.b64encode(b'{"paasconfig":[]').decode(),
    envelope(entries=[]), envelope(accounts=[{"user": "other", "password": "private"}]),
    envelope(accounts=[{"user": "user", "password": "private"}] * 2),
    envelope(entries=[{"name": "clickhouse-pms-password", "info": []}] * 2),
    base64.b64encode(b'{"paasconfig":[],"paasconfig":[]}').decode(),
])
def test_pms_rejects_missing_duplicate_and_malformed_accounts(plaintext):
    from data_converge.sql_execution.credentials import parse_pms_password
    with pytest.raises(ValueError) as caught:
        parse_pms_password(plaintext, "user")
    assert "private" not in str(caught.value)


def make_sdk(tmp_path, body):
    sdk = tmp_path / "dsf" / "crypto"
    sdk.mkdir(parents=True)
    (sdk.parent / "__init__.py").write_text("")
    (sdk / "__init__.py").write_text("")
    (sdk / "xms_crypto.py").write_text(body)


def sdk_path(monkeypatch, tmp_path):
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join((str(tmp_path), str(SOURCE_ROOT))))


def test_real_worker_decrypts_file_and_suppresses_sdk_output(tmp_path, monkeypatch, capfd):
    make_sdk(tmp_path, "import os\nprint('SDK PRIVATE stdout')\nos.write(2, b'SDK PRIVATE stderr')\n"
             "def xms_decrypt(ciphertext):\n    assert ciphertext == 'synthetic ciphertext'\n    return " + repr(envelope()) + "\n")
    sdk_path(monkeypatch, tmp_path)
    (tmp_path / "ciphertext").write_text("synthetic ciphertext")
    assert load_config(write_config(tmp_path, password_file="ciphertext"))["clickhouse"]["password"] == " exact password "
    captured = capfd.readouterr()
    assert "PRIVATE" not in captured.out + captured.err


@pytest.mark.parametrize("body", [
    "raise RuntimeError('PRIVATE password')\n",
    "def xms_decrypt(ciphertext):\n    raise RuntimeError('PRIVATE password')\n",
])
def test_sdk_failures_are_redacted(tmp_path, monkeypatch, capfd, body):
    make_sdk(tmp_path, body)
    sdk_path(monkeypatch, tmp_path)
    (tmp_path / "ciphertext").write_text("synthetic ciphertext")
    with pytest.raises(ValueError) as caught:
        load_config(write_config(tmp_path, password_file="ciphertext"))
    assert "PRIVATE" not in str(caught.value)
    assert caught.value.__suppress_context__
    captured = capfd.readouterr()
    assert "PRIVATE" not in captured.out + captured.err


def test_decryptor_timeout_and_protocol_limits_are_bounded():
    from data_converge.sql_execution.credentials import run_decryptor
    for script, timeout in [("import time; time.sleep(10)", 0.1),
                            ("import sys; sys.stdout.buffer.write(b'x' * 2000000)", 2),
                            ("print('PRIVATE bogus frame')", 2)]:
        with pytest.raises(ValueError) as caught:
            run_decryptor([sys.executable, "-c", script], b"ciphertext", timeout)
        assert "PRIVATE" not in str(caught.value)


def test_worker_can_start_when_parent_was_imported_without_exported_pythonpath(tmp_path, monkeypatch):
    make_sdk(tmp_path, "def xms_decrypt(ciphertext):\n    return " + repr(envelope()) + "\n")
    # The application may add its source directory to sys.path without exporting it.
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    (tmp_path / "ciphertext").write_text("synthetic ciphertext")
    assert load_config(write_config(tmp_path, password_file="ciphertext"))["clickhouse"]["password"] == " exact password "
