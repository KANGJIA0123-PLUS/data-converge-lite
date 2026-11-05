"""Fail CI only when pytest reports a nodeid outside the approved baseline."""
from __future__ import annotations

import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "tools" / "test-baseline.txt"
TEST_ROOT = ROOT / "Services" / "Service" / "data-converge" / "test"
RESULT = re.compile(r"^(?:FAILED|ERROR)\s+(\S+)(?:\s|$)", re.MULTILINE)
ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def _baseline() -> set[str]:
    return {line.strip() for line in BASELINE.read_text().splitlines() if line.strip()}


def _run() -> tuple[int, str]:
    env = dict(os.environ)
    env['TZ'] = 'Asia/Shanghai'
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    source = ROOT / "Services" / "Service" / "data-converge" / "src"
    env["PYTHONPATH"] = str(source) + os.pathsep + env.get("PYTHONPATH", "")
    command = [sys.executable, "-m", "pytest", "-c", "/dev/null", "-p",
               "no:cacheprovider", str(TEST_ROOT), "--tb=short", "-q", "-rfE"]
    result = subprocess.run(command, cwd=ROOT, env=env, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return result.returncode, result.stdout


def main() -> int:
    baseline = _baseline()
    code, output = _run()
    clean_output = ANSI.sub('', output)
    current = {nodeid for nodeid in RESULT.findall(clean_output) if nodeid}
    new = sorted(current - baseline)
    resolved = sorted(baseline - current)
    print(output, end="")
    print(f"BASELINE_FAILURES={len(baseline)} CURRENT_FAILURES={len(current)}")
    if code not in (0, 1) or (code == 1 and not current):
        print(f"TEST_RUN_FAILED: pytest_exit_code={code}")
        return code or 1
    if resolved:
        print("RESOLVED_BASELINE_FAILURES:")
        print("\n".join(resolved))
    if new:
        print("NEW_FAILURES:")
        print("\n".join(new))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
