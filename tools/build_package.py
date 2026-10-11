#!/usr/bin/env python3
"""Build the platform bin/lib archive without modifying or compiling the checkout."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import tarfile
import tempfile


ROOT = Path(__file__).resolve().parents[1]


def package_member(member: tarfile.TarInfo) -> tarfile.TarInfo | None:
    path = Path(member.name)
    if any(p in {"__pycache__", ".git", ".venv", ".DS_Store", ".pytest_cache"}
           or p.endswith('.egg-info') for p in path.parts):
        return None
    if path.suffix in {".pyc", ".pyo"}:
        return None
    if member.isfile() and path.suffix == ".sh":
        member.mode |= 0o111
    return member


def build_package(output: Path) -> Path:
    output = output.expanduser().resolve()
    if output.is_relative_to(ROOT / "Services") or output.is_relative_to(ROOT / "tools"):
        raise ValueError("Package output must not be inside source directories; use build/ or another output directory.")
    if not (output.name.endswith(".tar.gz") or output.name.endswith(".tgz")):
        raise ValueError("Package output must be a .tar.gz or .tgz file.")
    images = ROOT / "Services/Package/images"
    application = ROOT / "Services/Service/data-converge"
    for required in [images / "bin/start.sh", application / "src/data_converge/main.py", application / "resources/sql"]:
        if not required.exists():
            raise ValueError(f"Required package input is missing: {required.relative_to(ROOT)}")
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=".data-converge-", suffix=".tar.gz", dir=output.parent)
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        with tarfile.open(temporary, "w:gz") as archive:
            for path in sorted(images.iterdir()):
                if path.name != "lib":
                    archive.add(path, arcname=path.name, filter=package_member)
            for name in ["src", "resources"]:
                archive.add(application / name, arcname=f"lib/data-converge/{name}", filter=package_member)
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "build/data-converge-lite.tar.gz")
    args = parser.parse_args()
    try:
        output = build_package(args.output)
    except (OSError, ValueError, tarfile.TarError) as exc:
        parser.error(str(exc))
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
