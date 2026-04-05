#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

#!/usr/bin/env python3
"""Verify checked-in C API generated fragments are up to date without Git."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
REGEN_ALL = ROOT / "tools/c_api_gen/regen_all.py"
CLANG_FORMAT_STYLE_FILE = ROOT / ".clang-format"
GENERATED_DIRS = (
    Path("include/rocksdb/c_api_gen"),
    Path("db/c_api_gen"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Regenerate the checked-in C API fragments and compare them against "
            "a pre-regen snapshot without relying on git diff."
        )
    )
    parser.add_argument(
        "--clang-format",
        default="clang-format",
        help=(
            "clang-format binary to canonicalize both the pre-regen snapshot "
            "and regenerated output before comparison (default: clang-format)"
        ),
    )
    return parser.parse_args()


def resolve_executable(command: str) -> str:
    if Path(command).parent != Path():
        if Path(command).exists():
            return str(Path(command))
        raise FileNotFoundError(f"Executable not found: {command}")

    resolved = shutil.which(command)
    if resolved is None:
        raise FileNotFoundError(
            f"{command} not found in PATH. Install it or pass --clang-format."
        )
    return resolved


def copy_generated_dirs(destination_root: Path) -> None:
    for rel_dir in GENERATED_DIRS:
        source = ROOT / rel_dir
        if not source.is_dir():
            raise FileNotFoundError(f"Generated directory missing: {source}")

        destination = destination_root / rel_dir
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, destination)


def collect_inc_files(root: Path) -> list[str]:
    inc_files: list[str] = []
    for rel_dir in GENERATED_DIRS:
        directory = root / rel_dir
        if not directory.is_dir():
            raise FileNotFoundError(f"Generated directory missing: {directory}")
        inc_files.extend(str(path) for path in sorted(directory.rglob("*.inc")))
    return inc_files


def format_generated_dirs(root: Path, clang_format: str) -> None:
    inc_files = collect_inc_files(root)
    if not inc_files:
        raise RuntimeError(f"No generated .inc files found under {root}")
    subprocess.run(
        [
            clang_format,
            f"--style=file:{CLANG_FORMAT_STYLE_FILE}",
            "-i",
            *inc_files,
        ],
        cwd=root,
        check=True,
    )


def compare_snapshots(work_root: Path) -> int:
    proc = subprocess.run(
        ["diff", "-ruN", "checked_in", "regenerated"],
        cwd=work_root,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode == 0:
        print("C API generated files are up to date.")
        return 0

    if proc.returncode == 1:
        sys.stderr.write(
            "C API generated files are out of date. Re-run "
            "`python3 tools/c_api_gen/regen_all.py` and commit the updated "
            "files under include/rocksdb/c_api_gen/ and db/c_api_gen/.\n"
        )
        if proc.stdout:
            sys.stderr.write(proc.stdout)
        return 1

    if proc.stdout:
        sys.stderr.write(proc.stdout)
    if proc.stderr:
        sys.stderr.write(proc.stderr)
    raise RuntimeError(f"diff failed with exit code {proc.returncode}")


def main() -> int:
    args = parse_args()
    clang_format = resolve_executable(args.clang_format)

    with tempfile.TemporaryDirectory(prefix="rocksdb-c-api-gen-verify-") as tmp:
        work_root = Path(tmp)
        checked_in_root = work_root / "checked_in"
        regenerated_root = work_root / "regenerated"

        copy_generated_dirs(checked_in_root)
        format_generated_dirs(checked_in_root, clang_format)

        subprocess.run([sys.executable, str(REGEN_ALL)], cwd=ROOT, check=True)
        format_generated_dirs(ROOT, clang_format)

        copy_generated_dirs(regenerated_root)
        return compare_snapshots(work_root)


if __name__ == "__main__":
    raise SystemExit(main())
