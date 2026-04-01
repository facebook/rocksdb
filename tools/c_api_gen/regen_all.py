#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

#!/usr/bin/env python3
"""Regenerate all checked-in RocksDB C API generated fragments."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
GEN = ROOT / "tools/c_api_gen/generate_c_api.py"
AUTO_GEN = ROOT / "tools/c_api_gen/auto_simple_bindings.py"
SPEC = ROOT / "tools/c_api_gen/spec.json"
GENERATED_INCLUDE_ROOT = ROOT / "include/rocksdb/c_api_gen"
GENERATED_SOURCE_ROOT = ROOT / "db/c_api_gen"
CLANG_FORMAT = shutil.which("clang-format")


JOBS = [
    (
        ["DB data operations", "WriteBatch", "Transaction", "TransactionDB simple"],
        GENERATED_INCLUDE_ROOT / "c_generated_subset.h.inc",
        GENERATED_SOURCE_ROOT / "c_generated_subset.cc.inc",
    ),
    (
        ["DB data operations"],
        GENERATED_INCLUDE_ROOT / "c_generated_db_simple_subset.h.inc",
        GENERATED_SOURCE_ROOT / "c_generated_db_simple_subset.cc.inc",
    ),
    (
        ["WriteBatch"],
        GENERATED_INCLUDE_ROOT / "c_generated_writebatch_subset.h.inc",
        GENERATED_SOURCE_ROOT / "c_generated_writebatch_subset.cc.inc",
    ),
    (
        ["Transaction"],
        GENERATED_INCLUDE_ROOT / "c_generated_transaction_subset.h.inc",
        GENERATED_SOURCE_ROOT / "c_generated_transaction_subset.cc.inc",
    ),
    (
        ["TransactionDB simple"],
        GENERATED_INCLUDE_ROOT / "c_generated_transactiondb_subset.h.inc",
        GENERATED_SOURCE_ROOT / "c_generated_transactiondb_subset.cc.inc",
    ),
    (
        ["BlockBasedOptions simple"],
        GENERATED_INCLUDE_ROOT / "c_generated_block_based_options_subset.h.inc",
        GENERATED_SOURCE_ROOT / "c_generated_block_based_options_subset.cc.inc",
    ),
    (
        ["JobInfo metadata simple"],
        GENERATED_INCLUDE_ROOT / "c_generated_jobinfo_metadata_subset.h.inc",
        GENERATED_SOURCE_ROOT / "c_generated_jobinfo_metadata_subset.cc.inc",
    ),
    (
        ["CuckooOptions simple"],
        GENERATED_INCLUDE_ROOT / "c_generated_cuckoo_options_subset.h.inc",
        GENERATED_SOURCE_ROOT / "c_generated_cuckoo_options_subset.cc.inc",
    ),
    (
        ["Options simple"],
        GENERATED_INCLUDE_ROOT / "c_generated_options_subset.h.inc",
        GENERATED_SOURCE_ROOT / "c_generated_options_subset.cc.inc",
    ),
]


def run_job(sections: list[str], header_out: Path, source_out: Path) -> None:
    cmd = [
        sys.executable,
        str(GEN),
        "--spec",
        str(SPEC),
    ]
    for section in sections:
        cmd.extend(["--section", section])
    cmd.extend(
        [
            "--header-out",
            str(header_out),
            "--source-out",
            str(source_out),
        ]
    )
    subprocess.run(cmd, cwd=ROOT, check=True)


def format_generated_outputs() -> None:
    if CLANG_FORMAT is None:
        print("warning: clang-format not found in PATH; skipping formatting", file=sys.stderr)
        return

    generated_paths = sorted(GENERATED_INCLUDE_ROOT.glob("*.inc")) + sorted(
        GENERATED_SOURCE_ROOT.glob("*.inc")
    )
    subprocess.run(
        [CLANG_FORMAT, "-i", *(str(path) for path in generated_paths)],
        cwd=ROOT,
        check=True,
    )


def main() -> int:
    for sections, header_out, source_out in JOBS:
        run_job(sections, header_out, source_out)
    subprocess.run([sys.executable, str(AUTO_GEN)], cwd=ROOT, check=True)
    format_generated_outputs()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
