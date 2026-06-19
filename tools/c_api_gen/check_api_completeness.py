#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

#!/usr/bin/env python3
"""Check that the public C API is link-complete and free of duplicate symbols.

Every function declared in ``include/rocksdb/c.h`` with
``extern ROCKSDB_LIBRARY_API`` must have exactly one definition in ``db/c.cc``.

This is a cheap, dependency-free safety net (no clang/libclang required) that
guards against a whole class of code-generation migration bugs: a wrapper whose
declaration is kept while its hand-written implementation is removed and never
regenerated. Such a function compiles fine into ``librocksdb`` (the missing
symbol is simply absent from the archive) and is invisible to RocksDB's own C
test unless that test happens to call it -- but it breaks every downstream
language binding (Rust, Go, Python, ...) that links against the symbol.

It also flags duplicate definitions, which would otherwise fail only at link
time with a less obvious error.

Run:
    python3 tools/c_api_gen/check_api_completeness.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
C_HEADER = ROOT / "include/rocksdb/c.h"
C_SOURCE = ROOT / "db/c.cc"

# A public C API declaration looks like:
#   extern ROCKSDB_LIBRARY_API <return type> rocksdb_foo(...);
# The return type may be on the same line or the following one, so anchor on
# the export macro and capture the first rocksdb_* identifier that is
# immediately followed by '('.
DECL_RE = re.compile(
    r"extern\s+ROCKSDB_LIBRARY_API\b[\s\S]*?\b(rocksdb_[A-Za-z0-9_]+)\s*\(",
)

# A definition in c.cc is a top-level (column 0) function whose name is a
# rocksdb_* identifier immediately followed by '('. Return types/pointers may
# precede the name on the same line (e.g. "rocksdb_t* rocksdb_open(").
DEF_RE = re.compile(
    r"^(?:[A-Za-z_][\w:<>,\s\*&]*?\s[\*&]*)?(rocksdb_[A-Za-z0-9_]+)\s*\(",
    re.MULTILINE,
)

# Function-pointer typedefs etc. are not function definitions; the patterns
# above already exclude lines containing "(*name)" because the identifier must
# be directly followed by '('.


def declared_functions(header_text: str) -> set[str]:
    return set(DECL_RE.findall(header_text))


def defined_functions(source_text: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for name in DEF_RE.findall(source_text):
        counts[name] = counts.get(name, 0) + 1
    return counts


def main() -> int:
    if not C_HEADER.is_file() or not C_SOURCE.is_file():
        sys.stderr.write(
            f"error: expected {C_HEADER} and {C_SOURCE} to exist\n"
        )
        return 2

    declared = declared_functions(C_HEADER.read_text())
    defined = defined_functions(C_SOURCE.read_text())

    missing = sorted(name for name in declared if name not in defined)
    duplicated = sorted(name for name, n in defined.items() if n > 1)

    status = 0
    if missing:
        status = 1
        sys.stderr.write(
            f"error: {len(missing)} C API function(s) are declared in "
            f"{C_HEADER.relative_to(ROOT)} but have no definition in "
            f"{C_SOURCE.relative_to(ROOT)}:\n"
        )
        for name in missing:
            sys.stderr.write(f"  - {name}\n")
        sys.stderr.write(
            "\nEach such function breaks downstream bindings at link time.\n"
            "Add the implementation to tools/c_api_gen/c_base.cc (or via the\n"
            "generators) and rerun python3 tools/c_api_gen/regen_all.py.\n"
        )

    if duplicated:
        status = 1
        sys.stderr.write(
            f"error: {len(duplicated)} C API function(s) are defined more than "
            f"once in {C_SOURCE.relative_to(ROOT)}:\n"
        )
        for name in duplicated:
            sys.stderr.write(f"  - {name} ({defined[name]} definitions)\n")

    if status == 0:
        print(
            f"C API is link-complete: all {len(declared)} declared functions "
            "have exactly one definition."
        )
    return status


if __name__ == "__main__":
    raise SystemExit(main())
