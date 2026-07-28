#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the Apache file in the root directory).

#!/usr/bin/env python3
"""Check the public C API for backward-incompatible changes vs a reference.

RocksDB's C API (include/rocksdb/c.h) has a strong backward-compatibility
guarantee. Because most wrappers are now code-generated from the C++ headers, a
careless C++ change (renamed/removed field, changed field type) or a generator
change can silently remove a public C function or change its signature -- an
ABI/API break for every downstream language binding.

This is a robust, signature-level gate (it compares declared function
signatures, not function bodies, so it has no false positives from formatting
or body-token differences). For every function present in the reference
revision's c.h it requires that the current c.h still declares it with an
identical signature (parameter names are ignored; they do not affect the C
ABI). New functions are reported but never fail the check.

Intentional, reviewed incompatibilities (e.g. removing a function that was
declared but never implemented) must be recorded in
api_compatibility_allowlist.json with a reason.

Usage:
    python3 tools/c_api_gen/check_api_compatibility.py [--ref main]
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
C_HEADER_REL = "include/rocksdb/c.h"
C_HEADER = ROOT / C_HEADER_REL
ALLOWLIST_PATH = ROOT / "tools/c_api_gen/api_compatibility_allowlist.json"

# C type-name tokens that are never a parameter *name*. Used to tell
# "unsigned char" (type, no name) apart from "rocksdb_t db" (type + name).
_TYPE_KEYWORDS = frozenset(
    {
        "void",
        "char",
        "short",
        "int",
        "long",
        "float",
        "double",
        "unsigned",
        "signed",
        "bool",
        "const",
        "size_t",
        "ssize_t",
        "intptr_t",
        "uintptr_t",
        "ptrdiff_t",
    }
)

DECL_RE = re.compile(
    r"extern\s+ROCKSDB_LIBRARY_API\s+([\s\S]*?)\b(rocksdb_[A-Za-z0-9_]+)\s*\("
    r"([^;]*?)\)\s*;",
)


def _strip_param_name(param: str) -> str:
    """Return the parameter type with any trailing parameter name removed."""
    param = " ".join(param.replace("[]", " []").split())
    if not param:
        return param
    tokens = param.split()
    last = tokens[-1]
    if (
        last in _TYPE_KEYWORDS
        or last.endswith("_t")
        or "*" in last
        or last == "[]"
    ):
        return param  # unnamed parameter; nothing to strip
    if len(tokens) > 1 and re.fullmatch(r"[A-Za-z_]\w*", last):
        return " ".join(tokens[:-1])
    return param


def parse_signatures(header_text: str) -> dict[str, str]:
    """Map function name -> normalized signature string (param names removed)."""
    sigs: dict[str, str] = {}
    for match in DECL_RE.finditer(header_text):
        ret = " ".join(match.group(1).split())
        name = match.group(2)
        params = [
            _strip_param_name(p) for p in match.group(3).split(",") if p.strip()
        ]
        sigs[name] = f"{ret} {name}({', '.join(params)})"
    return sigs


def parse_symbols(header_text: str) -> set[str]:
    """Non-function public identifiers: enum constants and typedef names.

    These are part of the C API contract too (e.g. rocksdb_txndb_write_policy_*,
    the rocksdb tickers enum). A rocksdb_* token followed by '=', ',' or '}' is
    an enum constant; one introduced/closed by typedef is a type name. Anything
    followed by '(' is a function (handled by parse_signatures) and excluded.
    """
    functions = {m.group(2) for m in DECL_RE.finditer(header_text)}
    functions |= set(re.findall(r"\b(rocksdb_[A-Za-z0-9_]+)\s*\(", header_text))
    symbols: set[str] = set()
    for m in re.finditer(r"\b(rocksdb_[A-Za-z0-9_]+)\s*(?:=|,|\}|;)", header_text):
        symbols.add(m.group(1))
    symbols |= set(
        re.findall(r"typedef\s+struct\s+(rocksdb_[A-Za-z0-9_]+)", header_text)
    )
    return symbols - functions


def git_show(ref: str, path: str) -> str | None:
    proc = subprocess.run(
        ["git", "show", f"{ref}:{path}"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        return None
    return proc.stdout


def load_allowlist() -> dict[str, set[str]]:
    """Allowed incompatibilities keyed by kind: removed / changed / removed_symbol."""
    allowed = {"removed": set(), "changed": set(), "removed_symbol": set()}
    if not ALLOWLIST_PATH.exists():
        return allowed
    data = json.loads(ALLOWLIST_PATH.read_text())
    for entry in data.get("allow", []):
        kind = entry.get("kind")
        name = entry.get("function") or entry.get("symbol")
        if kind in allowed and name:
            allowed[kind].add(name)
    return allowed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ref",
        default="main",
        help=(
            "git revision to treat as the compatibility baseline (default: "
            "main). The current working-tree c.h must stay compatible with it."
        ),
    )
    args = parser.parse_args()

    ref_text = git_show(args.ref, C_HEADER_REL)
    if ref_text is None:
        sys.stderr.write(
            f"error: could not read {C_HEADER_REL} at revision '{args.ref}'. "
            "Pass an existing revision via --ref (e.g. --ref origin/main).\n"
        )
        return 2

    cur_text = C_HEADER.read_text()
    ref = parse_signatures(ref_text)
    cur = parse_signatures(cur_text)
    allowed = load_allowlist()

    removed = sorted(n for n in ref if n not in cur and n not in allowed["removed"])
    changed = sorted(
        n
        for n in ref
        if n in cur and ref[n] != cur[n] and n not in allowed["changed"]
    )
    new = sorted(n for n in cur if n not in ref)

    ref_symbols = parse_symbols(ref_text)
    cur_symbols = parse_symbols(cur_text)
    removed_symbols = sorted(
        s
        for s in ref_symbols
        if s not in cur_symbols and s not in allowed["removed_symbol"]
    )

    status = 0
    if removed:
        status = 1
        sys.stderr.write(
            f"error: {len(removed)} public C API function(s) present at "
            f"'{args.ref}' are missing now (backward-incompatible removal):\n"
        )
        for name in removed:
            sys.stderr.write(f"  - {name}\n")
    if changed:
        status = 1
        sys.stderr.write(
            f"error: {len(changed)} public C API function(s) changed signature "
            f"(backward-incompatible ABI change):\n"
        )
        for name in changed:
            sys.stderr.write(f"  - {name}\n")
            sys.stderr.write(f"      was: {ref[name]}\n")
            sys.stderr.write(f"      now: {cur[name]}\n")
    if removed_symbols:
        status = 1
        sys.stderr.write(
            f"error: {len(removed_symbols)} public C API symbol(s) (enum "
            f"constants / typedefs) present at '{args.ref}' are missing now "
            "(backward-incompatible removal):\n"
        )
        for name in removed_symbols:
            sys.stderr.write(f"  - {name}\n")
    if status != 0:
        sys.stderr.write(
            "\nIf a change is intentional and reviewed, record it in\n"
            f"{ALLOWLIST_PATH.relative_to(ROOT)} with a reason; otherwise restore "
            "the original declaration (see tools/c_api_gen/abi_type_overrides.json "
            "for pinning a generated C type).\n"
        )
    else:
        print(
            f"C API is backward-compatible with '{args.ref}': "
            f"{len(ref)} reference functions preserved, {len(new)} new; "
            f"{len(ref_symbols)} enum/typedef symbols preserved."
        )
    return status


if __name__ == "__main__":
    raise SystemExit(main())
