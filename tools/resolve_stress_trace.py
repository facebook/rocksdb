#!/usr/bin/env python3
"""
resolve_stress_trace.py — Resolve function addresses in stress trace dump files.

Reads raw stress trace files (produced by monitoring/stress_trace.{h,cc} on
crash) and replaces hex addresses with human-readable symbol names using
addr2line.

Usage:
    # Resolve a single trace file:
    python3 tools/resolve_stress_trace.py --binary db_stress trace-file.txt

    # Resolve all trace files from a crash (auto-finds db_stress in parent dir):
    python3 tools/resolve_stress_trace.py /tmp/rocksdb_crashtest_*/stress-trace-*.txt

    # Pipe-friendly (reads stdin if no files given):
    cat trace.txt | python3 tools/resolve_stress_trace.py --binary ./db_stress

Output goes to stdout. Use --in-place to overwrite the original files, or
--output-dir to write resolved files to a directory.
"""

import argparse
import os
import re
import subprocess
import sys
from collections import OrderedDict


def find_binary(trace_dir):
    """Try to find the db_stress binary near the trace files."""
    # Trace files are at <db>/../stress-trace-<pid>-*.txt
    # The db_stress binary might be in the working dir or the repo root
    candidates = [
        os.path.join(trace_dir, "db_stress"),
        os.path.join(trace_dir, "..", "db_stress"),
        os.path.join(os.getcwd(), "db_stress"),
    ]
    for c in candidates:
        if os.path.isfile(c) and os.access(c, os.X_OK):
            return os.path.realpath(c)
    return None


def batch_resolve(binary, addresses):
    """Resolve a batch of addresses using addr2line. Returns {addr: (func, file)}."""
    if not addresses:
        return {}

    addr_list = list(addresses)
    try:
        result = subprocess.run(
            ["addr2line", "-f", "-C", "-e", binary] + addr_list,
            capture_output=True, text=True, timeout=120
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return {}

    lines = result.stdout.strip().split("\n")
    resolved = {}
    for i in range(0, len(lines) - 1, 2):
        if i // 2 < len(addr_list):
            func = lines[i].strip()
            loc = lines[i + 1].strip() if i + 1 < len(lines) else "?"
            # Shorten the function name: strip rocksdb:: prefix and common noise
            func = func.replace("rocksdb::", "")
            # Shorten std::string
            func = re.sub(
                r'std::__cxx11::basic_string<char, std::char_traits<char>, '
                r'std::allocator<char>\s*>',
                'string', func)
            func = re.sub(r'std::basic_string<char>', 'string', func)
            # Shorten file path to just filename:line
            loc_short = loc
            if "/" in loc:
                loc_short = loc.rsplit("/", 1)[-1]
            resolved[addr_list[i // 2]] = (func, loc_short)
    return resolved


def extract_addresses(lines):
    """Extract all unique hex addresses from trace lines."""
    addrs = OrderedDict()
    for line in lines:
        m = re.search(r'\(0x([0-9a-f]+)\)', line)
        if m:
            addr = "0x" + m.group(1)
            addrs[addr] = True
    return list(addrs.keys())


def resolve_trace(lines, symbol_map):
    """Replace hex addresses with resolved symbols in trace lines."""
    out = []
    for line in lines:
        m = re.match(
            r'^(\s*\[\d+\]) ENTER .* \((0x[0-9a-f]+)\)$', line.rstrip())
        if m:
            prefix, addr = m.group(1), m.group(2)
            if addr in symbol_map:
                func, loc = symbol_map[addr]
                out.append(f"{prefix} {func}  [{loc}]")
            else:
                out.append(line.rstrip())
        else:
            out.append(line.rstrip())
    return out


def main():
    parser = argparse.ArgumentParser(
        description="Resolve addresses in stress trace dump files.")
    parser.add_argument("files", nargs="*",
                        help="Trace file(s) to resolve. Reads stdin if empty.")
    parser.add_argument("--binary", "-b",
                        help="Path to db_stress binary (auto-detected if omitted)")
    parser.add_argument("--in-place", "-i", action="store_true",
                        help="Overwrite original files with resolved output")
    parser.add_argument("--output-dir", "-o",
                        help="Write resolved files to this directory")
    args = parser.parse_args()

    # Read from stdin if no files
    if not args.files:
        lines = sys.stdin.readlines()
        binary = args.binary or find_binary(os.getcwd())
        if not binary:
            print("Error: --binary required (could not auto-detect db_stress)",
                  file=sys.stderr)
            sys.exit(1)
        addrs = extract_addresses(lines)
        symbol_map = batch_resolve(binary, addrs)
        for line in resolve_trace(lines, symbol_map):
            print(line)
        return

    # Auto-detect binary from first trace file's directory
    binary = args.binary
    if not binary:
        binary = find_binary(os.path.dirname(os.path.abspath(args.files[0])))
    if not binary:
        binary = find_binary(os.getcwd())
    if not binary:
        print("Error: --binary required (could not auto-detect db_stress)",
              file=sys.stderr)
        sys.exit(1)

    if args.output_dir:
        os.makedirs(args.output_dir, exist_ok=True)

    # Collect all unique addresses across all files first (one addr2line call)
    all_lines = {}  # filename -> lines
    all_addrs = OrderedDict()
    for fname in args.files:
        if not os.path.isfile(fname):
            print(f"Warning: {fname} not found, skipping", file=sys.stderr)
            continue
        with open(fname) as f:
            lines = f.readlines()
        all_lines[fname] = lines
        for addr in extract_addresses(lines):
            all_addrs[addr] = True

    if not all_addrs:
        print("No addresses found in trace files.", file=sys.stderr)
        return

    print(f"Resolving {len(all_addrs)} unique addresses from "
          f"{len(all_lines)} files using {binary}...", file=sys.stderr)

    symbol_map = batch_resolve(binary, list(all_addrs.keys()))
    resolved_count = sum(1 for _, (f, _) in symbol_map.items() if f != "??")
    print(f"Resolved {resolved_count}/{len(all_addrs)} symbols.", file=sys.stderr)

    # Process each file
    for fname, lines in all_lines.items():
        resolved = resolve_trace(lines, symbol_map)

        if args.in_place:
            with open(fname, "w") as f:
                for line in resolved:
                    f.write(line + "\n")
            print(f"  Resolved: {fname}", file=sys.stderr)
        elif args.output_dir:
            out_name = os.path.join(
                args.output_dir,
                os.path.basename(fname).replace(".txt", "-resolved.txt"))
            with open(out_name, "w") as f:
                for line in resolved:
                    f.write(line + "\n")
            print(f"  Wrote: {out_name}", file=sys.stderr)
        else:
            # Print to stdout with a header
            print(f"\n{'=' * 60}")
            print(f"  {os.path.basename(fname)}")
            print(f"{'=' * 60}")
            for line in resolved:
                print(line)


if __name__ == "__main__":
    main()
