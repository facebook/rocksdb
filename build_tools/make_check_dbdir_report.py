#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import argparse
import os
import sys


DB_MARKER_FILE_NAMES = {
    "CURRENT",
    "IDENTITY",
    "LOCK",
    "LOG",
}
DB_MARKER_PREFIXES = (
    "MANIFEST-",
    "OPTIONS-",
    "LOG.old.",
)
DB_MARKER_SUFFIXES = (
    ".blob",
    ".dbtmp",
    ".ldb",
    ".log",
    ".sst",
    ".sst.trash",
)


def human_readable_bytes(num_bytes):
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    value = float(num_bytes)
    unit_index = 0
    while value >= 1024.0 and unit_index + 1 < len(units):
        value /= 1024.0
        unit_index += 1
    if unit_index == 0:
        return f"{num_bytes}{units[unit_index]}"
    return f"{value:.2f}{units[unit_index]}"


def file_looks_db_related(filename):
    return (
        filename in DB_MARKER_FILE_NAMES
        or any(filename.startswith(prefix) for prefix in DB_MARKER_PREFIXES)
        or any(filename.endswith(suffix) for suffix in DB_MARKER_SUFFIXES)
    )


def format_filesystem_usage(path):
    if not hasattr(os, "statvfs"):
        return f"  {path}: filesystem usage unavailable on this platform"

    try:
        stats = os.statvfs(path)
    except OSError as exc:
        return f"  {path}: failed to collect filesystem usage: {exc}"

    block_size = stats.f_frsize or stats.f_bsize
    total_bytes = stats.f_blocks * block_size
    available_bytes = stats.f_bavail * block_size
    used_bytes = max(total_bytes - available_bytes, 0)
    used_pct = 0.0 if total_bytes == 0 else 100.0 * used_bytes / total_bytes
    return (
        f"  {path}: total={human_readable_bytes(total_bytes)} "
        f"used={human_readable_bytes(used_bytes)} "
        f"avail={human_readable_bytes(available_bytes)} "
        f"use={used_pct:.1f}%"
    )


def scan_directory(path, ancestor_is_db_dir=False):
    total_bytes = 0
    db_entries = []
    errors = []
    child_dirs = []
    has_db_marker = False

    try:
        with os.scandir(path) as iterator:
            children = sorted(list(iterator), key=lambda entry: entry.name)
    except OSError as exc:
        return 0, [], [(path, f"failed to enumerate directory contents: {exc}")]

    for child in children:
        try:
            if child.is_dir(follow_symlinks=False):
                child_dirs.append(child.path)
                continue

            if not child.is_file(follow_symlinks=False):
                continue

            file_size = child.stat(follow_symlinks=False).st_size
        except FileNotFoundError:
            continue
        except OSError as exc:
            errors.append((child.path, f"failed to stat child path: {exc}"))
            continue

        total_bytes += file_size
        if file_looks_db_related(child.name):
            has_db_marker = True

    current_is_db_dir = has_db_marker and not ancestor_is_db_dir
    for child_dir in child_dirs:
        child_total, child_db_entries, child_errors = scan_directory(
            child_dir, ancestor_is_db_dir or current_is_db_dir
        )
        total_bytes += child_total
        db_entries.extend(child_db_entries)
        errors.extend(child_errors)

    if current_is_db_dir:
        db_entries.append({"path": path, "bytes": total_bytes})

    return total_bytes, db_entries, errors


def collect_run_diagnostics(runs_root):
    run_entries = []
    db_entries = []
    errors = []

    try:
        with os.scandir(runs_root) as iterator:
            run_dirs = sorted(
                [entry for entry in iterator if entry.is_dir(follow_symlinks=False)],
                key=lambda entry: entry.name,
            )
    except FileNotFoundError:
        return {"run_entries": [], "db_entries": [], "errors": []}
    except OSError as exc:
        return {
            "run_entries": [],
            "db_entries": [],
            "errors": [(runs_root, f"failed to enumerate test runs: {exc}")],
        }

    for run_dir in run_dirs:
        run_bytes, run_db_entries, run_errors = scan_directory(run_dir.path)
        run_label = os.path.relpath(run_dir.path, runs_root)
        run_entries.append(
            {
                "name": run_label,
                "path": run_dir.path,
                "bytes": run_bytes,
            }
        )
        for db_entry in run_db_entries:
            relpath = os.path.relpath(db_entry["path"], run_dir.path)
            if relpath == ".":
                relpath = "<TEST_TMPDIR>"
            db_entries.append(
                {
                    "run_name": run_label,
                    "path": db_entry["path"],
                    "relpath": relpath,
                    "bytes": db_entry["bytes"],
                }
            )
        errors.extend(run_errors)

    run_entries.sort(key=lambda entry: (-entry["bytes"], entry["name"]))
    db_entries.sort(
        key=lambda entry: (-entry["bytes"], entry["run_name"], entry["relpath"])
    )
    return {
        "run_entries": run_entries,
        "db_entries": db_entries,
        "errors": errors,
    }


def build_make_check_disk_report(
    runs_root, test_tmpdir=None, top_n=10, include_dev_shm=True
):
    diagnostics = collect_run_diagnostics(runs_root)
    lines = ["=== make check disk usage diagnostics ===", "Filesystem usage:"]

    printed_paths = set()
    if include_dev_shm and os.path.isdir("/dev/shm"):
        lines.append(format_filesystem_usage("/dev/shm"))
        printed_paths.add(os.path.normpath("/dev/shm"))

    for path in [test_tmpdir, runs_root]:
        if not path:
            continue
        normalized = os.path.normpath(path)
        if normalized in printed_paths or not os.path.isdir(normalized):
            continue
        lines.append(format_filesystem_usage(normalized))
        printed_paths.add(normalized)

    lines.append(f"Runs root: {runs_root}")

    run_entries = diagnostics["run_entries"]
    db_entries = diagnostics["db_entries"]

    lines.append(f"Top {min(top_n, len(run_entries))} test tmpdirs by size:")
    if not run_entries:
        lines.append("  no per-test temp directories found")
    else:
        for index, entry in enumerate(run_entries[:top_n], start=1):
            lines.append(
                "  {}. {}  {}  path={}".format(
                    index,
                    human_readable_bytes(entry["bytes"]),
                    entry["name"],
                    entry["path"],
                )
            )

    lines.append(f"Top {min(top_n, len(db_entries))} DB-like directories by size:")
    if not db_entries:
        lines.append("  no DB-like directories found")
    else:
        for index, entry in enumerate(db_entries[:top_n], start=1):
            lines.append(
                "  {}. {}  owner={}  db_dir={}  path={}".format(
                    index,
                    human_readable_bytes(entry["bytes"]),
                    entry["run_name"],
                    entry["relpath"],
                    entry["path"],
                )
            )

    if diagnostics["errors"]:
        lines.append("Collection errors:")
        for path, error in diagnostics["errors"]:
            lines.append(f"  {path}: {error}")

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(
        description="Print disk-usage diagnostics for make check temp directories."
    )
    parser.add_argument("--runs_root", required=True)
    parser.add_argument("--test_tmpdir")
    parser.add_argument("--top_n", type=int, default=10)
    parser.add_argument("--no_dev_shm", action="store_true", default=False)
    args = parser.parse_args()

    report = build_make_check_disk_report(
        args.runs_root,
        args.test_tmpdir,
        args.top_n,
        not args.no_dev_shm,
    )
    sys.stdout.write(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
