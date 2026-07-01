#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

# pyre-strict

from __future__ import annotations

import argparse
import logging
import os
import random
import re
import subprocess
import sys
from concurrent.futures import as_completed, ThreadPoolExecutor

# Run as a plain script (not an installed package), so this directory is not on the
# import path; add it so the sibling runner_* imports below resolve (hence each E402).
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.append(_HERE)

from runner_execute import _get_gdb, execute  # noqa: E402
from runner_build import (  # noqa: E402
    build_runs,
    INJECTION_SITES,
    REMOTE_COMPACTION_ENTRY_FN,
    Run,
)
from runner_report import report  # noqa: E402

logger: logging.Logger = logging.getLogger("runner")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report_dir = os.path.abspath(args.report_dir)
    stress_cmd = os.path.abspath(args.stress_cmd)
    parallel_runs = max(args.parallel, 1)
    base_seed = args.seed or random.randint(1, 2**64)  # 0 -> a fresh seed each run

    os.makedirs(report_dir, exist_ok=True)
    _setup_logging(report_dir)

    # Fail fast on the deterministic, whole-run problems before doing any work.
    if not os.path.exists(stress_cmd):
        logger.error("stress_cmd not found at %s", stress_cmd)
        return 2
    try:
        verify_injection_site(stress_cmd, args.op)
    except RuntimeError as e:
        logger.error("%s", e)
        return 2

    logger.info(
        "runner: op=%s runs=%d base_seed=%d report_dir=%s parallel=%d gdb=%s",
        args.op,
        args.runs,
        base_seed,
        report_dir,
        parallel_runs,
        _get_gdb(),
    )
    # One number reproduces everything: run i used seed base_seed + i, so the whole set
    # replays with --seed=base_seed, and a single run i with --seed=<base_seed+i> --runs 1.
    logger.info("reproduce with --seed=%d (run i used seed %d+i)", base_seed, base_seed)
    injector_py = os.path.join(_HERE, "injector.py")
    runs = build_runs(
        args.op, args.runs, report_dir, base_seed, args.randomize_stress_flags
    )
    execute_all(runs, injector_py, stress_cmd, parallel_runs)
    report(runs, args.op, report_dir, base_seed)
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CPU corruption injection runner: runs db_stress under gdb and, "
        "per run, injects CPU corruption into a single db_stress operation (a write, "
        "flush, or compaction), then classifies the outcome."
    )
    parser.add_argument(
        "--op", required=True, choices=tuple(INJECTION_SITES), help="op type to inject"
    )
    parser.add_argument(
        "--runs",
        type=int,
        required=True,
        help="how many runs to do (each run injects into one op).",
    )
    parser.add_argument(
        "--stress_cmd", required=True, help="path to the db_stress binary"
    )
    parser.add_argument(
        "--report_dir",
        required=True,
        help="output dir for ALL artifacts: runner.log, summary.json, and one "
        "run_XXXXX/ per run (gdb.log, inject.json, data_corruption.*.json).",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=1,
        help="how many runs to execute at once (default 1); each db_stress run is "
        "itself single-threaded, so raise this to use more cores.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="master seed for the run set. 0 (default) picks a fresh random seed each "
        "invocation (logged as base_seed=N); pass that N back via --seed to reproduce "
        "the same runs. Run i uses seed N+i.",
    )
    parser.add_argument(
        "--randomize_stress_flags",
        action="store_true",
        help="randomize every db_stress flag the runner does not pin, varying it run to "
        "run to widen coverage; the pinned flags (what makes an injected CPU corruption "
        "reachable and observable) stay fixed. Off by default.",
    )
    return parser.parse_args(argv)


def _setup_logging(report_dir: str) -> None:
    # Log to the console AND <report_dir>/runner.log, so runner-level logging lands
    # beside the per-run artifacts and the summary.
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    log_file = logging.FileHandler(os.path.join(report_dir, "runner.log"))
    log_file.setFormatter(fmt)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers = [console, log_file]


def verify_injection_site(stress_cmd: str, op: str) -> None:
    # Before running anything, confirm gdb can work with this op's injection site on this
    # db_stress build: every function we break on still resolves, and gdb can read its
    # source file:line. Checking entry_fn's source line alone catches a gdb too old to
    # read this build's debug info (which makes the telemetry's source come out "@ ?:0").
    site = INJECTION_SITES[op]
    functions = [site.entry_fn, *site.target_fns]
    # A randomize-mode compaction run may inject into remote compaction, breaking on the
    # remote job instead of site.entry_fn; verify that symbol resolves too (harmless to
    # check even for non-randomized runs).
    if op == "compaction":
        functions.append(REMOTE_COMPACTION_ENTRY_FN)
    functions = list(dict.fromkeys(functions))
    gdb_output = _run_gdb_check(stress_cmd, functions, site.entry_fn)

    missing = [fn for fn in functions if _function_not_found(gdb_output, fn)]
    if missing:
        raise RuntimeError(
            f"gdb could not set a breakpoint on these functions in "
            f"{os.path.basename(stress_cmd)} (renamed, fully inlined, or not in this "
            f"build?): {', '.join(missing)}\n{gdb_output}"
        )
    if _no_source_lines(gdb_output):
        raise RuntimeError(
            f"gdb resolved the functions but cannot read their source line numbers; "
            f"this gdb ({_get_gdb()}) is likely too old for this build -- point ICC_GDB at a "
            f"newer one.\n{gdb_output}"
        )
    logger.info("gdb check OK for op=%s: %s", op, ", ".join(functions))


def _run_gdb_check(stress_cmd: str, functions: list[str], entry_fn: str) -> str:
    # gdb loads db_stress only for its symbols (it never runs it, so no db_stress flags
    # are needed): break on each function, ask entry_fn's source line, then quit. --nx
    # ignores the user's ~/.gdbinit so their gdb config can't change the output.
    commands: list[str] = []
    for function in functions:
        commands += ["-ex", f"break {function}"]
    commands += ["-ex", f"info line {entry_fn}", "-ex", "quit"]
    try:
        finished = subprocess.run(
            [_get_gdb(), "--batch", "--nx", *commands, stress_cmd],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=180,
            check=False,
        )
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"gdb check timed out: {e}") from e
    except OSError as e:
        raise RuntimeError(f"could not launch gdb ({_get_gdb()}): {e}") from e
    return finished.stdout.decode("utf-8", "replace")


def _function_not_found(gdb_output: str, function: str) -> bool:
    short_name = function.rsplit("::", 1)[-1]  # "rocksdb::MemTable::Add" -> "Add"
    markers = (
        f'Function "{function}" not defined',
        f'Function "{short_name}" not defined',
        f'No symbol "{function}"',
        f'No symbol "{short_name}"',
    )
    return any(marker in gdb_output for marker in markers)


def _no_source_lines(gdb_output: str) -> bool:
    # A gdb too old for this build's debug info resolves no source line (the telemetry's
    # source would come out "@ ?:0"): a Dwarf Error, or `info line` yielding no "Line N
    # of ..." at all. A function's cold-split (.cold) range legitimately has no line info,
    # so that benign "No line number information" message alone is not a failure.
    if "Dwarf Error" in gdb_output:
        return True
    return re.search(r"Line \d+ of ", gdb_output) is None


def execute_all(
    runs: list[Run],
    injector_py: str,
    stress_cmd: str,
    parallel_runs: int,
) -> None:
    with ThreadPoolExecutor(max_workers=parallel_runs) as pool:
        runs_by_future = {
            pool.submit(execute, run, injector_py, stress_cmd): run for run in runs
        }
        for future in as_completed(runs_by_future):
            run = runs_by_future[future]
            try:
                future.result()
            except Exception:  # noqa: BLE001 -- one run's crash is its own ERROR, never aborts the rest
                run.outcome = "ERROR"
                logger.exception("run %d crashed", run.index)
            logger.info("run %d: %s", run.index, run.outcome)


if __name__ == "__main__":
    sys.exit(main())
