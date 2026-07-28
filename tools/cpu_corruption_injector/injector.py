#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

# pyre-unsafe
# Imports `gdb` (only inside gdb's embedded Python, not via pip/buck), so this file
# and its sibling injector_* modules are not buck/pyre targets.

from __future__ import annotations

import argparse
import os
import random
import sys
import traceback
from dataclasses import dataclass, field
from typing import Optional

import gdb  # provided by gdb's embedded Python (not pip-importable)

# gdb runs this file as a plain script (not a package), so its directory is not on
# sys.path; append it (at the END, so a sibling can never shadow a stdlib module) so
# the injector_* imports below resolve.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.append(_HERE)

# These imports must follow the sys.path bootstrap above, so flake8's E402
# ("module level import not at top of file") is expected here -- hence the noqa.
import injector_navigate as nav  # noqa: E402
from injector_register_corruption import CorruptedInstruction  # noqa: E402
from injector_telemetry import write_inject_record_json  # noqa: E402


@dataclass
class InjectRecord:
    # outcome of this run's injection attempt:
    #   injected | op_not_reached | target_fn_not_reached | no_critical_instruction
    #   | error (a bug in the injector itself, distinct from a db_stress crash)
    injection_result: str = "op_not_reached"
    # fatal signal db_stress took (e.g. SIGSEGV), else None; set only when
    # injection_result == "injected" (an injection-caused db_stress crash).
    db_stress_crash_signal: Optional[str] = None
    op: str = ""
    op_index: int = 0
    entry_fn: str = ""
    target_fn: str = ""
    critical_instruction_index: int = 0
    corruptions: list[CorruptedInstruction] = field(default_factory=list)
    ops_seen: int = 0
    critical_instructions_seen: int = 0


@dataclass
class Args:
    op: str
    op_index: int
    entry_fn: str
    target_fn: str
    dir: str
    seed: int


def parse_args() -> Args:
    parser = argparse.ArgumentParser(
        description="db_stress gdb cpu-corruption injector"
    )
    parser.add_argument("--op", required=True, choices=["write", "flush", "compaction"])
    parser.add_argument("--op_index", type=int, required=True)
    parser.add_argument(
        "--entry_fn",
        required=True,
        help="entry function whose op_index-th call is the op instance to corrupt",
    )
    parser.add_argument(
        "--target_fn",
        required=True,
        # entry_fn is too large/expensive to single-step, so we step a smaller inner
        # function and corrupt a register there.
        help="inner function we single-step and corrupt a register in",
    )
    parser.add_argument("--dir", required=True, help="output directory for inject.json")
    parser.add_argument("--seed", type=int, default=0)
    # parse_known_args ignores anything gdb may have left in argv.
    parsed, _ = parser.parse_known_args(sys.argv[1:])
    return Args(
        op=parsed.op,
        op_index=max(parsed.op_index, 1),
        entry_fn=parsed.entry_fn,
        target_fn=parsed.target_fn,
        dir=parsed.dir,
        seed=parsed.seed,
    )


def run(record: InjectRecord, args: Args) -> None:
    record.op = args.op
    record.op_index = args.op_index
    record.entry_fn = args.entry_fn
    record.target_fn = args.target_fn

    out_path = os.path.join(args.dir, "inject.json")
    # Baseline record so inject.json always exists even if gdb dies mid-run; every
    # later write_inject_record_json truncates and rewrites the file, so the final write wins.
    write_inject_record_json(out_path, record)

    rng = random.Random(args.seed)
    nav.setup_gdb()
    site = nav.Navigator(args.entry_fn, args.target_fn)

    # Start db_stress, stopped at its very first instruction. reach_op then sets a
    # breakpoint on the op's entry_fn and runs to its op_index-th call -- the op
    # instance we corrupt.
    gdb.execute("starti")
    if not site.reach_op(args.op_index):
        record.injection_result = "op_not_reached"
        _finish_run_and_write(record, out_path, site)
        return
    if not site.reach_target_fn_call():
        record.injection_result = "target_fn_not_reached"
        _finish_run_and_write(record, out_path, site)
        return

    critical_instruction_index = _pick_critical_instruction_to_corrupt(site, rng)
    if critical_instruction_index is None:
        record.injection_result = "no_critical_instruction"
        _finish_run_and_write(record, out_path, site)
        return
    record.critical_instruction_index = critical_instruction_index

    record.injection_result = (
        "no_critical_instruction"  # until a corruption lands below
    )
    # Corrupt the chosen critical instruction in this op type's next target_fn call; if
    # that call has no critical instruction at that index, try the next call.
    while site.reach_target_fn_call():
        corruption, record.critical_instructions_seen = (
            site.corrupt_critical_instruction(critical_instruction_index, rng)
        )
        if corruption is not None:
            record.corruptions = [corruption]
            record.injection_result = "injected"
            write_inject_record_json(out_path, record)  # checkpoint: injection happened
            break
    _finish_run_and_write(record, out_path, site)


def _pick_critical_instruction_to_corrupt(
    site: nav.Navigator, rng: random.Random
) -> Optional[int]:
    """Single-step target_fn calls until one has critical instructions, then return
    a random index among them; None if none before the op ended. (Corrupting happens
    on a later call -- a call cannot be single-stepped twice.)"""
    while True:
        critical_instruction_count = site.count_critical_instructions()
        if critical_instruction_count > 0:
            return rng.randrange(critical_instruction_count)
        if not site.reach_target_fn_call():
            return None


def _finish_run_and_write(
    record: InjectRecord, out_path: str, site: nav.Navigator
) -> None:
    # The navigator owns db_stress's end-of-run lifecycle (kill-if-crashed vs
    # run-to-exit); we only record what it reports and write the file.
    record.ops_seen = site.ops_seen()
    record.db_stress_crash_signal = nav.finish_db_stress()
    write_inject_record_json(out_path, record)


def main() -> None:
    args = parse_args()
    record = InjectRecord()
    out_path = os.path.join(args.dir, "inject.json")
    try:
        run(record, args)
    # Blind-except (flake8 BLE001) on purpose: a bug in the injector must never abort
    # gdb and leave db_stress wedged. Log it, mark the run errored, and still end
    # db_stress + write inject.json.
    except Exception as exc:  # noqa: BLE001
        gdb.write(f"injector.py: fatal: {exc}\n{traceback.format_exc()}\n", gdb.STDERR)
        record.injection_result = "error"
        # The navigator owns ending db_stress (kill if crashed, else run to exit).
        record.db_stress_crash_signal = nav.finish_db_stress()
        write_inject_record_json(out_path, record)


main()
