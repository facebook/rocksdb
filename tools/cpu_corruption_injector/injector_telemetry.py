#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

# pyre-unsafe
# Runs ONLY inside the gdb python runtime (imports `gdb`); not a buck/pyre target.

# This module is the injector's OBSERVABILITY layer: it captures where a corruption
# landed (CorruptionDetails) and serializes the whole per-run record to inject.json.
# It only reads the other modules' types, never the reverse, so the import graph stays
# acyclic (everyone imports telemetry; telemetry imports none of them).
#
# inject.json data model -- each type is defined where its logic lives:
#
#   InjectRecord                (injector.py)        one per run: injection_result
#                                                    (injected / op_not_reached / ...)
#                                                    + op context
#     .corruptions: list of
#       CorruptedInstruction    (injector_register_corruption.py)  a single flip
#         .instruction:
#           CriticalInstruction (injector_critical_instruction.py) reg / type / text
#         .before, .after       the register value the flip changed (hex / lane list)
#         .details:
#           CorruptionDetails   (this module)        where the flip landed in source

from __future__ import annotations

import json
from dataclasses import dataclass

import gdb  # provided by gdb's embedded Python (not pip-importable)


@dataclass
class CorruptionDetails:
    """Where a corruption landed in source -- enough to triage it afterwards.
    source      "function @ file:line" of the corrupted instruction, e.g.
                "rocksdb::Slice::compare @ include/rocksdb/slice.h:285".
    call_chain  the call chain from that instruction up to and including target_fn
                (the op's value-carrying function), innermost first -- inlined leaf
                frames plus the physical callers above them, e.g.
                ["rocksdb::Slice::compare @ include/rocksdb/slice.h:285",
                 "...BytewiseComparatorImpl::Compare @ util/comparator.cc:37",
                 "rocksdb::MemTable::Add @ db/memtable.cc:602"].
    """

    source: str
    call_chain: list[str]


def capture_corruption_details(target_fn: str) -> CorruptionDetails:
    """Snapshot where the instruction at $pc is in source. Call it BEFORE a
    corrupt-after-exec corruption steps past the instruction, so $pc still points at
    the instruction we corrupt. `source` is the innermost frame (the exact corrupted
    expression); `call_chain` extends up to target_fn for caller context."""
    call_chain = _call_chain_to_target(target_fn)
    source = call_chain[0] if call_chain else _func_offset()
    return CorruptionDetails(source=source, call_chain=call_chain)


def inject_record_to_json(inject_record) -> dict[str, object]:
    return {
        "injection_result": inject_record.injection_result,
        "db_stress_crash_signal": inject_record.db_stress_crash_signal,
        "op": inject_record.op,
        "op_index": inject_record.op_index,
        "entry_fn": inject_record.entry_fn,
        "target_fn": inject_record.target_fn,
        "critical_instruction_index": inject_record.critical_instruction_index,
        "corruptions": [
            _corrupted_instruction_to_json(c) for c in inject_record.corruptions
        ],
        "ops_seen": inject_record.ops_seen,
        "critical_instructions_seen": inject_record.critical_instructions_seen,
    }


def write_inject_record_json(path: str, inject_record) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(inject_record_to_json(inject_record), f, indent=2)
            f.write("\n")
    except OSError as e:
        gdb.write(f"injector.py: failed to write {path}: {e}\n", gdb.STDERR)


def _corrupted_instruction_to_json(corrupted) -> dict[str, object]:
    instruction = corrupted.instruction  # the CriticalInstruction we corrupted
    return {
        "instruction": instruction.text,
        "register": instruction.reg,
        "corruption_type": instruction.corruption_type,
        "before": corrupted.before,
        "after": corrupted.after,
        "details": {
            "source": corrupted.details.source,
            "call_chain": corrupted.details.call_chain,
        },
    }


def _func_offset() -> str:
    """ "function+offset" at $pc -- the `source` fallback when the pc has no inline frame
    (e.g. a leaf with no line info)."""
    try:
        pc = int(gdb.selected_frame().pc())
        out = gdb.execute(f"info symbol {hex(pc)}", to_string=True).strip()
    except gdb.error:
        return "?"
    # `info symbol` prints e.g. "rocksdb::MemTable::Add(...) + 123 in section .text of
    # /path"; we drop the " in section ..." tail and return "rocksdb::MemTable::Add(...)
    # + 123".
    return out.split(" in section ", 1)[0].strip()


def _call_chain_to_target(target_fn: str) -> list[str]:
    """The call chain at $pc, innermost first, as ["function @ file:line", ...], up to
    and INCLUDING target_fn -- the op's value-carrying function we corrupt inside.

    It spans both the inlined leaf chain (gdb exposes each inlined function as an
    INLINE_FRAME sharing the pc, so the leaf pins the exact corrupted expression) and
    the physical frames above them, up to target_fn. We stop at target_fn because
    frames above it are generic op dispatch, not part of the corrupted expression's
    context. A compiler-generated frame (e.g. a non-virtual thunk) has no source line
    -> "@ ?:0"; that is expected, and walking past it now reaches the real callers
    instead of dead-ending there. Capped at 32 frames in case target_fn is not found."""
    frames: list[str] = []
    try:
        frame = gdb.newest_frame()
    except gdb.error:
        return frames
    while frame is not None and len(frames) < 32:
        try:
            function_name = frame.name() or "?"
            sal = frame.find_sal()  # gdb's symbol-and-line lookup for this frame's pc
            file_name = sal.symtab.filename if sal and sal.symtab else "?"
            line = sal.line if sal else 0
            frames.append(f"{function_name} @ {file_name}:{line}")
            if target_fn in function_name:
                break  # reached the op's value-carrying function -- stop here
            frame = frame.older()
        except gdb.error:
            break
    return frames
