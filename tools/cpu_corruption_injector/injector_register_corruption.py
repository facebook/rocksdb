#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

# pyre-unsafe
# Runs ONLY inside the gdb python runtime (imports `gdb`); not a buck/pyre target.

from __future__ import annotations

import random
from dataclasses import dataclass

import gdb  # provided by gdb's embedded Python (not pip-importable)

from injector_critical_instruction import CriticalInstruction, simd_int64_lanes
from injector_telemetry import capture_corruption_details, CorruptionDetails

# eflags bit positions we flip: CF, ZF, SF, OF -- the condition flags a branch reads.
_EFLAGS_BITS = (0, 6, 7, 11)


@dataclass
class CorruptedInstruction:
    """A CriticalInstruction after we corrupted it, plus what we observed: the register
    value before and after the corruption, and the details to locate the instruction. (This
    is what one element of inject.json's "corruptions" is serialized from.)"""

    instruction: CriticalInstruction
    before: str
    after: str
    details: CorruptionDetails


def apply_corruption(
    critical_instruction: CriticalInstruction, rng: random.Random, target_fn: str
) -> CorruptedInstruction:
    """Corrupt the critical_instruction we are stopped on and return a
    CorruptedInstruction. If corrupt_after_exec, step the instruction first so we corrupt
    the value it just produced; otherwise corrupt now, before it consumes the register.
    A corruption that produces no observable change means the decoder mis-selected the
    operand -> the before != after assert (fail fast). target_fn bounds the captured
    call chain (see capture_corruption_details)."""
    details = capture_corruption_details(
        target_fn
    )  # before a possible step changes $pc
    if critical_instruction.corrupt_after_exec:
        gdb.execute("stepi")  # run the instruction so we corrupt the value it produced
    reg = critical_instruction.reg
    before = read_register(reg)
    _corrupt_register(reg, critical_instruction.corruption_type, rng)
    after = read_register(reg)
    assert before != after, f"corruption produced no change: {before!r} == {after!r}"
    return CorruptedInstruction(critical_instruction, before, after, details)


def _corrupt_register(reg: str, corruption_type: str, rng: random.Random) -> None:
    """Corrupt `reg` the way corruption_type says. Raises gdb.error if the write fails;
    callers fail fast on that, since the decoder only yields operands we can corrupt.

    We compute the XOR mask in Python and embed it as a DECIMAL literal, rather than
    writing the shift in gdb. The hazard is the gdb-side shift `1 << n` for large n
    (lane_bit_flip goes up to 1 << 63): gdb types the literal `1` as a 32-bit C `int`, so a
    shift of 32+ overflows to 0 -- e.g. gdb evaluates `1 << 40` as 0, silently a no-op.
    A whole decimal literal is safe -- gdb types an integer constant by its magnitude
    (C's rule), so the same mask written out, 1099511627776, is taken as a 64-bit
    `long`."""
    if corruption_type == "bit_flip":
        # Flip a random bit in the low byte (bits 0-7). Every GP register is >= 8 bits,
        # so a low-byte flip is always observable, and the low bytes hold most key/value
        # data -- the highest-yield, simplest choice.
        gdb.execute(f"set ${reg} = ${reg} ^ {1 << rng.randrange(8)}")
        return
    if corruption_type == "flag_flip":
        gdb.execute(f"set ${reg} = ${reg} ^ {1 << rng.choice(_EFLAGS_BITS)}")
        return
    if corruption_type == "lane_bit_flip":
        # gdb cannot XOR a whole vector register, only a scalar lane member -- so we
        # MUST address a lane just to write to the register at all. We then flip ONE
        # bit: the lane choice picks which 64-bit chunk of the value, the bit choice the
        # bit within it. Single-bit (not a whole-lane scribble) on purpose: a bit flip
        # is the realistic CPU fault we model and it matches bit_flip/flag_flip;
        # addressing the lane is just gdb's required mechanism, not a reason to corrupt
        # more.
        lane_expr, lane_count = simd_int64_lanes(reg)
        assert lane_expr is not None, f"no int64 lane view for vector reg {reg}"
        lane = rng.randrange(lane_count)
        gdb.execute(
            f"set ${lane_expr}[{lane}] = ${lane_expr}[{lane}] ^ {1 << rng.randrange(64)}"
        )
        return
    raise AssertionError(f"unknown corruption_type {corruption_type!r} (reg={reg})")


def read_register(reg: str) -> str:
    """The register's value as a string, for the before/after telemetry. The read
    matches the register kind:
      - eflags            -> hex of its low 32 bits (eflags is a 32-bit register)
      - a vector register -> its int64 lanes as a list, e.g. "[123,456]"
      - any other (GP)    -> hex of its low 64 bits
    gdb.parse_and_eval returns a gdb.Value; int(...) turns it into a Python int, and
    for a vector we index the lane members (lanes[i])."""
    if reg == "eflags":
        try:
            return hex(int(gdb.parse_and_eval("$eflags")) & 0xFFFFFFFF)
        except gdb.error:
            return "<eflags?>"
    if "xmm" in reg or "ymm" in reg or "zmm" in reg:
        lane_expr, lane_count = simd_int64_lanes(reg)
        if lane_expr is None:
            return "<vector?>"
        try:
            lanes = gdb.parse_and_eval(f"${lane_expr}")
            return "[" + ",".join(str(int(lanes[i])) for i in range(lane_count)) + "]"
        except gdb.error:
            return "<vector?>"
    try:
        return hex(int(gdb.parse_and_eval(f"${reg}")) & 0xFFFFFFFFFFFFFFFF)
    except gdb.error:
        return "<reg?>"
