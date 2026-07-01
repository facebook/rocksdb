#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

# pyre-unsafe
"""Unit tests for injector_critical_instruction.decode_instruction -- one
representative case per decode path (REAL `x/i` lines from gdb on db_stress).

decode_instruction is pure string parsing, but the module it lives in does
`import gdb` at the top. `gdb` is not a pip package -- it only exists inside gdb's
embedded Python interpreter, so a plain `python3` test run would fail at that import
with ModuleNotFoundError. To test the parser standalone, we register a fake `gdb`
module in sys.modules first (see below), so the import succeeds.
"""

from __future__ import annotations

import os
import sys
import types
import unittest

# The module under test lives one directory up; add that dir to sys.path so the import
# below resolves. Append (not insert(0)) puts it at the END of the search path, so a
# sibling file here can't shadow a stdlib module that happens to share its name.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Register a stand-in `gdb` so the module's top-level `import gdb` finds this instead
# of failing. The decoder never calls gdb -- it only references gdb.error in except
# clauses -- so a single exception class is all the stub needs.
if "gdb" not in sys.modules:
    stub = types.ModuleType("gdb")
    stub.error = type("error", (Exception,), {})
    sys.modules["gdb"] = stub

from injector_critical_instruction import decode_instruction  # noqa: E402

# (name, x/i line, expected reg, expected corruption_type, corrupt_after_exec).
# corruption_type "" means the decoder rejects the line. One representative case per
# decode path.
_CASES: list[tuple[str, str, str, str, bool]] = [
    # memory load into a GP register -> bit_flip on the dest, corrupt AFTER the load.
    ("load_gp_reg", "   0x6f42e4:\tmov    0x8(%r8),%rbx", "rbx", "bit_flip", True),
    # store of a GP register to non-stack memory -> bit_flip on the source, BEFORE.
    ("store_gp_reg", "   0x6f4400:\tmov    %rbx,0x10(%r14)", "rbx", "bit_flip", False),
    # cmp/test -> flag_flip on eflags, corrupt AFTER it executes.
    ("cmp_sets_flags", "   0x6f4313:\tcmp    $0x80,%ebx", "eflags", "flag_flip", True),
    # rejected: reg-reg copy (no memory operand).
    ("reject_reg_reg_copy", "   0x6f430a:\tmov    %ebx,%r13d", "", "", False),
    # rejected: lea (address math, not a value move).
    ("reject_lea", "   0x6f439c:\tlea    0x2750(%r14),%rdx", "", "", False),
    # rejected: stack spill (memory operand is rbp-relative).
    ("reject_stack_spill", "   0x6f42e8:\tmov    %r9,-0xf8(%rbp)", "", "", False),
    # rejected: indexed/scaled stack spill (rbp base with index, not plain "(%rbp)").
    (
        "reject_stack_spill_indexed",
        "   0x6f42ec:\tmov    %r9,0x10(%rbp,%rcx,8)",
        "",
        "",
        False,
    ),
    # rejected: load into a control register (rsp).
    ("reject_load_control_reg", "   0x6f4600:\tmov    0x8(%r8),%rsp", "", "", False),
]


class DecodeInsnTest(unittest.TestCase):
    def test_representative_cases(self) -> None:
        # subTest (stdlib) rather than @parameterized.expand: this test runs standalone
        # under plain python3 (the module under test imports gdb, so it cannot be a buck
        # target) and `parameterized` is not in the devserver stdlib. subTest still
        # reports each case independently and keeps going after a failing case.
        for name, line, reg, corruption_type, corrupt_after_exec in _CASES:
            with self.subTest(name=name):
                d = decode_instruction(line, can_corrupt_int64_lane=lambda _r: True)
                if corruption_type == "":
                    self.assertIsNone(d, f"expected reject for {line!r}")
                    continue
                self.assertIsNotNone(d, f"expected decode for {line!r}")
                self.assertEqual(
                    (d.reg, d.corruption_type, d.corrupt_after_exec),
                    (reg, corruption_type, corrupt_after_exec),
                    line,
                )

    def test_simd_load_is_lane_bit_flip_when_addressable(self) -> None:
        d = decode_instruction(
            "   0x6f4500:\tvmovdqu 0x0(%r14),%xmm3",
            can_corrupt_int64_lane=lambda _r: True,
        )
        self.assertEqual(
            (d.reg, d.corruption_type, d.corrupt_after_exec),
            ("xmm3", "lane_bit_flip", True),
        )

    def test_simd_skipped_when_not_addressable(self) -> None:
        # If gdb cannot address the lanes, the decoder skips it (the "only corruptible
        # operands" contract).
        d = decode_instruction(
            "   0x6f4500:\tvmovdqu 0x0(%r14),%xmm3",
            can_corrupt_int64_lane=lambda _r: False,
        )
        self.assertIsNone(d)
