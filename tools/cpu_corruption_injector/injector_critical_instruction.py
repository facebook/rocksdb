#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

# pyre-unsafe
# Runs ONLY inside the gdb python runtime (imports `gdb`); not a buck/pyre target.

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

import gdb  # provided by gdb's embedded Python (not pip-importable)

# This decoder reads ONE disassembled x86-64 instruction and decides whether to
# corrupt it, which register to corrupt, and how. A corruption models a CPU fault: we
# mangle the value INSIDE a CPU register as the instruction handles it -- we do NOT
# corrupt bytes in memory.
#
# classify_current_instruction() forces AT&T disassembly and fails fast on any
# non-x86-64 target, so these assumptions always hold. AT&T writes the source operand
# first and the destination last -- "mov src, dst" (the reverse of Intel syntax).
#
# We corrupt exactly two things; everything else (compute, lea, reg-reg copies,
# immediates, stack spills) is left alone:
#   - a cmp/test    -> flip a bit of the eflags it produces (a mis-evaluated branch)
#   - a MEMORY move -> corrupt the register holding the value being moved

# re.compile precompiles a pattern we match once per stepped instruction.
# Any move form -- general-purpose (mov, movzx, movsxd, ...) or vector (vmovdqu,
# movaps, movq, ...). They all start with "mov" or "vmov", so one prefix matches all
# (no trailing \b on purpose -- we WANT every mov* form). vmovdqu is how glibc memcpy
# copies a value, the bulk case we most want to catch.
_MOV_RE = re.compile(r"^v?mov")
# cmp / test set the eflags a following branch reads. [bwlq]? allows the AT&T size
# suffixes (cmpq, testb, ...); the trailing \b stops it matching cmpxchg (an atomic
# compare-and-exchange) or cmpsd (an SSE compare) -- different instructions whose
# flags/semantics are not the simple branch decision we model.
_CMP_TEST_RE = re.compile(r"^(cmp|test)[bwlq]?\b")

# A register operand token, e.g. %rax, %eax, %r10d, %xmm3, %ymm12, %zmm5. The capture
# group is the bare name (no '%'). This matches by SHAPE only -- it cannot tell a data
# register from a control one -- so _NON_DATA_REGS below removes the control registers.
_REG_TOK_RE = re.compile(r"%([a-z][a-z0-9]+|xmm\d+|ymm\d+|zmm\d+|r\d+[bwd]?)")

# Registers we must NOT corrupt: the instruction pointer, stack/frame pointers, and
# segment registers. They hold control state, not key/value data, so flipping them is
# a control-flow crash rather than a value fault (and gdb types them as pointers,
# which rejects the XOR we use). _REG_TOK_RE above already allowlists register-shaped
# tokens; this short, fixed set just removes the few control registers that share that
# shape. We do NOT positively allowlist data registers by regex even though the vector
# set (xmm/ymm/zmm \d+) is clean, because the general-purpose set is NOT: data and
# control GP registers overlap in shape -- rax is data, rsp is control, both "r..",
# each in 4 widths -- so a positive pattern would be large and would STILL have to
# special-case rsp/rbp/rip. Excluding the small control set is simpler and safer.
_NON_DATA_REGS = frozenset(
    {
        "rip",
        "eip",
        "ip",
        "rsp",
        "esp",
        "sp",
        "spl",
        "rbp",
        "ebp",
        "bp",
        "bpl",
        "fs",
        "gs",
        "cs",
        "ds",
        "es",
        "ss",
    }
)


@dataclass
class CriticalInstruction:
    """An x86-64 instruction we CHOOSE to corrupt to simulate a CPU fault that can
    flow into stored data (we do not fault the CPU; we mimic one by flipping a
    register's bits at the right moment).

    reg                -- the register we corrupt, e.g. "rbx", "xmm3", or "eflags".
    corruption_type    -- how we corrupt it (this is the corruption_type in
                          inject.json):
                            bit_flip  -- flip a bit of a general-purpose register
                            flag_flip -- flip a bit of the eflags register (a cmp/test)
                            lane_bit_flip -- flip a 64-bit lane of a vector register
    corrupt_after_exec -- True:  corrupt AFTER the instruction runs, because it
                          PRODUCES the value we corrupt (a load destination, or
                          cmp/test flags). False: corrupt BEFORE it runs, because it
                          CONSUMES the register we corrupt (a store to memory).
    text               -- the disassembled instruction, e.g. "mov 0x8(%r8),%rbx".
    """

    reg: str
    corruption_type: str
    corrupt_after_exec: bool
    text: str = ""


def simd_int64_lanes(reg: str) -> tuple[Optional[str], int]:
    """How to address a vector register as 64-bit integer lanes, returned as
    (lane_expr, lane_count), or (None, 0) if gdb cannot. We corrupt vector data one
    64-bit lane at a time.

    gdb exposes a vector register as a struct with several "member" views of the same
    bits; we use the int64 ones: `$xmm0.v2_int64` views the 128-bit xmm0 as 2 int64s,
    `$ymm0.v4_int64` views the 256-bit ymm0 as 4, `$zmm0.v8_int64` views the 512-bit
    zmm0 as 8. lane_expr is that member string (e.g. "xmm0.v2_int64"); lane_count is
    its number of lanes. We probe widest-first; parse_and_eval raises gdb.error if a
    member does not exist, which is how we find the one this register has."""
    for member, lane_count in (("v8_int64", 8), ("v4_int64", 4), ("v2_int64", 2)):
        try:
            gdb.parse_and_eval(f"${reg}.{member}")  # exists? else gdb.error
            return f"{reg}.{member}", lane_count
        except gdb.error:
            continue
    return None, 0


def _gdb_can_corrupt_int64_lane(reg: str) -> bool:
    """Whether gdb can address `reg` as 64-bit int lanes -- i.e. whether we can corrupt
    it. A deliberate scope choice: we only corrupt vector registers gdb exposes as
    int64 lanes; anything else is skipped. We need only the yes/no here, so we drop the
    lane count. (SIMD coverage is surfaced by the campaign report's corruption_type
    breakdown: if lane_bit_flip stays near zero, we are losing SIMD coverage here. TODO:
    widen lane support beyond the int64 view.)"""
    lane_expr, _ = simd_int64_lanes(reg)
    return lane_expr is not None


_arch_verified = False


def classify_current_instruction() -> Optional[CriticalInstruction]:
    """Decode the instruction at the current program counter. `x/i $pc` asks gdb to
    disassemble one instruction ('i') at $pc; we parse that single text line. The
    first call also verifies the target is x86-64 and forces AT&T disassembly (once),
    so callers never have to know about that low-level requirement."""
    global _arch_verified
    if not _arch_verified:
        _require_x86_att()
        _arch_verified = True
    return decode_instruction(gdb.execute("x/i $pc", to_string=True))


def _require_x86_att() -> None:
    """Fail fast unless the target is x86-64, and force AT&T disassembly so this
    decoder's syntax assumptions hold no matter how the user's gdb is configured.
    Runs once, lazily, from classify_current_instruction (a frame exists by then)."""
    gdb.execute("set disassembly-flavor att")
    arch = gdb.selected_frame().architecture().name()
    assert "x86-64" in arch, (
        f"cpu-corruption injector supports only x86-64 (AT&T); got architecture "
        f"{arch!r}"
    )


def decode_instruction(
    raw_xi_line: str, can_corrupt_int64_lane=_gdb_can_corrupt_int64_lane
) -> Optional[CriticalInstruction]:
    """Decode one `x/i` line into a CriticalInstruction, or None if it is not an
    instruction we corrupt. `can_corrupt_int64_lane(reg)` answers whether gdb can
    address a vector register's int64 lanes; it is a parameter so unit tests can feed
    REAL `x/i` lines without a live gdb."""
    text = _strip_to_instruction(raw_xi_line)
    if not text:
        return None
    mnemonic, operands = _split_mnemonic_operands(text)
    if _CMP_TEST_RE.match(mnemonic):
        # cmp/test: corrupt the eflags it produces, after it runs.
        return CriticalInstruction("eflags", "flag_flip", True, text)
    if _MOV_RE.match(mnemonic):
        return _critical_instruction_for_memory_move(
            operands, text, can_corrupt_int64_lane
        )
    return None


def _critical_instruction_for_memory_move(
    operands: list[str], text: str, can_corrupt_int64_lane
) -> Optional[CriticalInstruction]:
    """The CriticalInstruction for a memory move we corrupt -- a mov that carries a
    value between a MEMORY location and a DATA register. Returns None when either side
    of the move disqualifies it:
      memory side   -- no memory operand at all (a reg-reg copy or immediate load: no
                       value moves through memory), or the memory operand is the stack
                       (rbp/rsp: a spill/reload, i.e. compiler bookkeeping of a
                       temporary, not key/value data);
      register side -- the register operand is a control register (rsp/rbp/rip/segment),
                       which _data_register rejects as non-value data."""
    if not _has_memory_operand(operands):
        return None
    if "(%rbp" in text or "(%rsp" in text:  # rbp/rsp = frame/stack pointer
        return None

    # Load vs store by the source operand (AT&T writes the source first):
    #   "mov MEM, REG"  source is memory -> a load:  corrupt the dest register AFTER
    #   "mov REG, MEM"  source is a reg  -> a store: corrupt the source register BEFORE
    if "(" in operands[0]:
        reg, after_exec = _data_register(operands[-1]), True
    else:
        reg, after_exec = _data_register(operands[0]), False
    if (
        reg is None
    ):  # the register side is a control register (caught by _data_register)
        return None

    # The register decides the corruption_type -- two cases by construction: a vector
    # register -> lane_bit_flip (only if gdb can address its int64 lanes), else a
    # general-purpose register -> bit_flip.
    if "xmm" in reg or "ymm" in reg or "zmm" in reg:
        return (
            CriticalInstruction(reg, "lane_bit_flip", after_exec, text)
            if can_corrupt_int64_lane(reg)
            else None
        )
    return CriticalInstruction(reg, "bit_flip", after_exec, text)


def _strip_to_instruction(raw_xi_line: str) -> str:
    """Reduce a raw `x/i` line to just the instruction text. gdb returns e.g.
    "=> 0x4f2e <sym+20>:\tmov 0x8(%r8),%rbx  # comment"; we strip it in three steps to
    leave "mov 0x8(%r8),%rbx". (.strip() removes surrounding whitespace incl. tabs.)"""
    line = raw_xi_line.strip()
    if line.startswith("=>"):
        line = line[2:].strip()  # drop the "=>" current-pc marker
    colon = line.find(":")
    if colon != -1:
        line = line[colon + 1 :].strip()  # drop the "0x4f2e <sym+20>:" address prefix
    hashpos = line.find("#")
    if hashpos != -1:
        line = line[:hashpos].strip()  # drop a trailing "# ..." comment
    return line


def _split_mnemonic_operands(text: str) -> tuple[str, list[str]]:
    """Split "mov 0x8(%r8),%rbx" into ("mov", ["0x8(%r8)", "%rbx"]): the mnemonic
    (first whitespace-delimited word) and its operand list. We need the operands to
    find the register and to tell a load from a store. An instruction with no operands
    (ret, nop, ...) yields []; that is not an error -- it simply is not corruptible."""
    parts = text.split(None, 1)  # split on whitespace into [mnemonic, rest], at most 2
    mnemonic = parts[0].lower()
    operands = _split_operands(parts[1]) if len(parts) > 1 else []
    return mnemonic, operands


def _split_operands(operand_text: str) -> list[str]:
    """Split the operand text on top-level commas. We cannot use str.split(",")
    because a memory operand carries its own commas inside parentheses, e.g.
    0x10(%r14,%rcx,8); there is no stdlib paren-aware splitter, so we scan characters
    and split only at commas seen outside any "(...)"."""
    operands: list[str] = []
    depth = 0  # how many unclosed "(" we are currently inside
    current = ""  # the operand accumulated so far
    for ch in operand_text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:  # a top-level comma ends the current operand
            operands.append(current.strip())
            current = ""
        else:
            current += ch
    if current.strip():  # flush the final operand (there is no trailing comma)
        operands.append(current.strip())
    return operands


def _has_memory_operand(operands: list[str]) -> bool:
    """True if any operand dereferences memory (has "(", e.g. 0x8(%r8)). A move with
    no memory operand is a reg-reg copy or an immediate load -- no value travels, so
    there is nothing for us to corrupt."""
    return any("(" in operand for operand in operands)


def _data_register(operand: str) -> Optional[str]:
    """The bare data-register name of an operand, e.g. "%rbx" -> "rbx", or None if the
    operand is a memory dereference (has "(") or a control register we must not
    corrupt."""
    if "(" in operand:
        return None
    # m is a re.Match if the WHOLE operand is a register token (e.g. "%rbx"), else None.
    m = _REG_TOK_RE.fullmatch(operand.strip())
    if not m:
        return None
    name = m.group(1)  # capture group 1: the name without the '%', e.g. "%rbx" -> "rbx"
    return None if name in _NON_DATA_REGS else name
