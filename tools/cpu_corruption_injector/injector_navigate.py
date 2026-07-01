#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

# pyre-unsafe
# Runs ONLY inside the gdb python runtime (imports `gdb`); not a buck/pyre target.

from __future__ import annotations

import random
import re
from typing import Optional

import gdb  # provided by gdb's embedded Python (not pip-importable)

from injector_critical_instruction import classify_current_instruction
from injector_register_corruption import apply_corruption, CorruptedInstruction

# When single-stepping a target_fn call (to count + corrupt its instructions) we step
# THROUGH rocksdb's own code by default; these are the EXTRA, non-rocksdb libraries
# we ALSO step through -- data/integrity primitives (memcpy, (de)compression,
# checksum) that carry key/value bytes. Any other callee (jemalloc, std::, libc
# runtime) we `finish` straight back out of, to keep corruptions on value/data movement
# rather than allocator/runtime bookkeeping. This is a broad filter: rocksdb's OWN
# allocator (e.g. rocksdb::Arena) is rocksdb:: code and so IS stepped -- only
# NON-rocksdb runtime noise is excluded. The same filter runs while counting and
# while corrupting, so the critical instructions of one target_fn call line up with
# another's -- SIMILARLY, not identically: data-dependent branches (e.g. key/value
# size) can make a later call run a different instruction sequence, so a counted
# index is a best-effort corruption target (a call with fewer criticals at that index
# corrupts nothing; the caller handles that by retrying on the next target_fn call).
_EXTRA_ALLOWED_LIBRARY_TOKENS = (
    "memmove",
    "memcpy",
    "zstd",
    "snappy",
    "lz4",
    "zlib",
    "brotli",
    "basic_string",
    "char_traits",
    "xxh",
    "xxph",
    "crc32",
    "hash_bytes",
)


# Per-call single-step cap. A single gdb `stepi` is slow (~ms), and the outer per-run
# timeout (runner_execute RUN_TIMEOUT) would kill the WHOLE run if one pathological
# target_fn call (a giant key loop) stepped forever. This cap lets us give up on one
# such call and move to the next, still producing an injection within the timeout.
_MAX_STEPS_PER_CALL = 20000


# db_stress lifecycle / error-handling model.
#
# WHY this is not a simple yes/no: we corrupt a CPU register, which often makes a
# later instruction read a bad address -> db_stress takes a fatal signal (SIGSEGV).
# By default gdb hands that signal to db_stress, whose own handler forks a SECOND gdb
# to print a stack trace; two gdbs on one process collide and the run hangs. So we
# tell gdb to freeze db_stress and hand control to us WITHOUT delivering the signal
# (handle ... stop nopass, see setup_gdb). That one choice creates a third,
# in-between condition -- alive, but crashed -- that a two-way alive/dead check misses.
#
# So db_stress is always in exactly ONE of three states; db_stress_state() returns
# it, and callers can rely on that:
#
#   EXITED    the process is gone -- db_stress's own exit, or our kill.
#   HEALTHY   alive and stopped where we put it (a breakpoint, or mid single-step).
#   <signal>  alive but frozen on a fatal signal name ("SIGSEGV", ...) = CRASHED.
#             nopass left the signal un-delivered, so a continue/stepi would only
#             re-fault at the same pc and never make progress -- so the only way for
#             us to end a crashed db_stress is to kill it.
#
# Lifecycle of one run (db_stress runs UNDER gdb throughout); both ways end EXITED,
# and finish_db_stress() is the one place that drives the current state to EXITED:
#
#   HEALTHY:  finish_db_stress lets db_stress exit on its own       --> EXITED
#   CRASHED:  finish_db_stress kills it (the only way to end a crash) --> EXITED
#
# (A corruption's bad value being used is what turns HEALTHY into CRASHED.)
EXITED = "exited"
HEALTHY = "healthy"


def db_stress_state() -> str:
    """Read db_stress's current state -- the one value finish_db_stress acts on (see
    the note above):
      - EXITED   db_stress is gone -- it exited on its own (when we let it finish), or
                 we killed it. A crash never lands here -- at any time, including during
                 db_stress's finishing, handle-nopass freezes db_stress alive on the
                 signal instead.
      - <signal> frozen on a fatal signal -- e.g. our corruption's SIGSEGV, or db_stress's
                 own SIGABRT (an assert/abort); the value is that signal name.
      - HEALTHY  alive, stopped where we put it.
    Computed fresh each call."""
    # gdb.selected_inferior() is gdb's handle to the db_stress process; pid 0, or no
    # process to query at all, both mean it is gone.
    try:
        if gdb.selected_inferior().pid == 0:
            return EXITED
    except gdb.error:
        return EXITED
    try:
        out = gdb.execute("info program", to_string=True)
    except gdb.error:
        return (
            HEALTHY  # cannot read status -> assume alive; the next step reveals a crash
        )
    match = re.search(r"stopped with signal (SIG[A-Z0-9]+)", out)
    return match.group(1) if match else HEALTHY


def kill_db_stress() -> None:
    try:
        gdb.execute("kill")
    except gdb.error:
        # Usually db_stress is already dead. If a kill ever fails for another reason,
        # runner_execute's per-run timeout killpg's the whole gdb+db_stress process
        # group, so nothing is left orphaned.
        pass


def setup_gdb() -> None:
    # Pagination: gdb pauses long output every screenful with a "---Type <return> to
    # continue---" prompt; in batch mode that prompt blocks forever. Off.
    gdb.execute("set pagination off")
    # Confirm: gdb asks interactive y/n questions ("Delete all breakpoints?"); off so
    # scripted commands never block on a prompt.
    gdb.execute("set confirm off")
    # Signal handling -- the source of most of the lifecycle complexity in this file,
    # done to avoid TWO gdbs fighting over db_stress. On a fatal signal gdb's DEFAULT
    # is to deliver it to db_stress, whose own signal handler forks a NESTED gdb to
    # dump a stack trace -- that nested gdb fights ours for the process and wedges the
    # run. So for these signals we set:
    #   stop   -- gdb takes control and returns to this script (we inspect + record).
    #   nopass -- do NOT forward the signal to db_stress, so its handler never runs
    #             and no nested gdb is spawned. The faulting instruction is left
    #             un-retired, so db_stress is now ALIVE-but-stopped-on-a-signal (the
    #             "crashed" state db_stress_state() reports); a continue/stepi would
    #             just re-fault at the same pc, which is why we KILL rather than
    #             resume a crashed db_stress.
    #   print  -- gdb writes a one-line "Program received signal ..." to gdb.log
    #             (diagnostic only; runner_execute scans gdb.log for harness deaths).
    gdb.execute("handle SIGSEGV SIGBUS SIGABRT SIGFPE stop nopass print")


class Navigator:
    """Drives gdb through ONE db_stress run to the op INSTANCE we inject into (the
    op_index-th call of entry_fn). `_target_fn_breakpoint` is the standing breakpoint
    reused across this op instance's target_fn calls (-1 until installed)."""

    def __init__(self, entry_fn: str, target_fn: str) -> None:
        self.entry_fn = entry_fn
        self.target_fn = target_fn
        self._target_fn_breakpoint = -1
        self._ops_seen = 0

    def reach_op(self, op_index: int) -> bool:
        """Run db_stress to the op_index-th op instance: break on entry_fn (which
        db_stress calls exactly once per op), skip the first op_index-1 hits, then
        stop on the op_index-th. True if we stopped there, False if db_stress ended
        first. op_index is 1-based (1 = the first op).

        gdb's `break` does not return the new breakpoint's number to Python, so we
        read it back via _last_breakpoint_num(). We record ops_seen from the
        breakpoint's hit count, then DELETE the breakpoint -- keeping it would stop on
        every entry_fn call AFTER this op instance too, starving the rest of the run
        (scoping to our op is instead done by _entry_fn_on_stack in
        reach_target_fn_call). So ops_seen is the EXACT op total when the op was not
        reached (db_stress exited first), else op_index (a lower bound)."""
        assert op_index >= 1, f"op_index is 1-based, got {op_index}"
        gdb.execute(f"break {self.entry_fn}")
        breakpoint_num = _last_breakpoint_num()
        if op_index > 1:
            # `ignore N k` only sets a skip counter; `continue` does the running. So
            # ignore the first op_index-1 hits, then continue runs to the next
            # (op_index-th) hit. For op_index == 1 there is nothing to skip.
            gdb.execute(f"ignore {breakpoint_num} {op_index - 1}")
        reached = False
        try:
            gdb.execute("continue")
            reached = db_stress_state() == HEALTHY
        except gdb.error:
            reached = False
        self._ops_seen = _breakpoint_hit_count(breakpoint_num)
        _delete_breakpoint(breakpoint_num)
        return reached

    def ops_seen(self) -> int:
        return self._ops_seen

    def reach_target_fn_call(self) -> bool:
        """Continue until db_stress next enters target_fn under an op of OUR TYPE --
        this op instance, or (acceptably) a later instance of the same type. Installs
        the standing target_fn breakpoint on the first call and reuses it afterwards.
        Returns False once our op type is no longer on the stack, or db_stress exits /
        crashes."""
        if db_stress_state() != HEALTHY:
            return False  # db_stress already exited / crashed -- nothing to reach
        if self._target_fn_breakpoint < 0:
            gdb.execute(f"break {self.target_fn}")
            self._target_fn_breakpoint = _last_breakpoint_num()
        try:
            gdb.execute("continue")
        except gdb.error:
            return False  # db_stress exited
        if db_stress_state() != HEALTHY:
            return False
        # The target_fn breakpoint fires on EVERY target_fn call in db_stress, and
        # target_fn is SHARED across op types (e.g. BlockBuilder::Add is called by
        # both flush and compaction). We accept a call only if OUR entry_fn is an
        # ancestor frame -- that scopes us to the right OP TYPE (flush's call has
        # FlushJob::Run on the stack, compaction's has CompactionJob::Run). It does
        # NOT pin a single op INSTANCE, and that is fine: same op TYPE is all we need,
        # so warmup may count critical instructions in one instance and the corruption land
        # in a later instance of that same type (same code -> a SIMILAR, best-effort
        # instruction index space). For the write op entry_fn == target_fn, so the
        # current frame trivially passes.
        return _entry_fn_on_stack(self.entry_fn)

    def count_critical_instructions(self) -> int:
        """Count this target_fn call's critical instructions (no corruption)."""
        return self._walk_target_fn_call(corrupt_index=None)[1]

    def corrupt_critical_instruction(
        self, corrupt_index: int, rng: random.Random
    ) -> tuple[Optional[CorruptedInstruction], int]:
        """Corrupt this call's corrupt_index-th critical instruction (None if absent)."""
        return self._walk_target_fn_call(corrupt_index, rng)

    def _walk_target_fn_call(
        self, corrupt_index: Optional[int], rng: Optional[random.Random] = None
    ) -> tuple[Optional[CorruptedInstruction], int]:
        call_start_depth = _frame_depth()
        index = 0
        for _ in range(_MAX_STEPS_PER_CALL):
            instruction = classify_current_instruction()
            if instruction is not None:
                if corrupt_index is not None and index == corrupt_index:
                    return apply_corruption(instruction, rng, self.target_fn), index + 1
                index += 1
            if _step_one_instruction() != HEALTHY or _frame_depth() < call_start_depth:
                break
        return None, index


def finish_db_stress() -> Optional[str]:
    """Finish this run's db_stress and return its crash signal (None if it exited
    cleanly). The ONE place that ends a still-running db_stress, acting on
    db_stress_state():
      - HEALTHY  let db_stress make its own exit (db_stress's own shutdown, not ours).
                 Delete our breakpoints first, or the final `continue` just stops on
                 them again -- and that exit may itself end in a crash, handled next.
      - CRASHED  KILL it -- a crash from before we got here, OR one during the exit
                 above. Under handle-nopass (see setup_gdb) db_stress is frozen on the
                 un-delivered signal; a `continue` would only re-fault, so killing is
                 the only way we can end it.
      - EXITED   nothing to do (db_stress already exited, or we killed it earlier).
    """
    state = db_stress_state()
    if state == HEALTHY:
        try:
            gdb.execute("delete")
        except gdb.error:
            pass  # no breakpoints to delete -- fine
        try:
            gdb.execute("continue")
        except gdb.error:
            pass  # db_stress made its own exit during the continue
        # Finishing ends db_stress one of two ways: its own exit, or a crash raised
        # inside its own handling. Still HEALTHY would mean neither happened -- a bug
        # in our breakpoint cleanup, so fail fast.
        state = db_stress_state()
        assert state != HEALTHY, f"db_stress did not finish (state={state})"
    if state == EXITED:
        return None
    assert state not in (EXITED, HEALTHY), f"expected a crash signal, got {state}"
    kill_db_stress()
    return state


def _step_one_instruction() -> str:
    """Advance db_stress by one machine instruction and RETURN its state afterwards
    (EXITED / HEALTHY / a fatal signal name). Returning it -- rather than the caller
    calling db_stress_state() again -- avoids a second `info program` per step in the
    hot stepping loop. Beyond the bare `stepi`:
      - if db_stress is no longer HEALTHY (it exited, or an earlier corruption made it read a bad address and it crashed), return that state at once. We do NOT
        kill a crash here -- finish_db_stress performs the single kill later; because
        callers stop stepping a non-HEALTHY db_stress, it never re-faults (setup_gdb).
      - else, if the step descended INTO a callee that is NOT rocksdb or an extra
        library, `finish` straight back out (we only corrupt value/data-movement
        instructions, not allocator/std/runtime ones)."""
    depth_before = _frame_depth()
    gdb.execute("stepi")
    state = db_stress_state()
    if state != HEALTHY:
        return state
    if _frame_depth() > depth_before and not _in_allowed_module(_source_location()):
        try:
            gdb.execute("finish")
        except gdb.error:
            # `finish` runs until the current function returns, which needs gdb to
            # UNWIND it (work out its return address from debug/unwind metadata). A
            # frame lacking that metadata (hand-written asm, stripped library) cannot
            # be unwound; nothing sensible to do but stay put and keep stepping.
            pass
    return HEALTHY


def _in_allowed_module(source_location: str) -> bool:
    """Whether the frame described by `source_location` ("fn @ file:line") is code we
    step THROUGH: rocksdb's own functions, or one of a few extra libraries (memcpy,
    (de)compression, checksum). Everything else (jemalloc, std::, libc runtime) we
    `finish` out of. We match on the function-name PREFIX for rocksdb so a wrapper
    like std::__atomic_base<rocksdb::...> is NOT taken for rocksdb code."""
    # source_location is "fn @ file:line"; take the fn part (maxsplit 1 so a " @ "
    # inside the path cannot break it), trim, lowercase for case-insensitive match.
    # `token in fn` is a substring test.
    fn = source_location.split(" @ ", 1)[0].strip().lower()
    if fn.startswith("rocksdb::") or "thunk to rocksdb::" in fn:
        return True
    return any(token in fn for token in _EXTRA_ALLOWED_LIBRARY_TOKENS)


def _source_location() -> str:
    """The current frame's "fn @ file:line", read from gdb. `frame.name()` is the
    function name; `find_sal()` is gdb's symbol-and-line lookup for the frame's pc.
    Returns "?" placeholders where symbols are unavailable."""
    try:
        frame = gdb.selected_frame()
        fn = frame.name() or "?"
        sal = frame.find_sal()
        filename = sal.symtab.filename if sal and sal.symtab else "?"
        line = sal.line if sal else 0
        return f"{fn} @ {filename}:{line}"
    except gdb.error:
        return "?"


def _entry_fn_on_stack(entry_fn: str) -> bool:
    """True if `entry_fn` is on db_stress's current call stack (an ancestor frame).
    Read-only walk like _frame_depth. Substring match (entry_fn in the frame name) so
    a decorated/qualified frame name still matches the plain ENTRY_FN string."""
    try:
        frame = gdb.newest_frame()
        while frame is not None:
            if entry_fn in (frame.name() or ""):
                return True
            frame = frame.older()
    except gdb.error:
        return False
    return False


def _frame_depth() -> int:
    """Number of frames on db_stress's current call stack. Read-only: walking
    gdb.newest_frame() -> .older() inspects frames, it does not change the selected
    frame or advance execution."""
    depth = 0
    try:
        frame = gdb.newest_frame()
        while frame is not None:
            depth += 1
            frame = frame.older()
    except gdb.error:
        pass
    return depth


def _last_breakpoint_num() -> int:
    """The number of the most recently created breakpoint. gdb's `break` command does
    not return it to Python, so we read it off the breakpoint list."""
    breakpoints = gdb.breakpoints()
    return breakpoints[-1].number if breakpoints else -1


def _delete_breakpoint(num: int) -> None:
    if num > 0:  # -1 is the "not installed" sentinel
        try:
            gdb.execute(f"delete {num}")
        except gdb.error:
            pass  # already gone -- fine


def _breakpoint_hit_count(num: int) -> int:
    """Times breakpoint `num` was hit (gdb counts hits even while it is ignored)."""
    for breakpoint in gdb.breakpoints() or []:
        if breakpoint.number == num:
            try:
                return int(breakpoint.hit_count)
            except (gdb.error, TypeError, ValueError):
                return 0
    return 0
