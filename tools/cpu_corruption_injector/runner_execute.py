#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

# pyre-strict

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess

from runner_build import Run

logger: logging.Logger = logging.getLogger("runner")


def _get_gdb() -> str:
    # gdb that drives db_stress. Override ICC_GDB to point at a newer gdb if the
    # telemetry's source comes out "@ ?:0" (an old gdb that cannot read this build's
    # debug info). Read on use, not at import, per Meta's lazy-import guidance.
    return os.environ.get("ICC_GDB", "/usr/bin/gdb")


def _get_run_timeout_sec() -> float:
    # Hard per-run cap so one gdb+db_stress that hangs or deadlocks cannot block the
    # whole run set; single-stepping is slow, so keep it generous.
    return float(os.environ.get("ICC_RUN_TIMEOUT", "600"))


# Every bucket classify() can return; runner_report iterates this so the set of outcomes
# lives in one place.
OUTCOMES: tuple[str, ...] = (
    "SDC",
    "CORRUPTION",
    "CRASH",
    "NO_EFFECT",
    "NO_INJECTION",
    "ERROR",
)

# db_stress's read-back tags a corruption it catches with a "kind"; map each kind to our
# result bucket (these are the only kinds db_stress emits).
_CORRUPTION_BUCKET: dict[str, str] = {
    "lost": "SDC",
    "resurrected": "SDC",
    "wrong-value": "SDC",
    "detected-corruption": "CORRUPTION",
}


def execute(run: Run, injector_py: str, stress_cmd: str) -> None:
    try:
        _run_gdb(
            _gdb_command(injector_py, stress_cmd, run),
            os.path.join(run.dir, "gdb.log"),
        )
    except subprocess.TimeoutExpired:
        logger.warning(
            "run %d: timed out after %.0fs", run.index, _get_run_timeout_sec()
        )
        run.outcome = "ERROR"
        return
    except (
        OSError
    ) as e:  # gdb itself failed to launch -- a runner failure, not an outcome
        logger.warning("run %d: gdb launch failed: %s", run.index, e)
        run.outcome = "ERROR"
        return
    run.inject_json = _read_json(os.path.join(run.dir, "inject.json"))
    run.outcome = classify(run)


def classify(run: Run) -> str:
    # ERROR is a runner-level failure (gdb or the injector itself), never a db_stress
    # outcome. injection_result is authoritative: only a corruption that actually landed
    # can be SDC / CORRUPTION / CRASH / NO_EFFECT.
    inject = run.inject_json
    if inject is None:  # gdb died before the injector wrote inject.json
        return "ERROR"
    injection_result = inject.get("injection_result")
    if injection_result == "error":
        return "ERROR"
    if (
        injection_result != "injected"
    ):  # the op was never reached, nothing was corrupted
        return "NO_INJECTION"
    kind = _data_corruption_kind(run.dir)
    if kind is not None:
        return _CORRUPTION_BUCKET[kind]
    return "CRASH" if inject.get("db_stress_crash_signal") is not None else "NO_EFFECT"


def _data_corruption_kind(run_dir: str) -> str | None:
    names = [
        name
        for name in os.listdir(run_dir)
        if name.startswith("data_corruption.") and name.endswith(".json")
    ]
    assert (
        len(names) <= 1
    ), f"expected <=1 data_corruption json in {run_dir}, got {names}"
    if not names:
        return None
    data = _read_json(os.path.join(run_dir, names[0]))
    return str(data["kind"]) if data is not None else None


def _gdb_command(injector_py: str, stress_cmd: str, run: Run) -> list[str]:
    # gdb's -x runs injector.py with an empty argv, so seed its argv with -iex first;
    # --args then hands db_stress and its flags to the inferior gdb launches.
    seed_argv = "py import sys; sys.argv=" + json.dumps(_injector_argv(run))
    return [
        _get_gdb(),
        "--batch",
        "--nx",
        "-iex",
        seed_argv,
        "-x",
        injector_py,
        "--args",
        stress_cmd,
        *run.stress_flags,
    ]


def _injector_argv(run: Run) -> list[str]:
    return [
        "injector.py",
        "--op",
        run.op,
        "--op_index",
        str(run.op_index),
        "--entry_fn",
        run.entry_fn,
        "--target_fn",
        run.target_fn,
        "--seed",
        str(run.seed),
        "--dir",
        run.dir,
    ]


def _run_gdb(command: list[str], log_path: str) -> None:
    with open(log_path, "wb") as log_file:
        proc = subprocess.Popen(
            command,
            stdout=log_file,
            stderr=subprocess.STDOUT,  # fold gdb's stderr into the same gdb.log
            # new session => its own process group, so a timeout kills gdb AND db_stress
            start_new_session=True,
        )
        try:
            proc.wait(timeout=_get_run_timeout_sec())
        except subprocess.TimeoutExpired:
            _kill_group(proc)
            proc.wait()
            raise


def _kill_group(proc: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except ProcessLookupError:
        pass  # already exited


def _read_json(path: str) -> dict[str, object] | None:
    # None if the file is missing or truncated (e.g. a crash mid-write); the caller reads
    # a missing inject.json as ERROR.
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None
