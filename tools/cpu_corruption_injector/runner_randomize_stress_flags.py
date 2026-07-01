#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

# pyre-unsafe -- interops with db_crashtest (an untyped sibling script: gen_cmd,
# default_params, blackbox_default_params), whose calls/attributes pyre-strict would
# reject as untyped.

from __future__ import annotations

import os
import random
import sys


# db_crashtest attributes/functions this file couples to. Assert they still exist so an
# upstream rename or refactor fails loudly HERE, naming the coupling, instead of as a bare
# AttributeError deep inside randomize_stress_flags.
_REQUIRED_DB_CRASHTEST_API = ("default_params", "blackbox_default_params", "gen_cmd")


def _db_crashtest():
    # Lazy import: db_crashtest lives in the parent tools/ dir and has import-time side
    # effects (prints a seed, resolves some randoms), so load it only when randomizing.
    tools = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if tools not in sys.path:
        sys.path.append(tools)  # append so it can't shadow stdlib
    import db_crashtest  # noqa: PLC0415

    missing = [
        name for name in _REQUIRED_DB_CRASHTEST_API if not hasattr(db_crashtest, name)
    ]
    if missing:
        raise AssertionError(
            f"db_crashtest is missing {missing}: its API changed. This tool pins a "
            "db_stress preset by feeding db_crashtest.gen_cmd; update "
            "runner_randomize_stress_flags.py to match."
        )
    return db_crashtest


# Each entry pins a db_crashtest flag whose randomized value would otherwise make
# finalize_and_sanitize() un-pin one of the preset flags; see
# db_crashtest.finalize_and_sanitize for the exact couplings. _assert_preset_survived()
# fails the run fast if this list is ever incomplete.
CLOSURE: dict[str, object] = {
    "memtablerep": "skip_list",
    "enable_compaction_filter": 0,
    "use_multiscan": 0,
    "test_batches_snapshots": 0,
    "inplace_update_support": 0,
    "user_timestamp_size": 0,
}

REMOTE_COMPACTION_CLOSURE: dict[str, object] = {
    "enable_blob_files": 0,
    "enable_blob_garbage_collection": 0,
    "allow_setting_blob_options_dynamically": 0,
    "remote_compaction_failure_fall_back_to_local": "false",
}


def randomize_stress_flags(
    per_run_stress_flags: dict[str, object],
    preset: dict[str, object],
    closure: dict[str, object] = CLOSURE,
) -> list[str]:
    # TODO: ideally db_crashtest.py would expose a flag generator that takes a pinned
    # preset, so we could drop the closure and the trial-and-error preset check below.
    db_crashtest = _db_crashtest()
    random.seed(int(per_run_stress_flags["seed"]))

    flags: dict[str, object] = dict(db_crashtest.default_params)
    flags.update(db_crashtest.blackbox_default_params)
    flags.update(preset)
    flags.update(closure)
    flags.update(per_run_stress_flags)

    cmd, finalized = db_crashtest.gen_cmd(flags, [])
    _assert_preset_survived(finalized, preset)
    return cmd[1:]  # cmd[0] is db_crashtest's own db_stress path; keep only the flags


def _assert_preset_survived(
    finalized: dict[str, object], preset: dict[str, object]
) -> None:
    for flag, value in preset.items():
        got = finalized.get(flag)
        if str(got) != str(value):
            raise AssertionError(
                f"preset flag {flag} changed {value!r} -> {got!r} under randomization; "
                "pin its gate in the closure."
            )
