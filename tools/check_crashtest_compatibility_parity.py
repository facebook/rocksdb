#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
"""Compare new and legacy db_crashtest compatibility sanitizers.

This is a rollout audit tool, not a unit test. It generates production-shaped
crash-test parameter sets, finalizes them with one compatibility system, then
passes the finalized output through the other system. Any changed key is
reported as a parity mismatch for triage.
"""

import argparse
import copy
import multiprocessing
import os
import random
import sys
from collections import Counter
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import db_crashtest
import db_crashtest_legacy_compatibility as legacy_compatibility


_DIRECTION_NEW_TO_OLD = "new-to-old"
_DIRECTION_OLD_TO_NEW = "old-to-new"
_DIRECTIONS = (_DIRECTION_NEW_TO_OLD, _DIRECTION_OLD_TO_NEW)
_MULTISCAN_PREFETCH_VALUES = {0, 64 * 1024, 256 * 1024}


def _patch_runtime_environment():
    # Keep the parity check focused on static compatibility rules. Runtime
    # environment checks are covered elsewhere and can otherwise make parity
    # results depend on the local filesystem or build mode.
    db_crashtest.is_release_mode = lambda: False
    db_crashtest.is_direct_io_supported = lambda path: True
    db_crashtest.is_remote_db = False


def _build_args(seed):
    rng = random.Random(seed)
    return SimpleNamespace(
        test_type=rng.choice(["blackbox", "whitebox"]),
        simple=rng.choice([False, True]),
        cf_consistency=rng.choice([False, True]),
        txn=rng.choice([False, True]),
        optimistic_txn=rng.choice([False, True]),
        test_best_efforts_recovery=rng.choice([False, True]),
        enable_ts=rng.choice([False, True]),
        test_multiops_txn=rng.choice([False, True]),
        stress_cmd=None,
        test_tiered_storage=rng.choice([False, True]),
        cleanup_cmd=None,
        print_stderr_separately=False,
    )


def _build_params(seed):
    random.seed(seed)
    params = db_crashtest.gen_cmd_params(_build_args(seed))
    params["db"] = "/tmp/test_db"
    return params


def _resolve_params(params):
    return {key: value() if callable(value) else value for key, value in params.items()}


def _run_dependency_sanitizer(params, seed):
    old_mode = os.environ.get(db_crashtest._COMPATIBILITY_MODE_ENV_VAR)
    os.environ[
        db_crashtest._COMPATIBILITY_MODE_ENV_VAR
    ] = db_crashtest._COMPATIBILITY_MODE_DEPENDENCY
    try:
        random.seed(seed)
        return db_crashtest.finalize_and_sanitize(copy.deepcopy(params))
    finally:
        if old_mode is None:
            os.environ.pop(db_crashtest._COMPATIBILITY_MODE_ENV_VAR, None)
        else:
            os.environ[db_crashtest._COMPATIBILITY_MODE_ENV_VAR] = old_mode


def _run_legacy_sanitizer(params, seed):
    resolved = _resolve_params(copy.deepcopy(params))
    random.seed(seed)
    return legacy_compatibility.run_legacy_compatibility_sanitizer(
        resolved,
        is_release_mode=db_crashtest.is_release_mode,
        is_direct_io_supported=db_crashtest.is_direct_io_supported,
        is_remote_db=db_crashtest.is_remote_db,
    )


def _diff_params(before, after):
    return {
        key: (before.get(key), after.get(key))
        for key in sorted(set(before) | set(after))
        if not _parity_values_equivalent(key, before.get(key), after.get(key))
    }


def _parity_values_equivalent(key, before, after):
    if before == after:
        return True
    if key == "multiscan_max_prefetch_memory_bytes":
        return before in _MULTISCAN_PREFETCH_VALUES and after in _MULTISCAN_PREFETCH_VALUES
    return False


def _compare_seed(direction, seed):
    params = _build_params(seed)
    sanitize_seed = seed + 1000000
    if direction == _DIRECTION_NEW_TO_OLD:
        first = _run_dependency_sanitizer(params, sanitize_seed)
        second = _run_legacy_sanitizer(first, sanitize_seed)
    elif direction == _DIRECTION_OLD_TO_NEW:
        first = _run_legacy_sanitizer(params, sanitize_seed)
        second = _run_dependency_sanitizer(first, sanitize_seed)
    else:
        raise ValueError(f"unsupported direction: {direction}")
    return _diff_params(first, second)


def _empty_summary():
    return {
        "trials": 0,
        "mismatch_seeds": 0,
        "key_counts": Counter(),
        "exception_counts": Counter(),
        "examples": {},
    }


def _record_example(examples, key, example, max_examples):
    key_examples = examples.setdefault(key, [])
    if len(key_examples) < max_examples:
        key_examples.append(example)


def _worker(task):
    direction, start_seed, trials, max_examples = task
    _patch_runtime_environment()
    summary = _empty_summary()
    for offset in range(trials):
        seed = start_seed + offset
        summary["trials"] += 1
        try:
            diff = _compare_seed(direction, seed)
        except SystemExit as exc:
            summary["mismatch_seeds"] += 1
            key = f"SystemExit({exc.code})"
            summary["exception_counts"][key] += 1
            _record_example(summary["examples"], key, {"seed": seed}, max_examples)
            continue
        except Exception as exc:
            summary["mismatch_seeds"] += 1
            key = f"{type(exc).__name__}: {exc}"
            summary["exception_counts"][key] += 1
            _record_example(summary["examples"], key, {"seed": seed}, max_examples)
            continue

        if not diff:
            continue
        summary["mismatch_seeds"] += 1
        for key, (before, after) in diff.items():
            summary["key_counts"][key] += 1
            _record_example(
                summary["examples"],
                key,
                {"seed": seed, "before": before, "after": after},
                max_examples,
            )
    return direction, summary


def _merge_summary(dest, src):
    dest["trials"] += src["trials"]
    dest["mismatch_seeds"] += src["mismatch_seeds"]
    dest["key_counts"].update(src["key_counts"])
    dest["exception_counts"].update(src["exception_counts"])
    for key, examples in src["examples"].items():
        dest["examples"].setdefault(key, []).extend(examples)


def _make_tasks(directions, total_trials, workers, seed_offset, max_examples):
    tasks = []
    trials_per_worker = total_trials // workers
    remainder = total_trials - trials_per_worker * workers
    for direction in directions:
        current_seed = seed_offset
        for worker_id in range(workers):
            trials = trials_per_worker + (1 if worker_id < remainder else 0)
            if trials == 0:
                continue
            tasks.append((direction, current_seed, trials, max_examples))
            current_seed += trials
    return tasks


def _print_summary(direction, summary, max_examples):
    print(f"\n{direction}")
    print("-" * len(direction))
    print(f"Trials: {summary['trials']}")
    print(f"Mismatch seeds: {summary['mismatch_seeds']}")

    if summary["exception_counts"]:
        print("\nExceptions:")
        for key, count in summary["exception_counts"].most_common():
            print(f"  {count:6d}  {key}")
            for example in summary["examples"].get(key, [])[:max_examples]:
                print(f"          seed={example['seed']}")

    if summary["key_counts"]:
        print("\nChanged keys:")
        for key, count in summary["key_counts"].most_common():
            print(f"  {count:6d}  {key}")
            for example in summary["examples"].get(key, [])[:max_examples]:
                print(
                    f"          seed={example['seed']} "
                    f"{example['before']!r} -> {example['after']!r}"
                )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Compare dependency-solver and legacy db_crashtest compatibility "
            "sanitizer output."
        )
    )
    parser.add_argument("--trials", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--seed-offset", type=int, default=0)
    parser.add_argument(
        "--direction",
        choices=("both",) + _DIRECTIONS,
        default="both",
    )
    parser.add_argument("--max-examples", type=int, default=3)
    parser.add_argument(
        "--allow-mismatches",
        action="store_true",
        help="Exit successfully even when parity mismatches are found.",
    )
    args = parser.parse_args()

    if args.trials < 0:
        parser.error("--trials must be non-negative")
    if args.workers <= 0:
        parser.error("--workers must be positive")

    directions = _DIRECTIONS if args.direction == "both" else (args.direction,)
    tasks = _make_tasks(
        directions,
        args.trials,
        min(args.workers, max(args.trials, 1)),
        args.seed_offset,
        args.max_examples,
    )

    summaries = {direction: _empty_summary() for direction in directions}
    if args.workers == 1:
        for task in tasks:
            direction, summary = _worker(task)
            _merge_summary(summaries[direction], summary)
    else:
        with multiprocessing.Pool(args.workers) as pool:
            for direction, summary in pool.imap_unordered(_worker, tasks):
                _merge_summary(summaries[direction], summary)

    mismatch_count = 0
    for direction in directions:
        _print_summary(direction, summaries[direction], args.max_examples)
        mismatch_count += summaries[direction]["mismatch_seeds"]

    if mismatch_count and not args.allow_mismatches:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
