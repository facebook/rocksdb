#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).
"""Parallel fuzz test for finalize_and_sanitize convergence.

Usage: python3 tools/fuzz_convergence.py [total_trials] [num_workers]

Defaults: 10,000,000 trials across 100 parallel workers.
Reports any oscillation (non-convergence) with full details.
Also checks idempotency: sanitize(sanitize(p)) == sanitize(p).
"""

import copy
import json
import multiprocessing
import os
import random
import sys
import time
import warnings

# Import from same directory
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))
import db_crashtest


# All override candidates for fuzzing — covers every flag that participates
# in incompatibility rules
_OVERRIDE_CANDIDATES = {
    "enable_blob_files": [0, 1],
    "enable_blob_garbage_collection": [0, 1],
    "use_txn": [0, 1],
    "use_optimistic_txn": [0, 1],
    "test_batches_snapshots": [0, 1],
    "remote_compaction_worker_threads": [0, 4],
    "mmap_read": [0, 1],
    "use_trie_index": [0, 1],
    "disable_wal": [0, 1],
    "atomic_flush": [0, 1],
    "inplace_update_support": [0, 1],
    "sync_fault_injection": [0, 1],
    "best_efforts_recovery": [0, 1],
    "user_timestamp_size": [0, 8],
    "persist_user_defined_timestamps": [0, 1],
    "enable_pipelined_write": [0, 1],
    "unordered_write": [0, 1],
    "compaction_style": [0, 1, 2],
    "test_secondary": [0, 1],
    "use_multiscan": [0, 1],
    "enable_compaction_filter": [0, 1],
    "test_multi_ops_txns": [0, 1],
    "prefix_size": [-1, 0, 4, 8],
    "two_write_queues": [0, 1],
    "use_put_entity_one_in": [0, 1, 5],
    "commit_bypass_memtable_one_in": [0, 100],
    "checkpoint_one_in": [0, 1000],
    "create_timestamped_snapshot_one_in": [0, 100],
    "txn_write_policy": [0, 1, 2],
    "memtablerep": ["skip_list", "vector", "hash_linkedlist"],
    "compression_type": ["none", "snappy", "zstd", "lz4"],
    "block_align": [0, 1],
    "compression_max_dict_bytes": [0, 16384],
    "open_files": [-1, 100],
    "write_fault_one_in": [0, 100],
    "use_write_buffer_manager": [0, 1],
    "skip_stats_update_on_db_open": [0, 1],
    "file_checksum_impl": ["none", "crc32c"],
    "allow_concurrent_memtable_write": [0, 1],
    "test_best_efforts_recovery": [0, 1],
    "test_cf_consistency": [0, 1],
    "manual_wal_flush_one_in": [0, 1000],
    "reopen": [0, 20],
    "column_families": [1, 4],
    "use_put_entity_one_in": [0, 1, 5],
    "write_dbid_to_manifest": [0, 1],
    "write_identity_file": [0, 1],
    "use_full_merge_v1": [0, 1],
    "index_type": [0, 2, 3],
    "partition_filters": [0, 1],
    "sst_file_manager_bytes_per_sec": [0, 1048576],
    "compression_manager": ["", "custom", "mixed", "autoskip"],
}

_FEATURE_SETS = []


def _init_feature_sets():
    global _FEATURE_SETS
    _FEATURE_SETS = [
        db_crashtest.txn_params,
        db_crashtest.optimistic_txn_params,
        db_crashtest.best_efforts_recovery_params,
        db_crashtest.ts_params,
        db_crashtest.blob_params,
    ]
    for attr in ["tiered_params", "multiops_txn_params", "cf_consistency_params",
                 "simple_default_params"]:
        if hasattr(db_crashtest, attr):
            _FEATURE_SETS.append(getattr(db_crashtest, attr))


def _build_fuzzed_params(seed):
    """Build a random config with aggressive overrides."""
    random.seed(seed)
    params = {k: v() if callable(v) else v for k, v in db_crashtest.default_params.items()}
    box = random.choice([db_crashtest.blackbox_default_params,
                         db_crashtest.whitebox_default_params])
    params.update({k: v() if callable(v) else v for k, v in box.items()})

    # Layer 0-3 random feature sets (more aggressive than normal)
    if _FEATURE_SETS:
        n = min(random.randint(0, 3), len(_FEATURE_SETS))
        for fp in random.sample(_FEATURE_SETS, n):
            params.update({k: v() if callable(v) else v for k, v in fp.items()})

    params.setdefault("db", "/tmp/test_db")

    # Apply 3-15 random overrides (very aggressive fuzzing)
    n_overrides = min(random.randint(3, 15), len(_OVERRIDE_CANDIDATES))
    for k in random.sample(list(_OVERRIDE_CANDIDATES.keys()), n_overrides):
        params[k] = random.choice(_OVERRIDE_CANDIDATES[k])

    return params


def _extract_relevant(params):
    """Extract key params for debugging."""
    keys = [
        "disable_wal", "best_efforts_recovery", "atomic_flush",
        "inplace_update_support", "unordered_write", "use_txn",
        "txn_write_policy", "user_timestamp_size",
        "persist_user_defined_timestamps", "allow_concurrent_memtable_write",
        "test_best_efforts_recovery", "enable_compaction_filter",
        "sync_fault_injection", "commit_bypass_memtable_one_in",
        "enable_pipelined_write", "two_write_queues", "compaction_style",
    ]
    return {k: params.get(k) for k in keys if params.get(k) is not None}


def worker(args):
    """Worker function for parallel fuzzing."""
    worker_id, start_seed, num_trials = args
    _init_feature_sets()

    oscillations = []
    idempotency_failures = []
    # Keys that use random.choice in special rules — skip for idempotency
    skip_idempotency_keys = {"compression_type", "bottommost_compression_type"}

    for i in range(num_trials):
        seed = start_seed + i
        params = _build_fuzzed_params(seed)

        # Test 1: Convergence
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            random.seed(seed + 777777)
            result = db_crashtest.finalize_and_sanitize(params)
            if any("converge" in str(x.message) for x in w):
                oscillations.append({
                    "seed": seed,
                    "params": _extract_relevant(params),
                })

        # Test 2: Idempotency (every 5th trial to save time)
        if i % 5 == 0:
            random.seed(seed + 777777)
            result2 = db_crashtest.finalize_and_sanitize(copy.deepcopy(result))
            for k in set(result.keys()) | set(result2.keys()):
                if k in skip_idempotency_keys:
                    continue
                if result.get(k) != result2.get(k):
                    idempotency_failures.append({
                        "seed": seed,
                        "key": k,
                        "pass1": result.get(k),
                        "pass2": result2.get(k),
                    })
                    break  # one failure per trial is enough

        # Progress every 100K
        if (i + 1) % 100000 == 0:
            sys.stderr.write(f"  Worker {worker_id}: {i+1}/{num_trials}\n")
            sys.stderr.flush()

    return {
        "worker_id": worker_id,
        "trials": num_trials,
        "oscillations": oscillations,
        "idempotency_failures": idempotency_failures,
    }


def main():
    total_trials = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000_000
    num_workers = int(sys.argv[2]) if len(sys.argv) > 2 else 100

    trials_per_worker = total_trials // num_workers
    remaining = total_trials - trials_per_worker * num_workers

    # Distribute work
    tasks = []
    seed_offset = random.randint(0, 2**32)
    current_seed = seed_offset
    for w in range(num_workers):
        n = trials_per_worker + (1 if w < remaining else 0)
        tasks.append((w, current_seed, n))
        current_seed += n

    print(f"Fuzzing finalize_and_sanitize convergence")
    print(f"  Total trials: {total_trials:,}")
    print(f"  Workers: {num_workers}")
    print(f"  Trials/worker: {trials_per_worker:,}")
    print(f"  Seed offset: {seed_offset}")
    print(f"  Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    sys.stdout.flush()

    start_time = time.time()

    with multiprocessing.Pool(num_workers) as pool:
        results = pool.map(worker, tasks)

    elapsed = time.time() - start_time

    # Aggregate results
    total_oscillations = []
    total_idempotency = []
    total_tested = 0

    for r in results:
        total_tested += r["trials"]
        total_oscillations.extend(r["oscillations"])
        total_idempotency.extend(r["idempotency_failures"])

    print(f"\n{'='*70}")
    print(f"RESULTS")
    print(f"{'='*70}")
    print(f"  Trials:               {total_tested:,}")
    print(f"  Elapsed:              {elapsed:.1f}s ({total_tested/elapsed:,.0f} trials/sec)")
    print(f"  Oscillations:         {len(total_oscillations)}")
    print(f"  Idempotency failures: {len(total_idempotency)}")
    print()

    if total_oscillations:
        print(f"OSCILLATION DETAILS ({len(total_oscillations)} found):")
        # Deduplicate by param signature
        seen = set()
        for osc in total_oscillations:
            sig = json.dumps(osc["params"], sort_keys=True)
            if sig not in seen:
                seen.add(sig)
                print(f"  Seed {osc['seed']}:")
                for k, v in sorted(osc["params"].items()):
                    print(f"    {k}={v}")
                print()
        print(f"  ({len(seen)} unique patterns)")

    if total_idempotency:
        print(f"\nIDEMPOTENCY FAILURES ({len(total_idempotency)} found):")
        seen = set()
        for fail in total_idempotency[:20]:
            sig = fail["key"]
            if sig not in seen:
                seen.add(sig)
                print(f"  Seed {fail['seed']}: {fail['key']} "
                      f"pass1={fail['pass1']} pass2={fail['pass2']}")

    if not total_oscillations and not total_idempotency:
        print("ALL CLEAR — no oscillations or idempotency failures!")

    # Write results to file
    result_file = f"/tmp/fuzz_convergence_results_{int(time.time())}.json"
    with open(result_file, "w") as f:
        json.dump({
            "total_trials": total_tested,
            "elapsed_seconds": elapsed,
            "seed_offset": seed_offset,
            "oscillations": total_oscillations,
            "idempotency_failures": total_idempotency,
        }, f, indent=2)
    print(f"\nDetailed results: {result_file}")

    sys.exit(1 if total_oscillations or total_idempotency else 0)


if __name__ == "__main__":
    main()
