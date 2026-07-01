#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

# pyre-strict

from __future__ import annotations

import os
import random
from dataclasses import dataclass, field
from typing import Optional

from runner_randomize_stress_flags import (
    CLOSURE,
    randomize_stress_flags,
    REMOTE_COMPACTION_CLOSURE,
)


@dataclass(frozen=True)
class InjectionSite:
    # entry_fn runs once per op; we stop on its op_index-th call to pick the op to
    # corrupt. We do not single-step entry_fn itself (for flush/compaction it is huge);
    # target_fns are the smaller inner functions we single-step inside that op to find a
    # critical instruction to corrupt (one target_fn is chosen per run).
    entry_fn: str
    target_fns: list[str]


INJECTION_SITES: dict[str, InjectionSite] = {
    "write": InjectionSite(
        entry_fn="rocksdb::MemTable::Add",
        target_fns=["rocksdb::MemTable::Add"],
    ),
    "flush": InjectionSite(
        entry_fn="rocksdb::FlushJob::Run",
        target_fns=[
            "rocksdb::CompactionIterator::NextFromInput",
            "rocksdb::BlockBuilder::Add",
        ],
    ),
    "compaction": InjectionSite(
        entry_fn="rocksdb::CompactionJob::Run",
        target_fns=[
            "rocksdb::CompactionIterator::NextFromInput",
            "rocksdb::BlockBuilder::Add",
        ],
    ),
}

# When a compaction run randomly injects into REMOTE compaction, the work runs on the
# compaction service's worker thread, so the injector must break here instead of the local
# CompactionJob::Run entry_fn (see build_runs).
REMOTE_COMPACTION_ENTRY_FN = "rocksdb::CompactionServiceCompactionJob::Run"

# Small SDC-safe domain for the randomized max_key. db_crashtest's own range is far too
# large -- a single injected flip in a huge keyspace rarely lands on a multi-version key,
# washing out the SDC signal. Every choice here still produces a multi-version layout.
MAX_KEY_CHOICES: list[int] = [10, 100, 1000, 10000]


# The complete db_stress config every run uses (the groups below merged). Together they
# keep one injected CPU corruption isolated and observable, so they are load-bearing for
# both correctness and detection effectiveness -- changing them weakens the signal.

# A mixed write/delete workload; reads and iterators are off so a single injected
# corruption stays visible across the following ops.
_MIXED_WORKLOAD: dict[str, object] = {
    "ops_per_thread": 1000,
    "max_key": 1000,
    "value_size_mult": 8,
    "column_families": 1,
    "nooverwritepercent": 0,
    "writepercent": 50,
    "delpercent": 40,
    "delrangepercent": 10,
    "readpercent": 0,
    "iterpercent": 0,
    "prefixpercent": 0,
    "flush_one_in": 20,
    "compact_range_one_in": 10,
    "compact_files_one_in": 10,
    # 64 KB, small on purpose: makes flush/compaction emit several SST files so there
    # are more critical instructions to corrupt.
    "target_file_size_base": 65536,
    "reopen": 0,
    "destroy_db_initially": 1,
    "clear_column_family_one_in": 0,
    "disable_wal": 0,
}

# Keep the injected op the only thing running, on one thread at a time, so (1) the op +
# its injected corruption + the post-op read-back form a clean attributable sequence, and
# (2) no sibling thread ever sits on the single-step target breakpoint and steals gdb's
# current frame mid-step. Each flag pins off one source of a competing thread.
_SERIALIZED: dict[str, object] = {
    "threads": 1,  # one workload thread
    "num_dbs": 1,
    "disable_auto_compactions": 1,  # no surprise background compaction
    "write_buffer_size": 1 << 30,  # memtable never auto-flushes
    "remote_compaction_worker_threads": 0,
    "subcompactions": 1,  # one compaction thread, no parallel subcompactions
}

# Every integrity check on -- the high-value goal is surfacing silent data corruption
# that would otherwise pass the read-back unnoticed.
_SDC_PROTECTIONS: dict[str, object] = {
    "batch_protection_bytes_per_key": 8,
    "memtable_protection_bytes_per_key": 8,
    "block_protection_bytes_per_key": 8,
    "paranoid_file_checks": 1,
    "paranoid_memory_checks": 1,
    "memtable_verify_per_key_checksum_on_seek": 1,
    "detect_filter_construct_corruption": "true",
    "verify_checksum": 1,
    "compression_checksum": 1,
    "file_checksum_impl": "crc32c",
    # Extra read-time checksum verification. Caution: a "read returns OK but checksum
    # fails" outcome is confusing and can also reflect a pre-existing issue, not our
    # injection -- interpret these with care.
    "verify_checksum_one_in": 1,
    "verify_file_checksums_one_in": 1000,
}

# db_stress's own fault injection off, so the injected CPU corruption is the only fault.
_NO_OTHER_FAULTS: dict[str, object] = {
    "read_fault_one_in": 0,
    "write_fault_one_in": 0,
    "metadata_read_fault_one_in": 0,
    "metadata_write_fault_one_in": 0,
    "open_read_fault_one_in": 0,
    "open_write_fault_one_in": 0,
    "open_metadata_read_fault_one_in": 0,
    "open_metadata_write_fault_one_in": 0,
    "secondary_cache_fault_one_in": 0,
    "sync_fault_injection": 0,
}

DB_STRESS_PRESET: dict[str, object] = {
    **_MIXED_WORKLOAD,
    **_SERIALIZED,
    **_SDC_PROTECTIONS,
    **_NO_OTHER_FAULTS,
}


def _int_flag(name: str) -> int:
    return int(DB_STRESS_PRESET[name])  # type: ignore[arg-type]


def op_count_estimate(op: str) -> int:
    """Rough upper bound on how many of this op a run performs; the caller uses it to
    bound which op instance to target."""
    ops = _int_flag("ops_per_thread")
    if op == "write":
        write_pct = (
            _int_flag("writepercent")
            + _int_flag("delpercent")
            + _int_flag("delrangepercent")
        )
        return max(ops * write_pct // 100, 1)
    if op == "flush":
        return max(ops // _int_flag("flush_one_in"), 1)
    per_range = ops // _int_flag("compact_range_one_in")
    per_files = ops // _int_flag("compact_files_one_in")
    return max(per_range + per_files, 1)


@dataclass
class Run:
    # Inputs, chosen by build_runs():
    index: int
    op: str
    op_index: int
    entry_fn: str
    target_fn: str
    seed: int
    dir: str
    stress_flags: list[str] = field(default_factory=list)
    # Outcome, set by runner_execute.execute():
    outcome: str = ""  # the result bucket: SDC / CORRUPTION / CRASH / NO_EFFECT / ...
    # inject.json contents, kept because runner_report reads them for the pick spread;
    # db_stress's data_corruption.json is not kept (it only feeds `outcome`).
    inject_json: Optional[dict[str, object]] = None


def build_runs(
    op: str,
    run_count: int,
    report_dir: str,
    base_seed: int,
    randomize: bool = False,
) -> list[Run]:
    site = INJECTION_SITES[op]
    # op_index picks which op instance the injector targets. Warmup single-steps that
    # instance's target_fn call to choose a critical-instruction index; a later instance
    # may have fewer critical instructions, so the injector retries the next instances
    # (op_index+1, +2, ...) until one reaches that index. Pick op_index in the first third
    # of the estimate so enough later instances remain to retry into.
    op_index_max = max(op_count_estimate(op) // 3, 1)
    runs: list[Run] = []
    for index in range(run_count):
        seed = (base_seed + index) % 2**64
        rng = random.Random(seed)
        op_index = rng.randint(1, op_index_max)
        target_fn = rng.choice(site.target_fns)
        run_dir = os.path.join(report_dir, f"run_{index:05d}")
        os.makedirs(run_dir, exist_ok=True)
        per_run_stress_flags = _per_run_stress_flags(run_dir, seed)
        # Randomize per run for coverage -- e.g. max_key (below) exercises a different
        # key space, others vary LSM shape and version -- and a compaction run may flip
        # to remote compaction, a distinct code path from local with its own potential
        # CPU-corruption vulnerabilities.
        entry_fn = site.entry_fn
        if randomize:
            preset = dict(DB_STRESS_PRESET)
            closure = CLOSURE
            preset["max_key"] = rng.choice(MAX_KEY_CHOICES)
            if op == "compaction" and rng.choice([0, 1]):
                preset["remote_compaction_worker_threads"] = 1
                closure = {**CLOSURE, **REMOTE_COMPACTION_CLOSURE}
                entry_fn = REMOTE_COMPACTION_ENTRY_FN
            stress_flags = randomize_stress_flags(per_run_stress_flags, preset, closure)
        else:
            stress_flags = _fixed_stress_flags(per_run_stress_flags)
        runs.append(
            Run(
                index=index,
                op=op,
                op_index=op_index,
                entry_fn=entry_fn,
                target_fn=target_fn,
                seed=seed,
                dir=run_dir,
                stress_flags=stress_flags,
            )
        )
    return runs


def _per_run_stress_flags(run_dir: str, seed: int) -> dict[str, object]:
    # The db_stress flags unique to one run -- its own db / expected-values dirs (created
    # here), its seed, and the dir db_stress writes corruption findings to. Shared by both
    # the fixed and randomized flag paths.
    db = os.path.join(run_dir, "db")
    exp = os.path.join(run_dir, "exp")
    os.makedirs(db, exist_ok=True)
    os.makedirs(exp, exist_ok=True)
    return {
        "db": db,
        "expected_values_dir": exp,
        "seed": seed,
        "verify_cpu_corruption_dir": run_dir,
    }


def _fixed_stress_flags(per_run_stress_flags: dict[str, object]) -> list[str]:
    # The fixed db_stress flag list: this run's flags plus every DB_STRESS_PRESET flag.
    return [
        f"--{flag}={value}"
        for flag, value in {**per_run_stress_flags, **DB_STRESS_PRESET}.items()
    ]
