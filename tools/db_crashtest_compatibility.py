#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import random
import sys
from typing import Any, Dict, List, NamedTuple, Optional, Tuple


_MISSING_PREDICATE_DEFAULT = object()


def _is_udt_memtable_only(params):
    return (
        params.get("user_timestamp_size", 0) > 0
        and params.get("persist_user_defined_timestamps") == 0
    )


# =============================================================================
# Dependency Solver Primitives
# =============================================================================
#
# This is the dependency-solver implementation described in
# docs/components/stress_test/crashtest_feature_compatibility_solver.md.
# The core primitives are side-effect-free; finalize_and_sanitize() builds
# production domains from the already-sampled params and applies the solver.


class _CompatibilityAssignment(NamedTuple):
    key: str
    value: Any


class _CompatibilityPredicate(NamedTuple):
    key: str
    op: str
    value: Any
    default: Any = _MISSING_PREDICATE_DEFAULT


class _CompatibilityRangeConstraint(NamedTuple):
    key: str
    op: str
    value: Optional[Any] = None
    other_key: Optional[str] = None


class _CompatibilityRule(NamedTuple):
    name: str
    antecedents: Tuple[Any, ...]
    consequents: Tuple[Any, ...]
    reason: str = ""


class _CompatibilityClosure(NamedTuple):
    assignments: Dict[str, Any]
    provenance: Dict[str, str]
    range_constraints: List[_CompatibilityRangeConstraint]


class _CompatibilitySolverConflict(Exception):
    pass


def _a(key, value):
    return _CompatibilityAssignment(key, value)


def _p(key, op, value, default=_MISSING_PREDICATE_DEFAULT):
    return _CompatibilityPredicate(key, op, value, default)


def _range(key, op, value=None, other_key=None):
    return _CompatibilityRangeConstraint(key, op, value=value, other_key=other_key)


def _rule(name, antecedents, consequents, reason=""):
    return _CompatibilityRule(
        name=name,
        antecedents=tuple(antecedents),
        consequents=tuple(consequents),
        reason=reason,
    )


_OPERATION_PERCENT_KEYS = (
    "readpercent",
    "prefixpercent",
    "iterpercent",
    "writepercent",
    "delpercent",
    "delrangepercent",
)


_OPERATION_TOTAL_PERCENT_KEYS = _OPERATION_PERCENT_KEYS + ("customopspercent",)


OPERATION_TOTAL_PERCENT_KEYS = _OPERATION_TOTAL_PERCENT_KEYS


_COMPATIBILITY_ALWAYS_PINNED_KEYS = set(_OPERATION_TOTAL_PERCENT_KEYS) | {
    "num_dbs",
}


def _disable_op_assignment(op_name):
    return _CompatibilityAssignment(f"__disable_op__:{op_name}", True)


def _move_op_assignment(op_name, target_op_name):
    return _CompatibilityAssignment(f"__move_op__:{op_name}", target_op_name)


def _is_op_disabled(assignments, op_name):
    return (
        assignments.get(f"__disable_op__:{op_name}") is True
        or f"__move_op__:{op_name}" in assignments
    )


def _op_move_target(assignments, op_name):
    return assignments.get(f"__move_op__:{op_name}")


def _compare_values(lhs, op, rhs):
    if op == "==":
        return lhs == rhs
    if op == "!=":
        return lhs != rhs
    if op == ">":
        return lhs > rhs
    if op == ">=":
        return lhs >= rhs
    if op == "<":
        return lhs < rhs
    if op == "<=":
        return lhs <= rhs
    if op == "in":
        return lhs in rhs
    if op == "not_in":
        return lhs not in rhs
    raise ValueError(f"unsupported predicate operator: {op}")


def _antecedent_satisfied(antecedent, assignments, known_keys=None):
    if isinstance(antecedent, _CompatibilityAssignment):
        return assignments.get(antecedent.key) == antecedent.value
    if isinstance(antecedent, _CompatibilityPredicate):
        if antecedent.key in assignments:
            value = assignments[antecedent.key]
        elif known_keys is not None and antecedent.key in known_keys:
            return False
        elif antecedent.default is _MISSING_PREDICATE_DEFAULT:
            return False
        else:
            value = antecedent.default
        return _compare_values(value, antecedent.op, antecedent.value)
    raise TypeError(f"unsupported antecedent type: {type(antecedent).__name__}")


def _range_constraint_rhs(constraint, assignments):
    if constraint.other_key is not None:
        if constraint.other_key not in assignments:
            return None
        return assignments[constraint.other_key]
    return constraint.value


def _check_range_constraint(constraint, assignments, provenance):
    if constraint.key not in assignments:
        return
    rhs = _range_constraint_rhs(constraint, assignments)
    if rhs is None:
        return
    lhs = assignments[constraint.key]
    if _compare_values(lhs, constraint.op, rhs):
        return
    rhs_desc = (
        f"{constraint.other_key}={rhs}"
        if constraint.other_key is not None
        else repr(rhs)
    )
    raise _CompatibilitySolverConflict(
        "range constraint violated: "
        f"{constraint.key}={lhs} must be {constraint.op} {rhs_desc}; "
        f"{constraint.key} from {provenance.get(constraint.key, 'unknown')}"
    )


def _check_all_range_constraints(range_constraints, assignments, provenance):
    for constraint in range_constraints:
        _check_range_constraint(constraint, assignments, provenance)


def _apply_compatibility_assignment(
    assignments, provenance, assignment, source, protected_assignments=None
):
    if (
        protected_assignments is not None
        and assignment.key in protected_assignments
        and protected_assignments[assignment.key] != assignment.value
    ):
        raise _CompatibilitySolverConflict(
            "candidate would rewrite fixed assignment "
            f"{assignment.key}: fixed {protected_assignments[assignment.key]}, "
            f"new {assignment.value} from {source}"
        )
    if assignment.key in assignments:
        existing = assignments[assignment.key]
        if existing != assignment.value:
            raise _CompatibilitySolverConflict(
                "conflicting assignments for "
                f"{assignment.key}: existing {existing} from "
                f"{provenance.get(assignment.key, 'unknown')}; "
                f"new {assignment.value} from {source}"
            )
        return False
    assignments[assignment.key] = assignment.value
    provenance[assignment.key] = source
    return True


def _compute_compatibility_closure(
    seed_assignments,
    rules,
    protected_assignments=None,
    seed_provenance=None,
    known_keys=None,
):
    assignments = dict(seed_assignments)
    provenance = {
        key: f"seed {key}={value}" for key, value in assignments.items()
    }
    if seed_provenance is not None:
        provenance.update(seed_provenance)
    range_constraints = []
    seen_constraints = set()

    changed = True
    while changed:
        changed = False
        for rule in rules:
            if not all(
                _antecedent_satisfied(antecedent, assignments, known_keys)
                for antecedent in rule.antecedents
            ):
                continue
            source = f"rule {rule.name}"
            for consequent in rule.consequents:
                if isinstance(consequent, _CompatibilityAssignment):
                    if _apply_compatibility_assignment(
                        assignments,
                        provenance,
                        consequent,
                        source,
                        protected_assignments=protected_assignments,
                    ):
                        changed = True
                elif isinstance(consequent, _CompatibilityRangeConstraint):
                    if consequent not in seen_constraints:
                        range_constraints.append(consequent)
                        seen_constraints.add(consequent)
                        changed = True
                    _check_range_constraint(consequent, assignments, provenance)
                else:
                    raise TypeError(
                        "unsupported consequent type: "
                        f"{type(consequent).__name__}"
                    )
        _check_all_range_constraints(range_constraints, assignments, provenance)

    return _CompatibilityClosure(assignments, provenance, range_constraints)


def _shuffle_weighted_unique_domain(domain, rng, shuffle_candidates=True):
    candidates = list(domain)
    if shuffle_candidates:
        rng.shuffle(candidates)
    seen = set()
    unique_candidates = []
    for candidate in candidates:
        marker = repr(candidate)
        if marker in seen:
            continue
        seen.add(marker)
        unique_candidates.append(candidate)
    return unique_candidates


def _solve_compatibility_options(
    pinned_assignments,
    option_domains,
    rules,
    option_order=None,
    rng=None,
    shuffle_candidates=True,
    known_keys=None,
):
    if rng is None:
        rng = random

    if known_keys is None:
        known_keys = set(option_domains) | set(pinned_assignments)
    closure = _compute_compatibility_closure(
        pinned_assignments, rules, known_keys=known_keys
    )
    fixed = closure.assignments
    provenance = closure.provenance

    if option_order is None:
        option_order = list(option_domains)
        rng.shuffle(option_order)

    for option in option_order:
        if option in fixed:
            continue

        last_conflict = None
        for candidate in _shuffle_weighted_unique_domain(
            option_domains[option], rng, shuffle_candidates=shuffle_candidates
        ):
            trial_seed = dict(fixed)
            trial_seed[option] = candidate
            trial_provenance = dict(provenance)
            trial_provenance[option] = f"candidate {option}={candidate}"
            try:
                trial = _compute_compatibility_closure(
                    trial_seed,
                    rules,
                    protected_assignments=fixed,
                    seed_provenance=trial_provenance,
                    known_keys=known_keys,
                )
            except _CompatibilitySolverConflict as exc:
                last_conflict = exc
                continue
            fixed = trial.assignments
            provenance = trial.provenance
            break
        else:
            detail = f": {last_conflict}" if last_conflict is not None else ""
            raise _CompatibilitySolverConflict(
                f"no compatible candidate for {option}{detail}"
            )

    return _compute_compatibility_closure(
        fixed,
        rules,
        protected_assignments=fixed,
        seed_provenance=provenance,
        known_keys=known_keys,
    )


def _normalize_operation_mix(assignments):
    missing = [key for key in _OPERATION_PERCENT_KEYS if key not in assignments]
    if missing:
        raise _CompatibilitySolverConflict(
            "operation mix is missing percent keys: " + ", ".join(sorted(missing))
        )

    result = dict(assignments)
    total_keys = [key for key in _OPERATION_TOTAL_PERCENT_KEYS if key in result]
    initial_sum = sum(result[key] for key in total_keys)

    delrange_target = _op_move_target(result, "delrange")
    if delrange_target is not None and delrange_target != "del":
        raise _CompatibilitySolverConflict(
            f"unsupported delrange redistribution target: {delrange_target}"
        )
    if delrange_target == "del" or result.get("__disable_op__:delrange") is True:
        result["delpercent"] += result["delrangepercent"]
        result["delrangepercent"] = 0

    prefix_target = _op_move_target(result, "prefix")
    if prefix_target == "iter":
        result["iterpercent"] += result["prefixpercent"]
        result["prefixpercent"] = 0
    elif prefix_target == "read" or result.get("__disable_op__:prefix") is True:
        result["readpercent"] += result["prefixpercent"]
        result["prefixpercent"] = 0
    elif prefix_target is not None:
        raise _CompatibilitySolverConflict(
            f"unsupported prefix redistribution target: {prefix_target}"
        )

    iter_target = _op_move_target(result, "iter")
    if iter_target is not None and iter_target != "read":
        raise _CompatibilitySolverConflict(
            f"unsupported iter redistribution target: {iter_target}"
        )
    if iter_target == "read" or result.get("__disable_op__:iter") is True:
        result["readpercent"] += result["iterpercent"]
        result["iterpercent"] = 0

    final_sum = sum(result[key] for key in total_keys)
    if final_sum != initial_sum:
        raise _CompatibilitySolverConflict(
            "operation percent sum changed during normalization: "
            f"{initial_sum} -> {final_sum}"
        )
    return result


_PERCENT_DOMAIN = tuple(range(101))


_COMPATIBILITY_OPTION_DOMAINS = {
    "acquire_snapshot_one_in": (0, 100, 10000),
    "allow_concurrent_memtable_write": (0, 1),
    "allow_resumption_one_in": (0, 1, 2, 20),
    "allow_setting_blob_options_dynamically": (0, 1),
    "async_io": (0, 1),
    "backup_one_in": (0, 1000, 100000),
    "best_efforts_recovery": (0, 1),
    "blob_compaction_readahead_size": (0, 1048576, 4194304),
    "blob_direct_write_partitions": (0, 1, 2, 4, 8),
    "blob_file_starting_level": (0, 1, 2, 3),
    "blob_garbage_collection_age_cutoff": (0.0, 0.25, 0.5, 0.75, 1.0),
    "blob_garbage_collection_force_threshold": (0.5, 0.75, 1.0),
    "block_protection_bytes_per_key": (0, 1, 2, 4, 8),
    "cache_size": (0, 8388608, 33554432),
    "checkpoint_one_in": (0, 1000, 10000, 1000000),
    "check_multiget_consistency": (0, 1),
    "check_multiget_entity_consistency": (0, 1),
    "clear_column_family_one_in": (0, 10),
    "clear_wp_commit_cache_one_in": (0, 10),
    "column_families": (1, 4),
    "compact_range_one_in": (0, 1000, 1000000),
    "compaction_style": (0, 1, 2),
    "compaction_ttl": (0, 1, 2, 10, 100, 1000),
    "compression_max_dict_buffer_bytes": (0,),
    "compression_max_dict_bytes": (0, 16384),
    "compression_parallel_threads": (1, 2, 3, 4, 5, 8, 9, 16),
    "compression_type": ("none", "snappy", "zlib", "lz4", "lz4hc", "xpress", "zstd"),
    "compression_zstd_max_train_bytes": (0, 65536),
    "continuous_verification_interval": (0, 1000),
    "create_timestamped_snapshot_one_in": (0, 20, 50, 100),
    "daily_offpeak_time_utc": (
        "",
        "00:00-23:59",
        "04:00-08:00",
        "23:30-03:15",
    ),
    "db_write_buffer_size": (
        0,
        1,
        1024 * 1024,
        8 * 1024 * 1024,
        128 * 1024 * 1024,
    ),
    "delrangepercent": _PERCENT_DOMAIN,
    "atomic_flush": (0, 1),
    "commit_bypass_memtable_one_in": (0, 100, 500, 1000),
    "disable_wal": (0, 1),
    "enable_blob_direct_write": (0, 1),
    "enable_blob_files": (0, 1),
    "enable_blob_garbage_collection": (0, 1),
    "enable_compaction_filter": (0, 1),
    "enable_pipelined_write": (0, 1),
    "exclude_wal_from_write_fault_injection": (0, 1),
    "fifo_compaction_max_data_files_size_mb": (0, 100 * 1024),
    "fifo_compaction_max_table_files_size_mb": (0, 100 * 1024),
    "fifo_compaction_use_kv_ratio_compaction": (0, 1),
    "file_checksum_impl": ("none", "crc32c", "xxh64", "big"),
    "file_temperature_age_thresholds": (
        "",
        "{{temperature=kWarm;age=10}:{temperature=kCool;age=30}:{temperature=kCold;age=100}:{temperature=kIce;age=300}}",
        "{{temperature=kWarm;age=30}:{temperature=kCold;age=300}}",
        "{{temperature=kCold;age=100}}",
    ),
    "get_current_wal_file_one_in": (0, 10000, 1000000),
    "get_live_files_apis_one_in": (0, 10000, 1000000),
    "get_sorted_wal_files_one_in": (0, 10000, 1000000),
    "index_block_search_type": (0, 1, 2),
    "index_type": (0, 1, 2, 3),
    "ingest_external_file_one_in": (0, 1000, 1000000),
    "ingest_wbwi_one_in": (0, 100, 500),
    "inplace_update_support": (0, 1),
    "iterpercent": _PERCENT_DOMAIN,
    "last_level_temperature": (
        "kUnknown",
        "kHot",
        "kWarm",
        "kCool",
        "kCold",
        "kIce",
    ),
    "lock_wal_one_in": (0, 10000, 1000000),
    "manual_wal_flush_one_in": (0, 1000),
    "max_compaction_trigger_wakeup_seconds": (20, 30, 600, 43200),
    "max_sequential_skip_in_iterations": (1, 2, 8, 16, sys.maxsize),
    "max_write_buffer_size_to_maintain": (
        0,
        1024 * 1024,
        2 * 1024 * 1024,
        4 * 1024 * 1024,
        8 * 1024 * 1024,
        32 * 1024 * 1024,
    ),
    "max_write_buffer_number": (3, 10),
    "memtable_prefix_bloom_size_ratio": (0, 0.001, 0.01, 0.1, 0.5),
    "memtable_verify_per_key_checksum_on_seek": (0, 1),
    "memtable_whole_key_filtering": (0, 1),
    "memtablerep": ("skip_list", "vector", "hash_linkedlist"),
    "metadata_read_fault_one_in": (0, 32, 1000),
    "metadata_write_fault_one_in": (0, 128, 1000),
    "min_tombstones_for_range_conversion": (0, 2, 4, 16),
    "mmap_read": (0, 1),
    "multiscan_max_prefetch_memory_bytes": (0, 64 * 1024, 256 * 1024),
    "multiscan_use_async_io": (0, 1),
    "num_dbs": (1, 2, 3),
    "open_files_async": (0, 1),
    "open_files": (-1, 100, 500000),
    "open_metadata_read_fault_one_in": (0, 8),
    "open_metadata_write_fault_one_in": (0, 8),
    "open_read_fault_one_in": (0, 32),
    "open_write_fault_one_in": (0, 16),
    "paranoid_memory_checks": (0, 1),
    "partition_filters": (0, 1),
    "periodic_compaction_seconds": (0, 1, 2, 10, 100, 1000),
    "persist_user_defined_timestamps": (0, 1),
    "preclude_last_level_data_seconds": (-1, 0, 10, 60, 1200, 86400),
    "prefix_size": (-1, 1, 5, 7, 8),
    "prefixpercent": _PERCENT_DOMAIN,
    "read_fault_one_in": (0, 32, 1000),
    "read_triggered_compaction_threshold": (0.0, 0.001, 0.01),
    "rate_limit_auto_wal_flush": (0,),
    "recycle_log_file_num": (0,),
    "remote_compaction_worker_threads": (0, 1, 8),
    "reopen": (0, 20),
    "skip_stats_update_on_db_open": (0, 1),
    "skip_verifydb": (0, 1),
    "simple": (0, 1),
    "sst_file_manager_bytes_per_sec": (0, 104857600),
    "sst_file_manager_bytes_per_truncate": (0, 1048576),
    "sync": (0, 1),
    "sync_fault_injection": (0, 1),
    "sync_wal_one_in": (0,),
    "test_best_efforts_recovery": (0, 1),
    "test_batches_snapshots": (0, 1),
    "test_ingest_standalone_range_deletion_one_in": (0, 5, 10),
    "test_multi_ops_txns": (0, 1),
    "test_secondary": (0, 1),
    "test_type": ("blackbox", "whitebox"),
    "track_and_verify_wals": (0, 1),
    "two_write_queues": (0, 1),
    "txn_write_policy": (0, 1, 2),
    "unordered_write": (0, 1),
    "use_attribute_group": (0, 1),
    "use_blob_db": (0, 1),
    "use_direct_io_for_flush_and_compaction": (0, 1),
    "use_direct_reads": (0, 1),
    "use_full_merge_v1": (0, 1),
    "use_get_entity": (0, 1),
    "use_merge": (0, 1),
    "use_multi_cf_iterator": (0, 1),
    "use_multi_get_entity": (0, 1),
    "use_multiget": (0, 1),
    "use_multiscan": (0, 1),
    "use_optimistic_txn": (0, 1),
    "use_put_entity_one_in": (0, 1, 5, 10),
    "use_only_the_last_commit_time_batch_for_recovery": (0, 1),
    "use_sqfc_for_range_queries": (0, 1),
    "use_timed_put_one_in": (0, 1, 3, 5, 10),
    "use_trie_index": (0, 1),
    "use_udi_as_primary_index": (0, 1),
    "use_write_buffer_manager": (0, 1),
    "use_txn": (0, 1),
    "user_timestamp_size": (0, 8),
    "verify_db_one_in": (0, 1000, 10000, 100000),
    "verify_file_checksums_one_in": (0, 1000, 1000000),
    "write_dbid_to_manifest": (0, 1),
    "write_fault_one_in": (0, 32, 128, 1000),
    "write_buffer_size": (
        65536,
        1024 * 1024,
        4 * 1024 * 1024,
        32 * 1024 * 1024,
    ),
    "write_identity_file": (0, 1),
    "wp_commit_cache_bits": (0, 10),
    "wp_snapshot_cache_bits": (0, 1),
}


_COMPATIBILITY_RULES = (
    # Feature requirements.
    _rule(
        "feature_best_efforts_recovery",
        [_a("best_efforts_recovery", 1)],
        [
            _a("disable_wal", 1),
            _a("inplace_update_support", 0),
            _a("enable_blob_files", 0),
            _a("enable_blob_garbage_collection", 0),
            _a("allow_setting_blob_options_dynamically", 0),
            _a("enable_blob_direct_write", 0),
            _a("enable_compaction_filter", 0),
            _a("sync", 0),
            _a("write_fault_one_in", 0),
            _a("skip_verifydb", 1),
            _a("verify_db_one_in", 0),
        ],
        "Best-efforts recovery requires a WAL-disabled no-verify profile.",
    ),
    _rule(
        "feature_inplace_update_support",
        [_a("inplace_update_support", 1)],
        [
            _a("disable_wal", 0),
            _a("allow_concurrent_memtable_write", 0),
            _a("sync_fault_injection", 0),
            _a("manual_wal_flush_one_in", 0),
            _a("memtable_verify_per_key_checksum_on_seek", 0),
        ],
    ),
    _rule(
        "feature_udt_memtable_only",
        [_p("user_timestamp_size", ">", 0), _a("persist_user_defined_timestamps", 0)],
        [
            _a("atomic_flush", 0),
            _a("allow_concurrent_memtable_write", 0),
            _a("enable_blob_files", 0),
            _a("allow_setting_blob_options_dynamically", 0),
            _a("block_protection_bytes_per_key", 0),
            _a("use_multiget", 0),
            _a("use_multi_get_entity", 0),
        ],
    ),
    _rule(
        "feature_remote_compaction",
        [_p("remote_compaction_worker_threads", ">", 0)],
        [
            _a("enable_blob_files", 0),
            _a("enable_blob_garbage_collection", 0),
            _a("allow_setting_blob_options_dynamically", 0),
            _a("inplace_update_support", 0),
            _a("checkpoint_one_in", 0),
            _a("use_timed_put_one_in", 0),
            _a("test_secondary", 0),
            _a("mmap_read", 0),
            _a("open_metadata_write_fault_one_in", 0),
            _a("open_metadata_read_fault_one_in", 0),
            _a("open_write_fault_one_in", 0),
            _a("open_read_fault_one_in", 0),
            _a("sync_fault_injection", 0),
            _a("skip_stats_update_on_db_open", 0),
        ],
    ),
    _rule(
        "feature_blob_direct_write",
        [_a("enable_blob_direct_write", 1)],
        [
            _a("enable_blob_files", 1),
            _range("blob_direct_write_partitions", ">=", 1),
            _a("allow_concurrent_memtable_write", 0),
            _a("enable_pipelined_write", 0),
            _a("two_write_queues", 0),
            _a("unordered_write", 0),
            _a("inplace_update_support", 0),
            _a("use_blob_db", 0),
            _a("allow_setting_blob_options_dynamically", 0),
            _a("enable_blob_garbage_collection", 0),
            _a("blob_garbage_collection_age_cutoff", 0.0),
            _a("blob_garbage_collection_force_threshold", 1.0),
            _a("blob_compaction_readahead_size", 0),
            _a("blob_file_starting_level", 0),
            _a("use_merge", 0),
            _a("use_full_merge_v1", 0),
            _a("use_timed_put_one_in", 0),
            _a("use_attribute_group", 0),
            _a("user_timestamp_size", 0),
            _a("persist_user_defined_timestamps", 0),
            _a("create_timestamped_snapshot_one_in", 0),
            _a("use_txn", 0),
            _a("txn_write_policy", 0),
            _a("use_optimistic_txn", 0),
            _a("test_multi_ops_txns", 0),
            _a("commit_bypass_memtable_one_in", 0),
            _a("disable_wal", 1),
            _a("best_efforts_recovery", 0),
            _a("reopen", 0),
            _a("manual_wal_flush_one_in", 0),
            _a("sync_wal_one_in", 0),
            _a("lock_wal_one_in", 0),
            _a("get_sorted_wal_files_one_in", 0),
            _a("get_current_wal_file_one_in", 0),
            _a("track_and_verify_wals", 0),
            _a("rate_limit_auto_wal_flush", 0),
            _a("recycle_log_file_num", 0),
            _a("sync_fault_injection", 0),
            _a("write_fault_one_in", 0),
            _a("metadata_write_fault_one_in", 0),
            _a("read_fault_one_in", 0),
            _a("metadata_read_fault_one_in", 0),
            _a("open_metadata_write_fault_one_in", 0),
            _a("open_metadata_read_fault_one_in", 0),
            _a("open_write_fault_one_in", 0),
            _a("open_read_fault_one_in", 0),
            _a("remote_compaction_worker_threads", 0),
            _a("test_secondary", 0),
            _a("backup_one_in", 0),
            _a("checkpoint_one_in", 0),
            _a("get_live_files_apis_one_in", 0),
            _a("ingest_external_file_one_in", 0),
            _a("ingest_wbwi_one_in", 0),
        ],
    ),
    _rule(
        "feature_mmap_read",
        [_a("mmap_read", 1)],
        [
            _a("use_direct_io_for_flush_and_compaction", 0),
            _a("use_direct_reads", 0),
            _a("multiscan_use_async_io", 0),
        ],
    ),
    _rule(
        "feature_txn_non_write_committed",
        [_a("use_txn", 1), _p("txn_write_policy", "!=", 0)],
        [
            _a("sync_fault_injection", 0),
            _a("disable_wal", 0),
            _a("manual_wal_flush_one_in", 0),
            _a("use_put_entity_one_in", 0),
            _a("use_multi_cf_iterator", 0),
            _a("commit_bypass_memtable_one_in", 0),
            _a("remote_compaction_worker_threads", 0),
        ],
    ),
    _rule(
        "feature_unordered_write",
        [_a("unordered_write", 1), _a("txn_write_policy", 1)],
        [_a("allow_concurrent_memtable_write", 1)],
    ),
    _rule(
        "feature_commit_bypass_memtable",
        [_a("use_txn", 1), _p("commit_bypass_memtable_one_in", ">", 0)],
        [
            _a("enable_blob_files", 0),
            _a("allow_setting_blob_options_dynamically", 0),
            _a("allow_concurrent_memtable_write", 0),
            _a("use_put_entity_one_in", 0),
            _a("use_get_entity", 0),
            _a("use_multi_get_entity", 0),
            _a("enable_pipelined_write", 0),
            _a("use_attribute_group", 0),
        ],
    ),
    _rule(
        "feature_multiscan",
        [_a("use_multiscan", 1)],
        [
            _a("async_io", 0),
            _a("prefix_size", -1),
            _a("read_fault_one_in", 0),
            _a("memtable_prefix_bloom_size_ratio", 0),
            _a("max_sequential_skip_in_iterations", sys.maxsize),
            _a("min_tombstones_for_range_conversion", 0),
            _a("test_ingest_standalone_range_deletion_one_in", 0),
            _a("skip_stats_update_on_db_open", 0),
        ],
    ),
    # One-way consequence rules.
    _rule(
        "compression_dict_zero",
        [_a("compression_max_dict_bytes", 0)],
        [_a("compression_zstd_max_train_bytes", 0), _a("compression_max_dict_buffer_bytes", 0)],
    ),
    _rule(
        "compression_non_zstd",
        [_p("compression_type", "!=", "zstd")],
        [_a("compression_zstd_max_train_bytes", 0)],
    ),
    _rule("vector_memtable", [_a("memtablerep", "vector")], [_a("inplace_update_support", 0)]),
    _rule(
        "non_skiplist_memtable",
        [_p("memtablerep", "!=", "skip_list", default=None)],
        [_a("paranoid_memory_checks", 0), _a("memtable_verify_per_key_checksum_on_seek", 0)],
    ),
    _rule(
        "test_batches_snapshots",
        [_a("test_batches_snapshots", 1)],
        [
            _a("enable_compaction_filter", 0),
            _a("inplace_update_support", 0),
            _a("write_fault_one_in", 0),
            _a("metadata_write_fault_one_in", 0),
            _a("read_fault_one_in", 0),
            _a("metadata_read_fault_one_in", 0),
            _a("use_multiscan", 0),
        ],
    ),
    _rule("test_batches_snapshots_prefix", [_a("test_batches_snapshots", 1), _p("prefix_size", "<", 0)], [_a("prefix_size", 1)]),
    _rule(
        "trie_index",
        [_a("use_trie_index", 1)],
        [_a("mmap_read", 0), _a("compression_parallel_threads", 1)],
    ),
    _rule(
        "udi_primary_requires_trie",
        [_a("use_trie_index", 0)],
        [_a("use_udi_as_primary_index", 0)],
    ),
    _rule(
        "udi_primary_index_type",
        # Preserve compatible sampled index types; only clamp unsupported ones.
        [
            _a("use_trie_index", 1),
            _a("use_udi_as_primary_index", 1),
            _p("index_type", "not_in", {0, 3}),
        ],
        [_a("index_type", 0)],
    ),
    _rule(
        "udi_primary_incompatible_features",
        [_a("use_trie_index", 1), _a("use_udi_as_primary_index", 1)],
        [_a("partition_filters", 0), _a("backup_one_in", 0), _a("test_secondary", 0)],
    ),
    _rule("multi_key_ops_ingest_batches", [_a("test_batches_snapshots", 1)], [_a("ingest_external_file_one_in", 0)]),
    _rule("multi_key_ops_ingest_txn", [_a("use_txn", 1)], [_a("ingest_external_file_one_in", 0)]),
    _rule("multi_key_ops_ingest_udt", [_p("user_timestamp_size", ">", 0)], [_a("ingest_external_file_one_in", 0)]),
    _rule("multi_key_ops_delrange_batches", [_a("test_batches_snapshots", 1)], [_move_op_assignment("delrange", "del")]),
    _rule("multi_key_ops_delrange_txn", [_a("use_txn", 1)], [_move_op_assignment("delrange", "del")]),
    _rule(
        "udt_memtable_only_wal_profile",
        [
            _p("user_timestamp_size", ">", 0),
            _a("persist_user_defined_timestamps", 0),
            _a("best_efforts_recovery", 0),
            _a("test_best_efforts_recovery", 0),
        ],
        [_a("disable_wal", 0)],
    ),
    _rule(
        "udt_memtable_only_iteration_shape",
        [_p("user_timestamp_size", ">", 0), _a("persist_user_defined_timestamps", 0)],
        [_move_op_assignment("iter", "read")],
    ),
    _rule("inplace_update_fixed_delrange", [_a("inplace_update_support", 1)], [_move_op_assignment("delrange", "del")]),
    _rule("inplace_update_fixed_prefix", [_a("inplace_update_support", 1)], [_move_op_assignment("prefix", "read")]),
    _rule("multiscan_shape_delrange", [_a("use_multiscan", 1)], [_move_op_assignment("delrange", "del")]),
    _rule(
        "multiscan_shape_prefix",
        [
            _a("use_multiscan", 1),
            _p("inplace_update_support", "!=", 1),
            _p("enable_compaction_filter", "!=", 1),
        ],
        [_move_op_assignment("prefix", "iter")],
    ),
    _rule(
        "multiscan_prefetch",
        # Preserve compatible sampled prefetch sizes; only clamp unsupported ones.
        [
            _a("use_multiscan", 1),
            _p("multiscan_max_prefetch_memory_bytes", "not_in", {0, 64 * 1024, 256 * 1024}),
        ],
        [_a("multiscan_max_prefetch_memory_bytes", 0)],
    ),
    _rule("wal_disruption_ingest_sync_fault", [_a("sync_fault_injection", 1)], [_a("ingest_external_file_one_in", 0), _a("enable_compaction_filter", 0)]),
    _rule("wal_disruption_ingest_disable_wal", [_a("disable_wal", 1)], [_a("ingest_external_file_one_in", 0), _a("enable_compaction_filter", 0)]),
    _rule("wal_disruption_ingest_manual_flush", [_p("manual_wal_flush_one_in", ">", 0)], [_a("ingest_external_file_one_in", 0), _a("enable_compaction_filter", 0)]),
    _rule("unordered_write_requires_write_prepared", [_a("unordered_write", 1), _p("txn_write_policy", "!=", 1)], [_a("unordered_write", 0)]),
    _rule("timestamped_snapshot_unordered", [_p("create_timestamped_snapshot_one_in", ">", 0)], [_a("unordered_write", 0)]),
    _rule("timestamped_snapshot_policy", [_p("create_timestamped_snapshot_one_in", ">", 0), _p("txn_write_policy", "!=", 0)], [_a("create_timestamped_snapshot_one_in", 0)]),
    _rule(
        "disable_wal_core_consequences",
        [_a("disable_wal", 1)],
        [
            _a("sync", 0),
            _a("write_fault_one_in", 0),
            _a("test_batches_snapshots", 0),
            _a("reopen", 0),
            _a("manual_wal_flush_one_in", 0),
            _a("recycle_log_file_num", 0),
        ],
    ),
    _rule("disable_wal_pipelined_write", [_a("disable_wal", 1)], [_a("enable_pipelined_write", 0)]),
    _rule("disable_wal_atomic_flush_no_udt", [_a("disable_wal", 1), _p("user_timestamp_size", "<=", 0)], [_a("atomic_flush", 1)]),
    _rule("disable_wal_atomic_flush_persisted_udt", [_a("disable_wal", 1), _p("persist_user_defined_timestamps", "!=", 0)], [_a("atomic_flush", 1)]),
    _rule(
        "disable_wal_atomic_flush_non_ber",
        [_a("disable_wal", 1), _a("best_efforts_recovery", 0), _a("test_best_efforts_recovery", 0)],
        [_a("atomic_flush", 1)],
    ),
    _rule(
        "disable_wal_atomic_flush_udt_ber",
        [_a("disable_wal", 1), _p("user_timestamp_size", ">", 0), _a("persist_user_defined_timestamps", 0), _a("best_efforts_recovery", 1)],
        [_a("atomic_flush", 0)],
    ),
    _rule(
        "disable_wal_atomic_flush_udt_ber_test",
        [_a("disable_wal", 1), _p("user_timestamp_size", ">", 0), _a("persist_user_defined_timestamps", 0), _a("test_best_efforts_recovery", 1)],
        [_a("atomic_flush", 0)],
    ),
    _rule("range_conversion_sqfc", [_p("min_tombstones_for_range_conversion", ">", 0)], [_a("use_sqfc_for_range_queries", 0), _a("inplace_update_support", 0)]),
    _rule("open_files_limited", [_p("open_files", "!=", -1)], [_a("compaction_ttl", 0), _a("periodic_compaction_seconds", 0)]),
    _rule("fifo_compaction", [_a("compaction_style", 2)], [_a("compaction_ttl", 0), _a("periodic_compaction_seconds", 0), _a("open_files_async", 0), _a("preclude_last_level_data_seconds", 0), _a("last_level_temperature", "kUnknown")]),
    _rule("non_fifo_compaction", [_p("compaction_style", "!=", 2)], [_a("file_temperature_age_thresholds", ""), _a("fifo_compaction_max_data_files_size_mb", 0), _a("fifo_compaction_max_table_files_size_mb", 0), _a("fifo_compaction_use_kv_ratio_compaction", 0)]),
    _rule("partition_filters_index", [_a("partition_filters", 1), _p("index_type", "!=", 2)], [_a("partition_filters", 0)]),
    _rule("atomic_flush_pipelined", [_a("atomic_flush", 1)], [_a("enable_pipelined_write", 0)]),
    _rule("sst_file_manager_no_rate", [_a("sst_file_manager_bytes_per_sec", 0)], [_a("sst_file_manager_bytes_per_truncate", 0)]),
    _rule("sst_file_manager_secondary", [_a("test_secondary", 1)], [_a("sst_file_manager_bytes_per_truncate", 0)]),
    _rule("prefix_size_negative", [_a("prefix_size", -1), _p("use_multiscan", "!=", 1)], [_move_op_assignment("prefix", "read")]),
    _rule("simple_blackbox_prefix_iteration", [_p("prefix_size", "!=", -1), _a("simple", 1), _a("test_type", "blackbox")], [_move_op_assignment("iter", "read")]),
    _rule("prefix_bloom_no_prefix", [_a("prefix_size", -1), _a("memtable_whole_key_filtering", 0)], [_a("memtable_prefix_bloom_size_ratio", 0)]),
    _rule("two_write_queues", [_a("two_write_queues", 1)], [_a("enable_pipelined_write", 0)]),
    _rule("multi_ops_txns_faults", [_a("test_multi_ops_txns", 1)], [_a("write_fault_one_in", 0), _a("metadata_write_fault_one_in", 0), _a("read_fault_one_in", 0), _a("metadata_read_fault_one_in", 0)]),
    _rule("multi_ops_txns_write_prepared", [_a("test_multi_ops_txns", 1), _p("txn_write_policy", "!=", 0)], [_a("wp_snapshot_cache_bits", 1), _a("wp_commit_cache_bits", 10), _a("enable_pipelined_write", 0), _a("checkpoint_one_in", 0), _a("use_only_the_last_commit_time_batch_for_recovery", 1), _a("clear_wp_commit_cache_one_in", 10), _a("lock_wal_one_in", 0)]),
    _rule("put_entity_merge", [_p("use_put_entity_one_in", "!=", 0)], [_a("use_full_merge_v1", 0)]),
    _rule("file_checksum_none", [_a("file_checksum_impl", "none")], [_a("verify_file_checksums_one_in", 0)]),
    _rule("write_fault_buffer", [_p("write_fault_one_in", ">", 0)], [_range("max_write_buffer_number", ">=", 10)]),
    _rule("write_buffer_manager_cache", [_a("use_write_buffer_manager", 1), _p("cache_size", "<=", 0)], [_a("use_write_buffer_manager", 0)]),
    _rule("write_buffer_manager_db_buffer", [_a("use_write_buffer_manager", 1), _p("db_write_buffer_size", "<=", 0)], [_a("use_write_buffer_manager", 0)]),
    _rule("user_timestamps", [_p("user_timestamp_size", ">", 0)], [_a("index_block_search_type", 0), _a("use_trie_index", 0)]),
    _rule("compaction_filter_snapshots", [_a("enable_compaction_filter", 1)], [_a("acquire_snapshot_one_in", 0), _a("compact_range_one_in", 0), _move_op_assignment("iter", "read"), _a("check_multiget_consistency", 0), _a("check_multiget_entity_consistency", 0)]),
    _rule("compaction_filter_prefix_snapshots", [_a("enable_compaction_filter", 1)], [_move_op_assignment("prefix", "read")]),
    _rule("inplace_snapshots", [_a("inplace_update_support", 1)], [_a("acquire_snapshot_one_in", 0), _a("compact_range_one_in", 0), _move_op_assignment("iter", "read"), _move_op_assignment("prefix", "read"), _a("check_multiget_consistency", 0), _a("check_multiget_entity_consistency", 0)]),
    _rule("wal_write_error_reopen", [_a("disable_wal", 0), _p("reopen", ">", 0)], [_a("exclude_wal_from_write_fault_injection", 1), _a("metadata_write_fault_one_in", 0)]),
    _rule("wal_write_error_manual_flush", [_a("disable_wal", 0), _p("manual_wal_flush_one_in", ">", 0), _p("column_families", "!=", 1, default=None)], [_a("exclude_wal_from_write_fault_injection", 1), _a("metadata_write_fault_one_in", 0)]),
    _rule("wal_write_error_txn", [_a("disable_wal", 0), _p("use_txn", "!=", 0), _a("use_optimistic_txn", 0)], [_a("exclude_wal_from_write_fault_injection", 1), _a("metadata_write_fault_one_in", 0)]),
    _rule("periodic_compaction_offpeak", [_a("periodic_compaction_seconds", 0)], [_a("daily_offpeak_time_utc", "")]),
    _rule("read_triggered_compaction_wakeup", [_p("read_triggered_compaction_threshold", ">", 0)], [_a("max_compaction_trigger_wakeup_seconds", 20)]),
    _rule("put_entity_timed_put_exclusive", [_a("use_put_entity_one_in", 1)], [_a("use_timed_put_one_in", 0)]),
    _rule("put_entity_timed_put_coexist", [_p("use_put_entity_one_in", ">", 1), _a("use_timed_put_one_in", 1)], [_a("use_timed_put_one_in", 3)]),
    _rule("identity_file", [_a("write_dbid_to_manifest", 0), _a("write_identity_file", 0)], [_a("write_dbid_to_manifest", 1)]),
    _rule("checkpoint_lock_wal", [_p("checkpoint_one_in", "!=", 0)], [_a("lock_wal_one_in", 0)]),
    _rule("ingest_range_deletion_no_ingest", [_a("ingest_external_file_one_in", 0)], [_a("test_ingest_standalone_range_deletion_one_in", 0)]),
    _rule("ingest_range_deletion_no_delrange", [_a("delrangepercent", 0)], [_a("test_ingest_standalone_range_deletion_one_in", 0)]),
    _rule("ingest_range_deletion_moved_delrange", [_a("__move_op__:delrange", "del")], [_a("test_ingest_standalone_range_deletion_one_in", 0)]),
    _rule("ingest_range_deletion_disabled_delrange", [_a("__disable_op__:delrange", True)], [_a("test_ingest_standalone_range_deletion_one_in", 0)]),
    _rule("ingest_wbwi_pipelined", [_a("enable_pipelined_write", 1)], [_a("ingest_wbwi_one_in", 0)]),
    _rule("ingest_wbwi_unordered", [_a("unordered_write", 1)], [_a("ingest_wbwi_one_in", 0)]),
    _rule("ingest_wbwi_wal_enabled", [_a("disable_wal", 0)], [_a("ingest_wbwi_one_in", 0)]),
    _rule("ingest_wbwi_user_timestamps", [_p("user_timestamp_size", ">", 0)], [_a("ingest_wbwi_one_in", 0)]),
    _rule("test_secondary_continuous_verification", [_a("test_secondary", 1)], [_a("continuous_verification_interval", 0)]),
    _rule(
        "multi_db_incompatible_features",
        [_p("num_dbs", ">", 1)],
        [_a("clear_column_family_one_in", 0), _a("test_multi_ops_txns", 0)],
    ),
    _rule("skip_stats_open_files_async", [_a("skip_stats_update_on_db_open", 0)], [_a("open_files_async", 0)]),
    _rule("allow_resumption_requires_remote", [_a("remote_compaction_worker_threads", 0)], [_a("allow_resumption_one_in", 0)]),
    _rule("optimistic_txn_write_buffer_maintain", [_a("use_optimistic_txn", 1)], [_range("max_write_buffer_size_to_maintain", ">=", other_key="write_buffer_size")]),
)


_COMPATIBILITY_RULES_VALIDATED = False


def _is_derived_compatibility_key(key):
    return key.startswith("__disable_op__:") or key.startswith("__move_op__:")


def _validate_compatibility_rule_domains(
    rules,
    option_domains,
    computed_keys=(),
):
    declared_keys = set(option_domains) | set(computed_keys)
    errors = []

    def check_key(rule_name, context, key):
        if _is_derived_compatibility_key(key):
            return
        if key not in declared_keys:
            errors.append(f"{rule_name}: {context} key {key} has no domain")

    def check_assignment(rule_name, context, assignment):
        check_key(rule_name, context, assignment.key)
        if assignment.key in option_domains:
            domain = option_domains[assignment.key]
            if assignment.value not in domain:
                errors.append(
                    f"{rule_name}: {context} {assignment.key}={assignment.value} "
                    f"is outside domain {domain}"
                )

    for rule in rules:
        for antecedent in rule.antecedents:
            if isinstance(antecedent, _CompatibilityAssignment):
                check_assignment(rule.name, "antecedent", antecedent)
            elif isinstance(antecedent, _CompatibilityPredicate):
                check_key(rule.name, "antecedent", antecedent.key)
                if antecedent.op == "==" and antecedent.key in option_domains:
                    domain = option_domains[antecedent.key]
                    if antecedent.value not in domain:
                        errors.append(
                            f"{rule.name}: antecedent "
                            f"{antecedent.key}=={antecedent.value} "
                            f"is outside domain {domain}"
                        )
            else:
                errors.append(
                    f"{rule.name}: unsupported antecedent "
                    f"{type(antecedent).__name__}"
                )
        for consequent in rule.consequents:
            if isinstance(consequent, _CompatibilityAssignment):
                check_assignment(rule.name, "consequent", consequent)
            elif isinstance(consequent, _CompatibilityRangeConstraint):
                check_key(rule.name, "range constraint", consequent.key)
                if consequent.other_key is not None:
                    check_key(rule.name, "range constraint", consequent.other_key)
            else:
                errors.append(
                    f"{rule.name}: unsupported consequent "
                    f"{type(consequent).__name__}"
                )

    if errors:
        raise _CompatibilitySolverConflict(
            "invalid compatibility rule declarations:\n  " + "\n  ".join(errors)
        )


def _validate_compatibility_rules():
    global _COMPATIBILITY_RULES_VALIDATED
    if not _COMPATIBILITY_RULES_VALIDATED:
        _validate_compatibility_rule_domains(
            _COMPATIBILITY_RULES,
            _COMPATIBILITY_OPTION_DOMAINS,
        )
        _COMPATIBILITY_RULES_VALIDATED = True


def _compatibility_rule_dependency_edges(rules, option_keys):
    edges = {key: set() for key in option_keys}
    for rule in rules:
        base_antecedent_keys = [
            antecedent.key
            for antecedent in rule.antecedents
            if isinstance(antecedent, (_CompatibilityAssignment, _CompatibilityPredicate))
            and antecedent.key in option_keys
        ]
        for consequent in rule.consequents:
            antecedent_keys = list(base_antecedent_keys)
            consequent_keys = []
            if isinstance(consequent, _CompatibilityAssignment):
                if consequent.key in option_keys:
                    consequent_keys.append(consequent.key)
            elif isinstance(consequent, _CompatibilityRangeConstraint):
                if consequent.key in option_keys:
                    consequent_keys.append(consequent.key)
                if consequent.other_key in option_keys:
                    antecedent_keys.append(consequent.other_key)
            for src in antecedent_keys:
                for dst in consequent_keys:
                    if src != dst:
                        edges[src].add(dst)
    return edges


def _compatibility_rule_set_option_order(option_domains, rng):
    option_keys = set(option_domains)
    edges = _compatibility_rule_dependency_edges(_COMPATIBILITY_RULES, option_keys)
    index_by_key = {}
    lowlink_by_key = {}
    stack = []
    on_stack = set()
    components = []

    def strongconnect(key):
        index_by_key[key] = len(index_by_key)
        lowlink_by_key[key] = index_by_key[key]
        stack.append(key)
        on_stack.add(key)

        for dst in edges[key]:
            if dst not in index_by_key:
                strongconnect(dst)
                lowlink_by_key[key] = min(lowlink_by_key[key], lowlink_by_key[dst])
            elif dst in on_stack:
                lowlink_by_key[key] = min(lowlink_by_key[key], index_by_key[dst])

        if lowlink_by_key[key] == index_by_key[key]:
            component = []
            while True:
                member = stack.pop()
                on_stack.remove(member)
                component.append(member)
                if member == key:
                    break
            components.append(component)

    for key in option_domains:
        if key not in index_by_key:
            strongconnect(key)

    component_index = {}
    for idx, component in enumerate(components):
        for key in component:
            component_index[key] = idx

    component_edges = {idx: set() for idx in range(len(components))}
    component_indegree = {idx: 0 for idx in range(len(components))}
    for src in option_domains:
        src_component = component_index[src]
        for dst in edges[src]:
            dst_component = component_index[dst]
            if (
                src_component != dst_component
                and dst_component not in component_edges[src_component]
            ):
                component_edges[src_component].add(dst_component)
                component_indegree[dst_component] += 1

    ready = [idx for idx in component_indegree if component_indegree[idx] == 0]
    rng.shuffle(ready)
    ordered = []
    while ready:
        idx = ready.pop()
        component = list(components[idx])
        rng.shuffle(component)
        ordered.extend(component)
        next_ready = []
        for dst in component_edges[idx]:
            component_indegree[dst] -= 1
            if component_indegree[dst] == 0:
                next_ready.append(dst)
        rng.shuffle(next_ready)
        ready.extend(next_ready)
    return ordered


def _solve_compatibility_rule_set(
    pinned_assignments,
    option_domains=None,
    option_order=None,
    rng=None,
    shuffle_candidates=True,
    known_keys=None,
):
    _validate_compatibility_rules()
    if option_domains is None:
        option_domains = _COMPATIBILITY_OPTION_DOMAINS
    if rng is None:
        rng = random
    if option_order is None:
        option_order = _compatibility_rule_set_option_order(option_domains, rng)
    return _solve_compatibility_options(
        pinned_assignments,
        option_domains,
        _COMPATIBILITY_RULES,
        option_order=option_order,
        rng=rng,
        shuffle_candidates=shuffle_candidates,
        known_keys=known_keys,
    )


def _compatibility_domain_with_preferred_value_from(domain, preferred_value):
    return [preferred_value] + [
        candidate for candidate in domain if candidate != preferred_value
    ]


def _add_compatibility_domain_candidate(option_domains, key, value):
    if key not in option_domains:
        return
    if value not in option_domains[key]:
        option_domains[key].append(value)


def _insert_compatibility_domain_candidate(option_domains, key, value):
    if key not in option_domains:
        return
    if option_domains[key] and option_domains[key][0] == value:
        return
    if value in option_domains[key]:
        option_domains[key].remove(value)
    option_domains[key].insert(1, value)


def _compatibility_option_domains_for_params(params):
    option_domains = {}
    for key, domain in _COMPATIBILITY_OPTION_DOMAINS.items():
        if key in params:
            option_domains[key] = _compatibility_domain_with_preferred_value_from(
                list(domain), params[key]
            )
        else:
            option_domains[key] = list(domain)

    # Range constraints can depend on sampled numeric values outside the small
    # static regression domains. Add those sampled bounds as fallback candidates
    # so production can preserve or satisfy the actual generated value.
    for rule in _COMPATIBILITY_RULES:
        for consequent in rule.consequents:
            if not isinstance(consequent, _CompatibilityRangeConstraint):
                continue
            if consequent.other_key is not None:
                _add_compatibility_domain_candidate(
                    option_domains,
                    consequent.key,
                    params.get(consequent.other_key),
                )
                _insert_compatibility_domain_candidate(
                    option_domains,
                    consequent.other_key,
                    params.get(consequent.key),
                )
            else:
                _add_compatibility_domain_candidate(
                    option_domains,
                    consequent.key,
                    consequent.value,
                )
    return option_domains


def _compatibility_rule_option_keys(rule):
    keys = []
    for antecedent in rule.antecedents:
        if isinstance(antecedent, (_CompatibilityAssignment, _CompatibilityPredicate)):
            keys.append(antecedent.key)
    for consequent in rule.consequents:
        if isinstance(consequent, _CompatibilityAssignment):
            keys.append(consequent.key)
        elif isinstance(consequent, _CompatibilityRangeConstraint):
            keys.append(consequent.key)
            if consequent.other_key is not None:
                keys.append(consequent.other_key)
    return keys


def _compatibility_priority_option_order(
    option_domains,
    params,
    high_priority_keys,
    rng,
):
    priority_keys = set()

    def add_priority_key(key):
        if key in option_domains:
            priority_keys.add(key)

    for key in high_priority_keys:
        add_priority_key(key)

    for rule in _COMPATIBILITY_RULES:
        rule_keys = _compatibility_rule_option_keys(rule)
        if not any(key in high_priority_keys for key in rule_keys):
            continue
        if not all(
            _antecedent_satisfied(antecedent, params)
            for antecedent in rule.antecedents
        ):
            continue
        for key in rule_keys:
            add_priority_key(key)

    base_order = _compatibility_rule_set_option_order(option_domains, rng)
    return [key for key in base_order if key in priority_keys] + [
        key for key in base_order if key not in priority_keys
    ]


def _strip_derived_compatibility_keys(params):
    for key in list(params):
        if _is_derived_compatibility_key(key):
            del params[key]


def _compatibility_satisfied_assignment_consequents(assignments, original_keys):
    keys = set()
    for rule in _COMPATIBILITY_RULES:
        if not any(
            key in original_keys and not _is_derived_compatibility_key(key)
            for key in _compatibility_rule_option_keys(rule)
        ):
            continue
        if not all(
            _antecedent_satisfied(antecedent, assignments)
            for antecedent in rule.antecedents
        ):
            continue
        for consequent in rule.consequents:
            if isinstance(consequent, _CompatibilityAssignment):
                keys.add(consequent.key)
    return keys


def _run_compatibility_solver(dest_params, explicit_keys, special_keys):
    original_keys = set(dest_params)
    high_priority_keys = (
        set(explicit_keys) | set(special_keys) | _COMPATIBILITY_ALWAYS_PINNED_KEYS
    )
    pinned_assignments = {
        key: dest_params[key] for key in high_priority_keys if key in dest_params
    }
    high_priority_keys.update(
        _compute_compatibility_closure(
            pinned_assignments,
            _COMPATIBILITY_RULES,
            known_keys=original_keys,
        ).assignments
    )
    option_domains = _compatibility_option_domains_for_params(dest_params)
    option_order = _compatibility_priority_option_order(
        option_domains,
        dest_params,
        high_priority_keys,
        random,
    )
    solved = _solve_compatibility_rule_set(
        pinned_assignments,
        option_domains,
        option_order=option_order,
        rng=random,
        shuffle_candidates=False,
        known_keys=original_keys,
    )

    required_output_keys = _compatibility_satisfied_assignment_consequents(
        solved.assignments, original_keys
    )
    for key, value in solved.assignments.items():
        provenance = solved.provenance.get(key, "")
        if (
            key in original_keys
            or _is_derived_compatibility_key(key)
            or key in required_output_keys
            or provenance.startswith("rule ")
        ):
            dest_params[key] = value
    if all(key in dest_params for key in _OPERATION_PERCENT_KEYS):
        dest_params.update(_normalize_operation_mix(dest_params))
    _strip_derived_compatibility_keys(dest_params)


CompatibilitySolverConflict = _CompatibilitySolverConflict


def run_compatibility_solver(dest_params, explicit_keys, special_keys):
    return _run_compatibility_solver(dest_params, explicit_keys, special_keys)
