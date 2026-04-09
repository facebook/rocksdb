#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).
"""Regression tests for finalize_and_sanitize() convergence and correctness.

Run: python3 tools/test_db_crashtest.py
  or python3 -m pytest tools/test_db_crashtest.py -v  (if pytest is available)

Tests:
  1. Convergence: random configs always reach a fixed point (no oscillation).
  2. Idempotency: running sanitize twice gives the same result.
  3. Known conflict scenarios: specific configs that previously caused issues.
"""

import copy
import io
import os
import random
import sys
import unittest
from unittest import mock

# Import db_crashtest from same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db_crashtest


def _resolve_params(raw):
    """Resolve lambdas in a params dict."""
    return {k: v() if callable(v) else v for k, v in raw.items()}


def _build_base_params(seed):
    """Build a random but valid base config from default + feature params."""
    random.seed(seed)
    params = _resolve_params(db_crashtest.default_params)
    box = random.choice([
        db_crashtest.blackbox_default_params,
        db_crashtest.whitebox_default_params,
    ])
    params.update(_resolve_params(box))

    feature_sets = [
        db_crashtest.txn_params,
        db_crashtest.optimistic_txn_params,
        db_crashtest.best_efforts_recovery_params,
        db_crashtest.ts_params,
        db_crashtest.blob_params,
    ]
    for attr in ["tiered_params", "multiops_txn_params", "cf_consistency_params"]:
        if hasattr(db_crashtest, attr):
            feature_sets.append(getattr(db_crashtest, attr))

    # Layer 0-2 random feature sets
    for fp in random.sample(feature_sets, min(random.randint(0, 2), len(feature_sets))):
        params.update(_resolve_params(fp))

    params.setdefault("db", "/tmp/test_db")
    params.setdefault("column_families", random.choice([1, 4]))
    params.setdefault("reopen", random.choice([0, 20]))
    return params


# Flags that stress tests commonly override via --extra_flags
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
}


def _build_fuzzed_params(seed):
    """Build base params + random overrides simulating extra-flags."""
    params = _build_base_params(seed)
    random.seed(seed + 500000)
    n = min(random.randint(3, 10), len(_OVERRIDE_CANDIDATES))
    for k in random.sample(list(_OVERRIDE_CANDIDATES.keys()), n):
        params[k] = random.choice(_OVERRIDE_CANDIDATES[k])
    return params


def _new_blackbox_params():
    """Build a minimal blackbox config for targeted sanitize tests."""
    params = _resolve_params(db_crashtest.default_params)
    params.update(_resolve_params(db_crashtest.blackbox_default_params))
    params.setdefault("db", "/tmp/test_db")
    params.setdefault("column_families", 1)
    params.setdefault("reopen", 0)
    return params


class TestConvergence(unittest.TestCase):
    """finalize_and_sanitize must always reach a fixed point."""

    NUM_TRIALS = 5000

    def test_convergence_random(self):
        """Random configs converge within the iteration limit.

        Non-convergence is now a hard sys.exit(1), so if
        finalize_and_sanitize returns at all, it converged.
        """
        for trial in range(self.NUM_TRIALS):
            params = _build_fuzzed_params(seed=trial)
            db_crashtest.finalize_and_sanitize(params)


class TestIdempotency(unittest.TestCase):
    """Running sanitize on already-sanitized params should be a no-op."""

    NUM_TRIALS = 2000

    def test_idempotency_random(self):
        """sanitize(sanitize(p)) == sanitize(p) for random configs."""
        for trial in range(self.NUM_TRIALS):
            params = _build_fuzzed_params(seed=trial + 100000)
            # First pass
            random.seed(trial + 999999)
            result1 = db_crashtest.finalize_and_sanitize(params)
            # Second pass (on already-sanitized output)
            random.seed(trial + 999999)
            result2 = db_crashtest.finalize_and_sanitize(copy.deepcopy(result1))

            # Compare all keys (ignore special rules that use random.choice
            # since they may differ between runs)
            skip_keys = {"compression_type", "bottommost_compression_type", "cache_type", "compressed_secondary_cache_ratio"}
            for k in set(result1.keys()) | set(result2.keys()):
                if k in skip_keys:
                    continue
                self.assertEqual(
                    result1.get(k), result2.get(k),
                    f"Trial {trial}: key '{k}' not idempotent. "
                    f"Pass1={result1.get(k)}, Pass2={result2.get(k)}"
                )


class TestKnownConflicts(unittest.TestCase):
    """Specific configs that previously caused oscillation."""

    def _assert_converges(self, params, desc):
        """Assert params converge and return the result.

        Non-convergence is now a hard sys.exit(1), so if
        finalize_and_sanitize returns at all, it converged.
        """
        params.setdefault("db", "/tmp/test_db")
        params.setdefault("column_families", 1)
        params.setdefault("reopen", 0)
        result = db_crashtest.finalize_and_sanitize(params)
        return result

    def test_ber_plus_udt_memtable_only(self):
        """BER test keeps UDT memtable-only active with its WAL-disabled profile."""
        params = _new_blackbox_params()
        params.update({
            "best_efforts_recovery": 1,
            "user_timestamp_size": 8,
            "persist_user_defined_timestamps": 0,
            "test_best_efforts_recovery": 1,
        })
        result = self._assert_converges(params, "BER + UDT memtable-only")
        self.assertEqual(result["disable_wal"], 1)
        self.assertEqual(result["atomic_flush"], 0)
        self.assertEqual(result["persist_user_defined_timestamps"], 0)

    def test_udt_memtable_only_requires_wal_outside_ber_test(self):
        """UDT memtable-only re-enables WAL and disables iterator shapes outside BER tests."""
        params = _new_blackbox_params()
        params.update({
            "user_timestamp_size": 8,
            "persist_user_defined_timestamps": 0,
            "disable_wal": 1,
            "test_best_efforts_recovery": 0,
            "readpercent": 17,
            "iterpercent": 13,
            "prefixpercent": 0,
        })
        result = self._assert_converges(params, "UDT memtable-only outside BER")
        self.assertEqual(result["disable_wal"], 0)
        self.assertEqual(result["atomic_flush"], 0)
        self.assertEqual(result["iterpercent"], 0)
        self.assertEqual(result["readpercent"], 30)

    def test_ber_plus_txn_non_wc_both_random(self):
        """BER + non-WC txn both random: deterministic tiebreak disables BER."""
        params = _new_blackbox_params()
        params.update(_resolve_params(db_crashtest.txn_params))
        params.update({
            "best_efforts_recovery": 1,
            "use_txn": 1,
            "txn_write_policy": 1,
        })
        result = self._assert_converges(params, "BER + non-WC txn (both random)")
        self.assertEqual(result["best_efforts_recovery"], 0)
        self.assertEqual(result["txn_write_policy"], 1)
        self.assertEqual(result["disable_wal"], 0)

    def test_ber_explicit_beats_txn_non_wc(self):
        """BER explicit: non-WC txn must switch to write-committed."""
        params = _resolve_params(db_crashtest.default_params)
        params.update(_resolve_params(db_crashtest.blackbox_default_params))
        params.update(_resolve_params(db_crashtest.txn_params))
        params.update({
            "best_efforts_recovery": 1,
            "use_txn": 1,
            "txn_write_policy": 1,
        })
        params.setdefault("db", "/tmp/test_db")
        result = db_crashtest.finalize_and_sanitize(
            params, explicit_keys={"best_efforts_recovery"}
        )
        self.assertEqual(result["best_efforts_recovery"], 1)
        self.assertEqual(result["disable_wal"], 1)
        # txn_write_policy should be switched to write-committed (0)
        self.assertEqual(result.get("txn_write_policy"), 0)

    def test_disable_wal_explicit_beats_random_ber(self):
        """Explicit disable_wal=0 disables random BER instead of being rewritten."""
        params = _resolve_params(db_crashtest.default_params)
        params.update(_resolve_params(db_crashtest.blackbox_default_params))
        params.update({
            "best_efforts_recovery": 1,
            "disable_wal": 0,
        })
        params.setdefault("db", "/tmp/test_db")
        result = db_crashtest.finalize_and_sanitize(
            params, explicit_keys={"disable_wal"}
        )
        self.assertEqual(result["disable_wal"], 0)
        self.assertEqual(result["best_efforts_recovery"], 0)

    def test_txn_non_wc_explicit_beats_ber(self):
        """non-WC txn explicit: BER must be disabled."""
        params = _resolve_params(db_crashtest.default_params)
        params.update(_resolve_params(db_crashtest.blackbox_default_params))
        params.update(_resolve_params(db_crashtest.txn_params))
        params.update({
            "best_efforts_recovery": 1,
            "use_txn": 1,
            "txn_write_policy": 1,
        })
        params.setdefault("db", "/tmp/test_db")
        result = db_crashtest.finalize_and_sanitize(
            params, explicit_keys={"txn_write_policy"}
        )
        self.assertEqual(result["txn_write_policy"], 1)
        self.assertEqual(result["best_efforts_recovery"], 0)
        self.assertEqual(result["disable_wal"], 0)

    def test_inplace_plus_unordered_write_both_random(self):
        """inplace + unordered both random: deterministic tiebreak disables unordered."""
        params = _new_blackbox_params()
        params.update({
            "inplace_update_support": 1,
            "unordered_write": 1,
            "txn_write_policy": 1,
            "use_txn": 1,
            "test_batches_snapshots": 0,
            "best_efforts_recovery": 0,
            "memtablerep": "skip_list",
            "remote_compaction_worker_threads": 0,
        })
        result = self._assert_converges(params, "inplace + unordered (both random)")
        self.assertEqual(result["inplace_update_support"], 1)
        self.assertEqual(result["unordered_write"], 0)
        self.assertEqual(result["allow_concurrent_memtable_write"], 0)

    def test_inplace_explicit_beats_unordered_random(self):
        """inplace explicit via extra-flags: unordered_write must be disabled."""
        params = _resolve_params(db_crashtest.default_params)
        params.update(_resolve_params(db_crashtest.blackbox_default_params))
        params.update({
            "inplace_update_support": 1,
            "unordered_write": 1,
            "txn_write_policy": 1,
            "use_txn": 1,
            "test_batches_snapshots": 0,
            "best_efforts_recovery": 0,
            "memtablerep": "skip_list",
            "remote_compaction_worker_threads": 0,
        })
        # inplace is explicit, unordered is random → inplace wins
        params.setdefault("db", "/tmp/test_db")
        result = db_crashtest.finalize_and_sanitize(
            params, explicit_keys={"inplace_update_support"}
        )
        self.assertEqual(result["inplace_update_support"], 1)
        self.assertEqual(result["unordered_write"], 0)
        self.assertEqual(result["allow_concurrent_memtable_write"], 0)

    def test_unordered_explicit_beats_inplace_random(self):
        """unordered_write explicit via extra-flags: inplace must be disabled."""
        params = _resolve_params(db_crashtest.default_params)
        params.update(_resolve_params(db_crashtest.blackbox_default_params))
        params.update({
            "inplace_update_support": 1,
            "unordered_write": 1,
            "txn_write_policy": 1,
            "use_txn": 1,
            "test_batches_snapshots": 0,
            "best_efforts_recovery": 0,
            "memtablerep": "skip_list",
            "remote_compaction_worker_threads": 0,
        })
        # unordered is explicit, inplace is random → unordered wins
        params.setdefault("db", "/tmp/test_db")
        result = db_crashtest.finalize_and_sanitize(
            params, explicit_keys={"unordered_write"}
        )
        self.assertEqual(result["unordered_write"], 1)
        self.assertEqual(result["inplace_update_support"], 0)
        self.assertEqual(result["allow_concurrent_memtable_write"], 1)

    def test_unordered_write_requires_write_prepared(self):
        """unordered_write is disabled instead of rewriting txn_write_policy."""
        params = _new_blackbox_params()
        params.update({
            "unordered_write": 1,
            "use_txn": 1,
            "txn_write_policy": 0,
            "allow_concurrent_memtable_write": 0,
        })
        result = self._assert_converges(params, "unordered_write requires WP")
        self.assertEqual(result["unordered_write"], 0)
        self.assertEqual(result["txn_write_policy"], 0)
        self.assertEqual(result["allow_concurrent_memtable_write"], 0)

    def test_udt_memtable_only_plus_unordered_write_both_random(self):
        """UDT + unordered both random: deterministic tiebreak disables unordered."""
        params = _new_blackbox_params()
        params.update({
            "user_timestamp_size": 8,
            "persist_user_defined_timestamps": 0,
            "unordered_write": 1,
            "txn_write_policy": 1,
            "use_txn": 1,
        })
        result = self._assert_converges(params, "UDT + unordered (both random)")
        self.assertEqual(result["persist_user_defined_timestamps"], 0)
        self.assertEqual(result["unordered_write"], 0)
        self.assertEqual(result["allow_concurrent_memtable_write"], 0)

    def test_udt_explicit_beats_unordered_random(self):
        """UDT memtable-only explicit: unordered_write must be disabled."""
        params = _resolve_params(db_crashtest.default_params)
        params.update(_resolve_params(db_crashtest.blackbox_default_params))
        params.update({
            "user_timestamp_size": 8,
            "persist_user_defined_timestamps": 0,
            "unordered_write": 1,
            "txn_write_policy": 1,
            "use_txn": 1,
        })
        params.setdefault("db", "/tmp/test_db")
        result = db_crashtest.finalize_and_sanitize(
            params,
            explicit_keys={"user_timestamp_size", "persist_user_defined_timestamps"}
        )
        self.assertEqual(result["persist_user_defined_timestamps"], 0)  # UDT active
        self.assertEqual(result["unordered_write"], 0)
        self.assertEqual(result["allow_concurrent_memtable_write"], 0)

    def test_unordered_explicit_beats_udt_random(self):
        """unordered_write explicit: UDT memtable-only must be disabled."""
        params = _resolve_params(db_crashtest.default_params)
        params.update(_resolve_params(db_crashtest.blackbox_default_params))
        params.update({
            "user_timestamp_size": 8,
            "persist_user_defined_timestamps": 0,
            "unordered_write": 1,
            "txn_write_policy": 1,
            "use_txn": 1,
        })
        params.setdefault("db", "/tmp/test_db")
        result = db_crashtest.finalize_and_sanitize(
            params, explicit_keys={"unordered_write"}
        )
        self.assertEqual(result["unordered_write"], 1)
        # UDT disabled: persist_user_defined_timestamps set back to 1
        self.assertEqual(result.get("persist_user_defined_timestamps"), 1)
        self.assertEqual(result["allow_concurrent_memtable_write"], 1)

    def test_disable_wal_chain(self):
        """BER → disable_wal → atomic_flush, reopen=0, pipelined=0."""
        params = _resolve_params(db_crashtest.default_params)
        params.update(_resolve_params(db_crashtest.blackbox_default_params))
        params.update({
            "best_efforts_recovery": 1,
            "enable_pipelined_write": 1,
            "reopen": 20,
        })
        result = self._assert_converges(params, "BER chain")
        self.assertEqual(result["disable_wal"], 1)
        self.assertEqual(result["atomic_flush"], 1)
        self.assertEqual(result["reopen"], 0)
        self.assertEqual(result["enable_pipelined_write"], 0)

    def test_conflicting_explicit_flags_exits(self):
        """Two conflicting explicit flags should cause exit(1)."""
        params = _resolve_params(db_crashtest.default_params)
        params.update(_resolve_params(db_crashtest.blackbox_default_params))
        params.update({
            "best_efforts_recovery": 1,
            "inplace_update_support": 1,
        })
        params.setdefault("db", "/tmp/test_db")
        with self.assertRaises(SystemExit) as cm:
            db_crashtest.finalize_and_sanitize(
                params,
                explicit_keys={"best_efforts_recovery", "inplace_update_support"}
            )
        self.assertEqual(cm.exception.code, 1)

    def test_extra_flags_override(self):
        """Forced flags via extra-flags participate in sanitization."""
        params = _resolve_params(db_crashtest.default_params)
        params.update(_resolve_params(db_crashtest.blackbox_default_params))
        # Simulate extra-flags forcing blob + txn (incompatible)
        params.update({
            "enable_blob_files": 1,
            "use_txn": 1,
            "commit_bypass_memtable_one_in": 100,
        })
        result = self._assert_converges(params, "extra-flags blob+txn")
        # commit_bypass_memtable should disable blob
        self.assertEqual(result["enable_blob_files"], 0)

    def test_multiscan_shape_adjustments(self):
        """Multiscan keeps its prefix/delete normalization and bounded prefetch."""
        random.seed(0)
        params = _new_blackbox_params()
        params.update({
            "use_multiscan": 1,
            "test_batches_snapshots": 0,
            "use_txn": 0,
            "user_timestamp_size": 0,
            "enable_compaction_filter": 0,
            "inplace_update_support": 0,
            "delpercent": 3,
            "delrangepercent": 7,
            "iterpercent": 13,
            "prefixpercent": 11,
            "prefix_size": 4,
            "skip_stats_update_on_db_open": 1,
            "open_files_async": 1,
        })
        result = self._assert_converges(params, "multiscan adjustments")
        self.assertEqual(result["use_multiscan"], 1)
        self.assertEqual(result["prefix_size"], -1)
        self.assertEqual(result["delpercent"], 10)
        self.assertEqual(result["delrangepercent"], 0)
        self.assertEqual(result["iterpercent"], 24)
        self.assertEqual(result["prefixpercent"], 0)
        self.assertEqual(result["skip_stats_update_on_db_open"], 0)
        self.assertEqual(result["open_files_async"], 0)
        self.assertIn(
            result["multiscan_max_prefetch_memory_bytes"],
            {0, 64 * 1024, 256 * 1024},
        )

    def test_optimistic_txn_bumps_write_buffer_maintain(self):
        """use_optimistic_txn=1 bumps max_write_buffer_size_to_maintain up."""
        params = _resolve_params(db_crashtest.default_params)
        params.update(_resolve_params(db_crashtest.blackbox_default_params))
        params.update({
            "use_optimistic_txn": 1,
            "use_txn": 1,
            "write_buffer_size": 4 * 1024 * 1024,
            "max_write_buffer_size_to_maintain": 1024 * 1024,
        })
        result = self._assert_converges(
            params, "optimistic_txn: maintain < write_buffer"
        )
        self.assertGreaterEqual(
            result["max_write_buffer_size_to_maintain"],
            result["write_buffer_size"],
        )

    def test_optimistic_txn_already_valid_unchanged(self):
        """use_optimistic_txn=1 with valid maintain value is unchanged."""
        params = _resolve_params(db_crashtest.default_params)
        params.update(_resolve_params(db_crashtest.blackbox_default_params))
        params.update({
            "use_optimistic_txn": 1,
            "use_txn": 1,
            "write_buffer_size": 1024 * 1024,
            "max_write_buffer_size_to_maintain": 8 * 1024 * 1024,
        })
        result = self._assert_converges(
            params, "optimistic_txn: maintain already valid"
        )
        self.assertEqual(
            result["max_write_buffer_size_to_maintain"], 8 * 1024 * 1024,
        )


class TestGenCmd(unittest.TestCase):
    """Passthrough db_stress flags should take precedence and fail clearly."""

    def test_parse_extra_flag_value_normalizes_boolean_strings(self):
        self.assertEqual(db_crashtest._parse_extra_flag_value("true"), 1)
        self.assertEqual(db_crashtest._parse_extra_flag_value("FALSE"), 0)
        self.assertEqual(db_crashtest._parse_extra_flag_value("on"), 1)
        self.assertEqual(db_crashtest._parse_extra_flag_value(""), "")

    def test_gen_cmd_parses_boolean_string_passthrough_flags(self):
        random.seed(0)
        params = _new_blackbox_params()
        params.update({
            "skip_stats_update_on_db_open": 1,
            "open_files_async": 0,
            "enable_blob_files": 1,
        })

        with mock.patch.object(db_crashtest, "prepare_expected_values_dir"):
            cmd = db_crashtest.gen_cmd(
                params,
                ["--open_files_async=true", "--enable_blob_files=false"],
            )

        self.assertIn("--open_files_async=1", cmd)
        self.assertIn("--enable_blob_files=0", cmd)

    def test_gen_cmd_preserves_explicit_open_files_async(self):
        random.seed(0)
        params = _new_blackbox_params()
        params.update({
            "skip_stats_update_on_db_open": 0,
            "open_files_async": 0,
        })
        stderr = io.StringIO()

        with mock.patch.object(
            db_crashtest, "prepare_expected_values_dir"
        ), mock.patch("sys.stderr", new=stderr):
            cmd = db_crashtest.gen_cmd(params, ["--open_files_async=true"])

        self.assertIn("--open_files_async=1", cmd)
        self.assertIn("--skip_stats_update_on_db_open=1", cmd)
        self.assertNotIn("did not win after sanitization", stderr.getvalue())

    def test_gen_cmd_preserves_explicit_reopen(self):
        random.seed(0)
        params = _new_blackbox_params()
        params.update({
            "best_efforts_recovery": 1,
            "disable_wal": 1,
            "reopen": 0,
        })

        with mock.patch.object(db_crashtest, "prepare_expected_values_dir"):
            cmd = db_crashtest.gen_cmd(params, ["--reopen=20"])

        self.assertIn("--reopen=20", cmd)
        self.assertIn("--disable_wal=0", cmd)
        self.assertIn("--best_efforts_recovery=0", cmd)

    def test_gen_cmd_preserves_explicit_write_buffer_maintain(self):
        random.seed(0)
        params = _new_blackbox_params()
        params.update({
            "use_optimistic_txn": 1,
            "use_txn": 1,
            "write_buffer_size": 4 * 1024 * 1024,
            "max_write_buffer_size_to_maintain": 4 * 1024 * 1024,
        })

        with mock.patch.object(db_crashtest, "prepare_expected_values_dir"):
            cmd = db_crashtest.gen_cmd(
                params, ["--max_write_buffer_size_to_maintain=1048576"]
            )

        self.assertIn("--max_write_buffer_size_to_maintain=1048576", cmd)
        self.assertIn("--write_buffer_size=1048576", cmd)

    def test_gen_cmd_ignores_dbcrashtest_only_passthrough_flags(self):
        random.seed(0)
        params = _new_blackbox_params()
        stderr = io.StringIO()

        with mock.patch.object(
            db_crashtest, "prepare_expected_values_dir"
        ), mock.patch("sys.stderr", new=stderr):
            cmd = db_crashtest.gen_cmd(params, ["--duration=999"])

        self.assertFalse(any(arg.startswith("--duration=") for arg in cmd))
        self.assertIn("ignoring passthrough override", stderr.getvalue())
        self.assertIn("--duration=999", stderr.getvalue())

    def test_gen_cmd_exits_on_conflicting_explicit_flags(self):
        random.seed(0)
        params = _new_blackbox_params()
        params.update({
            "best_efforts_recovery": 0,
            "inplace_update_support": 0,
        })

        with mock.patch.object(db_crashtest, "prepare_expected_values_dir"):
            with self.assertRaises(SystemExit) as cm:
                db_crashtest.gen_cmd(
                    params,
                    ["--best_efforts_recovery=1", "--inplace_update_support=1"],
                )

        self.assertEqual(cm.exception.code, 1)


class TestSpecialRules(unittest.TestCase):
    """Special rules must still hold in the final sanitized config."""

    def test_release_mode_disables_unsupported_direct_io(self):
        """Unsupported direct IO stays disabled after the full sanitize loop."""
        params = _new_blackbox_params()
        params.update({
            "use_direct_reads": 1,
            "use_direct_io_for_flush_and_compaction": 1,
            "mock_direct_io": False,
        })

        with mock.patch.object(
            db_crashtest, "is_release_mode", return_value=True
        ), mock.patch.object(
            db_crashtest, "is_direct_io_supported", return_value=False
        ):
            result = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(result["use_direct_reads"], 0)
        self.assertEqual(result["use_direct_io_for_flush_and_compaction"], 0)
        self.assertFalse(result["mock_direct_io"])


def _extract_relevant(params):
    """Extract the most relevant params for debugging."""
    keys = [
        "disable_wal", "best_efforts_recovery", "atomic_flush",
        "inplace_update_support", "unordered_write", "use_txn",
        "txn_write_policy", "user_timestamp_size",
        "persist_user_defined_timestamps", "allow_concurrent_memtable_write",
        "test_best_efforts_recovery", "use_optimistic_txn",
        "write_buffer_size", "max_write_buffer_size_to_maintain",
    ]
    return {k: params.get(k) for k in keys}


if __name__ == "__main__":
    unittest.main(verbosity=2)
