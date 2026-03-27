#!/usr/bin/env python3
"""Regression tests for finalize_and_sanitize() convergence and correctness.

Run: python3 tools/test_db_crashtest.py
  or python3 -m pytest tools/test_db_crashtest.py -v  (if pytest is available)

Tests:
  1. Convergence: random configs always reach a fixed point (no oscillation).
  2. Idempotency: running sanitize twice gives the same result.
  3. Known conflict scenarios: specific configs that previously caused issues.
"""

import copy
import os
import random
import sys
import unittest

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


class TestConvergence(unittest.TestCase):
    """finalize_and_sanitize must always reach a fixed point."""

    NUM_TRIALS = 5000

    def test_convergence_random(self):
        """Random configs converge within the iteration limit."""
        import warnings
        for trial in range(self.NUM_TRIALS):
            params = _build_fuzzed_params(seed=trial)
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                db_crashtest.finalize_and_sanitize(params)
                convergence_warnings = [
                    x for x in w
                    if "did not converge" in str(x.message)
                ]
                self.assertEqual(
                    len(convergence_warnings), 0,
                    f"Trial {trial} did not converge. Seed={trial}. "
                    f"Relevant params: {_extract_relevant(params)}"
                )


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
        """Assert params converge and return the result."""
        import warnings
        params.setdefault("db", "/tmp/test_db")
        params.setdefault("column_families", 1)
        params.setdefault("reopen", 0)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = db_crashtest.finalize_and_sanitize(params)
            convergence_warnings = [
                x for x in w if "did not converge" in str(x.message)
            ]
            self.assertEqual(
                len(convergence_warnings), 0,
                f"{desc}: did not converge"
            )
        return result

    def test_ber_plus_udt_memtable_only(self):
        """BER + UDT memtable-only: BER needs WAL off, UDT wants WAL on."""
        params = _resolve_params(db_crashtest.default_params)
        params.update(_resolve_params(db_crashtest.blackbox_default_params))
        params.update({
            "best_efforts_recovery": 1,
            "user_timestamp_size": 8,
            "persist_user_defined_timestamps": 0,
            "test_best_efforts_recovery": 1,
        })
        result = self._assert_converges(params, "BER + UDT memtable-only")
        # BER should win: WAL must be disabled
        self.assertEqual(result["disable_wal"], 1)

    def test_ber_plus_txn_non_wc_both_random(self):
        """BER + non-WC txn both random: one must lose (conflict resolved)."""
        params = _resolve_params(db_crashtest.default_params)
        params.update(_resolve_params(db_crashtest.blackbox_default_params))
        params.update(_resolve_params(db_crashtest.txn_params))
        params.update({
            "best_efforts_recovery": 1,
            "use_txn": 1,
            "txn_write_policy": 1,
        })
        result = self._assert_converges(params, "BER + non-WC txn (both random)")
        # Either BER loses (best_efforts_recovery=0)
        #     or txn loses (txn_write_policy=0)
        ber_off = result.get("best_efforts_recovery") == 0
        txn_wc = result.get("txn_write_policy") == 0
        self.assertTrue(
            ber_off or txn_wc,
            f"Conflict not resolved: ber={result.get('best_efforts_recovery')}, "
            f"txn_write_policy={result.get('txn_write_policy')}"
        )

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
        """inplace_update + unordered_write both random: one must lose."""
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
        })
        result = self._assert_converges(params, "inplace + unordered (both random)")
        # Both random: either inplace loses (inplace=0, concurrent=1)
        #           or unordered loses (unordered=0, concurrent=0)
        # Either way: the conflict must be resolved (no inconsistent state)
        inplace_off = result.get("inplace_update_support") == 0
        unordered_off = result.get("unordered_write") == 0
        self.assertTrue(
            inplace_off or unordered_off,
            f"Conflict not resolved: inplace={result.get('inplace_update_support')}, "
            f"unordered={result.get('unordered_write')}"
        )

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
        })
        # unordered is explicit, inplace is random → unordered wins
        params.setdefault("db", "/tmp/test_db")
        result = db_crashtest.finalize_and_sanitize(
            params, explicit_keys={"unordered_write"}
        )
        self.assertEqual(result["unordered_write"], 1)
        self.assertEqual(result["inplace_update_support"], 0)
        self.assertEqual(result["allow_concurrent_memtable_write"], 1)

    def test_udt_memtable_only_plus_unordered_write_both_random(self):
        """UDT memtable-only + unordered_write both random: one must lose."""
        params = _resolve_params(db_crashtest.default_params)
        params.update(_resolve_params(db_crashtest.blackbox_default_params))
        params.update({
            "user_timestamp_size": 8,
            "persist_user_defined_timestamps": 0,
            "unordered_write": 1,
            "txn_write_policy": 1,
            "use_txn": 1,
        })
        result = self._assert_converges(params, "UDT + unordered (both random)")
        # Either UDT loses (persist=1) or unordered loses (unordered=0)
        udt_off = result.get("persist_user_defined_timestamps") == 1
        unordered_off = result.get("unordered_write") == 0
        self.assertTrue(
            udt_off or unordered_off,
            f"Conflict not resolved: persist={result.get('persist_user_defined_timestamps')}, "
            f"unordered={result.get('unordered_write')}"
        )

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


def _extract_relevant(params):
    """Extract the most relevant params for debugging."""
    keys = [
        "disable_wal", "best_efforts_recovery", "atomic_flush",
        "inplace_update_support", "unordered_write", "use_txn",
        "txn_write_policy", "user_timestamp_size",
        "persist_user_defined_timestamps", "allow_concurrent_memtable_write",
        "test_best_efforts_recovery",
    ]
    return {k: params.get(k) for k in keys}


if __name__ == "__main__":
    unittest.main(verbosity=2)
