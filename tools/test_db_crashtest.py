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
import db_crashtest_compatibility as compatibility


def _resolve_params(raw):
    """Resolve lambdas in a params dict."""
    return {k: v() if callable(v) else v for k, v in raw.items()}


def _ensure_test_db(params):
    """Install a non-empty db path for direct finalize_and_sanitize() tests."""
    if not params.get("db"):
        params["db"] = "/tmp/test_db"


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

    _ensure_test_db(params)
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
    _ensure_test_db(params)
    params.setdefault("column_families", 1)
    params.setdefault("reopen", 0)
    return params


class _NoShuffle:
    def shuffle(self, values):
        return None


class TestCompatibilityDependencySolver(unittest.TestCase):
    """Exercise the dependency-solver primitives directly."""

    def _a(self, key, value):
        return compatibility._CompatibilityAssignment(key, value)

    def _p(self, key, op, value, default=compatibility._MISSING_PREDICATE_DEFAULT):
        return compatibility._CompatibilityPredicate(key, op, value, default)

    def _rule(self, name, antecedents, consequents):
        return compatibility._CompatibilityRule(
            name=name,
            antecedents=tuple(antecedents),
            consequents=tuple(consequents),
        )

    def _preferred_domains(self, params, keys):
        return {
            key: compatibility._compatibility_domain_with_preferred_value_from(
                list(compatibility._COMPATIBILITY_OPTION_DOMAINS[key]), params[key]
            )
            for key in keys
        }

    def test_compatibility_rule_declarations_have_domains(self):
        """Every production rule key has a declared finite domain.

        This is the regression guard for future rule additions. A new rule
        cannot silently reference an option that the solver does not know how
        to randomize or validate.
        """
        compatibility._validate_compatibility_rules()

    def test_rule_set_randomized_generation_remains_conflict_free(self):
        """The full production rule set solves repeatedly without conflicts.

        This is the regression hook for new compatibility rules. It runs the
        candidate solver over the declared domains for deterministic seeds,
        using the rule-dependency ordering that keeps controllers ahead of
        dependent knobs while preserving random priority inside cycles.
        """
        for seed in range(100):
            result = compatibility._solve_compatibility_rule_set(
                {},
                compatibility._COMPATIBILITY_OPTION_DOMAINS,
                rng=random.Random(seed),
            )
            compatibility._compute_compatibility_closure(
                result.assignments,
                compatibility._COMPATIBILITY_RULES,
                protected_assignments=result.assignments,
                seed_provenance=result.provenance,
            )

    def test_disable_wal_rules_produce_expected_normalization(self):
        """WAL-disabled closure produces expected consequences.

        The test starts from a WAL-disabled config with incompatible sampled
        values. The solver pins only disable_wal, then rejects or skips
        conflicting candidates via closure and produces the expected pure
        consequences.
        """
        params = _new_blackbox_params()
        params.update({
            "disable_wal": 1,
            "sync": 1,
            "write_fault_one_in": 128,
            "test_batches_snapshots": 1,
            "reopen": 20,
            "manual_wal_flush_one_in": 1000,
            "recycle_log_file_num": 0,
        })
        expected = {
            "sync": 0,
            "write_fault_one_in": 0,
            "test_batches_snapshots": 0,
            "reopen": 0,
            "manual_wal_flush_one_in": 0,
            "recycle_log_file_num": 0,
        }

        result = compatibility._solve_compatibility_rule_set(
            {"disable_wal": 1},
            self._preferred_domains(params, expected.keys()),
            option_order=list(expected),
            rng=_NoShuffle(),
        )

        for key, value in expected.items():
            self.assertEqual(result.assignments[key], value)

    def test_open_files_async_rule_produces_expected_normalization(self):
        """Async-open closure produces the expected consequence.

        The sampled config enables open_files_async while stats update on DB
        open is not skipped. The rule should force open_files_async to 0.
        """
        params = _new_blackbox_params()
        params.update({
            "skip_stats_update_on_db_open": 0,
            "open_files_async": 1,
        })

        result = compatibility._solve_compatibility_rule_set(
            {"skip_stats_update_on_db_open": 0},
            self._preferred_domains(params, ["open_files_async"]),
            option_order=["open_files_async"],
            rng=_NoShuffle(),
        )

        self.assertEqual(result.assignments["open_files_async"], 0)

    def test_atomic_flush_rule_produces_expected_normalization(self):
        """Atomic-flush closure produces the expected consequence.

        The test pins atomic_flush and lets enable_pipelined_write start at its
        sampled incompatible value. Closure should force pipelined write off in
        the declared compatibility rule set.
        """
        params = _new_blackbox_params()
        params.update({
            "atomic_flush": 1,
            "disable_wal": 0,
            "enable_pipelined_write": 1,
        })

        result = compatibility._solve_compatibility_rule_set(
            {"atomic_flush": 1},
            self._preferred_domains(params, ["enable_pipelined_write"]),
            option_order=["enable_pipelined_write"],
            rng=_NoShuffle(),
        )

        self.assertEqual(
            result.assignments["enable_pipelined_write"],
            0,
        )

    def test_commit_bypass_rule_produces_expected_normalization(self):
        """Compound commit-bypass closure produces expected normalization.

        The rule only fires when use_txn is enabled and
        commit_bypass_memtable_one_in is positive. The test pins that compound
        antecedent and verifies the closure produces the same normalized values
        for the blob/entity consequences.
        """
        params = _new_blackbox_params()
        params.update({
            "use_txn": 1,
            "commit_bypass_memtable_one_in": 100,
            "enable_blob_files": 1,
            "allow_setting_blob_options_dynamically": 1,
            "allow_concurrent_memtable_write": 1,
            "use_put_entity_one_in": 5,
            "use_get_entity": 1,
            "use_multi_get_entity": 1,
            "enable_pipelined_write": 1,
            "use_attribute_group": 1,
        })
        expected = {
            "enable_blob_files": 0,
            "allow_setting_blob_options_dynamically": 0,
            "allow_concurrent_memtable_write": 0,
            "use_put_entity_one_in": 0,
            "use_get_entity": 0,
            "use_multi_get_entity": 0,
            "enable_pipelined_write": 0,
            "use_attribute_group": 0,
        }

        result = compatibility._solve_compatibility_rule_set(
            {
                "use_txn": 1,
                "commit_bypass_memtable_one_in": 100,
            },
            self._preferred_domains(params, expected.keys()),
            option_order=list(expected),
            rng=_NoShuffle(),
        )

        for key, value in expected.items():
            self.assertEqual(result.assignments[key], value)

    def test_optimistic_txn_range_rule_rejects_too_small_candidate(self):
        """Numeric range rules reject candidates without assigning magic values.

        The sampled maintain value is too small for optimistic transactions.
        The solver rejects that candidate and accepts the next declared value.
        """
        params = _new_blackbox_params()
        params.update({
            "use_optimistic_txn": 1,
            "write_buffer_size": 4 * 1024 * 1024,
            "max_write_buffer_size_to_maintain": 1024 * 1024,
        })

        result = compatibility._solve_compatibility_rule_set(
            {
                "use_optimistic_txn": 1,
                "write_buffer_size": 4 * 1024 * 1024,
            },
            {
                "max_write_buffer_size_to_maintain": [
                    1024 * 1024,
                    4 * 1024 * 1024,
                ],
            },
            option_order=["max_write_buffer_size_to_maintain"],
            rng=_NoShuffle(),
        )

        self.assertEqual(
            result.assignments["max_write_buffer_size_to_maintain"],
            4 * 1024 * 1024,
        )

    def test_pinned_numeric_conflict_reports_real_rule(self):
        """Pinned numeric conflicts in production rules fail early.

        All three optimistic-transaction values are fixed, so the solver cannot
        try a larger maintain value. This validates fast failure for the real
        numeric rule that will catch bad explicit flag combinations.
        """
        with self.assertRaisesRegex(
            compatibility._CompatibilitySolverConflict,
            "range constraint violated",
        ):
            compatibility._solve_compatibility_rule_set(
                {
                    "use_optimistic_txn": 1,
                    "write_buffer_size": 4 * 1024 * 1024,
                    "max_write_buffer_size_to_maintain": 1024 * 1024,
                },
                {},
                rng=_NoShuffle(),
            )

    def test_pinned_conflict_reports_real_rule(self):
        """Pinned conflicts in production rules fail before randomization.

        Explicit disable_wal=1 and reopen=20 are incompatible under the
        WAL-disabled rule. The solver should report a closure conflict
        immediately instead of trying to hide it by changing reopen.
        """
        with self.assertRaisesRegex(
            compatibility._CompatibilitySolverConflict,
            "reopen",
        ):
            compatibility._solve_compatibility_rule_set(
                {"disable_wal": 1, "reopen": 20},
                {},
                rng=_NoShuffle(),
            )

    def test_candidate_rejection_keeps_earlier_implied_value(self):
        """Later candidates that imply a fixed-value conflict are rejected.

        This pins the opt_a/opt_b/opt_c example from the design discussion:
        opt_a=0 fixes opt_c=True, so opt_b=0 is rejected because it would
        imply opt_c=False. The fallback opt_b=1 is accepted.
        """
        rules = [
            self._rule("a_requires_c", [self._a("opt_a", 0)], [
                self._a("opt_c", True),
            ]),
            self._rule("b_disables_c", [self._a("opt_b", 0)], [
                self._a("opt_c", False),
            ]),
        ]
        domains = {
            "opt_a": [0],
            "opt_b": [0, 1],
            "opt_c": [False, True],
        }

        result = compatibility._solve_compatibility_options(
            {},
            domains,
            rules,
            option_order=["opt_a", "opt_b", "opt_c"],
            rng=_NoShuffle(),
        )

        self.assertEqual(result.assignments["opt_a"], 0)
        self.assertEqual(result.assignments["opt_b"], 1)
        self.assertTrue(result.assignments["opt_c"])

    def test_pinned_conflict_fails_before_random_options(self):
        """Pinned roots are validated first and report unsatisfiable closure.

        Both opt_a=0 and opt_b=0 are fixed roots, and their transitive
        requirements assign incompatible values to opt_c. There is no random
        fallback, so the solver must fail immediately.
        """
        rules = [
            self._rule("a_requires_c", [self._a("opt_a", 0)], [
                self._a("opt_c", True),
            ]),
            self._rule("b_disables_c", [self._a("opt_b", 0)], [
                self._a("opt_c", False),
            ]),
        ]

        with self.assertRaises(compatibility._CompatibilitySolverConflict):
            compatibility._solve_compatibility_options(
                {"opt_a": 0, "opt_b": 0},
                {},
                rules,
                rng=_NoShuffle(),
            )

    def test_predicate_default_matches_missing_key(self):
        """Predicate defaults model legacy get() behavior for absent options."""
        result = compatibility._compute_compatibility_closure(
            {},
            [
                self._rule(
                    "missing_default",
                    [self._p("controller", "!=", "skip_list", default=None)],
                    [self._a("dependent", 0)],
                )
            ],
        )

        self.assertEqual(result.assignments["dependent"], 0)

    def test_predicate_default_does_not_replace_unresolved_known_key(self):
        """A known but unresolved option is not the same as a missing option."""
        result = compatibility._solve_compatibility_options(
            {},
            {
                "controller": ("skip_list",),
                "dependent": (1, 0),
            },
            [
                self._rule(
                    "missing_default",
                    [self._p("controller", "!=", "skip_list", default=None)],
                    [self._a("dependent", 0)],
                )
            ],
            option_order=["dependent", "controller"],
            rng=_NoShuffle(),
            shuffle_candidates=False,
        )

        self.assertEqual(result.assignments["controller"], "skip_list")
        self.assertEqual(result.assignments["dependent"], 1)

    def test_consistent_cycle_converges(self):
        """Consistent transitive cycles do not cause iteration failure.

        This models A=>B and B=>A with the same values. The closure should add
        the missing value once and stop, proving the solver is not doing a DAG
        walk that rejects harmless cycles.
        """
        rules = [
            self._rule("a_requires_b", [self._a("opt_a", 1)], [
                self._a("opt_b", 1),
            ]),
            self._rule("b_requires_a", [self._a("opt_b", 1)], [
                self._a("opt_a", 1),
            ]),
        ]

        result = compatibility._compute_compatibility_closure(
            {"opt_a": 1},
            rules,
        )

        self.assertEqual(result.assignments["opt_a"], 1)
        self.assertEqual(result.assignments["opt_b"], 1)

    def test_conflicting_cycle_reports_error(self):
        """Conflicting transitive cycles are caught during closure.

        This models A=>B and B=>not-A. The closure detects the contradictory
        assignment immediately instead of spinning in the fixed-point loop.
        """
        rules = [
            self._rule("a_requires_b", [self._a("opt_a", 1)], [
                self._a("opt_b", 1),
            ]),
            self._rule("b_disables_a", [self._a("opt_b", 1)], [
                self._a("opt_a", 0),
            ]),
        ]

        with self.assertRaisesRegex(
            compatibility._CompatibilitySolverConflict,
            "conflicting assignments for opt_a",
        ):
            compatibility._compute_compatibility_closure({"opt_a": 1}, rules)

    def test_no_compatible_candidate_reports_error(self):
        """Candidate exhaustion is reported when every value conflicts.

        opt_a=0 fixes opt_c=True. opt_b only has the value 0 available, which
        would imply opt_c=False. The test verifies the solver aborts instead of
        rewriting opt_c.
        """
        rules = [
            self._rule("a_requires_c", [self._a("opt_a", 0)], [
                self._a("opt_c", True),
            ]),
            self._rule("b_disables_c", [self._a("opt_b", 0)], [
                self._a("opt_c", False),
            ]),
        ]

        with self.assertRaisesRegex(
            compatibility._CompatibilitySolverConflict,
            "no compatible candidate for opt_b",
        ):
            compatibility._solve_compatibility_options(
                {},
                {"opt_a": [0], "opt_b": [0]},
                rules,
                option_order=["opt_a", "opt_b"],
                rng=_NoShuffle(),
            )

    def test_compound_rule_rejects_later_feature_candidate(self):
        """Compound antecedents behave like Horn rules during closure.

        Blob files are fixed on first. The later candidate
        commit_bypass_memtable_one_in=100 completes a compound antecedent that
        would require enable_blob_files=0, so it is rejected and the fallback
        value 0 is accepted.
        """
        rules = [
            self._rule(
                "commit_bypass_disables_blob",
                [
                    self._a("use_txn", 1),
                    self._p("commit_bypass_memtable_one_in", ">", 0),
                ],
                [self._a("enable_blob_files", 0)],
            ),
        ]
        domains = {
            "enable_blob_files": [1],
            "use_txn": [1],
            "commit_bypass_memtable_one_in": [100, 0],
        }

        result = compatibility._solve_compatibility_options(
            {},
            domains,
            rules,
            option_order=[
                "enable_blob_files",
                "use_txn",
                "commit_bypass_memtable_one_in",
            ],
            rng=_NoShuffle(),
        )

        self.assertEqual(result.assignments["enable_blob_files"], 1)
        self.assertEqual(result.assignments["commit_bypass_memtable_one_in"], 0)

    def test_numeric_range_constraint_filters_candidates(self):
        """Numeric constraints reject candidates without choosing magic values.

        Optimistic transaction mode requires
        max_write_buffer_size_to_maintain >= write_buffer_size. The solver
        tries 1 MiB first, rejects it, and accepts 4 MiB.
        """
        rules = [
            self._rule(
                "optimistic_txn_write_buffer_maintain",
                [self._a("use_optimistic_txn", 1)],
                [
                    compatibility._CompatibilityRangeConstraint(
                        "max_write_buffer_size_to_maintain",
                        ">=",
                        other_key="write_buffer_size",
                    ),
                ],
            ),
        ]
        domains = {
            "use_optimistic_txn": [1],
            "write_buffer_size": [4 * 1024 * 1024],
            "max_write_buffer_size_to_maintain": [
                1024 * 1024,
                4 * 1024 * 1024,
            ],
        }

        result = compatibility._solve_compatibility_options(
            {},
            domains,
            rules,
            option_order=[
                "use_optimistic_txn",
                "write_buffer_size",
                "max_write_buffer_size_to_maintain",
            ],
            rng=_NoShuffle(),
        )

        self.assertEqual(
            result.assignments["max_write_buffer_size_to_maintain"],
            4 * 1024 * 1024,
        )

    def test_operation_mix_normalizer_preserves_sum(self):
        """Operation-mix constraints redistribute disabled operation pressure.

        The dependency solver records disabled operation kinds. The group
        normalizer then transfers prefix/iter traffic into reads and range
        deletes into point deletes while preserving sum(percent)==100.
        """
        assignments = {
            "readpercent": 35,
            "prefixpercent": 15,
            "iterpercent": 10,
            "writepercent": 30,
            "delpercent": 5,
            "delrangepercent": 5,
            "nooverwritepercent": 0,
            "__disable_op__:prefix": True,
            "__disable_op__:iter": True,
            "__disable_op__:delrange": True,
        }

        result = compatibility._normalize_operation_mix(assignments)

        self.assertEqual(result["readpercent"], 60)
        self.assertEqual(result["prefixpercent"], 0)
        self.assertEqual(result["iterpercent"], 0)
        self.assertEqual(result["delpercent"], 10)
        self.assertEqual(result["delrangepercent"], 0)
        self.assertEqual(
            sum(result[key] for key in compatibility._OPERATION_PERCENT_KEYS),
            100,
        )

    def test_randomized_solver_generation_remains_conflict_free(self):
        """Repeated deterministic randomization catches bad rule interactions.

        This is the regression-test pattern for future rule additions: run the
        candidate solver over a representative rule set for many seeds, then
        validate closure, range constraints, and operation-mix invariants.
        """
        rules = [
            self._rule("disable_wal_reopen", [self._a("disable_wal", 1)], [
                self._a("reopen", 0),
                self._a("sync", 0),
            ]),
            self._rule("blob_direct_write_profile", [
                self._a("enable_blob_direct_write", 1),
            ], [
                self._a("disable_wal", 1),
                self._a("use_txn", 0),
            ]),
            self._rule("commit_bypass_disables_blob", [
                self._a("use_txn", 1),
                self._p("commit_bypass_memtable_one_in", ">", 0),
            ], [
                self._a("enable_blob_files", 0),
            ]),
            self._rule("multiscan_shape", [self._a("use_multiscan", 1)], [
                self._a("prefix_size", -1),
                compatibility._disable_op_assignment("prefix"),
                compatibility._disable_op_assignment("delrange"),
            ]),
            self._rule("prefix_disabled_without_prefix", [
                self._a("prefix_size", -1),
            ], [
                compatibility._disable_op_assignment("prefix"),
            ]),
            self._rule("optimistic_txn_write_buffer_maintain", [
                self._a("use_optimistic_txn", 1),
            ], [
                compatibility._CompatibilityRangeConstraint(
                    "max_write_buffer_size_to_maintain",
                    ">=",
                    other_key="write_buffer_size",
                ),
            ]),
        ]
        domains = {
            "disable_wal": [1, 0],
            "reopen": [20, 0],
            "sync": [1, 0],
            "enable_blob_direct_write": [1, 0],
            "enable_blob_files": [1, 0],
            "use_txn": [1, 0],
            "commit_bypass_memtable_one_in": [100, 0],
            "use_multiscan": [1, 0],
            "prefix_size": [8, -1],
            "use_optimistic_txn": [1, 0],
            "write_buffer_size": [4 * 1024 * 1024, 1024 * 1024],
            "max_write_buffer_size_to_maintain": [
                1024 * 1024,
                4 * 1024 * 1024,
            ],
        }
        pinned_mix = {
            "readpercent": 35,
            "prefixpercent": 15,
            "iterpercent": 10,
            "writepercent": 30,
            "delpercent": 5,
            "delrangepercent": 5,
            "nooverwritepercent": 0,
        }

        for seed in range(500):
            rng = random.Random(seed)
            result = compatibility._solve_compatibility_options(
                pinned_mix,
                domains,
                rules,
                rng=rng,
            )
            normalized = compatibility._normalize_operation_mix(result.assignments)

            self.assertEqual(
                sum(
                    normalized[key]
                    for key in compatibility._OPERATION_PERCENT_KEYS
                ),
                100,
                f"seed={seed}",
            )
            if compatibility._is_op_disabled(normalized, "prefix"):
                self.assertEqual(normalized["prefixpercent"], 0, f"seed={seed}")
            if compatibility._is_op_disabled(normalized, "delrange"):
                self.assertEqual(normalized["delrangepercent"], 0, f"seed={seed}")


class TestConvergence(unittest.TestCase):
    """finalize_and_sanitize must always reach a fixed point."""

    NUM_TRIALS = 1000

    def test_convergence_random(self):
        """Random configs converge within the iteration limit.

        Non-convergence is now a hard sys.exit(1), so if
        finalize_and_sanitize returns at all, it converged.
        """
        for trial in range(self.NUM_TRIALS):
            params = _build_fuzzed_params(seed=trial)
            db_crashtest.finalize_and_sanitize(params)


class TestCompatibilityMode(unittest.TestCase):
    """The compatibility resolver can be switched back to the old sanitizer."""

    def test_legacy_mode_uses_old_sanitizer(self):
        params = _new_blackbox_params()
        params.update({
            "disable_wal": 1,
            "reopen": 20,
            "sync": 1,
        })

        env = {
            db_crashtest._COMPATIBILITY_MODE_ENV_VAR: (
                db_crashtest._COMPATIBILITY_MODE_LEGACY
            )
        }
        with mock.patch.dict(os.environ, env):
            result = db_crashtest.finalize_and_sanitize(
                params,
                explicit_keys={"reopen", "sync"},
            )

        self.assertEqual(result["reopen"], 0)
        self.assertEqual(result["sync"], 0)


class TestIdempotency(unittest.TestCase):
    """Running sanitize on already-sanitized params should be a no-op."""

    NUM_TRIALS = 500

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

    def _assert_converges(self, params, desc, explicit_keys=None):
        """Assert params converge and return the result.

        Non-convergence is now a hard sys.exit(1), so if
        finalize_and_sanitize returns at all, it converged.
        """
        _ensure_test_db(params)
        params.setdefault("column_families", 1)
        params.setdefault("reopen", 0)
        result = db_crashtest.finalize_and_sanitize(
            params,
            explicit_keys=explicit_keys,
        )
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

    def test_blob_direct_write_preserves_entity_flags(self):
        """Blob direct write still covers compatible entity operations."""
        params = _new_blackbox_params()
        params.update({
            "enable_blob_direct_write": 1,
            "use_put_entity_one_in": 5,
            "use_get_entity": 1,
            "use_multi_get_entity": 1,
            "use_attribute_group": 0,
            "use_txn": 0,
            "commit_bypass_memtable_one_in": 0,
            "user_timestamp_size": 0,
            "persist_user_defined_timestamps": 0,
        })

        result = db_crashtest.finalize_and_sanitize(
            params, explicit_keys={"enable_blob_direct_write"}
        )

        self.assertEqual(result["enable_blob_direct_write"], 1)
        self.assertEqual(result["use_put_entity_one_in"], 5)
        self.assertEqual(result["use_get_entity"], 1)
        self.assertEqual(result["use_multi_get_entity"], 1)

    def test_remote_compaction_disables_skip_stats_update_on_open(self):
        """Remote compaction keeps DB-open stat loading enabled."""
        params = _new_blackbox_params()
        params.update({
            "remote_compaction_worker_threads": 4,
            "skip_stats_update_on_db_open": 1,
            "open_files_async": 1,
            "enable_blob_files": 0,
            "inplace_update_support": 0,
        })

        result = db_crashtest.finalize_and_sanitize(
            params, explicit_keys={"remote_compaction_worker_threads"}
        )

        self.assertEqual(result["remote_compaction_worker_threads"], 4)
        self.assertEqual(result["skip_stats_update_on_db_open"], 0)
        self.assertEqual(result["open_files_async"], 0)

    def test_compaction_filter_multiscan_clears_prefix_ops(self):
        """Compaction filter plus multiscan must not leave prefix ops enabled."""
        params = _new_blackbox_params()
        params.update({
            "enable_compaction_filter": 1,
            "use_multiscan": 1,
            "inplace_update_support": 0,
            "test_batches_snapshots": 0,
            "use_txn": 0,
            "user_timestamp_size": 0,
            "best_efforts_recovery": 0,
            "remote_compaction_worker_threads": 0,
            "prefix_size": 4,
            "readpercent": 30,
            "prefixpercent": 20,
            "iterpercent": 10,
            "writepercent": 30,
            "delpercent": 5,
            "delrangepercent": 5,
        })

        result = db_crashtest.finalize_and_sanitize(
            params,
            explicit_keys={
                "enable_compaction_filter",
                "use_multiscan",
                "inplace_update_support",
            },
        )

        self.assertEqual(result["enable_compaction_filter"], 1)
        self.assertEqual(result["use_multiscan"], 1)
        self.assertEqual(result["prefix_size"], -1)
        self.assertEqual(result["prefixpercent"], 0)
        self.assertEqual(result["iterpercent"], 0)
        self.assertEqual(result["readpercent"], 60)
        self.assertEqual(result["delrangepercent"], 0)
        self.assertEqual(result["delpercent"], 10)

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
        """BER + non-WC txn both random: random priority yields a valid winner."""
        params = _new_blackbox_params()
        params.update(_resolve_params(db_crashtest.txn_params))
        params.update({
            "best_efforts_recovery": 1,
            "use_txn": 1,
            "txn_write_policy": 1,
        })
        result = self._assert_converges(params, "BER + non-WC txn (both random)")
        self.assertFalse(
            result["best_efforts_recovery"] == 1
            and result["use_txn"] == 1
            and result["txn_write_policy"] != 0
        )
        if result["best_efforts_recovery"] == 1:
            self.assertEqual(result["disable_wal"], 1)

    def test_ber_explicit_beats_txn_non_wc(self):
        """BER explicit: non-WC txn must no longer be active.

        The dependency solver gives BER priority because it is explicit, but
        the random txn side can be disabled either by switching policy or by
        disabling txn. The test asserts the incompatible combination is gone.
        """
        params = _resolve_params(db_crashtest.default_params)
        params.update(_resolve_params(db_crashtest.blackbox_default_params))
        params.update(_resolve_params(db_crashtest.txn_params))
        params.update({
            "best_efforts_recovery": 1,
            "use_txn": 1,
            "txn_write_policy": 1,
        })
        _ensure_test_db(params)
        result = db_crashtest.finalize_and_sanitize(
            params, explicit_keys={"best_efforts_recovery"}
        )
        self.assertEqual(result["best_efforts_recovery"], 1)
        self.assertEqual(result["disable_wal"], 1)
        self.assertFalse(
            result.get("use_txn") == 1 and result.get("txn_write_policy") != 0
        )

    def test_disable_wal_explicit_beats_random_ber(self):
        """Explicit disable_wal=0 disables random BER instead of being rewritten."""
        params = _resolve_params(db_crashtest.default_params)
        params.update(_resolve_params(db_crashtest.blackbox_default_params))
        params.update({
            "best_efforts_recovery": 1,
            "disable_wal": 0,
        })
        _ensure_test_db(params)
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
        _ensure_test_db(params)
        result = db_crashtest.finalize_and_sanitize(
            params, explicit_keys={"txn_write_policy"}
        )
        self.assertEqual(result["txn_write_policy"], 1)
        self.assertEqual(result["best_efforts_recovery"], 0)
        self.assertEqual(result["disable_wal"], 0)

    def test_inplace_plus_unordered_write_both_random(self):
        """inplace + unordered both random: random priority yields a valid winner."""
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
            "min_tombstones_for_range_conversion": 0,
        })
        result = self._assert_converges(params, "inplace + unordered (both random)")
        self.assertFalse(
            result["inplace_update_support"] == 1
            and result["unordered_write"] == 1
        )
        if result["inplace_update_support"] == 1:
            self.assertEqual(result["allow_concurrent_memtable_write"], 0)
        if result["unordered_write"] == 1:
            self.assertEqual(result["allow_concurrent_memtable_write"], 1)

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
            "min_tombstones_for_range_conversion": 0,
        })
        # inplace is explicit, unordered is random -> inplace wins
        _ensure_test_db(params)
        result = db_crashtest.finalize_and_sanitize(
            params, explicit_keys={"inplace_update_support"}
        )
        self.assertEqual(result["inplace_update_support"], 1)
        self.assertEqual(result["unordered_write"], 0)
        self.assertEqual(result["allow_concurrent_memtable_write"], 0)

    def test_inplace_disables_memtable_checksum_on_seek(self):
        """inplace update is incompatible with per-key checksum checks on seek."""
        params = _new_blackbox_params()
        params.update({
            "inplace_update_support": 1,
            "memtable_verify_per_key_checksum_on_seek": 1,
            "memtablerep": "skip_list",
            "test_batches_snapshots": 0,
            "best_efforts_recovery": 0,
            "remote_compaction_worker_threads": 0,
            "min_tombstones_for_range_conversion": 0,
        })

        result = db_crashtest.finalize_and_sanitize(
            params, explicit_keys={"inplace_update_support"}
        )

        self.assertEqual(result["inplace_update_support"], 1)
        self.assertEqual(result["memtable_verify_per_key_checksum_on_seek"], 0)

    def test_skip_list_memtable_keeps_memory_checks_enabled(self):
        """Missing-key defaults must not fire before memtablerep is resolved."""
        params = _new_blackbox_params()
        params.update({
            "best_efforts_recovery": 0,
            "inplace_update_support": 0,
            "memtablerep": "skip_list",
            "memtable_verify_per_key_checksum_on_seek": 1,
            "min_tombstones_for_range_conversion": 0,
            "paranoid_memory_checks": 1,
            "remote_compaction_worker_threads": 0,
            "test_batches_snapshots": 0,
        })

        result = self._assert_converges(params, "skip-list memtable checks")

        self.assertEqual(result["memtablerep"], "skip_list")
        self.assertEqual(result["paranoid_memory_checks"], 1)
        self.assertEqual(result["memtable_verify_per_key_checksum_on_seek"], 1)

    def test_absent_memtable_rep_disables_memory_checks(self):
        """Absent memtablerep keeps the old sanitizer's non-skip-list default."""
        params = _new_blackbox_params()
        params.pop("memtablerep", None)
        params.update({
            "best_efforts_recovery": 0,
            "inplace_update_support": 0,
            "memtable_verify_per_key_checksum_on_seek": 1,
            "min_tombstones_for_range_conversion": 0,
            "paranoid_memory_checks": 1,
            "remote_compaction_worker_threads": 0,
            "test_batches_snapshots": 0,
        })

        result = self._assert_converges(params, "absent memtable rep")

        self.assertEqual(result["paranoid_memory_checks"], 0)
        self.assertEqual(result["memtable_verify_per_key_checksum_on_seek"], 0)

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
            "min_tombstones_for_range_conversion": 0,
        })
        # unordered is explicit, inplace is random -> unordered wins
        _ensure_test_db(params)
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
        """UDT + unordered both random: random priority yields a valid winner."""
        params = _new_blackbox_params()
        params.update({
            "user_timestamp_size": 8,
            "persist_user_defined_timestamps": 0,
            "unordered_write": 1,
            "txn_write_policy": 1,
            "use_txn": 1,
        })
        result = self._assert_converges(params, "UDT + unordered (both random)")
        udt_memtable_only = (
            result["user_timestamp_size"] > 0
            and result["persist_user_defined_timestamps"] == 0
        )
        self.assertFalse(udt_memtable_only and result["unordered_write"] == 1)
        if udt_memtable_only:
            self.assertEqual(result["allow_concurrent_memtable_write"], 0)
        if result["unordered_write"] == 1:
            self.assertEqual(result["allow_concurrent_memtable_write"], 1)

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
        _ensure_test_db(params)
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
        _ensure_test_db(params)
        result = db_crashtest.finalize_and_sanitize(
            params, explicit_keys={"unordered_write"}
        )
        self.assertEqual(result["unordered_write"], 1)
        # UDT memtable-only is disabled by changing at least one antecedent.
        self.assertFalse(
            result.get("user_timestamp_size", 0) > 0
            and result.get("persist_user_defined_timestamps", 0) == 0
        )
        self.assertEqual(result["allow_concurrent_memtable_write"], 1)

    def test_disable_wal_chain(self):
        """BER -> disable_wal -> atomic_flush, reopen=0, pipelined=0."""
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

    def test_disable_wal_udt_ber_keeps_pipelined_write_disabled(self):
        """Legacy disables pipelined write even when UDT later clears atomic flush."""
        params = _new_blackbox_params()
        params.update({
            "best_efforts_recovery": 1,
            "disable_wal": 1,
            "enable_pipelined_write": 1,
            "persist_user_defined_timestamps": 0,
            "test_best_efforts_recovery": 1,
            "user_timestamp_size": 8,
        })

        result = self._assert_converges(params, "disable WAL + UDT BER")

        self.assertEqual(result["disable_wal"], 1)
        self.assertEqual(result["atomic_flush"], 0)
        self.assertEqual(result["enable_pipelined_write"], 0)

    def test_conflicting_explicit_flags_exits(self):
        """Two conflicting explicit flags should cause exit(1)."""
        params = _resolve_params(db_crashtest.default_params)
        params.update(_resolve_params(db_crashtest.blackbox_default_params))
        params.update({
            "best_efforts_recovery": 1,
            "inplace_update_support": 1,
            "memtablerep": "skip_list",
            "min_tombstones_for_range_conversion": 0,
            "test_batches_snapshots": 0,
        })
        _ensure_test_db(params)
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
            "multiscan_max_prefetch_memory_bytes": 12345,
            "min_tombstones_for_range_conversion": 0,
        })
        result = self._assert_converges(
            params,
            "multiscan adjustments",
            explicit_keys={"use_multiscan", "min_tombstones_for_range_conversion"},
        )
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

    def test_multiscan_sets_prefetch_when_absent(self):
        """Multiscan materializes its computed prefetch option."""
        random.seed(0)
        params = _new_blackbox_params()
        params.update({
            "use_multiscan": 1,
            "test_batches_snapshots": 0,
            "use_txn": 0,
            "user_timestamp_size": 0,
            "enable_compaction_filter": 0,
            "inplace_update_support": 0,
            "min_tombstones_for_range_conversion": 0,
        })
        params.pop("multiscan_max_prefetch_memory_bytes", None)

        result = self._assert_converges(
            params,
            "multiscan prefetch absent",
            explicit_keys={"use_multiscan", "min_tombstones_for_range_conversion"},
        )

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

        cmd, _ = db_crashtest.gen_cmd(
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

        with mock.patch("sys.stderr", new=stderr):
            cmd, _ = db_crashtest.gen_cmd(params, ["--open_files_async=true"])

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

        cmd, _ = db_crashtest.gen_cmd(params, ["--reopen=20"])

        self.assertIn("--reopen=20", cmd)
        self.assertIn("--disable_wal=0", cmd)
        self.assertIn("--best_efforts_recovery=0", cmd)

    def test_gen_cmd_preserves_explicit_write_buffer_maintain(self):
        random.seed(0)
        params = _new_blackbox_params()
        params.update({
            "use_optimistic_txn": 1,
            "use_txn": 1,
            "txn_write_policy": 0,
            "test_batches_snapshots": 0,
            "test_multi_ops_txns": 0,
            "best_efforts_recovery": 0,
            "disable_wal": 0,
            "write_fault_one_in": 0,
            "mmap_read": 0,
            "min_tombstones_for_range_conversion": 0,
            "write_buffer_size": 4 * 1024 * 1024,
            "max_write_buffer_size_to_maintain": 4 * 1024 * 1024,
        })

        cmd, _ = db_crashtest.gen_cmd(
            params,
            [
                "--use_optimistic_txn=1",
                "--max_write_buffer_size_to_maintain=1048576",
            ],
        )

        self.assertIn("--use_optimistic_txn=1", cmd)
        self.assertIn("--max_write_buffer_size_to_maintain=1048576", cmd)
        self.assertIn("--write_buffer_size=1048576", cmd)

    def test_gen_cmd_ignores_dbcrashtest_only_passthrough_flags(self):
        random.seed(0)
        params = _new_blackbox_params()
        stderr = io.StringIO()

        with mock.patch("sys.stderr", new=stderr):
            cmd, _ = db_crashtest.gen_cmd(params, ["--duration=999"])

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

        with self.assertRaises(SystemExit) as cm:
            db_crashtest.gen_cmd(
                params,
                ["--best_efforts_recovery=1", "--inplace_update_support=1"],
            )

        self.assertEqual(cm.exception.code, 1)

    def test_legacy_mode_appends_passthrough_flags_after_sanitize(self):
        random.seed(0)
        params = _new_blackbox_params()
        params.update({
            "disable_wal": 1,
            "reopen": 0,
        })
        env = {
            db_crashtest._COMPATIBILITY_MODE_ENV_VAR: (
                db_crashtest._COMPATIBILITY_MODE_LEGACY
            )
        }

        with mock.patch.dict(os.environ, env):
            cmd, finalized = db_crashtest.gen_cmd(params, ["--reopen=20"])

        self.assertEqual(finalized["reopen"], 0)
        self.assertIn("--reopen=0", cmd)
        self.assertEqual(cmd[-1], "--reopen=20")


class TestSpecialRules(unittest.TestCase):
    """Special rules must still hold in the final sanitized config."""

    def test_release_mode_disables_unsupported_direct_io(self):
        """Unsupported direct IO stays disabled after the full sanitize loop."""
        params = _new_blackbox_params()
        params.update({
            "use_direct_reads": 1,
            "use_direct_io_for_compaction_reads": 1,
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
        self.assertEqual(result["use_direct_io_for_compaction_reads"], 0)
        self.assertEqual(result["use_direct_io_for_flush_and_compaction"], 0)
        self.assertFalse(result["mock_direct_io"])

    def test_mmap_read_disables_direct_io_read_modes(self):
        params = _new_blackbox_params()
        params.update({
            "mmap_read": 1,
            "use_direct_reads": 1,
            "use_direct_io_for_compaction_reads": 1,
            "use_direct_io_for_flush_and_compaction": 1,
            "multiscan_use_async_io": 1,
        })

        result = db_crashtest.finalize_and_sanitize(
            params,
            explicit_keys={"mmap_read"},
        )

        self.assertEqual(result["mmap_read"], 1)
        self.assertEqual(result["use_direct_reads"], 0)
        self.assertEqual(result["use_direct_io_for_compaction_reads"], 0)
        self.assertEqual(result["use_direct_io_for_flush_and_compaction"], 0)
        self.assertEqual(result["multiscan_use_async_io"], 0)

    def test_range_conversion_disables_sqfc_and_inplace_update(self):
        params = _new_blackbox_params()
        params.update({
            "min_tombstones_for_range_conversion": 2,
            "use_multiscan": 0,
            "use_sqfc_for_range_queries": 1,
            "inplace_update_support": 1,
            "test_batches_snapshots": 0,
            "memtablerep": "skip_list",
        })

        result = db_crashtest.finalize_and_sanitize(
            params,
            explicit_keys={"min_tombstones_for_range_conversion", "use_multiscan"},
        )

        self.assertEqual(result["min_tombstones_for_range_conversion"], 2)
        self.assertEqual(result["use_sqfc_for_range_queries"], 0)
        self.assertEqual(result["inplace_update_support"], 0)


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
