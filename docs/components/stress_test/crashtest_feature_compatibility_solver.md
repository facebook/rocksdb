# Crash Test Feature Compatibility Solver

## TLDR

This document describes the `db_crashtest.py` feature compatibility dependency
solver. The solver is wired into production `finalize_and_sanitize()` in
`tools/db_crashtest.py`; its implementation and rule declarations live in
`tools/db_crashtest_compatibility.py`.

The dependency solver is the default compatibility mode. During rollout,
`ROCKSDB_CRASHTEST_COMPATIBILITY_MODE=legacy` switches `db_crashtest.py` back
to the old in-place sanitizer and old passthrough-flag handling without
reverting the refactor.

Solver rules cover:

| Rule area | Coverage |
| --- | --- |
| Core WAL-disabled consequences | `disable_wal=1` forces `sync=0`, `reopen=0`, WAL flush/fault knobs off, and batched snapshots off. |
| Async open prerequisite | `skip_stats_update_on_db_open=0` forces `open_files_async=0`. |
| Atomic flush prerequisite | `atomic_flush=1` forces `enable_pipelined_write=0`. |
| Commit bypass compound rule | `use_txn=1 AND commit_bypass_memtable_one_in>0` disables incompatible blob/entity options. |
| Optimistic transaction numeric rule | `use_optimistic_txn=1` requires `max_write_buffer_size_to_maintain >= write_buffer_size`. |
| Multi-DB guard | `num_dbs>1` disables single-DB-only stress features such as `clear_column_family_one_in` and `test_multi_ops_txns`. |
| Remaining compatibility rules | Represented as one-level assignment, predicate, range, and operation-mix move rules where they are pure compatibility constraints. |

## Problem

Crash-test option compatibility has several kinds of dependencies:

| Dependency type | Meaning | Main complexity |
| --- | --- | --- |
| Feature compatibility | If a feature is active, force other options into a supported shape. | Rules can be transitive and can conflict with other active features. |
| Derived consequences | If a condition is true, normalize downstream options. | Rules need clear priority against explicit and runtime/computed values. |
| Workload shape | Move operation percentages to preserve `sum(percent)==100`. | Percentages have a shared invariant even though they remain individual flags. |

The resolver must choose random option values while respecting those
dependencies:

```text
pinned roots and accepted random choices
  -> transitive compatibility closure
  -> conflict detection
  -> normalized db_stress flags
```

The key property is monotonicity: once an option value is accepted, later random
choices cannot rewrite it. They must either be compatible, try another value, or
fail with a clear conflict.

## Design Goals

| Goal | Requirement |
| --- | --- |
| Unified model | Represent feature compatibility and consequences as dependencies between option assignments. |
| Clear priority | Explicit flags, runtime/computed facts, and operation percentage roots must win over ordinary random options. |
| Random coverage | Random options should still get fair priority against each other across runs. |
| No late surprises | Once an option value is accepted, later choices must not rewrite it. |
| Good errors | Conflicts should explain the fixed value, the conflicting implied value, and the rule path. |
| Static validation | Rule contradictions should be detected by unit tests before crash tests run. |
| Preserve workload shape | Operation percentages must preserve total operation pressure when disabling incompatible operation kinds. Standard operation buckets sum to 100; specialized modes can include `customopspercent` in that total. |

## Non-Goals

| Non-goal | Reason |
| --- | --- |
| Force every rule into a pure graph edge | Some rules are runtime-derived, numeric, or workload-shape normalizers. |
| Require manual contrapositives for every rule | Non-boolean contrapositives are often not concrete assignments and can drift. |

## Core Model

The solver operates on assignments and implication rules.

### Assignment

An assignment is a concrete value for an option:

```text
disable_wal = 1
atomic_flush = 0
compression_type = "zstd"
open_files = -1
```

The solver also supports derived facts that are not direct db_stress flags:

```text
disable_op(prefix)
disable_op(iter)
disable_op(delrange)
move_op(prefix, read)
move_op(iter, read)
move_op(delrange, del)
```

These derived facts are consumed by group normalizers, such as the operation
mix normalizer.

### Rule

A rule is an implication:

```text
antecedent -> consequents
```

For simple rules, the antecedent is a single assignment:

```text
disable_wal = 1 -> sync = 0
disable_wal = 1 -> reopen = 0
```

For compound rules, the antecedent is a set of assignments or predicates. This
is a Horn rule / hyperedge, not a simple graph edge:

```text
use_txn = 1 AND commit_bypass_memtable_one_in > 0
  -> enable_blob_files = 0
```

### Pinned Root

A pinned root is an assignment that cannot be rewritten by random choices:

| Root type | Examples |
| --- | --- |
| Explicit db_stress flag | `--disable_wal=1`, `--reopen=20` |
| Runtime/computed fact | release mode, unsupported direct IO, computed cache ratios |
| Operation percentage | `readpercent`, `prefixpercent`, `customopspercent` |

Pinned roots are solved first. If their closure conflicts, the run aborts
before choosing any random option.

### Random Option

A random option has a finite candidate domain, usually derived from its
configured sampling distribution.

Example:

```text
mmap_read in [0, 0, 1]
open_files_async in [0, 1]
compression_type in ["none", "snappy", "zlib", "lz4", "lz4hc", "xpress", "zstd"]
```

Repeated values preserve weighting.

## Selected Algorithm

The selected approach is:

```text
try candidate -> compute closure -> reject candidate if it conflicts with
already-fixed assignments
```

### High-Level Flow

1. Parse explicit db_stress flags.
2. Evaluate sampled parameters and apply runtime/computed rules.
3. Build the pinned root assignment set from explicit keys, runtime/computed
   keys changed by special rules, and operation percentage keys.
4. Compute transitive closure of pinned roots.
5. Abort if pinned closure contains a conflict.
6. Build a dependency-aware option order for the remaining options. Strongly
   connected components in the rule graph are randomized internally, and
   component order keeps controller options before dependent knobs. Options
   related to pinned roots are moved to the front.
7. Iterate through options in permutation order:
   - If the option is already determined by closure, skip it.
   - Otherwise, try candidate values in weighted-domain order. Production keeps
     the sampled value first; solver unit tests can enable candidate shuffling.
   - For each candidate, compute closure of `fixed + candidate`.
   - Reject the candidate if closure conflicts with fixed assignments.
   - Accept the first compatible candidate and add its closure to fixed.
   - Abort if no candidate is compatible.
8. Apply group normalizers, such as operation mix redistribution, and remove
   derived compatibility keys.
9. Rerun runtime/computed rules and solve again if those rules changed keys
   after compatibility solving.
10. Emit db_stress flags.

### Candidate Rejection Example

Rules:

```text
opt_a = 0 -> opt_c = true
opt_b = 0 -> opt_c = false
```

Order:

```text
opt_a, opt_b, opt_c
```

Step 1: choose `opt_a = 0`.

Closure:

```text
opt_a = 0
opt_c = true
```

Step 2: try candidate `opt_b = 0`.

Trial closure:

```text
opt_a = 0
opt_c = true
opt_b = 0
opt_c = false
```

This conflicts on `opt_c`, so `opt_b = 0` is rejected. If `opt_b` has another
candidate, such as `opt_b = 1`, the solver tries it. If `opt_b = 0` is pinned
or no compatible candidate exists, the solver aborts.

This is why the solver does not need to wait until `opt_c` appears in the
permutation. Closure is computed immediately after each candidate.

## Transitive Closure

Closure is computed by forward chaining:

```text
while new facts are being added:
  for each rule:
    if all antecedents are satisfied:
      add all consequents
      fail if a consequent conflicts with an existing fixed assignment
```

This is not a DAG traversal. Cycles are allowed when they are consistent:

```text
A = 1 -> B = 1
B = 1 -> A = 1
```

The bug is a conflicting closure:

```text
A = 1 -> B = 1
B = 1 -> A = 0
```

Closure should retain provenance so errors can explain why a value was implied.

Example error:

```text
Conflict:
  reopen = 20 from explicit flag --reopen=20
  reopen = 0 required by rule disable_wal_consequences
  disable_wal = 1 from explicit flag --disable_wal=1
```

## Contrapositive Rules

Contrapositive rules are not required for correctness under the selected
candidate-rejection model.

Rule:

```text
A = 1 -> B = 0
```

If `B = 1` was fixed earlier, then trying candidate `A = 1` immediately
computes closure and conflicts with fixed `B = 1`. The candidate is rejected.
The solver does not need an explicit `B = 1 -> A = 0` rule.

Contrapositives can still be useful as an optimization or for earlier pruning,
but they should not be manually required for every rule. For non-boolean and
range-valued options, a contrapositive often does not map to one concrete
assignment.

Example:

```text
compaction_style = FIFO -> periodic_compaction_seconds = 0
```

The contrapositive is:

```text
periodic_compaction_seconds != 0 -> compaction_style != FIFO
```

`compaction_style != FIFO` is a domain restriction, not one value, unless the
rule explicitly declares a fallback.

## Operation Mix Handling

Operation percentage flags are a special case. They have a global invariant:

```text
readpercent
+ prefixpercent
+ iterpercent
+ writepercent
+ delpercent
+ delrangepercent
== 100
```

Specialized workloads can include `customopspercent` in the selected operation
pressure; the compatibility normalizer preserves the total across the standard
operation percentage keys plus `customopspercent` when it is present.
`nooverwritepercent` is not part of this solver's operation-mix invariant.

The solver preserves operation pressure when disabling an operation kind. For
example, if prefix operations are disabled, their percentage is moved into reads
instead of being dropped.

Because of this invariant, operation percentage keys are pinned before ordinary
random options. They remain ordinary `db_stress` flags, but compatibility rules
record operation-move facts instead of rewriting individual percentages during
candidate solving.

An explicit operation percentage fixes the starting operation mix. It can still
be adjusted by group normalization if another compatibility rule disables or
moves that operation kind; command generation warns for this case instead of
treating it as an explicit-flag conflict.

### Pinned Percentage Keys

`db_crashtest.py` samples the initial operation percentages before the solver
runs:

```text
readpercent
prefixpercent
iterpercent
writepercent
delpercent
delrangepercent
customopspercent (when present)
```

The selected total must satisfy the workload's pressure invariant:

```text
sum(standard operation percentages + optional customopspercent) == 100
```

The dependency solver does not mutate individual percentage fields. It only
collects facts:

```text
disable_op(prefix)
disable_op(iter)
disable_op(delrange)
move_op(prefix, read)
move_op(prefix, iter)
move_op(iter, read)
move_op(delrange, del)
```

At the end, the operation-mix normalizer applies those facts atomically.

### Redistribution Policy

| Derived fact | Redistribution |
| --- | --- |
| `disable_op(prefix)` | `readpercent += prefixpercent`, `prefixpercent = 0` |
| `disable_op(iter)` | `readpercent += iterpercent`, `iterpercent = 0` |
| `disable_op(delrange)` | `delpercent += delrangepercent`, `delrangepercent = 0` |
| `move_op(prefix, read)` | `readpercent += prefixpercent`, `prefixpercent = 0` |
| `move_op(prefix, iter)` | `iterpercent += prefixpercent`, `prefixpercent = 0` |
| `move_op(iter, read)` | `readpercent += iterpercent`, `iterpercent = 0` |
| `move_op(delrange, del)` | `delpercent += delrangepercent`, `delrangepercent = 0` |

The normalizer validates that redistribution preserves the initial total.

### Concrete Example

Initial sampled mix:

| Operation | Initial percent |
| --- | ---: |
| `readpercent` | 35 |
| `prefixpercent` | 15 |
| `iterpercent` | 10 |
| `writepercent` | 30 |
| `delpercent` | 5 |
| `delrangepercent` | 5 |
| Sum | 100 |

Option order:

```text
use_txn, prefix_size, compaction_filter
```

Step 1: initial percent roots.

The sampled percent flags are pinned as fixed roots:

```text
readpercent = 35
prefixpercent = 15
iterpercent = 10
writepercent = 30
delpercent = 5
delrangepercent = 5
```

Step 2: `use_txn = 1`.

Rule:

```text
use_txn = 1 -> move_op(delrange, del)
```

Collected facts:

```text
move_op(delrange, del)
```

Step 3: `prefix_size = -1`.

Rule:

```text
prefix_size = -1 -> move_op(prefix, read)
```

Collected facts:

```text
move_op(delrange, del)
move_op(prefix, read)
```

Step 4: `enable_compaction_filter = 1`.

Rule:

```text
enable_compaction_filter = 1 -> move_op(iter, read)
```

Collected facts:

```text
move_op(delrange, del)
move_op(prefix, read)
move_op(iter, read)
```

Final normalization:

```text
delpercent += delrangepercent
delrangepercent = 0

readpercent += prefixpercent
prefixpercent = 0

readpercent += iterpercent
iterpercent = 0
```

Final emitted mix:

| Operation | Initial | Final | Reason |
| --- | ---: | ---: | --- |
| `readpercent` | 35 | 60 | absorbs prefix and iter |
| `prefixpercent` | 15 | 0 | prefix disabled |
| `iterpercent` | 10 | 0 | iter disabled |
| `writepercent` | 30 | 30 | unchanged |
| `delpercent` | 5 | 10 | absorbs delrange |
| `delrangepercent` | 5 | 0 | delrange disabled |
| Sum | 100 | 100 | invariant preserved |

### Why Sample Upfront

Sampling operation percentages before compatibility solving preserves workload
coverage: crash test first chooses broad workload pressure, then redirects
incompatible operation kinds into compatible ones.

If percentages were assigned only after all constraints were known, disabled
operation kinds would never receive initial probability mass, which changes the
coverage distribution.

## Numeric Constraints

Some rules are not concrete assignments. They are numeric constraints:

```text
use_optimistic_txn = 1
  -> max_write_buffer_size_to_maintain >= write_buffer_size

write_fault_one_in > 0
  -> max_write_buffer_number >= 10

enable_blob_direct_write = 1
  -> blob_direct_write_partitions >= 1
```

These should be modeled as range constraints, not implication edges that set
one arbitrary value.

Candidate evaluation handles them by filtering candidates:

```text
fixed:
  use_optimistic_txn = 1
  write_buffer_size = 4MB

candidate:
  max_write_buffer_size_to_maintain = 1MB

result:
  reject candidate because 1MB < 4MB
```

If both sides are pinned and violate the range, abort with a clear error.

## Runtime and Computed Rules

Some rules depend on the environment or compute derived values. They should not
be forced into the static dependency graph.

| Rule type | Examples | Handling |
| --- | --- | --- |
| Runtime facts | release mode disables read fault injection; filesystem lacks direct IO | Compute before random solving and pin changed keys as roots. |
| Computed values | tiered cache ratio; compressed secondary cache size | Treat as derived normalizer or computed output. |
| Random constrained fallback | compression manager `autoskip` choosing non-`none` compression | Keep in `_apply_special_rules()` so the changed keys are pinned before the next solve. |
| Finite compatibility fallback | multiscan prefetch rejecting unsupported values | Represent as an assignment rule over the declared finite domain. |

Runtime-derived facts have the same priority as explicit flags: random options
must not overwrite them.

## Rule Classification

Most compatibility rules belong in the static implication / Horn-rule solver.

| Rule type | Examples | Representation |
| --- | --- | --- |
| Feature-shaped compatibility | `best_efforts_recovery`, `blob_direct_write`, `remote_compaction`, `multiscan` | Horn or assignment implication rules. |
| Simple consequences | `disable_wal`, `checkpoint_one_in`, `write_identity_file` | Assignment implications. |
| Enum/domain consequences | FIFO compaction, non-FIFO compaction, partition filters | Assignment implications plus domain restrictions where needed. |
| Compound activation | `txn_non_write_committed`, `commit_bypass_memtable`, UDT memtable-only | Horn rules. |
| Shape guards | `num_dbs>1`, UDI primary index, memtable checksum-on-seek | Assignment implications. |

Rules that need special handling:

| Rule | Reason | Handling |
| --- | --- | --- |
| Multi-key operations with range deletes | Moves `delrangepercent` into `delpercent`. | `move_op(delrange, del)` plus operation-mix normalizer. |
| UDT memtable-only iteration shape | Moves `iterpercent` into `readpercent`. | `move_op(iter, read)` plus operation-mix normalizer. |
| Inplace-update fixed-across-runs shape | Moves `delrangepercent` / `prefixpercent` into compatible operations. | `move_op(delrange, del)`, `move_op(prefix, read)`. |
| Multiscan shape adjustment | Moves percentages and bounds prefetch memory. | Operation-mix normalizer plus assignment rule for unsupported prefetch sizes. |
| Prefix disabled by `prefix_size=-1` | Moves `prefixpercent` into `readpercent`. | `move_op(prefix, read)`. |
| Simple blackbox prefix iteration | Moves `iterpercent` into `readpercent`. | `move_op(iter, read)`. |
| Compaction-filter or inplace snapshot shape | Moves `iterpercent + prefixpercent` into `readpercent`. | `move_op(iter, read)`, `move_op(prefix, read)`. |
| Write-fault buffer size | Numeric lower bound on `max_write_buffer_number`. | Range constraint. |
| Optimistic transaction write-buffer maintain | Numeric relationship between two options. | Range constraint. |
| `_apply_special_rules` | Runtime/env/computed logic. | Pinned runtime roots and computed normalizers. |

## Conflict Detection

Conflicts are detected in two phases.

### Rule Declaration Validation

The implementation validates the declared shape of every rule as part of
solving, and unit tests call this validation directly:

| Check | Why |
| --- | --- |
| Every assignment and predicate key has a finite option domain or is a derived key. | Prevents new rules from referencing options the solver cannot try or validate. |
| Every assignment value appears in the declared domain. | Prevents impossible consequents from being added silently. |
| Every consequent type is supported. | Keeps rule declarations limited to assignments and range constraints. |

Contradictory implication chains are detected when tests or production solving
compute closure from concrete pinned roots and candidates.

Cycles are not rejected by themselves. Only conflicting closures are rejected.

### Runtime Candidate Validation

During random solving, each candidate is evaluated against already-fixed values:

```text
fixed = pinned roots + accepted random choices + their closures
candidate = option value being tried
trial = closure(fixed + candidate)
```

Reject the candidate if:

| Rejection reason | Example |
| --- | --- |
| Conflicts with fixed assignment | `disable_wal=1 -> reopen=0`, but `reopen=20` fixed earlier. |
| Violates numeric constraint | `max_write_buffer_size_to_maintain < write_buffer_size`. |
| Violates group invariant | Operation mix cannot be normalized without changing the selected total. |

If the option has no compatible candidate, abort. If the option was pinned, this
is reported as a pinned-root conflict rather than random candidate exhaustion.

## Pinned Root Conflict Examples

| Pinned assignments | Conflict |
| --- | --- |
| `disable_wal=1`, `reopen=20` | `disable_wal=1 -> reopen=0`. |
| `open_files_async=1`, `skip_stats_update_on_db_open=0` | `skip_stats_update_on_db_open=0 -> open_files_async=0`. |
| `use_optimistic_txn=1`, `write_buffer_size=4MB`, `max_write_buffer_size_to_maintain=1MB` | Numeric range requires maintain >= write buffer size. |
| direct IO unsupported, `use_direct_reads=1` | Runtime root requires direct reads disabled. |

These should fail fast with provenance.

## Production Output Filtering

The regression domains include options that may not be present in a particular
`db_crashtest.py` invocation. Production solving can use those domains as
fallback candidates, but `_run_compatibility_solver()` only writes a solved
assignment back to `dest_params` when one of these is true:

| Output condition | Why |
| --- | --- |
| The key existed in the original parameter set. | Preserve and sanitize options this invocation actually uses. |
| The key is a derived compatibility key. | Allow operation-mix facts to reach `_normalize_operation_mix()` before they are stripped. |
| The key is an assignment consequent of a satisfied rule touching an original key. | Emit downstream flags needed to enforce a compatibility rule. |
| The key was implied by a rule. | Emit solver-enforced consequences, even when the consequent key was not sampled initially. |

This prevents bare candidate choices from absent compatibility domains from
being emitted while still emitting rule-required consequences.

## Implementation Sketch

### Data Structures

```python
class _CompatibilityAssignment(NamedTuple):
    key: str
    value: Any

class _CompatibilityPredicate(NamedTuple):
    key: str
    op: str
    value: Any

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
```

### Closure Function

```python
def _compute_compatibility_closure(
    seed_assignments,
    rules,
    protected_assignments=None,
):
    assignments = dict(seed_assignments)
    provenance = initial_provenance(seed_assignments)
    range_constraints = []
    changed = True

    while changed:
        changed = False
        for rule in rules:
            if not antecedents_satisfied(rule, assignments):
                continue
            for consequent in rule.consequents:
                changed |= apply_consequent(
                    assignments,
                    provenance,
                    range_constraints,
                    consequent,
                    rule,
                )
        validate_range_constraints(range_constraints, assignments)

    return _CompatibilityClosure(assignments, provenance, range_constraints)
```

### Candidate Loop

```python
fixed = _compute_compatibility_closure(pinned_roots, rules).assignments

for option in option_order:
    if option in fixed:
        continue

    accepted = False
    for candidate in ordered_domain(option):
        try:
            trial = _compute_compatibility_closure(
                {**fixed, option: candidate},
                rules,
                protected_assignments=fixed,
            )
        except CompatibilitySolverConflict:
            continue
        fixed = trial.assignments
        accepted = True
        break

    if not accepted:
        raise CompatibilitySolverConflict(f"no compatible candidate for {option}")

fixed = _normalize_operation_mix(fixed)
_strip_derived_compatibility_keys(fixed)
```

## Implementation Components

| Component | Implementation | Responsibility |
| --- | --- | --- |
| Rule model | `_CompatibilityAssignment`, `_CompatibilityPredicate`, `_CompatibilityRangeConstraint`, `_CompatibilityRule` | Represent assignment, predicate, numeric-range, and Horn-style compatibility rules. |
| Rule declarations and domains | `_COMPATIBILITY_RULES`, `_COMPATIBILITY_OPTION_DOMAINS`, `_compatibility_option_domains_for_params()`, `_validate_compatibility_rules()` | Declare finite candidate domains, add production sampled numeric bounds, and validate that declared rule keys have domains. |
| Closure engine | `_compute_compatibility_closure()` | Apply transitive consequents, detect conflicting assignments, and validate range constraints for the current assignment set. |
| Candidate solver | `_solve_compatibility_options()`, `_solve_compatibility_rule_set()`, `_compatibility_rule_set_option_order()` | Try weighted candidates, reject candidates whose closure conflicts with fixed assignments, and order options by rule dependencies while preserving random order inside cycles. |
| Pinned-root priority | `run_compatibility_solver()`, `_compatibility_priority_option_order()`, `_apply_special_rules_and_get_changed_keys()`, `_COMPATIBILITY_ALWAYS_PINNED_KEYS` | Keep explicit flags, runtime/computed facts, and operation percentages ahead of ordinary randomized options. |
| Operation-mix normalizer | `_move_op_assignment()`, `_disable_op_assignment()`, `_normalize_operation_mix()`, `_OPERATION_PERCENT_KEYS`, `_OPERATION_TOTAL_PERCENT_KEYS` | Convert operation-move or disable facts into atomic percentage redistribution while preserving total operation pressure. |
| Production integration | `_apply_feature_compatibility_solver()`, `finalize_and_sanitize()` | Run runtime/computed rules, solve feature compatibility, rerun runtime/computed rules if needed, and emit sanitized db_stress parameters. |
| Rollout fallback | `ROCKSDB_CRASHTEST_COMPATIBILITY_MODE=legacy`, `tools/db_crashtest_legacy_compatibility.py`, `_apply_legacy_feature_compatibility_sanitizer()` | Keep an operational switch back to the old sanitizer while the dependency solver is validated in stress test production. |
| Parity audit | `tools/check_crashtest_compatibility_parity.py` | Generate production-shaped configs, pass finalized output through the other sanitizer, and report changed keys for rollout triage. |

## FAQ

| Question | Decision |
| --- | --- |
| Should contrapositives be generated? | No. Candidate rejection makes contrapositives unnecessary for correctness, and non-boolean contrapositives often map to domain restrictions rather than one concrete assignment. |
| Should operation mix be sampled before or after other options? | The initially selected operation percentages are pinned, solver rules only record operation-move or disable facts, and `_normalize_operation_mix()` redistributes at the end. |
| Should percentage redistribution remain exact? | Yes. Redistribution preserves selected workload pressure. Standard operation buckets sum to the selected total; when a specialized workload includes `customopspercent`, it is included in that preserved total. |
| Should all computed rules move into the graph? | No. Runtime and computed normalizers stay in `_apply_special_rules()` unless they become pure finite-domain constraints. |
| Is closure precomputed globally? | No. The implementation validates rule declarations statically, then computes closure during pinned-root validation and candidate evaluation because production inputs include runtime facts and sampled numeric values. |
| Does the solver reroll valid sampled values? | No. It prefers sampled values when they already satisfy compatibility. For example, UDI primary mode preserves `index_type` when it is already compatible, and multiscan prefetch only clamps unsupported values instead of rerolling every run. |
| How are chained operation moves handled? | `_normalize_operation_mix()` resolves moves in operation order, so chained facts such as `prefix -> iter` and `iter -> read` move the original prefix pressure to `readpercent` while preserving the selected total. |
| How can production switch back during rollout? | Set `ROCKSDB_CRASHTEST_COMPATIBILITY_MODE=legacy`. The default `dependency` mode continues to use the declarative solver. |
| How do we audit compatibility with the old sanitizer? | Run `python3 tools/check_crashtest_compatibility_parity.py --trials N --workers W --allow-mismatches`. `new-to-old` should stay clean; `old-to-new` reports old one-pass outputs that the dependency solver would further normalize. |
