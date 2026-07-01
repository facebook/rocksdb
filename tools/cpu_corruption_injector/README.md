# CPU Corruption Injector

## Overview

This tool measures the **outcome distribution** over many `db_stress` test runs.
In each `db_stress` test run, a single CPU-register bit-flip is injected into
exactly one `db_stress` operation (a write, flush, or compaction) running under
`gdb`. Aggregating over many such runs answers "when the CPU corrupts one
instruction inside that op, how often does RocksDB catch it, crash, silently
lose/return wrong data, or absorb it with no effect?".

It has three layers:

- **Detection** (`db_stress --verify_cpu_corruption_dir`): right after the
  injected op, `db_stress` reads the whole keyspace back and compares against its
  expected state, writing any finding to a `data_corruption.*.json` file.
- **Injection** (`injector*.py`): runs inside `gdb`, navigates to a randomly
  chosen instance of the op, stops at a random "critical" instruction -- one of
  the two kinds that can carry a fault into data: a **move** carrying a value
  between CPU and memory (a general-purpose or vector register), or a
  **branch/jump** (the `eflags` register) -- and flips one random bit in the
  register it uses. Control registers (`rsp`/`rbp`/`rip`/segment) are left alone.
- **Orchestration** (`runner*.py`): builds the run set, launches `gdb` per run,
  classifies each outcome, and aggregates them.

## Requirements

- A `db_stress` binary built with debug info and no inlining, so `gdb` can set
  breakpoints on the injection-site functions and read their source lines:

  ```bash
  make DEBUG_LEVEL=0 EXTRA_CXXFLAGS="-g -fno-inline" db_stress
  ```

- `gdb`. Defaults to `/usr/bin/gdb`; override with the `ICC_GDB` env var if the
  telemetry's source resolves to `@ ?:0` (a `gdb` too old to read this build's
  debug info).
- Optional per-run timeout via the `ICC_RUN_TIMEOUT` env var (seconds, default
  `600`). One hung `gdb`+`db_stress` cannot block the rest of the set.

## Entry point: `runner.py`

A **campaign** is many `db_stress` test **runs** of one op type. Each run is a
full single-threaded `db_stress` test that performs `ops_per_thread` ops, but the
injector corrupts exactly **one** op instance of the chosen type in that run
(e.g. the 51st Put of that run); the run is then classified into a single outcome
bucket.

- `--op` is one of `write`, `flush`, `compaction`.
- `--parallel N` runs `N` runs at once (each `db_stress` run is itself
  single-threaded, so raise this to use more cores).
- `--seed N` reproduces a set: run `i` uses seed `N+i`. With `--seed 0` (the
  default) a fresh random `base_seed` is chosen and logged; pass that value back
  via `--seed` to replay, and add `--runs 1` with `--seed=<base_seed+i>` to
  replay a single run `i`.

```bash
# a compaction campaign of 100 runs
python3 tools/cpu_corruption_injector/runner.py --op compaction --runs 100 \
    --stress_cmd <db_stress> --report_dir <dir>

# a flush campaign of 200 runs, 8 in parallel
python3 tools/cpu_corruption_injector/runner.py --op flush --runs 200 \
    --stress_cmd <db_stress> --report_dir <dir> --parallel 8

# reproduce an earlier set from its logged base_seed
python3 tools/cpu_corruption_injector/runner.py --op write --runs 100 \
    --stress_cmd <db_stress> --report_dir <dir> --seed 12345678
```

Before doing any work, `runner.py` preflights the build: it confirms `gdb` can
set breakpoints on the op's injection-site functions and read their source
lines, and fails fast otherwise.

## Outputs

Everything lands under `--report_dir`: two campaign-wide files at the top level,
plus one `run_XXXXX/` subdirectory per run.

```text
<report_dir>/
├── summary.json          # campaign-wide: outcome_counts + a record per run
├── runner.log            # campaign-wide: runner logging (also echoed to console)
├── run_00000/            # one subdirectory per run
│   ├── inject.json                 # what the injector did (WHERE / WHAT / HOW)
│   ├── data_corruption.<tid>.json  # the finding, only if this run flagged an SDC/corruption
│   ├── gdb.log                     # this run's gdb output
│   ├── db/                         # this run's db_stress DB (working dir)
│   └── exp/                        # this run's expected-values (working dir)
├── run_00001/
│   └── ...
└── ...
```

So `summary.json` and `runner.log` are at the top of `--report_dir`; the
per-run `gdb.log` / `inject.json` / `data_corruption.*.json` live inside each
`run_XXXXX/`.

Outcome buckets:

- **SDC**: silent data corruption -- the read-back succeeded but returned the
  wrong result (a lost key, a resurrected key, or a wrong value).
- **CORRUPTION**: `db_stress` detected a corruption via an integrity check
  (`Status::Corruption`).
- **CRASH**: the injected corruption crashed `db_stress` (a fatal signal).
- **NO_EFFECT**: the corruption landed but the run finished with correct data.
- **NO_INJECTION**: no bit was flipped this run. The op instance, its
  `target_fn`, and the instruction to corrupt are all picked **randomly** up
  front, so a run can occasionally target something it never reaches -- a random
  op instance beyond the ops this run actually performed, or a
  critical-instruction index that no later `target_fn` call happens to have.
  Should be rare; a high count means the injection sites (or the random ranges)
  need revisiting.
- **ERROR**: a harness/framework failure, **not** a `db_stress` data outcome --
  the injector hit a bug, `gdb` died before writing `inject.json`, or the run hit
  the `ICC_RUN_TIMEOUT` cap. Expected to be **0**; any non-zero count is a
  framework bug worth investigating.

## Reading a run's output across the 3 layers

Each run threads through all three layers: the injector records what it corrupted
(`inject.json`), `db_stress` records what the post-op read-back found
(`data_corruption.<tid>.json`), and the runner rolls every run up into
`summary.json`. The examples below are from a real 40-run compaction campaign:

```bash
python3 tools/cpu_corruption_injector/runner.py --op compaction --runs 40 \
    --seed 424242 --stress_cmd <db_stress> --report_dir <dir>
```

**Orchestration layer** -- `summary.json`'s `outcome_counts` (how many of the
campaign's runs landed in each bucket), the whole campaign at a glance:

| SDC | CORRUPTION | CRASH | NO_EFFECT | NO_INJECTION | ERROR |
| --- | ---------- | ----- | --------- | ------------ | ----- |
|   2 |         10 |     0 |        25 |            3 |     0 |

### A silent data corruption (SDC): `run_00023`

**Injection layer** -- `run_00023/inject.json`: an `eflags` flag-flip on the
compaction iterator's key-copy path; the op did NOT crash
(`db_stress_crash_signal: null`):

```json
{
  "injection_result": "injected",
  "db_stress_crash_signal": null,
  "op": "compaction",
  "op_index": 6,
  "entry_fn": "rocksdb::CompactionJob::Run",
  "target_fn": "rocksdb::CompactionIterator::NextFromInput",
  "critical_instruction_index": 0,
  "corruptions": [
    {
      "instruction": "cmp    $0x40,%rdx",
      "register": "eflags",
      "corruption_type": "flag_flip",
      "before": "0x287",
      "after": "0x286",
      "details": {
        "source": "__memmove_avx512_unaligned_erms @ ../sysdeps/x86_64/multiarch/memmove-vec-unaligned-erms.S:299",
        "call_chain": [
          "__memmove_avx512_unaligned_erms @ ../sysdeps/x86_64/multiarch/memmove-vec-unaligned-erms.S:299",
          "rocksdb::IterKey::SetKeyImpl @ ./db/dbformat.h:941",
          "rocksdb::IterKey::SetInternalKey @ ./db/dbformat.h:741",
          "rocksdb::IterKey::SetInternalKey @ ./db/dbformat.h:749",
          "rocksdb::CompactionIterator::NextFromInput @ db/compaction/compaction_iterator.cc:781"
        ]
      }
    }
  ],
  "ops_seen": 6,
  "critical_instructions_seen": 1
}
```

**Detection layer** -- `run_00023/data_corruption.<tid>.json`: the post-op
read-back found a committed key missing:

```json
{
  "kind": "lost",
  "cf": 0,
  "key": 202,
  "value_from_db": "",
  "value_from_expected": "010000000504070609080B0A0D0C0F0E",
  "op_status": "Get: NotFound"
}
```

Reading it: the flag flip mis-copied the compaction output key, dropping key 202
from the output SST. The op returned OK and no checksum fired, but reading key
202 back returns `NotFound` for a committed key, so the runner classifies the run
`SDC` (`kind=lost`) -- the bug class this tool exists to surface.

## Auxiliary entry point: `injector.py`

`injector.py` is **not run on its own** -- there is no `python3 injector.py`
usage. It only works inside `gdb`'s embedded Python (it does `import gdb`), so
`runner.py` runs it by launching `gdb` with the injector as gdb's script. Each
run's invocation looks like:

```bash
gdb --batch --nx \
    -iex 'py import sys; sys.argv=[...]' \
    -x tools/cpu_corruption_injector/injector.py \
    --args <db_stress> <db_stress flags...>
```

- `--batch` runs `gdb` non-interactively and exits when the script finishes;
  `--nx` skips the user's `~/.gdbinit` so their config can't change behavior.
- `-iex 'py ... sys.argv=[...]'` runs a Python command *before* the script loads,
  seeding `injector.py`'s `sys.argv` with its parameters (`--op`, `--op_index`,
  `--entry_fn`, `--target_fn`, `--seed`, `--dir`) -- gdb otherwise hands a `-x`
  script an empty argv.
- `-x tools/cpu_corruption_injector/injector.py` is the script `gdb` executes in
  its embedded Python.
- `--args <db_stress> <db_stress flags...>` is the program `gdb` launches and
  debugs -- the `db_stress` binary and its flags, which `gdb` starts, stops
  mid-op, and corrupts.
