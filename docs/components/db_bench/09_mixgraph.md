# MixGraph Workload Modeling

**Files:** `tools/db_bench_tool.cc`

## Overview

The `mixgraph` benchmark simulates realistic production workloads by modeling key access patterns, value size distributions, and scan lengths using statistical distributions. It supports configurable Get/Put/Seek ratios and optional sine-wave QPS control for simulating traffic patterns.

## Operation Ratios

The workload mix is controlled by three flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--mix_get_ratio` | 1.0 | Fraction of operations that are Get queries |
| `--mix_put_ratio` | 0.0 | Fraction of operations that are Put queries |
| `--mix_seek_ratio` | 0.0 | Fraction of operations that are Seek queries |

A `QueryDecider` distributes operations based on these ratios. Each operation in the main loop is randomly assigned Get, Put, or Seek based on the configured proportions.

## Key Distribution

MixGraph supports three key distribution models, selected based on flag values:

### Random (Default)

When `--key_dist_a == 0` or `--key_dist_b == 0`, keys are generated uniformly at random.

### Power-Law

When `--key_dist_a` and `--key_dist_b` are both non-zero and no prefix distribution is configured, keys follow a power CDF inversion: `key_seed = PowerCdfInversion(u, a, b)` where `u` is uniform in `[0, 1)`. This creates a hot-key distribution where a small fraction of keys receives most accesses.

### Two-Term Exponential (Prefix-Based)

When prefix distribution parameters are set (`--keyrange_dist_a`, `--keyrange_dist_b`, `--keyrange_dist_c`, `--keyrange_dist_d`) AND `--key_dist_a` and `--key_dist_b` are both non-zero, a `GenerateTwoTermExpKeys` object models key access using a two-term exponential distribution: `f(x) = a*exp(b*x) + c*exp(d*x)`. If either `key_dist_a` or `key_dist_b` is zero, the prefix model is ignored and random key generation is used instead.

The key space is divided into `--keyrange_num` prefix ranges. Each prefix range has its own key access distribution, enabling hierarchical hotspot modeling.

## Value Size Distribution

Put operations generate values with sizes drawn from a Generalized Pareto Distribution:

`val_size = ParetoCdfInversion(u, theta, k, sigma)`

Controlled by:

| Flag | Default | Description |
|------|---------|-------------|
| `--value_theta` | 0.0 | Location parameter |
| `--value_k` | 0.2615 | Shape parameter |
| `--value_sigma` | 25.45 | Scale parameter |

Values below 10 are set to 10. Values exceeding `mix_max_value_size` are reduced via modulo (`val_size % value_max`), not clamped (default max 1024 bytes).

## Scan Length Distribution

Seek operations generate scan lengths from a separate Generalized Pareto Distribution:

`scan_length = ParetoCdfInversion(u, iter_theta, iter_k, iter_sigma) % scan_len_max`

Controlled by:

| Flag | Default | Description |
|------|---------|-------------|
| `--iter_theta` | 0.0 | Location parameter |
| `--iter_k` | 2.517 | Shape parameter |
| `--iter_sigma` | 14.236 | Scale parameter |
| `--mix_max_scan_len` | 10000 | Maximum scan length |

## Sine-Wave QPS Control

When `--sine_mix_rate=true`, the total QPS follows a sine function over time:

`rate(t) = A * sin(B * t + C) + D`

Controlled by:

| Flag | Default | Description |
|------|---------|-------------|
| `--sine_a` | 1.0 | Amplitude |
| `--sine_b` | 1.0 | Frequency |
| `--sine_c` | 0.0 | Phase offset |
| `--sine_d` | 1.0 | Vertical offset (baseline rate) |
| `--sine_mix_rate_interval_milliseconds` | 10000 | Rate recalculation interval |
| `--sine_mix_rate_noise` | 0.0 | Random noise ratio (0.0 to 1.0) |

The total rate is split between reads and writes proportional to the configured ratios. Rate limiting uses `GenericRateLimiter` instances updated at each interval.

## Sine-Wave Write Rate Control

Separately from MixGraph, `--sine_write_rate=true` enables sine-wave rate control for the `DoWrite()` path used by write-only benchmarks (e.g., `fillrandom`). This uses the same `--sine_a/b/c/d` parameters but applies to write rate limiting independently of MixGraph. Do not confuse `--sine_write_rate` (write-only benchmarks) with `--sine_mix_rate` (MixGraph mixed workloads).

## Execution Flow

Step 1: Initialize `QueryDecider` with Get/Put/Seek ratios and configure key distribution model.

Step 2: Main loop:
- Generate random number and determine query type via `QueryDecider::GetType()`
- Generate key using the configured distribution model
- Execute the operation:
  - **Get**: `DB::Get()` with read rate limiting every 100 operations (only when `--sine_mix_rate` is true and rate limiters are initialized)
  - **Put**: `DB::Put()` with Pareto-distributed value size and write rate limiting every 100 operations (only when `--sine_mix_rate` is true)
  - **Seek**: Create iterator, `Seek()` to key, then scan for Pareto-distributed length
- Record latency via `FinishedOps()` with appropriate operation type

Step 3: Report summary with total Gets, Puts, Seeks, found ratio, average value size, and average scan length.

## Usage Example

```bash
# 80% reads, 15% writes, 5% seeks with power-law key distribution
./db_bench --benchmarks=mixgraph --use_existing_db=1 --num=10000000 \
  --mix_get_ratio=0.8 --mix_put_ratio=0.15 --mix_seek_ratio=0.05 \
  --key_dist_a=0.002312 --key_dist_b=0.3467 \
  --value_k=0.2615 --value_sigma=25.45 \
  --duration=60
```
