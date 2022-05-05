# RocksDB Micro-Benchmark

## Overview

RocksDB micro-benchmark is a set of tests for benchmarking a single component or simple DB operations. The test artificially generates input data and executes the same operation with it to collect and report performance metrics. As it's focusing on testing a single, well-defined operation, the result is more precise and reproducible, which also has its limitation of not representing a real production use case. The test author needs to carefully design the microbench to represent its true purpose.

The tests are based on [Google Benchmark](https://github.com/google/benchmark) library, which provides a standard framework for writing benchmarks.

## How to Run
### Prerequisite
Install the [Google Benchmark](https://github.com/google/benchmark) version `1.6.0` or above.

*Note: Google Benchmark `1.6.x` is incompatible with previous versions like `1.5.x`, please make sure you're using the newer version.*

### Build and Run
With `Makefile`:
```bash
$ DEBUG_LEVEL=0 make run_microbench
```
Or with cmake:
```bash
$ mkdir build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release -DWITH_BENCHMARK
$ make run_microbench
```

*Note: Please run the benchmark code in release build.*
### Run Single Test
Example:
```bash
$ make db_basic_bench
$ ./db_basic_bench --benchmark_filter=<TEST_NAME>
```

## Best Practices
#### * Use the Same Test Directory Setting as Unittest
Most of the Micro-benchmark tests use the same test directory setup as unittest, so it could be overridden by:
```bash
$ TEST_TMPDIR=/mydata/tmp/ ./db_basic_bench --benchmark_filter=<TEST_NAME>
```
Please also follow that when designing new tests.

#### * Avoid Using Debug API
Even though micro-benchmark is a test, avoid using internal Debug API like TEST_WaitForRun() which is designed for unittest. As benchmark tests are designed for release build, don't use any of that.

#### * Pay Attention to Local Optimization
As a micro-benchmark is focusing on a single component or area, make sure it is a key part for impacting the overall application performance.

The compiler might be able to optimize the code that not the same way as the whole application, and if the test data input is simple and small, it may be able to all cached in CPU memory, which is leading to a wrong metric. Take these into consideration when designing the tests.

#### * Names of user-defined counters/metrics has to be `[A-Za-z0-9_]`
It's a restriction of the metrics collecting and reporting system RocksDB is using internally. It will also help integrate with more systems.

#### * Minimize the Metrics Variation
Try reducing the test result variation, one way to check that is running the test multiple times and check the CV (Coefficient of Variation) reported by gbenchmark.
```bash
$ ./db_basic_bench --benchmark_filter=<TEST_NAME> --benchmark_repetitions=10
...
<TEST_NAME>_cv    3.2%
```
RocksDB has background compaction jobs which may cause the test result to vary a lot. If the micro-benchmark is not purposely testing the operation while compaction is in progress, it should wait for the compaction to finish (`db_impl->WaitForCompact()`) or disable auto-compaction.
