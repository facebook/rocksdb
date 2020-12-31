# Stress Test

To have an overview of what db_stress is and how to use it, refer to [the wiki](https://github.com/facebook/rocksdb/wiki/Stress-test).

This document describes the architecture and code structure of db_stress, in the hope of making it easier to add new tests.

In each db_stress process, multiple threads do operations and verifications on the same DB concurrently.

## Entry Point
The main function of the process is defined in `db_stress.cc`.
It determines whether gflags is installed, and if it is installed, calls the `db_stress_tool` function defined in `db_stress_tool.cc`.

## Flags
When starting a db_stress process, a bunch of options can be configured through command line arguments.
These options are declared in `db_stress_common.h` and defined in `db_stress_gflags.cc` through gflags.
The `db_stress_tool` function validates the specified options, sets up the test `Env`, then constructs and runs a `StressTest`.

## StressTest
`StressTest` is the base class for all stress tests, it's declared and defined in `db_stress_test_base.h` and `db_stress_test_base.cc`.
It defines how each testing thread operates and verifies the DB.
There are currently 3 different implementations of `StressTest`:
- `NonBatchedOpsStressTest` defined in `no_batched_ops_stress.cc`
- `BatchedOpsStressTest` defined in `batched_ops_stress.cc`
- `CfConsistencyStressTest` defined in `cf_consistency_stress.cc`

## ThreadBody, ThreadState and SharedState
Each testing thread runs the same function `ThreadBody` declared and defined in `db_stress_driver.h` and `db_stress_driver.cc`.
Each thread has its unique state `ThreadState`, and they share some state `SharedState` (such as the mutex for coordiating the threads), these states are declared and defined in `db_stress_shared_state.h` and `db_stress_shared_state.cc`.

## RunStressTest
`RunStressTest` is declared and defined in `db_stress_driver.h` and `db_stress_driver.cc`.
It creates the testing threads based on the configured number of threads, initializes `ThreadState` and `SharedState`,
then coordiates the threads to concurrently do operations on the DB and verify the DB content at certain time points.

## Statistics
Statistics (such as the number of operations) about the test are defined as `Stats` in `db_stress_stat.h`.
They are collected during running the tests. After finishing the test, they are reported to stdout.

## Fault Injection
The stress test is also called the crash test. It crashes the testing threads/process randomly.
There are 2 kinds of crash: blackbox and whitebox.
Blackbox crash means `kill -9` the testing process randomly.
Whitebox crash means crash at predefined points in certain code path.
For example, `utilities/fault_injection_fs.h` declares a set of classes related to filesystem to simulate crash when calling certain filesystem operations.

## Utilities
Utility functions and classes can be found in `db_stress_common.h`, `db_stress_compaction_filter.h`, `db_stress_listener.h`, `db_stress_table_properties_collector.h`, etc.
