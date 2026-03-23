# Ways to Contribute
Other than contributing code changes, there're lots of other ways to contribute to the RocksDB community. For example:
* Answer questions on the [user group](https://groups.google.com/g/rocksdb), [stackoverflow](https://stackoverflow.com/questions/tagged/rocksdb), [facebook group](https://www.facebook.com/groups/rocksdb.dev/);
* Report issue with detailed information, if possible attache a test with that;
* Investigate and fix an issue; We would appreciate if you could help to reproduce the issue, which is typically the hardest part. If you're looking for an open issue to work on, here is the list of issues [up for grab](https://github.com/facebook/rocksdb/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc+label%3Aup-for-grabs+no%3Aassignee);
* Review and test a PR. We have some PRs pending review, your reviewing or test can reduce the reviewing load on us;
* Submit new feature request or implementation. We welcome any feature suggestions, for new feature implementation, please create an issue with the design proposal first. It can help to get more engagement and discussion before reviewing your implementation;
* Help ongoing features development, here is list of [new features](https://github.com/facebook/rocksdb/wiki/Projects-Being-Developed) that's actively developing, help us to complete the feature development.

Note: Fixing single unambiguous typo will NOT be accepted as contribution. PR clarifying API comments or attempting to fix all typos in the repository is acceptable. 

# Before Code Contribution
Before contributing to RocksDB, please make sure that you are able to sign CLA. Your change will not be merged unless you have proper CLA signed. See https://code.facebook.com/cla for more information.

# Basic Development Workflow

As most open-source projects in github, RocksDB contributors work on their fork, and send pull requests to RocksDB’s facebook repo. After a reviewer approves the pull request, a RocksDB team member at Facebook will merge it.


# How to Run Unit Tests

## Build Systems

RocksDB uses gtest. The makefile used for _GNU make_ has some supports to help developers run all unit tests in parallel, which will be introduced below. If you use cmake, you can run the tests with `ctest`.

## Run Unit Tests In Parallel

In order to run unit tests in parallel, first install _GNU parallel_ on your host, and run
```
make all check [-j] 
```
You can specify number of parallel tests to run using environment variable `J=1`, for example:
```
make J=64 all check [-j]
```

If you switch between release and debug build, normal or lite build, or compiler or compiler options, call `make clean` first. So here is a safe routine to run all tests:

```
make clean
make J=64 all check [-j]
```

## Debug Single Unit Test Failures

RocksDB uses _gtest_. You can run specific unit test by running the test binary that contains it. If you use GNU make, the test binary will be just under your checkpoint. For example, test `DBBasicTest.OpenWhenOpen` is in binary `db_basic_test`, so just run
```
./db_basic_test
```
will run all tests in the binary.

gtest provides some useful command line parameters, and you can see them by calling `--help`:
```
./db_basic_test --help
```
 Here are some frequently used ones:

Run subset of tests using `--gtest_filter`. If you only want to run `DBBasicTest.OpenWhenOpen`, call
```
./db_basic_test --gtest_filter=“*DBBasicTest.OpenWhenOpen*”
```
By default, the test DB created by tests is cleared up even if test fails. You can try to preserve it by using `--gtest_throw_on_failure`. If you want to stop the debugger when assert fails, specify `--gtest_break_on_failure`. `KEEP_DB=1` environment variable is another way to preserve the test DB from being deleted at the end of a unit-test run, irrespective of whether the test fails or not:
```
KEEP_DB=1 ./db_basic_test --gtest_filter=DBBasicTest.Open
```

By default, the temporary test files will be under `/tmp/rocksdbtest-<number>/` (except when running in parallel they are under /dev/shm). You can override the location by using environment variable `TEST_TMPDIR`. For example:
```
TEST_TMPDIR=/dev/shm/my_dir ./db_basic_test
```
## Java Unit Tests

Sometimes we need to run Java tests too. Run
```
make jclean rocksdbjava jtest
```
You can put `-j` but sometimes it causes problem. Try to remove `-j` if you see problems.

## Some other build flavors

For more complicated code changes, we ask contributors to run more build flavors before sending the code review. The makefile for _GNU make_ has better pre-defined support for it, although it can be manually done in _CMake_ too.

To build with _AddressSanitizer (ASAN)_, set environment variable `COMPILE_WITH_ASAN`:
```
COMPILE_WITH_ASAN=1 make all check -j
```
To build with _ThreadSanitizer (TSAN)_, set environment variable `COMPILE_WITH_TSAN`:
```
COMPILE_WITH_TSAN=1 make all check -j
```
To run all `valgrind tests`:
```
make valgrind_test -j
```
To run _UndefinedBehaviorSanitizer (UBSAN), set environment variable `COMPILE_WITH_UBSAN`:
```
COMPILE_WITH_UBSAN=1 make all check -j
```
To run `llvm`'s analyzer, run
```
make analyze
```
# Folly Integration

RocksDB supports integration with [Folly](https://github.com/facebook/folly) in order to provide certain features, such as better ```LRUCache``` locking using ```folly::DistributedMutex```, faster hash tables using ```folly::F14FastMap```, async IO in ```MultiGet``` using folly coroutines etc. This is experimental as of now and can be enabled as follows -

## Install dependencies

This list is likely incomplete, but on Ubuntu I did:
```
sudo apt-get install openssl libssl-dev autoconf
```

## Checkout and build folly
This is a pre-requisite and is only required once. The build_folly step uses about 4G of space in /tmp.

```
make checkout_folly
make build_folly
```

## Build RocksDB with folly integration enabled
There are several flavors of folly integration. The coroutines functionality requires gcc 10 or newer, and cmake. The cmake command lines below are for an optimized build. Search cmake docs to learn about CMAKE_BUILD_TYPE if you want a debug build.

Build with full folly integration, including coroutines but without compression libraries
```
CC=gcc-10 CXX=g++-10 mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DUSE_COROUTINES=1 -DWITH_GFLAGS=1 -DROCKSDB_BUILD_SHARED=0 .. && make -j
```

Build with full folly integration, including coroutines and lz4 and zstd compression libraries but without setting CC or CXX paths. This only builds the db_bench binary to avoid the time and space needed to compile all binaries.
```
mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DWITH_LZ4=1 -DWITH_ZSTD=1 -DUSE_COROUTINES=1 -DWITH_GFLAGS=1 -DROCKSDB_BUILD_SHARED=0 .. && make -j db_bench
```

Build with partial folly integration (no coroutines) (requires gcc 7 or newer) -
```
USE_FOLLY=1 make -j
```

You can find this in the RocksDB LOG to confirm there is support for Folly
```
DMutex implementation: folly::DistributedMutex
```

# Code Style

RocksDB follows _Google C++ Style_: https://google.github.io/styleguide/cppguide.html  Note: a common pattern in existing RocksDB code is using non-nullable `Type*` for output parameters, in [the old Google C++ Style](https://stackoverflow.com/questions/26441220/googles-style-guide-about-input-output-parameters-as-pointers), but this guideline [has changed](https://github.com/google/styleguide/commit/7a7a2f510efe7d7fc#diff-bcadcf8be931ffdd5d6a65c60c266039cf1f96b7f35bfb772662db811214c5a0R1713). [The new guideline](https://google.github.io/styleguide/cppguide.html#Inputs_and_Outputs) prefers (non-const) references for output parameters.

For formatting, we limit each line to 80 characters. Most formatting can be done automatically by running
```
build_tools/format-diff.sh
```
or simply `make format` if you use _GNU make_. If you lack of dependencies to run it, the script will print out instructions for you to install them. 

# Requirements Before Sending a Pull Request
## HISTORY.md
Consider updating HISTORY.md to mention your change, especially if it's a bug fix, public API change or an awesome new feature.

## Pull Request Summary
We recommend a "Test Plan:" section is included in the pull request summary, which introduces what testing is done to validate the quality and performance of the change.

## Add Unit Tests
Almost all code changes need to go with changes in unit tests for validation. For new features, new unit tests or tests scenarios need to be added even if it has been validated manually. This is to make sure future contributors can rerun the tests to validate their changes don't cause problem with the feature.

## Simple Changes
Pull requests for simple changes can be sent after running all unit tests with any build favor and see all tests pass. If any public interface is changed, or Java code involved, Java tests also need to be run.

## Complex Changes
If the change is complicated enough, ASAN, TSAN and valgrind need to be run on your local environment before sending the pull request. If you run ASAN with higher version of llvm with covers almost all the functionality of valgrind, valgrind tests can be skipped.
It may be hard for developers who use Windows. Just try to use the best equivalence tools available in your environment.

## Changes with Higher Risk or Some Unknowns
For changes with higher risks, other than running all tests with multiple flavors, a crash test cycle (see [[Stress Test]]) needs to be executed and see no failure. If crash test doesn't cover the new feature, consider to add it there.
To run all crash test, run
```
make crash_test -j
make crash_test_with_atomic_flush -j
```
If you can't use _GNU make_, you can manually build db_stress binary, and run script:
```
  python -u tools/db_crashtest.py whitebox
  python -u tools/db_crashtest.py blackbox
  python -u tools/db_crashtest.py --simple whitebox
  python -u tools/db_crashtest.py --simple blackbox
  python -u tools/db_crashtest.py --cf_consistency blackbox
  python -u tools/db_crashtest.py --cf_consistency whitebox 
```

## Performance Improvement Changes
For changes that might impact performance, we suggest normal benchmarks are run to make sure there is no regression. Depending the actual performance, you may choose to run against a database backed by disks, or memory-backed file systems. Explain in the pull request summary why the performance environment is chosen, if it is not obvious. If the change is to improve performance, bring at least one benchmark test case that favors the improvement and show the improvements.
