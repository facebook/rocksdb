The reliability of RocksDB is critical. To keep it reliable while so many new features and improvements keep being made, besides normal unit tests, an important defense is continuous runs of stress test, called “crash test”, which proves to be an efficient way of catching correctness bugs.

Contrary to unit tests which tend to be: (1) deterministic; (2) covers one single functionality, or interaction with only a few features; (3) total run time is limited, crash tests to cover a different set of scenarios: 
1. Randomized. 
2. Covers combination of a wide range of features. 
3. Continuously running. 
4. Simulate failures. 

# How to Run It

Right now, there are two crash tests to run:

```bash
make crash_test -j
make crash_test_with_atomic_flush -j
```

Arguments can be passed to the crash test which will overwrite the default one. For eg:
```bash
export CRASH_TEST_EXT_ARGS="--use_direct_reads=1 --max_write_buffer_number=4"
make crash_test -j
```

If you can't use GNU make, you can manually build db_stress binary, and run script:
```bash
  python -u tools/db_crashtest.py whitebox
  python -u tools/db_crashtest.py blackbox
  python -u tools/db_crashtest.py --simple whitebox
  python -u tools/db_crashtest.py --simple blackbox
  python -u tools/db_crashtest.py --cf_consistency blackbox
  python -u tools/db_crashtest.py --cf_consistency whitebox
```
Arguments can be passed to the crash test which will overwrite the default one. For eg:
```
 python -u tools/db_crashtest.py whitebox --use_direct_reads=1 --max_write_buffer_number=4
```
To have it run continuously, it is recommended that different favors of builds are used. For example, we run normal build, ASAN, TSAN and UBSAN continuously.

If you want to run crash tests in release mode, you need to set the DEBUG_LEVEL environment to be 0.

If you want to provide the test directory, you need to set the TEST_TMPDIR environment with the test directory path.

# Basic Workflow 

The crash test run keeps running for several hours, and operates on one single DB. It crashes the program once a while. The database is reopened after the crash. After each crash, the database is restarted with a new set of options. Each option is turned on or off randomly. During each run, multiple operations are issued to the database concurrently with randomized inputs. Data validation is continuously executing, and after each restart, all the data is verified. Debug mode is used, so asserts can be triggered too. 

# Database Operations 
A database with a large number of rows (1M) in a number of column families(10) is created. We also create an array of large vectors to store the values of these rows in memory.

The rows are partitioned between multiple threads(32). Each thread is assigned to operate on a fixed contiguous portion of the rows. Each thread does a number of operations on its piece of the database and then verifies it with the in memory copy of the database which is also being updated as changes are made to the database.  As configured, there can be different number of operations, threads, rows, column families, operations per batch, iterations, and operations between database reopen. Different database options can also be modified which results in various configurations and features being tested. eg. checksums, time to live, universal/level compaction, compression types to use at different levels, maximum number of write buffers, minimum number of buffers to flush.

The goal is for the test to cover as many database operations as possible. Right now, it issues most write operations, put, write, delete, single delete, range delete. From the read part, it covers get, multiget and iterators, with or without snapshots. We also issue administrative operations, including compact range, compact files, creating checkpoints, create/drop column families, etc. Some transaction operations are covered too.

While it already covers a wide range of database operations, it doesn’t cover everything. Everyone is welcome to contribute to it and narrow the gap. 

# Data Validation 

Data is validated using several ways: 

1. In one test case, when we add a key to the database, the value is deterministically determined by the key itself. When we read the key back, we assert the value is expected.
2. Before the database crashes, RocksDB remembers whether a key should exist or not. We keep querying random keys and see whether it shows up or doesn’t show up in the database as expected. 
3. While writing anything to the database, we also write to an external log file. Between after restarts, the database state is compared with the log file. 
4. In another test case, we write the same key/value to multiple column families in a single write batch. We verify whether each column family contains completely the same data. 

# Periodically crash the database 

We periodically crash the system in two approach: “black box crash” or “white box crash”. 
The “black box crash” simply call Linux “kill -9” to shut down the program. 
With “white box crash”, some crash points are defined in several places in the code. The places are before and after RocksDB calls various file system operations. When the test program executes to these points, there is a chance that the program shuts down itself. The crash points are before or after file system operations, because only data written in file systems is used when the database is recovered from the crash. Between two file system operations, the data in the file system stays the same, so extra crashing points are unlikely to expose more bugs.




