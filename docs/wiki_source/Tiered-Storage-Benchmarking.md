Tiered storage is a feature still being developed. It can be benchmarked through db_bench with a simulated hybrid file system.

## Simulated Hybrid File System
Simulated Hybrid File System simulates a hybrid file system by book tracking files hinted to be on Warm tier and when reads are issued against these files, latency is injected and throughput is limited. The book tracking is persistent in a file when the file system is closed, and can be read back when it is opened again. Note that if the process crashes in the middle, the tracking information is lost. The source code: https://github.com/facebook/rocksdb/blob/6.23.fb/tools/simulated_hybrid_file_system.h

When running db_bench, option `-simulate_hybrid_fs_file=<metadata_file_name>` can enable this option.

## Bottommost tiering setting
The tiering setting can be enabled by asking RocksDB to always put bottommost files to another temperature tier. It can be set through `options.bottommost_temperature`. When setting through db_bench, when `-simulate_hybrid_fs_file` is used, `options.bottommost_temperature` is automatically set to be kWarm, which Simulated Hybrid File System will track.

## Example
We could populate DB with:
```
TEST_TMPDIR=/data/dbb ./db_bench -simulate_hybrid_fs_file="/tmp/hybrid_tracking" --benchmarks=fillrandom --num=10000000
```
And then run readwhilewriting with the same DB using multiple threads:
```
TEST_TMPDIR=/data/dbb ./db_bench -simulate_hybrid_fs_file="/tmp/hybrid_tracking" --benchmarks=readwhilewriting --num=10000000 -use_existing_db -threads=16 -benchmark_write_rate_limit=1048576 --duration=90
```
And you'll see even if it runs against a fast storage, the performance is simulated as the bottommost level is on HDD:
```
readwhilewriting :   76179.775 micros/op 206 ops/sec;    0.0 MB/s (1283 of 1999 found)
```