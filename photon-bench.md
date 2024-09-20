## Run db_bench

```bash
cd build
cp ../tools/benchmark.sh .
export DB_DIR=`pwd`/test-db
export WAL_DIR=$DB_DIR
export OUTPUT_DIR=$DB_DIR
export COMPRESSION_TYPE=none
export NUM_THREADS=16
export KEY_SIZE=75
export NUM_KEYS=10000000    # Require 10 GB disk space
export VALUE_SIZE=1024

# For large number of threads, you may edit benchmark.sh, and add `taskset -c 1-8` before the ./db_bench command.
# This would limit the CPU number for both thread and coroutine, in order to make a fair comparison.

# Clean page cache before every test
echo 3 > /proc/sys/vm/drop_caches

./benchmark.sh bulkload       # Generate data
./benchmark.sh readrandom     # Read test
./benchmark.sh overwrite      # Overwrite test (sync = 0)
./benchmark.sh updaterandom   # Update test (read first, then write, sync = 1)
```
