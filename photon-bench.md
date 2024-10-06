## Standalone db_bench tests

```bash
cd build
cp ../tools/benchmark.sh .

export DB_DIR=`pwd`/test-db
export WAL_DIR=$DB_DIR
export OUTPUT_DIR=$DB_DIR
export COMPRESSION_TYPE=none
export NUM_THREADS=64       # Concurrency
export KEY_SIZE=20
export VALUE_SIZE=400
export NUM_KEYS=100000000   # Require 40 GB disk space
export CACHE_SIZE=0         # Disable block cache. Need to remove --pin_l0_filter_and_index_blocks_in_cache=1 argument from benchmark.sh
export DURATION=60          # Only run 1 minutes

# In env.h, Photon now has hardcoded to use 8 vCPUs.
# In order to make a fair comparison, you may edit benchmark.sh, and add `taskset -c 1-8` before the ./db_bench command.
# This would limit the CPU number for both thread and coroutine.
# But it's not necessary, because essentially these two concurrency models are quite different.

# Clean page cache before every test
echo 3 > /proc/sys/vm/drop_caches

./benchmark.sh bulkload       # Generate data
./benchmark.sh readrandom     # Read test
./benchmark.sh overwrite      # Overwrite test (sync = 0)
./benchmark.sh updaterandom   # Update test (read first, then write, sync = 1)
```
