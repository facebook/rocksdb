## Build

```bash
# Auto convert code
./photon-auto-convert.sh

# Compile
cmake -B build -D FAIL_ON_WARNINGS=off -D WITH_LZ4=on -D CMAKE_BUILD_TYPE=Release
cmake --build build -t db_bench -j

# Run benchmark
cd build
cp ../tools/benchmark.sh .
export DB_DIR=`pwd`/test-db
export WAL_DIR=$DB_DIR
export OUTPUT_DIR=$DB_DIR
export COMPRESSION_TYPE=lz4
export NUM_KEYS=100000000    # Require 14 GB disk space

# Edit benchmark.sh, add `taskset -c 1,8` before the ./db_bench command.
# This would limit the CPU number for both thread and coroutine, in order to make a fair comparison.

# Clean page cache before every test
echo 3 > /proc/sys/vm/drop_caches

./benchmark.sh bulkload       # Generate data
./benchmark.sh readrandom     # Read test
./benchmark.sh overwrite      # Overwrite test (sync = 0)
./benchmark.sh updaterandom   # Update test (read first, then write, sync = 1)
```
