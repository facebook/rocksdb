## Build

```bash
# Build performance test
./photon-auto-convert.sh
cmake -B build -D INIT_PHOTON_IN_ROCKSDB=off -D FAIL_ON_WARNINGS=off -D WITH_LZ4=on -D WITH_SNAPPY=on -D CMAKE_BUILD_TYPE=Release
cmake --build build -t perf-client -t perf-server -j

# Build CI tests and db_bench
./photon-auto-convert.sh
cmake -B build -D INIT_PHOTON_IN_ROCKSDB=on -D FAIL_ON_WARNINGS=off -D WITH_LZ4=on -D WITH_SNAPPY=on -D CMAKE_BUILD_TYPE=Release
cmake --build build -t db_bench -j
```
