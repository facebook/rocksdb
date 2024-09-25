## Build

```bash
# Install dependencies
apt install libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev

# Build performance test on RPC client/server (Photon RocksDB)
./photon-auto-convert.sh
cmake -B build -D INIT_PHOTON_IN_ROCKSDB=off -D FAIL_ON_WARNINGS=off -D WITH_LZ4=on -D WITH_SNAPPY=on -D CMAKE_BUILD_TYPE=Release
cmake --build build -t perf-client -t perf-server -j `nproc`

# Build performance test on RPC client/server (Native RocksDB)
git checkout 6.1.2
git checkout photon-on-6.1.2 -- examples/ CMakeLists.txt
cmake -B build -D INIT_PHOTON_IN_ROCKSDB=off -D FAIL_ON_WARNINGS=off -D WITH_LZ4=on -D WITH_SNAPPY=on -D CMAKE_BUILD_TYPE=Release
cmake --build build -t perf-client -t perf-server -j `nproc`

# Build db_bench
./photon-auto-convert.sh
cmake -B build -D INIT_PHOTON_IN_ROCKSDB=on -D FAIL_ON_WARNINGS=off -D WITH_LZ4=on -D WITH_SNAPPY=on -D CMAKE_BUILD_TYPE=Release
cmake --build build -t db_bench -j `nproc`

# Build CI tests
./photon-auto-convert.sh
cmake -B build -D WITH_TESTS=on -D INIT_PHOTON_IN_ROCKSDB=on -D FAIL_ON_WARNINGS=off -D WITH_LZ4=on -D WITH_SNAPPY=on -D CMAKE_BUILD_TYPE=Debug
cmake --build build -j `nproc`
ulimit -n 100000
cd build && ctest .
```

```bash
# TODO
-D PHOTON_ENABLE_URING=on
```