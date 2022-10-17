## Build

```bash
./photon-auto-convert.sh
cd build
cmake -D FAIL_ON_WARNINGS=off -D WITH_LZ4=on -D WITH_SNAPPY=on -D ENABLE_URING=on -D CMAKE_BUILD_TYPE=Debug ..
make rocksdb -j
make simple_example -j
```
