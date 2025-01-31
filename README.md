## RocksDB: A Persistent Key-Value Store for Flash and RAM Storage


This is a modified LSM-tree based storage engine that enhances the performance of RocksDB when range deletes are invoved in workloads. To be specific, each range delete operation deletes all entries that are inserted into the LSM-tree before this operation and fall within a certain key space [$k_{start}$, $k_{end}$]. In other words, each range delete operation covers a two dimensional rectangle defined by a *key range* and a *sequence range*, which we call the "effective region". In this case, we employ a ***LSM-Rtree*** to store these range delete rectangles effectively. LSM-Rtree is composed of an in-memory Rtree and several disk levels with increasing size, and each level stores an on-disk Rtree. When the in-memory Rtree reaches the maximum size, it will be merged with the Rtree at Level 1, so on and so forth. In this process, the disk Rtree should be completely read out and re-wrtieen after being merged.

### LSM-Rtee & Range delete filter

In our implementation, the LSM-Rtree is defined as a class ***rangedelete_rep::LSM*** in the file `lsm.hpp`. Each LSM instance contains a in-memory ***Rtree*** defined in `RTree.h` and several ***DiskLevel*** defined in `diskLevel.hpp`. Each DiskLevel have several ***DiskRun*** defined in `diskRTree.hpp`. Moreover, we can adjust the size ratio and number of DiskRuns in each level by ourselves. 

Moreover, we introduce a ***range delete filter*** to enhance query performance. Here we employ a Bucket filter that map the entrire key space into a bit array, as defined in `bucket_filter.hpp`. More specifically, we divide the entire key space into M segments with equal length, as well as tailor the bit array into M same sized sub-array. Then, each key range segment is mapped to an certain sub-array. Hence, we can build a model for each segment to calculate the position of associated bit of an input key in O(1) time.

We integrate LSM-tree, range delete filter, and RocksDB in a ***RangeDeleteDB*** class. It provides diverse APIs including *write*, *range delete*, *point lookup*, and *range lookup* (TODO).

The folder storing the files mentioned above are structured as follows:

```md
include/rocksdb/
|
├── range_delete_filter
│   ├── lsm.hpp
│   ├── diskLevel.hpp
│   ├── diskRTree.hpp
│   ├── RTree.h
│   └── RTree_util.hpp
│
├── range_delete_rep
│   ├── bucket_filter.hpp
│   ├── bucket_model.hpp
│   └── range_filter_util.hpp
│
├── range_delete_db.hpp
├── range_delete_db_util.hpp
...
```

### Benchmark
Moreover, we provide a benchmark in the file `test_delete.cc` to run experiments.
Furthermore, gargage collection could be triggered after compaction in RocksDB via EventListener as is defined in `test_delete.h`.

```md
tools/
│
├── test_delete.h
├── test_delete.cc
...
```

Here is an example to run the benchmark

```
./tools/test_delete --mode=grd --full_rtree=true --workload=test --level_comp=2 --ksize=256 --kvsize=1024 --buffer_size=64 --rep_buffer_size=4096 --max_key=99999999 --prep_num=0 --write_num=200000 --read_num=1800000 --seek_num=0 --rdelete_num=100000 --rdelete_len=100
```

Please complie the project before running as follows:
```
cmake -DFAIL_ON_WARNINGS=0 -DCMAKE_BUILD_TYPE=Release ../
make  -j16
```
