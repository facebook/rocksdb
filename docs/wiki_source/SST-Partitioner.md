**SstPartitioner** is an interface in RocksDB that allows users to control how SST files are partitioned during compactions. Partitioning SST files can help compactions split files at meaningful boundaries (such as key prefixes), which reduces write amplification by limiting the propagation of SST files across the entire key space.

To use SstPartitioner, applications need to implement the `SstPartitioner` and `SstPartitionerFactory` interfaces found in `rocksdb/sst_partitioner.h`, and set `sst_partitioner_factory` in `ColumnFamilyOptions`. Similar to `CompactionFilterFactory`, `SstPartitionerFactory` creates an `SstPartitioner` for each compaction by calling `SstPartitionerFactory::CreatePartitioner`. The `ShouldPartition()` method will then be called for all keys during the compaction process.

## Interface

```c++
struct PartitionerRequest {
  PartitionerRequest(const Slice& prev_user_key_,
                     const Slice& current_user_key_,
                     uint64_t current_output_file_size_)
      : prev_user_key(&prev_user_key_),
        current_user_key(&current_user_key_),
        current_output_file_size(current_output_file_size_) {}
  const Slice* prev_user_key;
  const Slice* current_user_key;
  uint64_t current_output_file_size;
};

enum PartitionerResult : char {
  // Partitioner does not require to create new file
  kNotRequired = 0x0,
  // Partitioner is requesting forcefully to create new file
  kRequired = 0x1
  // Additional constants can be added
};

// It is called for all keys in compaction. When partitioner want to create
// new SST file it needs to return true. It means compaction job will finish
// current SST file where last key is "prev_user_key" parameter and start new
// SST file where first key is "current_user_key". Returns decision if
// partition boundary was detected and compaction should create new file.
virtual PartitionerResult ShouldPartition(const PartitionerRequest& request) = 0;

// Called with smallest and largest keys in SST file when compaction try to do
// trivial move. Returns true is partitioner allows to do trivial move.
virtual bool CanDoTrivialMove(const Slice& smallest_user_key,
                              const Slice& largest_user_key) = 0;
```

This feature has been available since 6.12.0 (https://github.com/facebook/rocksdb/pull/6957).