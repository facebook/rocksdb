The approximate size APIs allow a user to get a reasonably accurate guess of disk space and memory utilization of a key range.

# API Usage

The main APIs are ```GetApproximateSizes()``` and ```GetApproximateMemTableStats()```. The former takes a ```struct SizeApproximationOptions``` as an argument. It has the following fields -
* ```include_memtabtles``` - Indicates whether to count the memory usage of a given key range in memtables towards the overall size of the key range.
* ```include_files``` - Indicates whether to count the size of SST files occupied by the key range towards the overall size. At least one of this or ```include_memtabtles``` must be set to ```true```.
* ```files_size_error_margin``` - This option indicates the acceptable ratio of over/under estimation of file size to the actual file size. For example, a value of ```0.1``` means the approximate size will be within 10% of the actual size of the key range. The main purpose of this option is to make the calculation more efficient. Setting this to ```-1.0``` will force RocksDB to seek into SST files to accurately calculate the size, which will be more CPU intensive.

Example,
```cpp
  std::array<Range, NUM_RANGES> ranges;
  std::array<uint64_t, NUM_RANGES> sizes;
  SizeApproximationOptions options;

  options.include_memtabtles = true;
  options.files_size_error_margin = 0.1;

  ranges[0].start = start_key1;
  ranges[0].limit = end_key1;
  ranges[1].start = start_key2;
  ranges[1].limit = end_key2;

  Status s = GetApproximateSizes(options, column_family, ranges.data(), NUM_RANGES, sizes.data());
  // sizes[0] and sizes[1] contain the size in bytes for the respective ranges
```
The size estimated within SST files is size on disk, which is compressed. The size estimated in memtable is memory usage. It's up to the user to determine whether it makes sense to add these values together.

The API counterpart for memtable usage is ```GetApproximateMemTableStats```, which returns the number of entries and total size of a given key range. Example,

```cpp
  Range range;
  uint64_t count;
  uint64_t size;

  range.start = start_key;
  range.limit = end_key;

  Status s = GetApproximateMemTableStats(column_family, range, &count, &size);
```

The ```GetApproximateMemTableStats``` is only supported for memtables created by ```SkipListFactory```.

Note that the approximate size from SST files are size of compressed blocks. It might be significantly smaller than the actual key/value size.

# Implementation
## SST File Estimation
To estimate the size of a level, RocksDB first finds and adds up size of full files with in the range for non-L0 levels. Total size of partial files is also estimated. If they are not going to change the result based on ```files_size_error_margin```, they are skipped. Otherwise, RocksDB dives into the partial files one by one to estimate size included in the file.

For each file, it searches the index and figure out sum of all block sizes with in the range. RocksDB only queries index blocks where offsets of each block are kept. RocksDB finds the block for the start and end key of the range, and take a difference of the offsets of the two blocks. In the case where the file is only partial in one side, 0 or file size is used for the open side.

After RocksDB finishes each file, it adds the size to the total size, and re-evaluate whether ```files_size_error_margin``` is guaranteed. The query ends as soon as the condition is met.

## SkipList Memtable Size Estimation
Size in memtable is estimated as estimated number of entries, multiplied by average entry size. Number of entries within the range is estimated by looking at branches taken in binary search reaching the entry. For both of start and end key of the range, we estimate number of entries smaller than the key in the skip list, and take difference between the two. Number of keys smaller than a key is estimated by considering Next() called in the binary search reaching the key. For every Next() call, estimated count increases. The higher level the Next() call it, the more estimated count increased. This is a very rough estimation, but is effective for distinguishing a large range and a small range.