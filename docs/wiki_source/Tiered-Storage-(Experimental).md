# How to use
RocksDB [Tiered storage](https://en.wikipedia.org/wiki/Hierarchical_storage_management) feature can now assign the data to various types of storage media based on the temperature of the data (how hot the data is) within the same db column family. For example, the user can set the temperate of the last level to cold:
```
AdvancedColumnFamilyOptions.last_level_temperature = Temperature::kCold
```
Then the temperature information will be passed to the [FileSystem APIs](https://github.com/facebook/rocksdb/blob/cc2099803a1de4dab8aa748cb26b2650e740d197/include/rocksdb/file_system.h#L258) like [`NewRandomAccessFile()`](https://github.com/facebook/rocksdb/blob/cc2099803a1de4dab8aa748cb26b2650e740d197/include/rocksdb/file_system.h#L340), [`NewWritableFile()`](https://github.com/facebook/rocksdb/blob/cc2099803a1de4dab8aa748cb26b2650e740d197/include/rocksdb/file_system.h#L362), etc. It's up to the user to place the file in its corresponding storage with the implementation of its own `FileSystem`. Also use the temperature information to find the file in corresponding storage.
![](https://github.com/facebook/rocksdb/blob/gh-pages-old/pictures/compaction_temperature1.png)
In general, the high levels data are written most recently and more likely to be hot. Also high level data is much more likely to go though compaction, having them in a faster storage media can improve the compaction process.
Currently, only the last level temperature can be specified. Which has its limitation, for example for a skewed data set, the hot data set may be compacted frequently and compacted to the last level. To prevent that, a per-key based hot/cold data splitting compaction is introduced.

# Hot Data Time Range
If the data is skewed or major compaction (more likely for universal compaction), the recent inserted data may be compacted to the last level, which is stored in cold storage tier. To prevent that, the user can specify the hot data time range by:
```
AdvancedColumnFamilyOptions.preclude_last_level_data_seconds = 259200 // 3 days
```
Then the data written in the last 3 days, won't be compacted to the last level.

Internally, RocksDB compaction can split the hot and cold data in its last level compaction:
![](https://github.com/facebook/rocksdb/blob/gh-pages-old/pictures/tierd_compaction1.png)
A per-key based placement is implemented to place the data older than `now - preclude_last_level_data_seconds` to the last level (cold tier) and other data to penultimate level (hot tier). RocksDB uses the data sequence number to estimate its' insertion time. Once the feature is enabled, RocksDB samples the sequence number to time information and stores that with the SSTable. Based on that, compaction is able to estimate the data insertion time.

# Metrics
A last level vs. non last level read/write bytes are added to statistics:
https://github.com/facebook/rocksdb/blob/72a3fb3424c6605517d7ed09bb2004589aa287c0/include/rocksdb/statistics.h#L428-L432
IO context includes per temperature IO stats: https://github.com/facebook/rocksdb/blob/cc2099803a1de4dab8aa748cb26b2650e740d197/include/rocksdb/iostats_context.h#L78

# Update File Temperature
The file maybe moved between different tiered storage, the information can be synced back to RocksDB (otherwise it has to be handled by the customized user `FileSystem`):
```
experimental::UpdateManifestForFilesState()
```
Or command:
```
$ ldb update_manifest --update_temperatures
```

Internally, the file temperature information is tracked by the Manifest file: https://github.com/facebook/rocksdb/wiki/MANIFEST . During DB open/close or backup/restore, the temperature information is persistent there. If the file temperature is changed, for example, the user manually copied the file from cold storage to hot, RocksDB still think the file is in cold storage. The above command can have the db re-sync the temperature information.

# Limitations and Future Improvements
### 1. Key-range base hot/cold data
Currently, only time based hot/cold data separation is supported, which assumes the new data is hot. Which may not be the case, in some case, the specified key range is hotter than other. It maybe supported in the future (currently, as a workaround, the user could separate the hot and cold key-ranges into different column families if it's possible.)

### 2. Tiered Storage only support universal compaction
Universal compaction is more likely to compact recent inserted data to the cold tier (the last level), so the tiered compaction feature is first adapted universal compaction. For level compaction, it may cause infinite auto compaction if majority of data is hot, which cause large penultimate level which has compaction score > 1. But compaction is unable to place the data to the last level as majority of the data is hot. A improved compaction score calculation needs to be introduced for level compaction.