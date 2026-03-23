RocksDB provides a way to delete or modify key/value pairs based on custom logic in background. It is handy for implementing custom garbage collection, like removing expired keys based on TTL, or dropping a range of keys in the background. It can also update the value of an existing key.

To use compaction filter, applications need to implement the `CompactionFilter` interface found in rocksdb/compaction_filter.h and set it to `ColumnFamilyOptions`. Alternatively, applications can implement the `CompactionFilterFactory` interface, which gives the flexibility to create different compaction filter instance per (sub)compaction. The compaction filter factory also gets to know some context from the compaction (whether it is a full compaction or whether it is a manual compaction) through the given `CompactionFilter::Context` param. The factory can choose to return different compaction filter based on the context.

```c++
options.compaction_filter = new CustomCompactionFilter();
// or
options.compaction_filter_factory.reset(new CustomCompactionFilterFactory());
```

The two ways of providing compaction filter also come with different thread-safety requirement. If a single compaction filter is provided to RocksDB, it has to be thread-safe since multiple sub-compactions can run in parallel, and they all make use of the same compaction filter instance. If a compaction filter factory is provided, each sub-compaction will call the factory to create a compaction filter instance. It is thus guaranteed that each compaction filter instance will be access from a single thread and thread-safety of the compaction filter is not required. The compaction filter factory, though, can be accessed concurrently by sub-compactions.

Compaction filter will not be invoked during flush, despite arguably flush is a special type of compaction.

There are two sets of API that can be implement with compaction filter. The `Filter/FilterMergeOperand` API provide a simple callback to let compaction know whether to filter out a key/value pair. The `FilterV2` API extends the basic API by allowing changing the value, or dropping a range of keys starting from the current key.

Each time a (sub)compaction sees a new key from its input and when the value is a normal value, it invokes the compaction filter. Based on the result of the compaction filter:
* If it decide to keep the key, nothing will change.
* If it request to filter the key, the value is replaced by a deletion marker. Note that if the output level of the compaction is the bottom level, no deletion marker will need to be output.
* If it request to change the value, the value is replaced by the changed value.
* If it request to remove a range of keys by returning `kRemoveAndSkipUntil`, the compaction will skip over to `skip_until` (means `skip_until` will be the next possible key output by the compaction). This one is tricky because in this case the compaction does not insert deletion marker for the keys it skips. This means older version of the keys may reappears as a result. On the other hand, it is more efficient to simply dropping the keys, if the application know there aren't older versions of the keys, or reappearance of the older versions is fine.

If there are multiple versions of the same key from the input of the compaction, compaction filter will only be invoked once for the newest version. If the newest version is a deletion marker, compaction filter will not be invoked. However, it is possible the compaction filter is invoked on a deleted key, if the deletion marker isn't included in the input of the compaction.

When merge is being used, compaction filter is invoked per merge operand. The result of compaction filter is applied to the merge operand before merge operator is invoked.

Before release 6.0, if there is a snapshot taken later than the key/value pair, RocksDB always try to prevent the key/value pair  from being filtered by compaction filter so that users can preserve the same view from a snapshot, unless the compaction filter returns `IgnoreSnapshots() = true`. However, this feature is deleted since 6.0, after realized that the feature has a bug which can't be easily fixed. Since release 6.0, with compaction filter enabled, RocksDB always invoke filtering for any key, even if it knows it will make a snapshot not repeatable.