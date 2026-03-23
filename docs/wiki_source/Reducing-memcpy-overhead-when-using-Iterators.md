## Use case
In certain scenarios the user may need to Iterate over range of KVs and keep them in memory to process them.
A simple example could be something like this

```
    Iterator* iter = db_->NewIterator(ReadOptions());

    // Get the KVs from the DB
    std::vector<std::pair<std::string, std::string>> db_kvs;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      db_kvs.emplace_back(iter->key().ToString(), iter->value().ToString());
    }

    // Process the keys (in this case we simply sort them)
    auto kv_comparator = [](const std::pair<std::string, std::string>& kv1,
                            const std::pair<std::string, std::string>& kv2) {
      return -kv1.first.compare(kv2.first);
    };
    std::sort(db_kvs.begin(), db_kvs.end(), kv_comparator);

    for (size_t i = 0; i < db_kvs.size(); i++) {
      // Use processed kvs
    }

    delete iter;
```

In this example we simply load KVs from the DB into memory, sort them using a comparator that is different from DB comparator and then use the sorted keys.

## The Problem
The issue with this approach is in this line

	db_kvs.emplace_back(iter->key().ToString(), iter->value().ToString());

If our keys and/or values are huge the cost of copying the key from RocksDB into our `std::string`s will be significant and we cannot escape this overhead since `iter->key()` and `iter->value()` `Slice`s will be invalid the moment `iter->Next()` is called.

## The Solution
We have introduced a new option for `Iterator`s, `ReadOptions::pin_data`. When setting this option to true, RocksDB `Iterator` will pin the data blocks and guarantee that the `Slice`s returned by `Iterator::key()` and `Iterator::value()` will be valid as long as the `Iterator` is not deleted. 

```
    ReadOptions ro;
    // Tell RocksDB to keep the key and value `Slice`s valid as long as
    // the `Iterator` is not deleted
    ro.pin_data = true;
    Iterator* iter = db_->NewIterator(ro);

    // Get the KVs from the DB
    std::vector<std::pair<Slice, Slice>> db_kvs;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      // We check "rocksdb.iterator.is-key-pinned" property to make sure that
      // the key is actually pinned. There is currently no corresponding check
      // possible for the value.
      std::string is_key_pinned;
      iter->GetProperty("rocksdb.iterator.is-key-pinned", &is_key_pinned);
      assert(is_key_pinned == "1");

      // `iter->key()` and `iter->value()` `Slice`s will be valid as long as
      // `iter` is not deleted
      db_kvs.emplace_back(iter->key(), iter->value());
    }

    // Process the KVs (in this case we simply sort them)
    auto kv_comparator = [](const std::pair<Slice, Slice>& kv1,
                            const std::pair<Slice, Slice>& kv2) {
      return -kv1.first.compare(kv2.first);
    };
    std::sort(db_kvs.begin(), db_kvs.end(), kv_comparator);

    for (size_t i = 0; i < db_kvs.size(); i++) {
      // Use processed KVs
    }

    delete iter;
```

After setting `ReadOptions::pin_data` to true, now we can use `Iterator::key()` and `Iterator::value` `Slice`s without copying them

	db_kvs.emplace_back(iter->key(), iter->value());

## Requirements
Right now to support key `Slice` pinning, RocksDB must be created using BlockBased table with `BlockBasedTableOptions::use_delta_encoding` set to `false`.

	Options options;
	BlockBasedTableOptions table_options;
	table_options.use_delta_encoding = false;
	options.table_factory.reset(NewBlockBasedTableFactory(table_options));

To verify that the current key Slice is pinned and will be valid as long as the Iterator is not deleted,
We can check "rocksdb.iterator.is-key-pinned" Iterator property and assert that it's equal to `1`

	std::string is_key_pinned;
	iter->GetProperty("rocksdb.iterator.is-key-pinned", &is_key_pinned);
	assert(is_key_pinned == "1");

Value `Slice` pinning is supported as long as the value is stored inlined, e.g., `kTypeValue` records. So it does not work with features that store value externally like BlobDB, or that compose the value from multiple inputs, like merge operations.

To verify that the current value Slice is pinned and will be valid as long as the Iterator is not deleted, we can check "rocksdb.iterator.is-value-pinned" Iterator property and assert that it's equal to `1`

	std::string is_value_pinned;
	iter->GetProperty("rocksdb.iterator.is-value-pinned", &is_value_pinned);
	assert(is_value_pinned == "1");