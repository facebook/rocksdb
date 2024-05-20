* Added new `Iterator` property, "rocksdb.iterator.is-value-pinned", for checking whether the `Slice` returned by `Iterator::value()` can be used until the `Iterator` is destroyed.
