Since version 2.7, RocksDB supports a special type of iterator (named _tailing iterator_) optimized for a use case in which new data is read as soon as it's added to the database.
Its key features are:
* Tailing iterator doesn't create a snapshot when it's created. Therefore, it can also be used to read newly added data (whereas ordinary iterators won't see any record added after the iterator was created).
* It's optimized for doing sequential reads -- it might avoid doing potentially expensive seeks on SST files and immutable memtables in many cases.

To enable it, set `ReadOptions::tailing` to `true` when creating a new iterator. Note that tailing iterator currently supports only moving in the forward direction (in other words, `Prev()` and `SeekToLast()` are not supported).

Not all new data is guaranteed to be available to a tailing iterator. Seek() or SeekToFirst() on a tailing iterator can be thought of as creating an implicit snapshot -- anything written after it may, but is not guaranteed to be seen.

### Implementation details

A tailing iterator provides a merged view of two internal iterators:
* a _mutable_ iterator, used to access current memtable contents only
* an _immutable_ iterator, used to read data from SST files and immutable memtables

Both of these internal iterators are created by specifying `kMaxSequenceNumber`, effectively disabling filtering based on internal sequence numbers and enabling access to records inserted after the creation of these iterators.
In addition, each tailing iterator keeps track of the database's state changes (such as memtable flushes and compactions) and invalidates its internal iterators when it happens. This enables it to be always up-to-date.

Since SST files and immutable memtables don't change, a tailing iterator can often get away by performing a seek operation only on the mutable iterator. For this purpose, it maintains the interval
`(prev_key, current_key]` currently _covered_ by the immutable iterator (in other words, there are no records with key `k` such that `prev_key < k < current_key` neither in SST files nor immutable memtables).
Therefore, when `Seek(target)` is called and `target` is within that interval, immutable iterator is already at the correct position and it is not necessary to move it.

### Tailing Iterator with DeleteRange - Not Supported

Currently, the TailingIterator is not yet supported if there are range tombstones in RocksDB.

To read range tombstones from the active memtable using a forward iterator, RocksDB needs to fragment range tombstones to allow efficient scanning. Since the tailing iterator allows reading updates as soon as they are added to the memtable, it needs to check if a new range tombstone is added to the memtable during each iterator operation and redo the fragmentation. This has not yet been implemented for the forward iterator, and it currently returns `Status::NotSupported` if a range tombstone is found in the memtable or L0 files at the time of creation or found in the SST file when reaching a lower level.