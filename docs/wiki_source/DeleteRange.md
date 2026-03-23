DeleteRange is an operation designed to replace the following pattern where a user wants to delete a range of keys in the range `[start, end)`:

```c++
...
Slice start, end;
// set start and end
auto it = db->NewIterator(ReadOptions());

for (it->Seek(start); cmp->Compare(it->key(), end) < 0; it->Next()) {
  db->Delete(WriteOptions(), it->key());
}
...
```
 
This pattern requires performing a range scan, which prevents it from being an atomic operation, and makes it unsuitable for any performance-sensitive write path. To mitigate this, RocksDB provides a native operation to perform this task:
```c++
...
Slice start, end;
// set start and end
db->DeleteRange(WriteOptions(), start, end);
...
```

Under the hood, this creates a range tombstone represented as a single kv, which significantly speeds up write performance. Read performance with range tombstones is competitive to the scan-and-delete pattern. (For a more detailed performance analysis, see [the DeleteRange blog post](https://rocksdb.org/blog/2018/11/21/delete-range.html).

For implementation details, see [this page](https://github.com/facebook/rocksdb/wiki/DeleteRange-Implementation).