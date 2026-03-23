In RocksDB, the LSM tree consists of a list of SST files in the file system, besides WAL logs. After each compaction, compaction output files are added to the list while the input ones are removed from it. However, input files do not necessarily qualify to be deleted instantly, because some `get` or outstanding iterators might require the files to be kept around until their operations finished or iterators freed. In the rest of the page, we introduce how we keep this information.
 
The list of files in an LSM tree is kept in a data structure called `version`. In the end of a compaction or a mem table flush, a new `version` is created for the updated LSM tree. At one time, there is only one "current" `version` that represents the files in the up-to-date LSM tree. New `get` requests or new iterators will use the current `version` through the whole read process or life cycle of iterator. All `version`s used by `get` or iterators need to be kept. An out-of-date `version` that is not used by any `get` or iterator needs to be dropped. All files not used by any other version need to be deleted. For example,
 
If we start with a `version` with three files:
 
```
v1={f1, f2, f3} (current)
files on disk: f1, f2, f3
```
 
and now an iterator is created with it:

```
v1={f1, f2, f3} (current, used by iterator1)
files on disk: f1, f2, f3
```
Now a flush happens added `f4`, a new version is created:
```
v2={f1, f2, f3, f4} (current)
v1={f1, f2, f3} (used by iterator1)
files on disk: f1, f2, f3, f4
```
Now a compaction happened compact `f2`, `f3` and `f4` into a new file `f5` with a new `version` `v3` created:
```
v3={f1, f5} (current)
v2={f1, f2, f3, f4}
v1={f1, f2, f3} (used by iterator1)
files on disk: f1, f2, f3, f4, f5
```
Now `v2` is neither up-to-date, nor used by anyone, so it qualifies to be removed, together with `f4`. While `v1` still cannot be removed for it is still needed by `iterator1`:
```
v3={f1, f5} (current)
v1={f1, f2, f3} (used by iterator1)
files on disk: f1, f2, f3, f5
```
 Assuming now `iterator1` is destroyed:
```
v3={f1, f5} (current)
v1={f1, f2, f3}
files on disk: f1, f2, f3, f5
```
Now `v1` is not used nor up-to-date, so it can be removed, with file `f2`, `f3`:
```
v3={f1, f5} (current)
files on disk: f1, f5
```
This logic is implemented using reference counts. Both of an SST file and a `version` have a reference count. While we create a `version`, we incremented the reference counts for all files. If a `version` is not needed, all files’ of the version have their reference counts decremented. If a file’s reference count drops to 0, the file can be deleted.

In a similar way, each `version` has a reference count. When a `version` is created, it is an up-to-date one, so it has reference count 1. If the `version` is not up-to-date anymore, its reference count is decremented. Anyone who needs to work on the `version` has its reference count incremented by 1, and decremented by 1 when finishing using it. When a `version`’s reference count is 0, it should be removed. Either a `version` is up-to-date or someone is using it, its reference count is not 0, so it will be kept.

Sometimes a reader holds reference of a `version` directly, like the source `version` for a compaction. More often, a reader holds it indirectly through a data structure called `super version`, which holds reference counts for list of mem tables and a `version` -- a whole view of the DB. A reader only needs to increase and decrease one reference count, while it is the super version that holds the reference count of `version`. It also enables further optimization to avoid locking for the reference counting in most of the time. See the [blog post](http://rocksdb.org/blog/2014/06/27/avoid-expensive-locks-in-get.html).

RocksDB maintains all `version`s in the data structure called `VersionSet`, which also remembers who is the “current” `version`. Since each `column family` is a separate LSM, it also has its own list of `version`s with one that is "current". But there is only one `VersionSet` per DB that maintains `version`s for all column families.

