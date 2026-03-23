In this wiki we explain how files are deleted if they are not needed.

# SST Files
When compaction finishes, the input SST files are replaced by the output ones in the LSM-tree. However, they may not qualify for being deleted immediately. Ongoing operations depending the older version of the LSM-tree will prevent those files from being qualified to be dropped, until those operations finish. See [[How we keep track of live SST files]] for how the versioning of LSM-tree works.

The operations which can hold an older version of LSM-tree include:
1. Live iterators. Iterators pin the version of LSM-tree while they are created. All SST files from this version are prevented from being deleted. This is because an iterator reads data from a virtual snapshot, all SST files at the moment the iterator are preserved for data from the virtual snapshot to be available.
2. Ongoing compaction. Even if other compactions aren't compacting those SST files, the whole version of the LSM-tree is pinned.
3. Short window during Get(). In the short time a Get() is executed, the LSM-tree version is pinned to make sure it can read all the immutable SST files.
When no operation pins an old LSM-tree version containing an SST file anymore, the file is qualified to be deleted.

Those qualified files are deleted by two mechanisms:

## Reference counting
RocksDB keeps a reference count for each SST file in memory. Each version of the LSM-tree holds one reference count for all the SST files in this version. The operations depending on this version (explained above) in turn hold reference count to the version of the LSM-tree directly or indirectly through "super version". Once the reference count of a version drops to 0, it drops the reference counts for all SST files in it. If a file's SST reference count drops to 0, it can be deleted. Usually they are deleted immediately, with following exceptions:
1. The file is found to be not needed when closing an iterator. If users set `ReadOptions.background_purge_on_iterator_cleanup=true`, rather than deleting the file immediately, we schedule a background job to the high-pri thread pool (the same pool where **flush jobs** are deleted) to delete the file.
2. In Get() or some other operations, if it dereference one version of LSM-tree and cause some SST files to be stale. Rather than having those files to be deleted, they are saved. The next flush job will clean it up, or if some other SST files are being deleted by another thread, these files will be deleted together. In this way, we make sure in Get() no file deletion I/O is made. Be aware that, if no flush happens, the stale files may remain there to be deleted.
3. If users have called DB::DisableFileDeletions(). All files to be deleted will be hold. Once a DB::EnableFileDeletions() clears the file deletion restriction, it will delete all the SST files pending to be deleted.

## Listing all files to find stale files
Reference counting mechanism works for most of the case. However, reference count is not persistent so it is lost after DB restarts, so we need another mechanism to garbage collect files. We do this full garbage collection when restarting the DB, and periodically based on `options.background_purge_on_iterator_cleanup`. The later case is just to be safe.

In this full garbage collection mode, we list all the files in the DB directory, and check whether each file against all the live versions of LSM-trees and see whether the file is in use. For files not needed, we delete them. However, not all the SST files in the DB directory not in live LSM-tree is stale. Files being created for an ongoing compaction or flush should not be removed. To prevent it from happening, we take use of a good feature that new files are created using the file name of an incremental number. Before each flush or compaction job runs, we remember the number of latest SST file created at that time. If a full garbage collection runs before the job finishes, all SST files with number larger than that will be kept. The condition is released after the job is finished. Since multiple flush and compaction jobs can run in parallel, all SST files with number larger than number the earliest ongoing job remembers will be kept. It possible that we have some false positive, but they will be cleared up eventually.

# Log Files
A Log file is qualified to be deleted if all the data in it has been flushed to SST files. Determining an log file can qualify to be deleted is very straight-forward for single column family DBs, slightly more complicated for multi-column family DBs, and even more complicated when two-phase-commit (2PC) is enabled.

## DB With Single column family
A log file has a 1:1 mapping with a memtable. Once a memtable is flushed, the respective log file will be deleted. This is done in the end of the flush job.

## DB With Multiple Column Families
When there are multiple column families, a new log file is created when memtable for any column family is flushed. One log file can only be deleted when the data in it for all column families has been flushed to SST files. The way RocksDB implements it is for each column family to keep track of the earliest log file that still contains unflushed data for this column family. A log file can only be deleted if it is earlier than the earliest of the earliest log for all the column families.

## Two-Phase-Commit
In Two-Phase-Commit (2PC) case, there are two log entries for one write: one prepare and one commit. Only when committed data is flushed to memory, we can release logs containing prepare or commit to be deleted. There isn't a clear mapping between memtable and log files any more. For example, considering this sequence for one single column family DB:
```
--------------
001.log
   Prepare Tx1 Write (K1, V1)
   Prepare Tx2 Write (K2, V2)
   Commit Tx1
   Prepare Tx3 Write (K3, V3)
-------------- <= Memtable Flush   <<<< Point A
002.log
   Commit Tx2
   Prepare Tx4 Write (K4, V4)
-------------- <= Memtable Flush   <<<< Point B
003.log
   Commit Tx3
   Prepare Tx5 Write (K5, V5)
-------------- <= Memtable Flush   <<<< Point C
```
In _Point A_, although the memtable is flushed, 001.log is not qualified to be deleted, because Tx2 and Tx3 are not commited yet. Similarly, in _Point B_, 001.log still isn't qualified to be deleted, because Tx3 is not yet commited. Only in _Point C_, 001.log can be deleted. But 002.log still can't be deleted because of Tx4.

RocksDB use an intricate low-lock data structure to determine a log file is qualified to be deleted or not.