**Introduction**

File deletions can be rate limited in order to prevent the resultant IO from interfering with normal DB operations. File deletions can happen due to compactions obsoleting the compaction input files, files becoming obsolete due to deletion of column families, files left behind by failed flush/compaction/ingestion etc. The ```SstFileManager``` allows the user to control the rate of deletion.
A common reason for doing this is to prevent latency spikes on flash devices. A high rate of file deletion can cause excessive number of ```TRIM``` commands issued to the device, which can cause read latencies to spike.

**Implementation**

Deletion rate limit is configured through the ```SstFileManager``` (See [[Managing Disk Space Utilization]]). The API is as follows -
```
extern SstFileManager* NewSstFileManager(
    Env* env, std::shared_ptr<FileSystem> fs,
    std::shared_ptr<Logger> info_log = nullptr,
    std::string trash_dir = "", int64_t rate_bytes_per_sec = 0,
    bool delete_existing_trash = true, Status* status = nullptr,
    double max_trash_db_ratio = 0.25,
    uint64_t bytes_max_delete_chunk = 64 * 1024 * 1024);
```
The option ```rate_bytes_per_sec``` sets the rate of deletion. If an individual file size exceeds the rate, the file is deleted in chunks. The ```max_trash_db_ratio``` specifies the upper limit on trash size to live DB size ratio that can be tolerated before files are immediately deleted, overriding the rate limit. The ```bytes_max_delete_chunk``` specifies the number of bytes to delete from a file in a single ```ftruncate(2)``` system call. That, in turn, limits the size of ```TRIM``` command requests sent to the flash device and helps read latencies.

**Limitations**

* WAL files are not subject to the rate limit
* Blob files are always subject to rate limit, irrespective of trash to DB ratio