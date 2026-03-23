There are multiple functions available for users to build a replication system with RocksDB as the storage engine for single node.

## Functions for full DB physical copy
[[Checkpoints]] can build a physical snapshot of the database to another file system directory, with files are hard linked to the source as much as possible.

An alternative is to use `GetLiveFiles()`, combining with `DisableFileDeletion()` and `EnableFileDeletion()`. `GetLiveFiles()` returns a full list of files needed to be copied for the database. In order to prevent them from being compacted while copying them, users can first call `DisableFileDeletion()`, and then retrieve the file list from `GetLiveFiles()`, copy them, and finally call `EnableFileDeletion()`.

## Function for streaming updates, and catching up
Through most systems distribute updates to replicas as an independent component, RocksDB also provides some APIs for doing it.

Incremental replication needs to be able to find and tail all the recent changes to the database. The API `GetUpdatesSince` allows an application to _tail_ the RocksDB transaction log. It can continuously fetch transactions from the RocksDB transaction log and apply them to a remote replica or a remote backup.

A replication system typically wants to annotate each Put with some arbitrary metadata. This metadata may be used to detect loops in the replication pipeline. It can also be used to timestamp and sequence transactions. For this purpose, RocksDB supports an API called `PutLogData` that an application may use to annotate each Put with metadata. This metadata is stored only in the transaction log and is not stored in the data files. The metadata inserted via `PutLogData` can be retrieved via the `GetUpdatesSince` API.

RocksDB transaction logs are created in the database directory. When a log file is no longer needed, it is moved to the archive directory. The reason for the existence of the archive directory is because a replication stream that is falling behind might need to retrieve transactions from a log file that is way in the past. The API `GetSortedWalFiles` returns a list of all transaction log files.