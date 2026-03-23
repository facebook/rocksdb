_NOTE: We've renamed the APIs from `Start()/StartV2()` to `Schedule()` and from `WaitForComplete()/WaitForCompleteV2()` to `Wait()` while adding support for generic unique identifier in 9.1.0._ Please refer to this [PR](https://github.com/facebook/rocksdb/pull/12384) for details.

The Remote Compaction feature enables the user to run the compaction remotely, it could be a different process or even on a remote host. It separates the background compaction from the primary host, which has performance benefits and improves flexibility. Especially if the compactions are offloaded to a remote host, there won't be a background compaction job impacting the read/write requests. And on the remote host that is dedicated for compaction, it can be tuned only for compaction and used for running compactions from different DBs.
Currently, the remote host has to have access to the DB to run the compaction.

Here is an overview of the Remote Compaction feature:

<img src="https://github.com/facebook/rocksdb/blob/gh-pages-old/pictures/remote_compaction_overview.png" width=60% height=60%>

#### 1. Schedule
The first step is primary DB triggers the compaction, instead of running the compaction locally, it sends the compaction information to a callback in `CompactionService`. The user needs to implement the ~~`CompactionService::Start()`~~`CompactionService::Schedule()`, which sends the compaction information to a remote process to schedule the compaction.

#### 2. Compact
On the remote Compaction Worker side, it needs to run `DB::OpenAndCompact()` with the compaction information sent from the primary. Based on the compaction information, the worker opens the DB in read-only mode and runs the compaction. The compaction worker cannot change the LSM tree, it outputs the compaction result to a temporary location that the user needs to set.

#### 3. Return Result
Once the compaction is done, the compaction result needs to be sent back to primary, which includes the metadata about the compacted SSTs and some internal information. The same as scheduling, the user needs to implement the communication between primary and compaction workers.

#### 4. Install & Purge
The primary is waiting for the result by callback ~~`CompactionService::WaitForComplete()`~~ `CompactionService::Wait()`. The result should be passed to that API and return function call. After that, the primary will install the result by renaming the result SST files in the temporary workplace to the LSM files. Then the compaction input files will be purged.
As RocksDB is renaming the result SST files, make sure the temporary workplace and the DB are on the same file system. If not, the user needs to copy the file to the DB file system before returning the `Wait()` call.

Here is the overview of the API between Primary and Compaction Worker. The Compaction Service part needs to be implemented by the user and set by `Options.CompactionService`.

<img src="https://github.com/facebook/rocksdb/blob/gh-pages-old/pictures/remote_compaction_interface.png" width=70% height=70%>