# Overview
It is possible to cap the total amount of disk space used by a RocksDB database instance, or multiple instances in aggregate. This might be useful in cases where a file system is shared by multiple applications and the other applications need to be insulated from unlimited database growth.

The tracking of disk space utilization and, optionally, limiting the database size is done by ```rocksdb::SstFileManager```. It is allocated by calling ```NewSstFileManager()``` and the returned object is assigned to ```DBOptions::sst_file_manager```. It is possible for multiple DB instances to share an ```SstFileManager```.

# Usage
## Tracking DB Size
A caller can get the total disk space used by the DB by calling ```SstFileManager::GetTotalSize()```. It returns the total size, in bytes, of all the SST files. WAL files are not included in the calculation. If the same ```SstFileManager``` object is shared by multiple DB instances, ```GetTotalSize()``` will report the size across all the instances.

## Limiting the DB Size
By calling ```SstFileManager::SetMaxAllowedSpaceUsage()``` and, optionally,  ```SstFileManager::SetCompactionBufferSize()```, it is possible to set limits on how much disk space is used. Both functions accept a single argument specifying the desired size in bytes. The former sets a hard limit on the DB size, and the latter specifies headroom that should be reserved before deciding whether to allow a compaction to proceed or not.

Setting the max DB size limit can impact the operation of the DB in the following ways -
1. Every time a new SST file is created, either by flush or compaction, ```SstFileManager::OnAddFile()``` is called to update the total size used. If this causes the total size to go above the limit, the ```ErrorHandler::bg_error_``` variable is set to ```Status::SpaceLimit()``` and the DB instance that created the SST file goes into read-only mode. For more information on how to recover from this situation, see [[Background Error Handling]].
2. Before starting a compaction, RocksDB will check if there is enough room to create the output SST files. This is done by calling ```SstFileManager::EnoughRoomForCompaction()```. This function conservatively estimates the output size as the sum of the sizes of all the input SST files to the compaction. If the output size, plus the compaction buffer size if its set, will cause the total size to exceed the limit set by ```SstFileManager::SetMaxAllowedSpaceUsage()```, the compaction is not allowed to proceed. The compaction thread will sleep for 1 second before adding the column family back to the compaction queue. So this effectively throttles the compaction rate.