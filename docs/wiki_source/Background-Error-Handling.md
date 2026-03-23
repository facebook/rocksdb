Currently in RocksDB, an error during a write operation (write to WAL, Memtable Flush, background compaction etc) may cause the database instance to go into read-only mode and further user writes may not be accepted. The variable ```ErrorHandler::bg_error_``` is set to the failure ```Status```. The ```DBOptions::paranoid_checks``` option controls how aggressively RocksDB will check and flag errors. The default value of this option is true. The table below shows the various scenarios where errors are considered and potentially affect database operations.

> _NOTE: ErrorHandler is not a public API class, so this article discusses implementation details_

When error occurs, ```ErrorHandler::SetBGError``` is called. If the error status is I/O related, failure ```IOStatus``` is passed in with more I/O related information. Auto recovery logic may be triggered.

| Error Reason | When bg_error_ is set |
-----------------|-----------------------
| Sync WAL (```BackgroundErrorReason::kWriteCallback```) | Always |
| Memtable insert failure (```BackgroundErrorReason::kMemTable```) | Always |
| Memtable flush (```BackgroundErrorReason::kFlush```) | DBOptions::paranoid_checks is true |
| ```SstFileManager::IsMaxAllowedSpaceReached()``` reports max allowed space reached during memtable flush (```BackgroundErrorReason::kFlush```) |	Always |
| ```SstFileManager::IsMaxAllowedSpaceReached()``` reports max allowed space reached during compaction (```BackgroundErrorReason::kCompaction```) | Always |
| ```DB::CompactFiles``` (```BackgroundErrorReason::kCompaction```) | DBOptions::paranoid_checks is true |
| Background compaction (```BackgroundErrorReason:::kCompaction```) | DBOptions::paranoid_checks is true |
| Write (```BackgroundErrorReason::kWriteCallback```) | DBOptions::paranoid_checks is true |

# Detection
If the database instance goes into read-only mode, the following foreground operations will return the error status on all subsequent calls -
* ```DB::Write```, ```DB::Put```, ```DB::Delete```, ```DB::SingleDelete```, ```DB::DeleteRange```, ```DB::Merge```
* ```DB::IngestExternalFile```
* ```DB::CompactFiles```
* ```DB::CompactRange```
* ```DB::Flush```

The returned ```Status``` will indicate the error code, sub-code as well as severity. The severity of the error can be determined by calling ```Status::severity()```. There are 4 severity levels and they are defined as follows -
1. ```Status::Severity::kSoftError``` - Errors of this severity do not prevent writes to the DB, but it does mean that the DB is in a degraded mode. Background compactions and flush may not be able to run in a timely manner.
2. ```Status::Severity::kHardError``` - The DB is in read-only mode, but it can be transitioned back to read-write mode once the cause of the error has been addressed.
3. ```Status::Severity::kFatalError``` - The DB is in read-only mode. The only way to recover is to close the DB, remedy the underlying cause of the error, and then re-open the DB.
4. ```Status::Severity::kUnrecoverableError``` - This is the highest severity and indicates a corruption in the database. It may be possible to close and re-open the DB, but the contents of the database may no longer be correct.  

In addition to the above, a notification callback ```EventListener::OnBackgroundError``` will be called as soon as the background error is encountered.

# Recovery
There are 3 possible ways to recover from a background error without shutting down the database -
1. The ```EventListener::OnBackgroundError``` callback can override the error status if it determines that its not serious enough to stop further writes to the DB. It can do so by setting the ```bg_error``` parameter. Doing so can be risky, as RocksDB may not be able to guarantee the consistency of the DB. Check the ```BackgorundErrorReason``` and severity of the error before overriding it.
2. Call ```DB::Resume()``` to manually resume the DB and put it in read-write mode. This function will flush memtables for all the column families, clear the error, purge any obsolete files, and restart background flush and compaction operations. 
3. Automatic recovery from background errors. This is done by polling the system to ensure the underlying error condition is resolved, and then following the same steps as ```DB::Resume()``` to restore write capability. Notification callbacks ```EventListener::OnErrorRecoveryBegin``` and ```EventListener::OnErrorRecoveryCompleted``` are called at the start and end of the recovery process respectively, to inform the user of the status. The retry behavior can be controlled by setting ```max_bgerror_resume_count``` and ```bgerror_resume_retry_interval``` in ```DBOptions```. 

# Auto Recovery Situations
At present, the automatic recovery is supported in the following scenarios -
1. ENOSPC error from the filesystem
2. IO errors reported by the ```FileSystem``` as retryable (typically transient errors such as network outages). When WAL is not in use, the database will continue to buffer writes in the memtable (i.e the database remains in read-write mode). Writes may eventually stall once ```max_write_buffer_number``` memtables are accumulated.
3. Errors during WAL sync, recovery is done only if 2PC is not in use and manual_wal_flush is not true (see #12995).

Automatic recovery is applied in the following conditions -
1. ```max_bgerror_resume_count``` is not 0
2. The ```IOStatus``` is retryable I/O error.
3. If the ```BackgorundErrorReason``` is compaction related, a new compaction is automatically rescheduled (it does not controlled by ```max_bgerror_resume_count```)
4. If the ```BackgorundErrorReason``` is flush related (SST file write or Manifest write during flush) or WAL related.

Note that, if the background error is compaction related or it happens during flush or manifest write when WAL is DISABLED, the ```bg_error_``` is mapped to ```Status::Severity::kSoftError```. In other cases, the ```bg_error_``` is mapped to ```Status::Severity::kHardError```.

We may add more cases in the future.