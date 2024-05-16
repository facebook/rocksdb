Changed the contract of LockWAL() without changing the behavior. Some logical state changes like file ingestion are not blocked by LockWAL().
