# Introduction

Every application is unique and requires a certain consistency guarantee from RocksDB. Every committed record in RocksDB is persisted. The uncommitted records are recorded in [write-ahead-log (WAL)](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format). When RocksDB is shutdown cleanly, all uncommitted data is committed before shutdown and hence consistency is always guaranteed. When RocksDB is killed or the machine is restarted, on restart RocksDB needs to restore itself to a consistent state.

One of the important recovery operations is to replay uncommitted records in WAL. The different WAL recovery modes define the behavior of WAL replay. 

# WAL Recovery Modes

## kTolerateCorruptedTailRecords

In this mode, the WAL replay ignores any error discovered at the tail of the log. The rational is that, on unclean shutdown there can be incomplete writes at the tail of the log. This is a heuristic mode, the system cannot differentiate between corruption at the tail of the log and incomplete write. Any other IO error, will be considered as data corruption.

This mode is acceptable for most application since this provides a reasonable tradeoff between starting RocksDB after an unclean shutdown and consistency.

## kAbsoluteConsistency

In this mode, any IO error during WAL replay is considered as data corruption. This mode is ideal for application that cannot afford to loose even a single record and/or have other means of recovering uncommitted data. 

## kPointInTimeRecovery

In this mode, the WAL replay is stopped after encountering an IO error. The system is recovered to a point-in-time where it is consistent. This is ideal for systems with replicas. Data from another replica can be used to replay past the "point-in-time" where the system is recovered to. (This is the default as of version 6.6.)

## kSkipAnyCorruptedRecords

In this mode, any IO error while reading the log is ignored. The system tries to recover as much data as possible. This is ideal for disaster recovery.