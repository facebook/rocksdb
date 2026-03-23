Journals or [Logs](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) are the metadata that describes a data system's history of state.

Journals are key to RocksDB' integrity and recovery. RocksDB has two types of journals: Write Ahead Log (WAL) for journaling the in-memory state updates, and MANIFEST for journaling the on-disk state updates. RocksDB's transaction module also relies on journaling, especially WAL, for transaction commit, recovery, and abort.

More details in:

* [WAL](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-%28WAL%29)
* [MANIFEST](https://github.com/facebook/rocksdb/wiki/MANIFEST)
* [Track WAL in MANIFEST](https://github.com/facebook/rocksdb/wiki/Track-WAL-in-MANIFEST)
