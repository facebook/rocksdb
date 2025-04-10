* Add a new CF option `tombstone_scan_flush_trigger` that triggers a flush of the memtable if an iterator's Seek()/Next() scans over a certain number of tombstones from the memtable.
