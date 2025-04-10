* Add a new CF option `memtable_op_scan_flush_trigger` that triggers a flush of the memtable if an iterator's Seek()/Next() scans over a certain number of invisible entries from the memtable.
