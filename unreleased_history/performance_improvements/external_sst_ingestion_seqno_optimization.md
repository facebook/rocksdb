* Add a new table property "rocksdb.key.smallest.seqno" which records the smallest sequence number of all keys in file. It makes ingesting DB generated files faster by
avoiding scanning the whole file to find the smallest sequence number.
