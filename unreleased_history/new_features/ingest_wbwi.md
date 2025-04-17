* Introduce API `IngestWriteBatchWithIndex()` for ingesting updates into DB while bypassing memtable writes. This improves performance when writing a large write batch to the DB.
