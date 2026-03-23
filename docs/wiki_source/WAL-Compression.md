WAL compression is a feature to append compressed records to the WAL. It uses streaming compression to find match phrases across records, which leads to better compression ratios than block based compression on record boundaries. 

# Overview

The RocksDB SST files contain compressed KV pairs. However, when the KVs are first written by the user to the DB with WAL enabled, they are written to the WAL in uncompressed format. This may bloat the size of the WALs relative to the DB size. If the DB is on networked storage and the WAL is replicated, it would add to the IO and storage overhead. Supporting WAL compression addresses these limitations.

The WAL is written and read in a streaming fashion. Writes to the DB are packed into logical records and appended to the WAL file. RocksDB allocates and physically writes to the WAL file in 32KB chunks, and logical records that cross the 32KB boundary are broken up into physical records (or fragments). Compression is done at the logical record level and then broken up into physical records. We use streaming compression, which allows the compression buffer to be flushed at the logical record boundary, but would also allow subsequent logical records to reference match phrases in previous records, resulting in a minimal loss of compression factor compared to a block based compression.

This is particularly useful for users that have very long and repetitive keys, which is not a problem in SST files, but the WAL files are disproportionately large. It may not be as beneficial if WAL writes are small and more frequently sync'ed to disk.

# Configuration

WAL compression can be enabled by setting the ```wal_compression``` option in ```DBOptions```. At present, only ZSTD compression is supported. This option cannot be dynamically changed. Regardless of the option setting, RocksDB will be able to read compressed WAL files from a previous instance if they exist.