Since RocksDB version 3.1, we started defining version macros in `include/rocksdb/version.h`:

    #define ROCKSDB_MAJOR <major version>
    #define ROCKSDB_MINOR <minor version>
    #define ROCKSDB_PATCH <patch version>

That way, you can make your code compile and work with multiple versions of RocksDB, even though we change some of the API you might be using.