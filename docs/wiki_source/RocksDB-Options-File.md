In RocksDB 4.3, we add a set of features that makes managing RocksDB options easier.

1. Each RocksDB database will now automatically persist its current set of options into a file on every successful call of DB::Open(), SetOptions(), and CreateColumnFamily() / DropColumnFamily().

2. [LoadLatestOptions() / LoadOptionsFromFile()](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/utilities/options_util.h#L20-L58): A function that constructs RocksDB options object from an options file.

3. [CheckOptionsCompatibility](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/utilities/options_util.h#L64-L77): A function that performs compatibility check on two sets of RocksDB options.

With the above options file support, developers no longer need to maintain the full set of options of a previously-created RocksDB instance.  In addition, when changing options is needed, CheckOptionsCompatibility() can further make sure the resulting set of Options can successfully open the same RocksDB database without corrupting the underlying data.

## Example
Here's a running example showing how the new features can make managing RocksDB options easier.  A more complete example can be found in [examples/options_file_example.cc](https://github.com/facebook/rocksdb/blob/main/examples/options_file_example.cc).

Suppose we open a RocksDB database, create a new column family on-the-fly while the database is running, and then close the database:

    s = DB::Open(rocksdb_options, path_to_db, &db);
    ...
    // Create column family, and rocksdb will persist the options.
    ColumnFamilyHandle* cf;
    s = db->CreateColumnFamily(ColumnFamilyOptions(), "new_cf", &cf);
    ...
    // close DB
    delete cf;
    delete db;

Since in RocksDB 4.3 or later, each RocksDB instance will automatically store its latest set of options into a options file, we can use that file to construct the options next time when we want to open the DB.  This is different from RocksDB 4.2 or older version where we need to remember all the options of each the column families in order to successfully open a DB.  Now let's see how it works.

First, we call LoadLatestOptions() to load the latest set of options used by the target RocksDB database:

    ConfigOptions cfg_opts;
    DBOptions loaded_db_opt;
    std::vector<ColumnFamilyDescriptor> loaded_cf_descs;
    LoadLatestOptions(cfg_opts, path_to_db, &loaded_db_opt, &loaded_cf_descs);

## Unsupported Options

Since C++ does not have reflection, the following user-defined functions and pointer-typed options will only be initialized with default values.  Detailed information can be found in rocksdb/utilities/options_util.h:

    * env
    * memtable_factory
    * compaction_filter_factory
    * prefix_extractor
    * comparator
    * merge_operator
    * compaction_filter

For those un-supported user-defined functions, developers will need to specify them manually.  In this example, we initialize Cache in BlockBasedTableOptions and CompactionFilter:

    for (size_t i = 0; i < loaded_cf_descs.size(); ++i) {
      auto* loaded_bbt_opt = loaded_cf_descs[0].options.table_factory->GetOptions<BlockBasedTableOptions>());
      loaded_bbt_opt->block_cache = cache;
    }

    loaded_cf_descs[0].options.compaction_filter = new MyCompactionFilter();

Now we perform sanity check to make sure the set of options is safe to open the target database:

    Status s = CheckOptionsCompatibility(cfg_opts, kDBPath, db_options, loaded_cf_descs);

If the return value indicates OK status, we can proceed and use the loaded set of options to open the target RocksDB database:

    s = DB::Open(loaded_db_opt, kDBPath, loaded_cf_descs, &handles, &db);

## Ignoring unknown options
In cases where an options file of a newer version is used with an older RocksDB version (say, when downgrading due to a bug), the older RocksDB version might not know about some newer options. `ignore_unknown_options` flag can be used to handle such cases. By setting the `ConfigOptions.ignore_unknown_options=true`, unknown options will be ignored. By default it is set to `false`.

# RocksDB Options File Format
RocksDB options file is a text file that follows the [INI file format](https://en.wikipedia.org/wiki/INI_file).  Each RocksDB options file has one Version section, one DBOptions section, and one CFOptions and TableOptions section for each column family.  Below is an example RocksDB options file.  A complete example can be found in [examples/rocksdb_option_file_example.ini](https://github.com/facebook/rocksdb/blob/main/examples/rocksdb_option_file_example.ini):

    [Version]
      rocksdb_version=4.3.0
      options_file_version=1.1
    [DBOptions]
      stats_dump_period_sec=600
      max_manifest_file_size=18446744073709551615
      bytes_per_sync=8388608
      delayed_write_rate=2097152
      WAL_ttl_seconds=0
      ...
    [CFOptions "default"]
      compaction_style=kCompactionStyleLevel
      compaction_filter=nullptr
      num_levels=6
      table_factory=BlockBasedTable
      comparator=leveldb.BytewiseComparator
      compression_per_level=kNoCompression:kNoCompression:kNoCompression:kSnappyCompression:kSnappyCompression:kSnappyCompression
      ...
    [TableOptions/BlockBasedTable "default"]
      format_version=2
      whole_key_filtering=true
      skip_table_builder_flush=false
      no_block_cache=false
      checksum=kCRC32c
      filter_policy=rocksdb.BuiltinBloomFilter
      ....