This page is intended to set you on the right path to tuning RocksDB if you are a Java user.

Introductory information on how to set up and tune RocksDB as a C++ user is to be found in:
[[Setup Options and Basic Tuning]].
Further, detailed information on how to tune RocksDB as a C++ user is to be found in: [[RocksDB Tuning Guide]]. You should refer to that guide for
definitive information.

## Java Tuning Example

This is a translation of the tuning of two example parameters using Java, instead of C++.
It is based on the one in [[Setup Options and Basic Tuning]].
With this example in hand, you should be able to translate any of the tuning parameters described there into their Java equivalent.

## Write Buffer Size

This can be set either per Database and/or per Column Family.

### Global Write Buffer Size

This is the maximum write buffer size used throughout the database. It is overridden when the option is set for a column family.

It represents the amount of data to build up in memory (backed by an unsorted log on disk) before converting to a sorted on-disk file. The default is 64 MB.

You need to budget for 2 x your worst case memory use. If you don't have enough memory for this, you should reduce this value. Otherwise, it is not recommended to change this option.

In C++ the option is set thus
```C++
options.write_buffer_size = 64 << 20;
```
An equivalent piece of Java, wrapped with database opening, is this:

```Java
    final int WRITE_BUFFER_SIZE = 64 << 20;

    final Options options = new Options().setWriteBufferSize(WRITE_BUFFER_SIZE);

    try (final RocksDB db = RocksDB.open(options,
            dbFolder.getRoot().getAbsolutePath())) {
      
      // do some work

    } catch (final RocksDBException rocksDBException) {
      rocksDBException.printStackTrace();
    }
```
Because this option is a mutable option, you can also set it during a run of the database
```Java
    try (final RocksDB db = RocksDB.open(new Options(),
            dbFolder.getRoot().getAbsolutePath())) {

      // do some work

      final int WRITE_BUFFER_SIZE = 64 << 20;

      final MutableColumnFamilyOptions mutableOptions =
              MutableColumnFamilyOptions.builder()
                      .setWriteBufferSize(WRITE_BUFFER_SIZE)
                      .build();

      db.setOptions(mutableOptions);

    } catch (final RocksDBException rocksDBException) {
      rocksDBException.printStackTrace();
    }
```

### Column Family Write Buffer Size

This is the maximum write buffer size used for an individual Column Family.
You can set this option at database open, or you can modify it later.

In C++ the option is set thus
```C++
cf_options.write_buffer_size = 64 << 20;
```

Set the mutable column family option:
```Java
    try (final RocksDB db = RocksDB.open(new DBOptions(),
            dbFolder.getRoot().getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles)) {

      // do some work

      final int WRITE_BUFFER_SIZE = 64 << 20;

      final MutableColumnFamilyOptions mutableOptions =
              MutableColumnFamilyOptions.builder()
                      .setWriteBufferSize(WRITE_BUFFER_SIZE)
                      .build();

      final ColumnFamilyHandle newColumnFamilyHandle = columnFamilyHandles.get(1);
      db.setOptions(newColumnFamilyHandle, mutableOptions);

    } catch (final RocksDBException rocksDBException) {
      rocksDBException.printStackTrace();
    }
```

## Max Total WAL Size

This option applies to the whole database only.
```C++
options.max_total_wal_size = 64 << 28;
```
You can set it at open
```Java
    final long MAX_TOTAL_WAL_SIZE = 64L << 28;

    final Options options = new Options().setMaxTotalWalSize(MAX_TOTAL_WAL_SIZE);

    try (final RocksDB db = RocksDB.open(options,
            dbFolder.getRoot().getAbsolutePath())) {

      // do some work

    } catch (final RocksDBException rocksDBException) {
      rocksDBException.printStackTrace();
    }
```
or, because it is a mutable *database* option, you can set it at runtime, like this
```Java
    try (final RocksDB db = RocksDB.open(new Options(),
            dbFolder.getRoot().getAbsolutePath())) {

      // do some work

      final long MAX_TOTAL_WAL_SIZE = 64L << 28;

      final MutableDBOptions mutableDBOptions =
              MutableDBOptions.builder()
                      .setMaxTotalWalSize(MAX_TOTAL_WAL_SIZE)
                      .build();

      db.setDBOptions(mutableDBOptions);;

    } catch (final RocksDBException rocksDBException) {
      rocksDBException.printStackTrace();
    }
```


