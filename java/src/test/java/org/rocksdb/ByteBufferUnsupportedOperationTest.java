package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.util.ReverseBytewiseComparator;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ByteBufferUnsupportedOperationTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  public static class Handler {
    private final RocksDB database;
    private final Map<UUID, ColumnFamilyHandle> columnFamilies;

    public Handler(final String path, final Options options) throws RocksDBException {
      RocksDB.destroyDB(path, options);
      this.database = RocksDB.open(options, path);
      this.columnFamilies = new ConcurrentHashMap<>();
    }

    public void addTable(final UUID streamID) throws RocksDBException {
      final ColumnFamilyOptions tableOptions = new ColumnFamilyOptions();
      tableOptions.optimizeUniversalStyleCompaction();
      final ComparatorOptions comparatorOptions = new ComparatorOptions();
      //comparatorOptions.setReusedSynchronisationType(ReusedSynchronisationType.ADAPTIVE_MUTEX);
      tableOptions.setComparator(new ReverseBytewiseComparator(comparatorOptions));
      final ColumnFamilyDescriptor tableDescriptor = new ColumnFamilyDescriptor(streamID.toString().getBytes(StandardCharsets.UTF_8), tableOptions);
      final ColumnFamilyHandle tableHandle = database.createColumnFamily(tableDescriptor);
      columnFamilies.put(streamID, tableHandle);
    }

    public void updateAll(final List<byte[][]> keyValuePairs, final UUID streamID)
        throws RocksDBException {
      final ColumnFamilyHandle currTable = columnFamilies.get(streamID);
      try (final WriteBatch batchedWrite = new WriteBatch();
           final WriteOptions writeOptions = new WriteOptions()) {
        for (final byte[][] pair : keyValuePairs) {
          final byte[] keyBytes = pair[0];
          final byte[] valueBytes = pair[1];
          batchedWrite.put(currTable, keyBytes, valueBytes);
        }
        database.write(writeOptions, batchedWrite);
      }
    }
    public boolean containsValue(final byte[] encodedValue, final UUID streamID) {
      try (final RocksIterator iter = database.newIterator(columnFamilies.get(streamID))) {
        iter.seekToFirst();
        while (iter.isValid()) {
          final byte[] val = iter.value();
          if (Arrays.equals(val, encodedValue)) {
            return true;
          }
          iter.next();
        }
      }
      return false;
    }


    public void close() {
      for (final ColumnFamilyHandle handle : columnFamilies.values()) {
        handle.close();
      }
      database.close();
    }
  }

  private void inner(final int n) throws RocksDBException {
    final Options opts = new Options();
    opts.setCreateIfMissing(true);
    final Handler handler = new Handler("testDB", opts);
    final UUID stream1 = UUID.randomUUID();

    final List<byte[][]> entries = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      final byte[] value = value(i);
      final byte[] key = key(i);
      entries.add(new byte[][]{key, value});
    }
    handler.addTable(stream1);
    handler.updateAll(entries, stream1);

    for (int i = 0; i < n; i++) {
      final byte[] val = value(i);
      final boolean hasValue = handler.containsValue(val, stream1);
      if (!hasValue) {
        throw new IllegalStateException("not has value " + i);
      }
    }

    handler.close();
  }

  private static byte[] key(final int i) {
    return ("key" + i).getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] value(final int i) {
    return ("value" + i).getBytes(StandardCharsets.UTF_8);
  }

  @Test public void unsupportedOperation() throws RocksDBException {
    final int n = 10_000;
    final int repeatTest = 100;

    // the error is not always reproducible... let's try to increase the odds by repeating the main test body
    for (int i = 0; i < repeatTest; i++) {
      inner(n);
      System.out.println("done " + i);
    }
  }
}
