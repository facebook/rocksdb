package org.rocksdb.test;

import org.rocksdb.*;
import org.rocksdb.util.ReverseBytewiseComparator;

    import java.nio.charset.StandardCharsets;
    import java.util.ArrayList;
    import java.util.Arrays;
    import java.util.List;
    import java.util.Map;
    import java.util.UUID;
    import java.util.concurrent.ConcurrentHashMap;

public class ByteBufferUnsupportedOperationMain {

  static {
    RocksDB.loadLibrary();
  }

  public static class Handler {

    private final RocksDB database;
    private final Map<UUID, ColumnFamilyHandle> columnFamilies;

    public Handler(String path, Options options) throws RocksDBException {
      RocksDB.destroyDB(path, options);
      this.database = RocksDB.open(options, path);
      this.columnFamilies = new ConcurrentHashMap<>();
    }

    public void addTable(UUID streamID) throws RocksDBException {
      ColumnFamilyOptions tableOptions = new ColumnFamilyOptions();
      tableOptions.optimizeUniversalStyleCompaction();
      ComparatorOptions comparatorOptions = new ComparatorOptions();
      //comparatorOptions.setReusedSynchronisationType(ReusedSynchronisationType.ADAPTIVE_MUTEX);
      tableOptions.setComparator(new ReverseBytewiseComparator(comparatorOptions));
      ColumnFamilyDescriptor tableDescriptor = new ColumnFamilyDescriptor(streamID.toString().getBytes(StandardCharsets.UTF_8), tableOptions);
      ColumnFamilyHandle tableHandle = database.createColumnFamily(tableDescriptor);
      columnFamilies.put(streamID, tableHandle);
    }

    public void updateAll(List<byte[][]> keyValuePairs, UUID streamID)
        throws RocksDBException {
      ColumnFamilyHandle currTable = columnFamilies.get(streamID);
      try (WriteBatch batchedWrite = new WriteBatch();
           WriteOptions writeOptions = new WriteOptions()) {
        for (byte[][] pair : keyValuePairs) {
          byte[] keyBytes = pair[0];
          byte[] valueBytes = pair[1];
          batchedWrite.put(currTable, keyBytes, valueBytes);
        }
        database.write(writeOptions, batchedWrite);
      }
    }

    // a silly loop trying to locale value
    public boolean containsValue(byte[] encodedValue, UUID streamID) {
      try (RocksIterator iter = database.newIterator(columnFamilies.get(streamID))) {
        iter.seekToFirst();
        while (iter.isValid()) {
          byte[] val = iter.value();
          if (Arrays.equals(val, encodedValue)) {
            return true;
          }
          iter.next();
        }
      }
      return false;
    }


    public void close() {
      for (ColumnFamilyHandle handle : columnFamilies.values()) {
        handle.close();
      }
      database.close();
    }
  }

  private static void test(int n) throws RocksDBException {
    Options opts = new Options();
    opts.setCreateIfMissing(true);
    Handler handler = new Handler("testDB", opts);
    UUID stream1 = UUID.randomUUID();

    List<byte[][]> entries = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      byte[] value = value(i);
      byte[] key = key(i);
      entries.add(new byte[][]{key, value});
    }
    handler.addTable(stream1);
    handler.updateAll(entries, stream1);

    for (int i = 0; i < n; i++) {
      byte[] val = value(i);
      boolean hasValue = handler.containsValue(val, stream1);
      if (!hasValue) {
        throw new IllegalStateException("not has value " + i);
      }
    }

    handler.close();
  }

  private static byte[] key(int i) {
    return ("key" + i).getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] value(int i) {
    return ("value" + i).getBytes(StandardCharsets.UTF_8);
  }

  public static void main(String[] args) throws RocksDBException {
    int n = 1_000;
    int repeatTest = 100;

    // the error is not always reproducible... let's try to increase the odds by repeating the main test body
    for (int i = 0; i < repeatTest; i++) {
      test(n);
      System.out.println("done " + i);
    }
  }

}

