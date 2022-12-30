package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class DBFFITest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  protected void usingFFI(final Function<DBFFI, Void> test) throws RocksDBException {
    final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes()));
    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
             columnFamilyHandleList); final DBFFI dbffi = new DBFFI(db)) {
      test.apply(dbffi);
    }
  }

  @Test public void get() throws RocksDBException {
    usingFFI(dbFFI -> {
      try {
        final RocksDB db = dbFFI.getRocksDB();
        db.put(db.getDefaultColumnFamily(), "key1".getBytes(), "value1".getBytes());
        dbFFI.get("key1");
        dbFFI.get("key2");
      } catch (final RocksDBException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
  }
}
