package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RefCountTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();
  @Rule public TemporaryFolder dbFolder2 = new TemporaryFolder();

  @Test
  public void testUseClosedHandle() {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));

      // Closing here means that the following iterator is not valid
      cfHandle.close();

      db.newIterator(cfHandle);
    } catch (RocksDBException rocksDBException) {
      rocksDBException.printStackTrace();
      Assert.fail();
    }
  }

  @Test public void emptyIteratorFailsSeek() throws RocksDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
      db.put("key1".getBytes(), "value1".getBytes());
      db.put("key2".getBytes(), "value2".getBytes());

      final RocksIterator iterator = db.newIterator(cfHandle);
      iterator.seekToFirst();
      assertThat(iterator.isValid()).isFalse();
    }
  }

  /**
   * What about when we close the CF after making the iterator ?
   */
  @Test
  public void testUseClosedLaterHandle() {
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
      db.put(cfHandle, "key1".getBytes(), "value1".getBytes());
      db.put(cfHandle, "key2".getBytes(), "value2".getBytes());

      try (final RocksIterator iterator = db.newIterator(cfHandle)) {
        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());
        assertThat(iterator.value()).isEqualTo("value1".getBytes());

        // Create this before we close the CF, and we're OK
        // Except that if we haven't closed it at the end (closing the DB), we get:
        // Assertion failed: (last_ref), function ~ColumnFamilySet, file column_family.cc, line 1494.
        final RocksIterator iterator2 = db.newIterator(cfHandle);
        // Closing half way - but it turns out that's completely fine
        // because the reference to the CF held by the iterator is deeper than the CFH
        // It's actually a CFD, down in the bowels of RocksDB.
        cfHandle.close();

        iterator.next();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key2".getBytes());
        assertThat(iterator.value()).isEqualTo("value2".getBytes());
      }

    } catch (RocksDBException rocksDBException) {
      rocksDBException.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void test2CopiesOfColumnFamily() throws RocksDBException {
    ColumnFamilyHandle cfHandle = null;
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
    }

    final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
    try (final RocksDB db2 = RocksDB.open(new DBOptions(), dbFolder.getRoot().getAbsolutePath(),
        List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
            new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8))),
        cfHandles)) {
      db2.put(cfHandles.get(1), "key1".getBytes(), "value1".getBytes());

      //The handle we have in our hand is not valid (it was closed with a previous DB open), and a crash happens.
      final byte[] valueBytes = db2.get(cfHandle, "key1".getBytes());

      assertThat(valueBytes).isEqualTo("value1".getBytes());
    }

    assertThat(cfHandle.getName()).isEqualTo(cfHandles.get(1).getName());
    assertThat(cfHandle.nativeHandle_).isEqualTo(cfHandles.get(1).nativeHandle_);
  }

  @Test
  public void testClosedCFHandle() throws RocksDBException {

    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
      db.put(cfHandle, "key1".getBytes(), "value1".getBytes());

      // This disposes of the handle, so the get should fail. It doesn't.
      cfHandle.close();
      final byte[] valueBytes = db.get(cfHandle, "key1".getBytes());
      assertThat(valueBytes).isEqualTo("value1".getBytes());
    }
  }

  @Test
  public void test2Databases() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
      db.put(cfHandle, "key1".getBytes(), "value1".getBytes());
      try (final RocksDB db2 = RocksDB.open(dbFolder2.getRoot().getAbsolutePath())) {

        // We are fetching from a valid open handle on another database.
        // We get null, i.e. the handle doesn't get found.
        final byte[] bytesFromOtherDB = db2.get(cfHandle, "key1".getBytes());
        Assert.fail("Why no error ?");
      }
    }
  }

  @Test
  public void test2DatabasesClose() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
      db.put(cfHandle, "key1".getBytes(), "value1".getBytes());
      try (final RocksDB db2 = RocksDB.open(dbFolder2.getRoot().getAbsolutePath())) {

        // We are fetching from a valid open handle on another database.
        // We get null, i.e. the handle doesn't get found.
        final byte[] bytesFromOtherDB = db2.get(cfHandle, "key1".getBytes());
        Assert.fail("Why no error ?");
      }
    }
  }

  @Test
  public void readOptions() {
    ReadOptions readOptions = new ReadOptions();
    ReadOptions readOptions1 = new ReadOptions(readOptions);

    // They don't share the same underlying read options. Fair enough.
    assertThat(readOptions.nativeHandle_).isEqualTo(readOptions1.nativeHandle_);
  }
}
