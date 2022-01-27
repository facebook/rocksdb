package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

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
  public void testIteratorFromClosedCFHandle() throws RocksDBException {
    WeakDB weakDB = null;
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      weakDB = db.createWeakDB();
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));

      // Closing here means that the following iterator is not valid
      cfHandle.close();

      try {
        db.newIterator(cfHandle);
        Assert.fail("Iterator with a closed handle, we expect it to fail");
      } catch (RocksDBRuntimeException rocksDBRuntimeException) {
        rocksDBRuntimeException.printStackTrace();
        assertThat(rocksDBRuntimeException.getMessage()).contains("RocksDB native reference was previously closed");
      }
    }
    assertThat(weakDB.isDatabaseOpen()).isFalse();
  }

  @Test
  public void testIteratorCreateDelete() throws RocksDBException {
    WeakDB weakDB;
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      weakDB = db.createWeakDB();
      assertThat(db.isLastReference()).isTrue();
      final RocksIterator iterator = db.newIterator();
      assertThat(db.isLastReference()).isFalse();
      iterator.close();
      assertThat(db.isLastReference()).isTrue();
    }
    assertThat(weakDB.isDatabaseOpen()).isFalse();
  }

  @Test
  public void testIteratorCreateDeleteFromCFHandle() throws RocksDBException {
    WeakDB weakDB;
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      weakDB = db.createWeakDB();
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));

      assertThat(db.isLastReference()).isTrue();
      final RocksIterator iteratorCF = db.newIterator(cfHandle);
      assertThat(db.isLastReference()).isFalse();
      assertThat(iteratorCF.isLastReference()).isTrue();
      iteratorCF.close();
      assertThat(db.isLastReference()).isTrue();
    }
    assertThat(weakDB.isDatabaseOpen()).isFalse();
  }

  @Test
  public void testUseClosedHandle() throws RocksDBException {
    WeakDB weakDB = null;
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      weakDB = db.createWeakDB();
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));

      // Closing here means that the following iterator is not valid
      cfHandle.close();

      // The old API used to SEGV here. Now we check for a closed handle.
      db.newIterator(cfHandle);
      Assert.fail("Iterator with a closed handle, we expected it to fail");
    } catch (RocksDBRuntimeException rocksDBRuntimeException) {
      // Expected failure path here.
      assertThat(rocksDBRuntimeException.getMessage()).contains("RocksDB native reference was previously closed");
    }
    assertThat(weakDB.isDatabaseOpen()).isFalse();
  }

  @Test public void openIteratorCausesAssertionFailure() throws RocksDBException {
    WeakDB weakDB = null;
    RocksIterator iterator = null;
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      weakDB = db.createWeakDB();
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
      db.put("key1".getBytes(), "value1".getBytes());
      db.put("key2".getBytes(), "value2".getBytes());

      iterator = db.newIterator(cfHandle);
      iterator.seekToFirst();
      assertThat(iterator.isValid()).isFalse();

      assertThat(db.isLastReference()).isFalse();

      // Closing the DB, closes the CF, but the iterator still exists.
      // Assertion failed: (last_ref), function ~ColumnFamilySet, file column_family.cc, line 1494.
    }
    assertThat(weakDB.isDatabaseOpen()).isTrue();
    iterator.close();
    assertThat(weakDB.isDatabaseOpen()).isFalse();
  }

  /**
   * The default column family is "different".
   *
   * @throws RocksDBException
   */
  @Test
  public void rocksDefaultCF() throws RocksDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             this.dbFolder.getRoot().getAbsolutePath())) {
      db.put("key".getBytes(), "value".getBytes());
      db.getDefaultColumnFamily().close();
    }
  }

  /**
   * Using a DB after we closed it causes a SEGV.
   *
   * @throws RocksDBException
   */
  @Test
  public void rocksDBClosedDBAccess() throws RocksDBException {

    RocksDB closedDB;
    WeakDB weakDB;
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             this.dbFolder.getRoot().getAbsolutePath())) {
      db.put("key".getBytes(), "value".getBytes());
      weakDB = db.createWeakDB();
      closedDB = db;
    }
    assertThat(weakDB.isDatabaseOpen()).isFalse();
    //And we crap out accessing the closed DB
    try {
      assertThat(closedDB.get("key".getBytes())).isEqualTo("value".getBytes());
      fail("Expect an exception because the DB we accessed was closed");
    } catch (RocksDBRuntimeException rocksDBRuntimeException) {
      assertThat(rocksDBRuntimeException.getMessage()).contains("RocksDB native reference was previously closed");
    }
  }

  /**
   * What about when we close the CF after making the iterator ?
   * That's fine, and this one works (we autoclose the iterator after using it).
   */
  @Test
  public void testIterateClosedCF() throws RocksDBException {
    WeakDB weakDB = null;
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      weakDB = db.createWeakDB();
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
      db.put(cfHandle, "key1".getBytes(), "value1".getBytes());
      db.put(cfHandle, "key2".getBytes(), "value2".getBytes());

      try (final RocksIterator iterator = db.newIterator(cfHandle)) {
        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());
        assertThat(iterator.value()).isEqualTo("value1".getBytes());

        // Closing half way - but it turns out that's fine for the iterator,
        // because the reference to the CF held by the iterator is deeper than the CFH
        cfHandle.close();

        iterator.next();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key2".getBytes());
        assertThat(iterator.value()).isEqualTo("value2".getBytes());
      }
    }
    assertThat(weakDB.isDatabaseOpen()).isFalse();
  }

  /**
   * SEGV when creating a new iterator (iterator2) after the column family handle is closed.
   *
   * How to fix this ?
   * Simple version, if we know the handle is closed, we throw an exception in db.newIterator
   * Systematic version, using std::shared_ptr of the handles (column family, iterator)
   */
  @Test
  public void testUseCFAfterClose() throws RocksDBException {
    WeakDB weakDB = null;
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      weakDB = db.createWeakDB();
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
      db.put(cfHandle, "key1".getBytes(), "value1".getBytes());
      db.put(cfHandle, "key2".getBytes(), "value2".getBytes());

      try (final RocksIterator iterator = db.newIterator(cfHandle)) {
        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());
        assertThat(iterator.value()).isEqualTo("value1".getBytes());


        // Close the CF
        cfHandle.close();

        // Create this after we close the CF, using the closed CF, and SEGV
        try {
          final RocksIterator iterator2 = db.newIterator(cfHandle);
          fail("New iterator should throw exception as DB is closed");
        } catch (RocksDBRuntimeException rocksDBRuntimeException) {
          assertThat(rocksDBRuntimeException.getMessage()).contains("RocksDB native reference was previously closed");
        }
      }
    }
    assertThat(weakDB.isDatabaseOpen()).isFalse();
  }

  /**
   * What about when we close the CF after making the iterator ?
   * Then we get a C++ assertion
   */
  @Test
  public void testCloseCFWithDanglingIterator() {

    WeakDB weakDB = null;

    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      weakDB = db.createWeakDB();

      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
      db.put(cfHandle, "key1".getBytes(), "value1".getBytes());
      db.put(cfHandle, "key2".getBytes(), "value2".getBytes());

      try (final RocksIterator iterator = db.newIterator(cfHandle)) {
        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());
        assertThat(iterator.value()).isEqualTo("value1".getBytes());

        // Create this before we close the CF, and it is happily created
        try (final RocksIterator iterator2 = db.newIterator(cfHandle)) {
          iterator2.seekToLast();
          assertThat(iterator2.isValid()).isTrue();
          assertThat(iterator2.key()).isEqualTo("key2".getBytes());
          assertThat(iterator2.value()).isEqualTo("value2".getBytes());
        }

        // Now close the CF - but it turns out that's fine for the (FIRST) iterator,
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

    assertThat(weakDB.isDatabaseOpen()).isFalse();
  }

  /**
   * What about when we close the CF after making the iterator ?
   * Then we get a C++ assertion
   */
  @Test
  public void testCloseDBWithDanglingIterator() {

    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
      db.put(cfHandle, "key1".getBytes(), "value1".getBytes());
      db.put(cfHandle, "key2".getBytes(), "value2".getBytes());

      final WeakDB weakDB;

      try (final RocksIterator iterator = db.newIterator(cfHandle)) {
        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());
        assertThat(iterator.value()).isEqualTo("value1".getBytes());

        // Close the db, but the open iterator should hold a reference, so we can continue to use it.
        // The model is that open iterator(s) prolong the lifetime of the underlying C++ database,
        // it is still "open" after db.close(), until the iterator itself is closed.
        assertThat(db.isLastReference()).isFalse();
        weakDB = db.createWeakDB();
        assertThat(weakDB.isDatabaseOpen()).isTrue();
        db.close();

        // Because the iterator is holding it open
        assertThat(weakDB.isDatabaseOpen()).isTrue();

        assertThat(iterator.isValid()).isTrue();
        iterator.next();
        assertThat(iterator.key()).isEqualTo("key2".getBytes());
        assertThat(iterator.value()).isEqualTo("value2".getBytes());
      }

      // Iterator release has allowed DB to close
      assertThat(weakDB.isDatabaseOpen()).isFalse();

    } catch (RocksDBException rocksDBException) {
      rocksDBException.printStackTrace();
      Assert.fail(rocksDBException.getMessage());
    }
  }

  /**
   * Invalid iterator causes C++ assertion
   */
  @Test
  public void testIteratorNextWithoutInitialSeek() {
    WeakDB weakDB = null;
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
      weakDB = db.createWeakDB();
      db.put(cfHandle, "key1".getBytes(), "value1".getBytes());
      db.put(cfHandle, "key2".getBytes(), "value2".getBytes());

      try (final RocksIterator iterator = db.newIterator(cfHandle)) {
        //Iterator hasn't seek()-ed, so we get a valid assertion in C++
        //Assertion failed: (valid_), function Next, file db_iter.cc, line 129.
        iterator.next();
        fail("Iterator next() without seek() should not be valid");
      } catch (RocksDBException e) {
        assertThat(e.getMessage()).contains("Invalid iterator");
      }

      try (final RocksIterator iterator = db.newIterator(cfHandle)) {
        //Iterator hasn't seek()-ed, so we get a valid assertion in C++
        //Assertion failed: (valid_), function Next, file db_iter.cc, line 129.
        iterator.prev();
        fail("Iterator prev() without seek() should not be valid");
      } catch (RocksDBException e) {
        assertThat(e.getMessage()).contains("Invalid iterator");
      }

      try (final RocksIterator iterator = db.newIterator(cfHandle)) {
        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());
        assertThat(iterator.value()).isEqualTo("value1".getBytes());

        iterator.next();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key2".getBytes());
        assertThat(iterator.value()).isEqualTo("value2".getBytes());
      }

    } catch (RocksDBException rocksDBException) {
      rocksDBException.printStackTrace();
      Assert.fail();
    }
    assertThat(weakDB.isDatabaseOpen()).isFalse();
  }

  @Test
  public void test2CopiesOfColumnFamily() throws RocksDBException {
    WeakDB weakDB = null;
    ColumnFamilyHandle cfHandle = null;
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
      weakDB = db.createWeakDB();
    }
    assertThat(weakDB.isDatabaseOpen()).isFalse();

    final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
    columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
    columnFamilyDescriptors.add(new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
    final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
    try (final RocksDB db2 = RocksDB.open(new DBOptions(), dbFolder.getRoot().getAbsolutePath(),
        columnFamilyDescriptors,
        cfHandles)) {
      weakDB = db2.createWeakDB();
      db2.put(cfHandles.get(1), "key1".getBytes(), "value1".getBytes());

      //The handle we have in our hand is not valid (it was closed with a previous DB open), and a crash happens.
      try {
        final byte[] valueBytes = db2.get(cfHandle, "key1".getBytes());
        fail("Expect exception for already closed cfHandle");
      } catch (RocksDBException rocksDBException) {
        assertThat(rocksDBException.getMessage()).contains("The DB associated with an object is already closed.");
      }

      assertThat(db2.get(cfHandles.get(1), "key1".getBytes())).isEqualTo("value1".getBytes());
      assertThat(cfHandles.get(1).getName()).isEqualTo("new_cf".getBytes());
    }
    assertThat(weakDB.isDatabaseOpen()).isFalse();
  }

  @Test
  public void testClosedCFHandle() throws RocksDBException {

    WeakDB weakDB = null;
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      weakDB = db.createWeakDB();
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
      db.put(cfHandle, "key1".getBytes(), "value1".getBytes());

      // This disposes of the handle, so you might think the get should fail. It doesn't.
      // Could be a timing issue ?
      cfHandle.close();

      try {
        final byte[] valueBytes = db.get(cfHandle, "key1".getBytes());
        Assert.fail("Get to close CF should fail");
      } catch (RocksDBRuntimeException rocksDBRuntimeException) {
        assertThat(rocksDBRuntimeException.getMessage()).contains("RocksDB native reference was previously closed");
      }
    }
    assertThat(weakDB.isDatabaseOpen()).isFalse();
  }

  @Test
  public void test2Databases() throws RocksDBException {

    WeakDB weakDB = null;
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      weakDB = db.createWeakDB();
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
      db.put(cfHandle, "key1".getBytes(), "value1".getBytes());

      assertThat(db.get(cfHandle, "key1".getBytes())).isEqualTo("value1".getBytes());

      byte[] bytesFromOtherDB = "not the correct answer".getBytes();
      try (final RocksDB db2 = RocksDB.open(dbFolder2.getRoot().getAbsolutePath())) {

        // We are fetching from a valid open handle on another database.
        // We introduced some checking code in the get() to ensure the DB matches.
        bytesFromOtherDB = db2.get(cfHandle, "key1".getBytes());
        Assert.fail("Why does a CFHandle from DB 1 not error on DB 2 ?");
      } catch (RocksDBException exception) {
        assertThat(exception.getMessage()).contains("Invalid ColumnFamilyHandle.");
        assertThat(bytesFromOtherDB).isEqualTo("not the correct answer".getBytes());
      }
    }
    assertThat(weakDB.isDatabaseOpen()).isFalse();
  }
}
