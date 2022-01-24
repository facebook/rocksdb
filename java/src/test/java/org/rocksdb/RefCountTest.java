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
  public void testIteratorFromClosedCFHandle() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
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
      assertThat(db.isLastReference()).isTrue();
    }
  }

  @Test
  public void testIteratorCreateDelete() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db.isLastReference()).isTrue();
      final RocksIterator iterator = db.newIterator();
      assertThat(db.isLastReference()).isFalse();
      iterator.close();
      assertThat(db.isLastReference()).isTrue();
    }
  }

  @Test
  public void testIteratorCreateDeleteFromCFHandle() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));

      assertThat(db.isLastReference()).isTrue();
      final RocksIterator iteratorCF = db.newIterator(cfHandle);
      assertThat(db.isLastReference()).isFalse();
      assertThat(iteratorCF.isLastReference()).isTrue();
      iteratorCF.close();
      assertThat(db.isLastReference()).isTrue();
    }
  }

  @Test
  public void testUseClosedHandle() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
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
  }

  @Test public void openIteratorCausesAssertionFailure() throws RocksDBException {
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

      assertThat(db.isLastReference()).isFalse();

      // Closing the DB, closes the CF, but the iterator still exists.
      // Assertion failed: (last_ref), function ~ColumnFamilySet, file column_family.cc, line 1494.
    }
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
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             this.dbFolder.getRoot().getAbsolutePath())) {
      db.put("key".getBytes(), "value".getBytes());
      closedDB = db;
    }
    //And we crap out accessing the closed DB
    assertThat(closedDB.get("key".getBytes())).isEqualTo("value".getBytes());
  }

  /**
   * What about when we close the CF after making the iterator ?
   * That's fine, and this one works (we autoclose the iterator after using it).
   */
  @Test
  public void testIterateClosedCF() {
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

        // Closing half way - but it turns out that's fine for the iterator,
        // because the reference to the CF held by the iterator is deeper than the CFH
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

  /**
   * SEGV when creating a new iterator (iterator2) after the column family handle is closed.
   *
   * How to fix this ?
   * Simple version, if we know the handle is closed, we throw an exception in db.newIterator
   * Systematic version, using std::shared_ptr of the handles (column family, iterator)
   */
  @Test
  public void testUseCFAfterClose() {
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


        // Close the CF
        cfHandle.close();

        // Create this after we close the CF, using the closed CF, and SEGV
        final RocksIterator iterator2 = db.newIterator(cfHandle);
      }

    } catch (RocksDBException rocksDBException) {
      rocksDBException.printStackTrace();
      Assert.fail();
    }
  }

  /**
   * What about when we close the CF after making the iterator ?
   * Then we get a C++ assertion
   */
  @Test
  public void testCloseCFWithDanglingIterator() {
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

        // Create this before we close the CF, and it is happily created
        final RocksIterator iterator2 = db.newIterator(cfHandle);

        // Now close the CF - but it turns out that's fine for the (FIRST) iterator,
        // because the reference to the CF held by the iterator is deeper than the CFH
        // It's actually a CFD, down in the bowels of RocksDB.
        cfHandle.close();

        iterator.next();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key2".getBytes());
        assertThat(iterator.value()).isEqualTo("value2".getBytes());

        // Here (autoclosing a CF) is where C++ asserts
        // Assertion failed: (last_ref), function ~ColumnFamilySet, file column_family.cc, line 1494.
      }

    } catch (RocksDBException rocksDBException) {
      rocksDBException.printStackTrace();
      Assert.fail();
    }
  }

  /**
   * What about when we close the CF after making the iterator ?
   * Then we get a C++ assertion
   */
  @Test
  public void testCloseDBWithDanglingIterator() {

    RocksIterator iterator = null;

    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
      db.put(cfHandle, "key1".getBytes(), "value1".getBytes());
      db.put(cfHandle, "key2".getBytes(), "value2".getBytes());

      iterator = db.newIterator(cfHandle);
      iterator.seekToFirst();
      assertThat(iterator.isValid()).isTrue();
      assertThat(iterator.key()).isEqualTo("key1".getBytes());
      assertThat(iterator.value()).isEqualTo("value1".getBytes());

      // Doing this beforehand makes no difference: cfHandle.close();
      // It's the underlying CF object that does the check, not the handle...
      // Closing the DB closes contained CFs, which complains the iterator is still open
      // Assertion failed: (last_ref), function ~ColumnFamilySet, file column_family.cc, line 1494.
      //
      // What could we do instead ?
      // 1. The close() should "succeed" (of course)
      // 2a. The subsequent use of iterator should throw a (Java) exception
      // 2a(1). Because the close has happened fully by the time we return
      // 2b. The subsequent use of iterator should succeed
      // 2b(1). The database close() should finally be enacted only after the iterator is close()d.
      // Because the iterator holds a reference.
      // 2b(2). We need to be aware that the iterator can hold the database open.
      // 2a(2). Holding references to all iterators (as we now do for column family handles)
      // would fix the dangling reference to the database.
      db.close();

      assertThat(iterator.isValid()).isTrue();
      assertThat(iterator.key()).isEqualTo("key2".getBytes());
      assertThat(iterator.value()).isEqualTo("value2".getBytes());

    } catch (RocksDBException rocksDBException) {
      rocksDBException.printStackTrace();
      Assert.fail();
    }
  }

  /**
   * Invalid iterator causes C++ assertion
   */
  @Test
  public void testIteratorNextWithoutInitialSeek() {
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
      db.put(cfHandle, "key1".getBytes(), "value1".getBytes());
      db.put(cfHandle, "key2".getBytes(), "value2".getBytes());

      try (final RocksIterator iterator = db.newIterator(cfHandle)) {
        //Iterator hasn't seek()-ed, so we get a valid assertion in C++
        //iterator.seekToFirst();
        //Assertion failed: (valid_), function Next, file db_iter.cc, line 129.
        iterator.next();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());
        assertThat(iterator.value()).isEqualTo("value1".getBytes());
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

    final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
    columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
    columnFamilyDescriptors.add(new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
    final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
    try (final RocksDB db2 = RocksDB.open(new DBOptions(), dbFolder.getRoot().getAbsolutePath(),
        columnFamilyDescriptors,
        cfHandles)) {
      db2.put(cfHandles.get(1), "key1".getBytes(), "value1".getBytes());

      //The handle we have in our hand is not valid (it was closed with a previous DB open), and a crash happens.
      final byte[] valueBytes = db2.get(cfHandle, "key1".getBytes());

      assertThat(valueBytes).isEqualTo("value1".getBytes());
    }

    assertThat(cfHandle.getName()).isEqualTo(cfHandles.get(1).getName());
    assertThat(cfHandle.getNative()).isEqualTo(cfHandles.get(1).getNative());
  }

  @Test
  public void testClosedCFHandle() throws RocksDBException {

    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
      db.put(cfHandle, "key1".getBytes(), "value1".getBytes());

      // This disposes of the handle, so you might think the get should fail. It doesn't.
      // Could be a timing issue ?
      cfHandle.close();

      // Oh, now this is crashing (SEGV)
      final byte[] valueBytes = db.get(cfHandle, "key1".getBytes());
      assertThat(valueBytes).isEqualTo("value1".getBytes());
      Assert.fail("Why are we able to get() from a closed CF handle ?");
    }
  }

  @Test
  public void test2Databases() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));
      db.put(cfHandle, "key1".getBytes(), "value1".getBytes());

      byte[] bytesFromOtherDB = "not the correct answer".getBytes();
      try (final RocksDB db2 = RocksDB.open(dbFolder2.getRoot().getAbsolutePath())) {

        // We are fetching from a valid open handle on another database.
        // We get null, i.e. the handle doesn't get found.
        bytesFromOtherDB = db2.get(cfHandle, "key1".getBytes());
        Assert.fail("Why does a CFHandle from DB 1 not error on DB 2 ?");
      } catch (RocksDBException exception) {
        assertThat(bytesFromOtherDB).isEqualTo(null);
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
