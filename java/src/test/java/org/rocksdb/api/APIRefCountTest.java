package org.rocksdb.api;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksNativeLibraryResource;
import org.rocksdb.api.*;
import org.rocksdb.api.ColumnFamilyDescriptor;
import org.rocksdb.api.ColumnFamilyHandle;
import org.rocksdb.api.RocksDB;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class APIRefCountTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void testUseClosedHandle() {
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    // have to open default column family
    columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
        org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
    // open the new one, too
    //columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
    //    "new_cf".getBytes(), new ColumnFamilyOptions()));

    try (final RocksDB db = RocksDB.open(new DBOptions(), dbFolder.getRoot().getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles)) {

      assertThat(columnFamilyHandles.size()).isEqualTo(1);

      //final ColumnFamilyHandle cfHandle = db.createColumnFamily(
      //    new ColumnFamilyDescriptor("new_cf".getBytes(StandardCharsets.UTF_8)));

      // Closing here means that the following iterator is not valid
      //cfHandle.close();

      // And we SEGV deep in C++, because the CFHandle is a reference to a deleted CF object.
      //db.newIterator(cfHandle);
      //Assert.fail("Iterator with a closed handle, we expect it to fail");
    } catch (RocksDBException rocksDBException) {
      // Expected failure path here.
      rocksDBException.printStackTrace();
    }
  }
}
