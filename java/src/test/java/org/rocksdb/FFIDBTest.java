package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FFIDBTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  protected void usingFFI(final Function<FFIDB, Void> test) throws RocksDBException {
    final List<ColumnFamilyDescriptor> cfDescriptors =
        Arrays.asList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
            new ColumnFamilyDescriptor("new_cf".getBytes()));
    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try (final DBOptions options =
             new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(
             options, dbFolder.getRoot().getAbsolutePath(), cfDescriptors, columnFamilyHandleList);
         final FFIDB FFIDB = new FFIDB(db)) {
      test.apply(FFIDB);
    }
  }

  @Test
  public void getPinnableSlice() throws RocksDBException {
    usingFFI(dbFFI -> {
      try {
        final RocksDB db = dbFFI.getRocksDB();
        db.put(db.getDefaultColumnFamily(), "key1".getBytes(), "value1".getBytes());
        var getPinnableSlice = dbFFI.getPinnableSlice("key2");
        assertThat(getPinnableSlice.code()).isEqualTo(Status.Code.NotFound);
        getPinnableSlice = dbFFI.getPinnableSlice("key1");
        assertThat(getPinnableSlice.code()).isEqualTo(Status.Code.Ok);
        assertThat(getPinnableSlice.pinnableSlice().get().data()).isNotSameAs(Optional.empty());
        final byte[] bytes = getPinnableSlice.pinnableSlice().get().data().toArray(ValueLayout.JAVA_BYTE);
        getPinnableSlice.pinnableSlice().get().reset();
        assertThat(new String(bytes, StandardCharsets.UTF_8)).isEqualTo("value1");
      } catch (final RocksDBException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
  }

  @Test
  public void get() throws RocksDBException {
    usingFFI(dbFFI -> {
      final RocksDB db = dbFFI.getRocksDB();
      try {
        db.put(db.getDefaultColumnFamily(), "key1".getBytes(), "value1".getBytes());
        var getBytes = dbFFI.get("key2");
        assertThat(getBytes.code()).isEqualTo(Status.Code.NotFound);
        getBytes = dbFFI.get("key1");
        assertThat(getBytes.code()).isEqualTo(Status.Code.Ok);
        assertThat(new String(getBytes.value(), StandardCharsets.UTF_8)).isEqualTo("value1");
      } catch (final RocksDBException e) {
        throw new RuntimeException(e);
      }

      return null;
    });
  }
}
