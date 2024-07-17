package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WriteBatchJavaNativeTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void put1() throws RocksDBException {
    try (WriteBatchJavaNative wb = new WriteBatchJavaNative(256)) {
      WriteBatchTestInternalHelper.setSequence(wb.getWriteBatch(), 100);
      wb.put("k1".getBytes(), "v1".getBytes());
      wb.flush();

      assertThat(new String(getContents(wb), StandardCharsets.UTF_8)).isEqualTo("Put(k1, v1)@100");
    }
  }

  @Test
  public void putN() throws RocksDBException, UnsupportedEncodingException {
    try (WriteBatchJavaNative wb = new WriteBatchJavaNative(256)) {
      WriteBatchTestInternalHelper.setSequence(wb.getWriteBatch(), 100);
      wb.put("k1".getBytes(), "v1".getBytes());
      wb.put("k02".getBytes(), "v02".getBytes());
      wb.put("k03".getBytes(), "v03".getBytes());
      wb.put("k04".getBytes(), "v04".getBytes());
      wb.put("k05".getBytes(), "v05".getBytes());
      wb.put("k06".getBytes(), "v06".getBytes());
      wb.put("k07".getBytes(), "v07".getBytes());
      wb.put("k08".getBytes(), "v08".getBytes());
      wb.put("k09".getBytes(), "v09".getBytes());
      wb.put("k10".getBytes(), "v10".getBytes());

      wb.flush();

      assertThat(new String(getContents(wb), StandardCharsets.UTF_8))
          .isEqualTo("Put(k02, v02)@101Put(k03, v03)@102Put(k04, v04)@103Put(k05, v05)@104Put(k06, v06)@105" +
              "Put(k07, v07)@106Put(k08, v08)@107Put(k09, v09)@108Put(k1, v1)@100Put(k10, v10)@109");
    }
  }

  @Test
  public void putCF() throws RocksDBException {
    final ColumnFamilyDescriptor newColumnFamily =
        new ColumnFamilyDescriptor("WriteBatchJavaNativeTest".getBytes(StandardCharsets.UTF_8));
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
         final ColumnFamilyHandle cf = db.createColumnFamily(newColumnFamily)) {
      try (WriteBatchJavaNative wb = new WriteBatchJavaNative(256)) {
        WriteBatchTestInternalHelper.setSequence(wb.getWriteBatch(), 100);
        wb.put("k1".getBytes(), "v1".getBytes());
        wb.put(cf, "k1".getBytes(), "cf_v1".getBytes());
        wb.flush();

        assertThat(new String(getContents(wb), StandardCharsets.UTF_8)).isEqualTo("Put(k1, v1)@100");
      }
    }
  }

  static byte[] getContents(final WriteBatchJavaNative wb) {
    return WriteBatchTest.getContents(wb.nativeHandle_);
  }
}
