package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class WriteBatchJavaNativeTest {
  @Parameterized.Parameter(0) public Class<? extends WriteBatchJavaNative> writeBatchWrapperClass;

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  private WriteBatchJavaNative construct(int reserved_bytes) {
    try {
      return writeBatchWrapperClass.getDeclaredConstructor(int.class).newInstance(reserved_bytes);
    } catch (Exception e) {
      throw new RuntimeException(
          "Exception calling constructor invoked by unit test for " + getClass().getName(), e);
    }
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        new Object[][] {{WriteBatchJavaNativeArray.class}, {WriteBatchJavaNativeDirect.class}});
  }

  @Test
  public void put1() throws RocksDBException {
    try (WriteBatchJavaNative wb = construct(256)) {
      WriteBatchTestInternalHelper.setSequence(wb.getWriteBatch(), 100);
      wb.put("k1".getBytes(), "v1".getBytes());
      wb.flush();

      assertThat(new String(getContents(wb), StandardCharsets.UTF_8)).isEqualTo("Put(k1, v1)@100");
    }
  }

  @Test
  public void put1BB() throws RocksDBException {
    try (WriteBatchJavaNative wb = construct(256)) {
      WriteBatchTestInternalHelper.setSequence(wb.getWriteBatch(), 100);
      wb.put(ByteBuffer.wrap("k1".getBytes()), ByteBuffer.wrap("v1".getBytes()));
      wb.flush();

      assertThat(new String(getContents(wb), StandardCharsets.UTF_8)).isEqualTo("Put(k1, v1)@100");
    }
  }

  @Test
  public void putN() throws RocksDBException, UnsupportedEncodingException {
    try (WriteBatchJavaNative wb = construct(256)) {
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
          .isEqualTo("Put(k02, v02)@101Put(k03, v03)@102Put(k04, v04)@103Put(k05, v05)@104Put(k06, "
              + "v06)@105"
              + "Put(k07, v07)@106Put(k08, v08)@107Put(k09, v09)@108Put(k1, v1)@100Put(k10, "
              + "v10)@109");
    }
  }

  @Test
  public void putNBB() throws RocksDBException, UnsupportedEncodingException {
    try (WriteBatchJavaNative wb = construct(256)) {
      WriteBatchTestInternalHelper.setSequence(wb.getWriteBatch(), 100);
      wb.put(ByteBuffer.wrap("k1".getBytes()), ByteBuffer.wrap("v1".getBytes()));
      wb.put(ByteBuffer.wrap("k02".getBytes()), ByteBuffer.wrap("v02".getBytes()));
      wb.put(ByteBuffer.wrap("k03".getBytes()), ByteBuffer.wrap("v03".getBytes()));
      wb.put(ByteBuffer.wrap("k04".getBytes()), ByteBuffer.wrap("v04".getBytes()));
      wb.put(ByteBuffer.wrap("k05".getBytes()), ByteBuffer.wrap("v05".getBytes()));
      wb.put(ByteBuffer.wrap("k06".getBytes()), ByteBuffer.wrap("v06".getBytes()));
      wb.put(ByteBuffer.wrap("k07".getBytes()), ByteBuffer.wrap("v07".getBytes()));
      wb.put(ByteBuffer.wrap("k08".getBytes()), ByteBuffer.wrap("v08".getBytes()));
      wb.put(ByteBuffer.wrap("k09".getBytes()), ByteBuffer.wrap("v09".getBytes()));
      wb.put(ByteBuffer.wrap("k10".getBytes()), ByteBuffer.wrap("v10".getBytes()));

      wb.flush();

      assertThat(new String(getContents(wb), StandardCharsets.UTF_8))
          .isEqualTo("Put(k02, v02)@101Put(k03, v03)@102Put(k04, v04)@103Put(k05, v05)@104Put(k06, "
              + "v06)@105"
              + "Put(k07, v07)@106Put(k08, v08)@107Put(k09, v09)@108Put(k1, v1)@100Put(k10, "
              + "v10)@109");
    }
  }

  @Test
  public void putNExpanding() throws RocksDBException, UnsupportedEncodingException {
    try (WriteBatchJavaNative wb = construct(16)) {
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
          .isEqualTo("Put(k02, v02)@101Put(k03, v03)@102Put(k04, v04)@103Put(k05, v05)@104Put(k06, "
              + "v06)@105"
              + "Put(k07, v07)@106Put(k08, v08)@107Put(k09, v09)@108Put(k1, v1)@100Put(k10, "
              + "v10)@109");
    }
  }

  @Test
  public void putNExpandingBB() throws RocksDBException, UnsupportedEncodingException {
    try (WriteBatchJavaNative wb = construct(16)) {
      WriteBatchTestInternalHelper.setSequence(wb.getWriteBatch(), 100);
      wb.put(ByteBuffer.wrap("k1".getBytes()), ByteBuffer.wrap("v1".getBytes()));
      wb.put(ByteBuffer.wrap("k02".getBytes()), ByteBuffer.wrap("v02".getBytes()));
      wb.put(ByteBuffer.wrap("k03".getBytes()), ByteBuffer.wrap("v03".getBytes()));
      wb.put(ByteBuffer.wrap("k04".getBytes()), ByteBuffer.wrap("v04".getBytes()));
      wb.put(ByteBuffer.wrap("k05".getBytes()), ByteBuffer.wrap("v05".getBytes()));
      wb.put(ByteBuffer.wrap("k06".getBytes()), ByteBuffer.wrap("v06".getBytes()));
      wb.put(ByteBuffer.wrap("k07".getBytes()), ByteBuffer.wrap("v07".getBytes()));
      wb.put(ByteBuffer.wrap("k08".getBytes()), ByteBuffer.wrap("v08".getBytes()));
      wb.put(ByteBuffer.wrap("k09".getBytes()), ByteBuffer.wrap("v09".getBytes()));
      wb.put(ByteBuffer.wrap("k10".getBytes()), ByteBuffer.wrap("v10".getBytes()));

      wb.flush();

      assertThat(new String(getContents(wb), StandardCharsets.UTF_8))
          .isEqualTo("Put(k02, v02)@101Put(k03, v03)@102Put(k04, v04)@103Put(k05, v05)@104Put(k06, "
              + "v06)@105"
              + "Put(k07, v07)@106Put(k08, v08)@107Put(k09, v09)@108Put(k1, v1)@100Put(k10, "
              + "v10)@109");
    }
  }

  @Test
  public void putCF() throws RocksDBException {
    final ColumnFamilyDescriptor newColumnFamily =
        new ColumnFamilyDescriptor("WriteBatchJavaNativeTest".getBytes(StandardCharsets.UTF_8));
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
         final ColumnFamilyHandle cf = db.createColumnFamily(newColumnFamily)) {
      try (WriteBatchJavaNative wb = construct(256)) {
        WriteBatchTestInternalHelper.setSequence(wb.getWriteBatch(), 100);
        wb.put("k1".getBytes(), "v1".getBytes());
        wb.put(db.getDefaultColumnFamily(), "k1".getBytes(), "cf_v1".getBytes());
        wb.flush();

        assertThat(new String(getContents(wb), StandardCharsets.UTF_8))
            .isEqualTo("Put(k1, cf_v1)@101Put(k1, v1)@100");
      }
    }
  }

  @Test public void putAndFlushAndReadFromDB() throws RocksDBException {

    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      try (WriteBatchJavaNative wb = construct(256)) {
        wb.put("k1".getBytes(), "v1".getBytes());
        wb.put("k2".getBytes(), "v2".getBytes());
        wb.flush();
        db.write(new WriteOptions(), wb);
        byte[] v1 = db.get("k1".getBytes());
        assertThat(v1).isEqualTo("v1".getBytes());
      }
    }
  }

  static byte[] getContents(final WriteBatchJavaNative wb) {
    return WriteBatchTest.getContents(wb.nativeHandle_);
  }
}
