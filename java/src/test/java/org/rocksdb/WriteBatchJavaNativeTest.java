package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class WriteBatchJavaNativeTest {
  @Parameterized.Parameter(0) public Function<Integer, WriteBatchJavaNative> writeBatchAllocator;

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Function<Integer, WriteBatchJavaNative>> data() {
    List<Function<Integer, WriteBatchJavaNative>> allocators = new ArrayList<>();
    allocators.add(WriteBatchJavaNative::allocate);
    allocators.add(WriteBatchJavaNative::allocateDirect);
    return allocators;
  }

  @Test
  public void put1() throws RocksDBException {
    try (WriteBatchJavaNative wb = writeBatchAllocator.apply(256)) {
      wb.setSequence(100);
      wb.put("k1".getBytes(), "v1".getBytes());
      wb.flush();

      assertThat(new String(getContents(wb), StandardCharsets.UTF_8)).isEqualTo("Put(k1, v1)@100");
    }
  }

  @Test
  public void put1BB() throws RocksDBException {
    try (WriteBatchJavaNative wb = writeBatchAllocator.apply(256)) {
      wb.setSequence(100);
      wb.put(ByteBuffer.wrap("k1".getBytes()), ByteBuffer.wrap("v1".getBytes()));
      wb.flush();

      assertThat(new String(getContents(wb), StandardCharsets.UTF_8)).isEqualTo("Put(k1, v1)@100");
    }
  }

  @Test
  public void putN() throws RocksDBException {
    try (WriteBatchJavaNative wb = writeBatchAllocator.apply(256)) {
      wb.setSequence(100);
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
  public void putNBB() throws RocksDBException {
    try (WriteBatchJavaNative wb = writeBatchAllocator.apply(256)) {
      wb.setSequence(100);
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
  public void putNExpanding() throws RocksDBException {
    try (WriteBatchJavaNative wb = writeBatchAllocator.apply(64)) {
      wb.setSequence(100);
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
  public void putNExpandingBB() throws RocksDBException {
    try (WriteBatchJavaNative wb = writeBatchAllocator.apply(64)) {
      wb.setSequence(100);
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
      try (WriteBatchJavaNative wb = writeBatchAllocator.apply(256)) {
        wb.setSequence(100);
        wb.put("k1".getBytes(), "v1".getBytes());
        wb.put(db.getDefaultColumnFamily(), "k1".getBytes(), "cf_v1".getBytes());
        wb.flush();

        assertThat(new String(getContents(wb), StandardCharsets.UTF_8))
            .isEqualTo("Put(k1, cf_v1)@101Put(k1, v1)@100");
      }
    }
  }

  @Test
  public void putAndFlushAndReadFromDB() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      try (WriteBatchJavaNative wb = writeBatchAllocator.apply(256)) {
        wb.put("k1".getBytes(), "v1".getBytes());
        wb.put("k2".getBytes(), "v2".getBytes());
        db.write(new WriteOptions(), wb);

        byte[] v1 = db.get("k1".getBytes());
        assertThat(v1).isEqualTo("v1".getBytes());
      }
    }
  }

  private String stringOfSize(final int size, final String repeat) {
    if (repeat.isEmpty()) {
      throw new IllegalArgumentException("Append string is empty");
    }
    StringBuilder sb = new StringBuilder();
    while (sb.length() < size) sb.append(repeat);
    return sb.substring(0, size);
  }

  @Test
  public void putTooBigForBuffer() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      try (WriteBatchJavaNative wb = writeBatchAllocator.apply(256)) {
        wb.put("k1".getBytes(), stringOfSize(512, "v1").getBytes());
        db.write(new WriteOptions(), wb);

        byte[] v1 = db.get("k1".getBytes());
        assertThat(v1).isEqualTo(stringOfSize(512, "v1").getBytes());
      }
    }
  }

  @Test
  public void putSmallBigSmall() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      try (WriteBatchJavaNative wb = writeBatchAllocator.apply(256)) {
        // small writes go into the buffer
        wb.put("k0".getBytes(), "v0".getBytes());
        wb.put("k3".getBytes(), "v3".getBytes());
        // large write overflows the buffer, and forces flush of the buffer
        // before the large write happens "direct"
        wb.put("k1".getBytes(), stringOfSize(512, "v1").getBytes());
        // further small write goes to the buffer
        wb.put("k2".getBytes(), "v2".getBytes());
        db.write(new WriteOptions(), wb);

        byte[] v1 = db.get("k1".getBytes());
        assertThat(v1).isEqualTo(stringOfSize(512, "v1").getBytes());
        assertThat("v0".getBytes()).isEqualTo(db.get("k0".getBytes()));
        assertThat("v3".getBytes()).isEqualTo(db.get("k3".getBytes()));
        assertThat("v2".getBytes()).isEqualTo(db.get("k2".getBytes()));
      }
    }
  }

  @Test
  public void put256Bug() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      try (WriteBatchJavaNative wb = writeBatchAllocator.apply(16384)) {
        final int repeat = 10;
        final int keySize = 16;
        final int valueSize = 4096;
        for (int i = 0; i < repeat; i++) {
          wb.put(stringOfSize(keySize, "k" + i).getBytes(),
              stringOfSize(valueSize, "v" + i).getBytes());
        }
        db.write(new WriteOptions(), wb);

        byte[] v1 = db.get(stringOfSize(keySize, "k1").getBytes());
        assertThat(v1).isEqualTo(stringOfSize(valueSize, "v1").getBytes());
        byte[] v9 = db.get(stringOfSize(keySize, "k9").getBytes());
        assertThat(v9).isEqualTo(stringOfSize(valueSize, "v9").getBytes());
      }
    }
  }

  @Test
  public void allocation() throws RocksDBException {
    WriteBatchJavaNative wb1 = WriteBatchJavaNative.allocate(1000);
    wb1.setSequence(42);
    wb1.put("k1".getBytes(),"v1".getBytes());
    wb1.put("k2".getBytes(),"v2".getBytes());
    wb1.flush();
    assertThat(new String(getContents(wb1))).isEqualTo("Put(k1, v1)@42Put(k2, v2)@43");

    WriteBatchJavaNative wb2 = WriteBatchJavaNative.allocate(1000);
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(0);
    wb1.close();
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(1);
    wb2.close();
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(2);

    WriteBatchJavaNative wb3 = WriteBatchJavaNative.allocate(1000);
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(1);

    WriteBatchJavaNative wb4 = WriteBatchJavaNative.allocate(2000);
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(1);
    wb4.close();
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(2);
    WriteBatchJavaNative wb5 = WriteBatchJavaNative.allocate(1000);
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(1);
    WriteBatchJavaNative wb6 = WriteBatchJavaNative.allocate(1000);
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(0);
    wb5.close();
    wb6.close();
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(2);
    WriteBatchJavaNative wb7 = WriteBatchJavaNative.allocate(2000);
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(1);
    WriteBatchJavaNative wb8 = WriteBatchJavaNative.allocate(1000);
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(0);
  }

  @Test
  public void allocationDirect() throws RocksDBException {
    WriteBatchJavaNative wb1 = WriteBatchJavaNative.allocateDirect(1000);
    wb1.setSequence(42);
    wb1.put("k1".getBytes(),"v1".getBytes());
    wb1.put("k2".getBytes(),"v2".getBytes());
    wb1.flush();
    assertThat(new String(getContents(wb1))).isEqualTo("Put(k1, v1)@42Put(k2, v2)@43");

    WriteBatchJavaNative wb2 = WriteBatchJavaNative.allocateDirect(1000);
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(0);
    wb1.close();
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(1);
    wb2.close();
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(2);

    WriteBatchJavaNative wb3 = WriteBatchJavaNative.allocateDirect(1000);
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(1);

    WriteBatchJavaNative wb4 = WriteBatchJavaNative.allocateDirect(2000);
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(1);
    wb4.close();
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(2);
    WriteBatchJavaNative wb5 = WriteBatchJavaNative.allocateDirect(1000);
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(1);
    WriteBatchJavaNative wb6 = WriteBatchJavaNative.allocateDirect(1000);
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(0);
    wb5.close();
    wb6.close();
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(2);
    WriteBatchJavaNative wb7 = WriteBatchJavaNative.allocateDirect(2000);
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(1);
    WriteBatchJavaNative wb8 = WriteBatchJavaNative.allocateDirect(1000);
    assertThat(WriteBatchJavaNative.cacheSize()).isEqualTo(0);
  }

  @After
  public void clearCaches() {
    WriteBatchJavaNative.clearCaches();
  }

  static byte[] getContents(final WriteBatchJavaNative wb) {
    return WriteBatchTest.getContents(wb.getNativeHandle());
  }
}
