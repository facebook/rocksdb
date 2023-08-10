package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class KeyExistTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  List<ColumnFamilyDescriptor> cfDescriptors;
  List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
  RocksDB db;
  @Before
  public void before() throws RocksDBException {
    cfDescriptors = Arrays.asList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes()));
    final DBOptions options =
        new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);

    db = RocksDB.open(
        options, dbFolder.getRoot().getAbsolutePath(), cfDescriptors, columnFamilyHandleList);
  }

  @After
  public void after() {
    for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
      columnFamilyHandle.close();
    }
    db.close();
  }

  @Test
  public void keyExist() throws RocksDBException {
    db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));
    boolean exists = db.keyExist("key".getBytes(UTF_8));
    assertThat(exists).isTrue();
    exists = db.keyExist("key2".getBytes(UTF_8));
    assertThat(exists).isFalse();
  }

  @Test
  public void keyExistColumnFamily() throws RocksDBException {
    byte[] key1 = "keyBBCF0".getBytes(UTF_8);
    byte[] key2 = "keyBBCF1".getBytes(UTF_8);
    db.put(columnFamilyHandleList.get(0), key1, "valueBBCF0".getBytes(UTF_8));
    db.put(columnFamilyHandleList.get(1), key2, "valueBBCF1".getBytes(UTF_8));

    assertThat(db.keyExist(columnFamilyHandleList.get(0), key1)).isTrue();
    assertThat(db.keyExist(columnFamilyHandleList.get(0), key2)).isFalse();

    assertThat(db.keyExist(columnFamilyHandleList.get(1), key1)).isFalse();
    assertThat(db.keyExist(columnFamilyHandleList.get(1), key2)).isTrue();
  }

  @Test
  public void keyExistReadOptions() throws RocksDBException {
    try (final ReadOptions readOptions = new ReadOptions()) {
      db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));
      boolean exists = db.keyExist(readOptions, "key".getBytes(UTF_8));
      assertThat(exists).isTrue();
      exists = db.keyExist("key2".getBytes(UTF_8));
      assertThat(exists).isFalse();
    }
  }

  @Test
  public void keyExistAfterDelete() throws RocksDBException {
    db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));
    boolean exists = db.keyExist(null, null, "key".getBytes(UTF_8), 0, 3);
    assertThat(exists).isTrue();
    db.delete("key".getBytes(UTF_8));
    exists = db.keyExist(null, null, "key".getBytes(UTF_8), 0, 3);
    assertThat(exists).isFalse();
  }

  @Test
  public void keyExistArrayIndexOutOfBoundsException() throws RocksDBException {
    db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));
    exceptionRule.expect(ArrayIndexOutOfBoundsException.class);
    db.keyExist(null, null, "key".getBytes(UTF_8), 0, 5);
  }

  @Test
  public void keyExistArrayIndexOutOfBoundsExceptionWrongOffset() throws RocksDBException {
    db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));
    exceptionRule.expect(ArrayIndexOutOfBoundsException.class);
    db.keyExist(null, null, "key".getBytes(UTF_8), 6, 2);
  }
}
