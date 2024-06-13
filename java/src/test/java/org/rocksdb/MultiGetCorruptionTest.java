package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class MultiGetCorruptionTest {
  private static final byte[] KEY = "key".getBytes(UTF_8);
  private static final byte[] VALUE;

  static {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      sb.append("value" + i + "\n");
    }
    VALUE = sb.toString().getBytes(UTF_8);
  }

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void getKeyException() throws RocksDBException, IOException {
    createCorruptedDatabase();
    try (Options options = new Options().setCreateIfMissing(true).setParanoidChecks(true);
         RocksDB db = RocksDB.openReadOnly(options, dbFolder.getRoot().getAbsolutePath())) {
      exception.expect(RocksDBException.class); // We need to be sure, exception is thrown only
      db.get(KEY); //   on GET operation. Require careful data corruption.
    }
  }

  @Test
  public void multiGetKeyException() throws RocksDBException, IOException {
    createCorruptedDatabase();
    try (Options options = new Options().setCreateIfMissing(true).setParanoidChecks(true);
         RocksDB db = RocksDB.openReadOnly(options, dbFolder.getRoot().getAbsolutePath())) {
      // exception.expect(RocksDBException.class);
      List<byte[]> keys = new ArrayList<>();
      keys.add(KEY);
      List<byte[]> result = db.multiGetAsList(keys);
      assertThat(result).isNotNull();
    }
  }

  private void createCorruptedDatabase() throws RocksDBException, IOException {
    try (Options options = new Options().setCreateIfMissing(true).setParanoidChecks(true);
         RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      db.put(KEY, VALUE);
      db.flush(new FlushOptions().setWaitForFlush(true));
    }

    File[] files = dbFolder.getRoot().listFiles((dir, name) -> name.endsWith("sst"));
    assertThat(files).hasSize(1);

    try (RandomAccessFile file = new RandomAccessFile(files[0], "rw")) {
      file.seek(30);
      file.write("corrupted".getBytes(UTF_8));
    }
  }
}
