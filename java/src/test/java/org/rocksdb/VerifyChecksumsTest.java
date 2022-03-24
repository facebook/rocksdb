package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.text.MessageFormat;

import static org.assertj.core.api.Assertions.assertThat;

public class VerifyChecksumsTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void verifyChecksums() throws RocksDBException {

    final String dbPath = dbFolder.getRoot().getAbsolutePath();

    //noinspection SingleStatementInBlock
    try (final Statistics statistics = new Statistics(); final Options options =
        new Options()
            .setCreateIfMissing(true)
            .setStatistics(statistics)) {
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        for (int i = 0; i < 1000000; i++) {
          //noinspection ObjectAllocationInLoop
          db.put((MessageFormat.format("key{0}", i)).getBytes(), (MessageFormat.format("value{0}", i)).getBytes());
        }
        db.flush(new FlushOptions());
      }

      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        final ReadOptions readOptions = new ReadOptions();
        readOptions.setReadaheadSize(32 * 1024);
        readOptions.setFillCache(false);
        //readOptions.setVerifyChecksums(false);
        try (final RocksIterator rocksIterator = db.newIterator(readOptions)) {
          rocksIterator.seekToFirst();
          rocksIterator.status();
          while (rocksIterator.isValid()) {
            byte[] k = rocksIterator.key();
            byte[] v = rocksIterator.value();
            rocksIterator.next();
            rocksIterator.status();
          }

        }
        assertThat(statistics.getTickerCount(TickerType.VERIFY_CHECKSUM_READ_BYTES)).isEqualTo(43);
      }
    }
  }
}
