package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class VerifyChecksumsTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @SuppressWarnings("ObjectAllocationInLoop")
  @Test
  public void verifyChecksums() throws RocksDBException {
    final String dbPath = dbFolder.getRoot().getAbsolutePath();

    final int KV_COUNT = 10000;
    final List<String> elements = new ArrayList<>();
    for (int i = 0; i < KV_COUNT; i++) elements.add(MessageFormat.format("{0,number,#}", i));
    final List<String> sortedElements = new ArrayList<>(elements);
    Collections.sort(sortedElements);

    // noinspection SingleStatementInBlock
    try (final Statistics statistics = new Statistics();
         final Options options = new Options().setCreateIfMissing(true).setStatistics(statistics)) {
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        for (int i = 0; i < KV_COUNT; i++) {
          // noinspection ObjectAllocationInLoop
          final String key = MessageFormat.format("key{0}", elements.get(i));
          final String value = MessageFormat.format("value{0}", elements.get(i));
          System.out.println(key + "-->" + value);
          db.put(key.getBytes(), value.getBytes());
        }
        db.flush(new FlushOptions());
      }

      final long beforeChecksumComputeCount =
          statistics.getTickerCount(TickerType.BLOCK_CHECKSUM_COMPUTE_COUNT);

      for (final boolean verifyFlag : new boolean[] {false, true}) {
        try (final RocksDB db = RocksDB.open(options, dbPath)) {
          final ReadOptions readOptions = new ReadOptions();
          readOptions.setReadaheadSize(32 * 1024);
          readOptions.setFillCache(false);
          readOptions.setVerifyChecksums(verifyFlag);
          int i = 0;
          try (final RocksIterator rocksIterator = db.newIterator(readOptions)) {
            rocksIterator.seekToFirst();
            rocksIterator.status();
            while (rocksIterator.isValid()) {
              final byte[] key = rocksIterator.key();
              final byte[] value = rocksIterator.value();
              System.out.println(new String(key) + "-->" + new String(value));
              assertThat(key).isEqualTo(
                  (MessageFormat.format("key{0}", sortedElements.get(i))).getBytes());
              assertThat(value).isEqualTo(
                  (MessageFormat.format("value{0}", sortedElements.get(i))).getBytes());
              rocksIterator.next();
              rocksIterator.status();
              i++;
            }
          }
          assertThat(i).isEqualTo(KV_COUNT);
          if (verifyFlag) {
            // We don't need to be exact - we are checking that the checksums happen
            // exactly how many depends on block size etc etc, so may not be entirely stable
            assertThat(statistics.getTickerCount(TickerType.BLOCK_CHECKSUM_COMPUTE_COUNT))
                .isGreaterThan(beforeChecksumComputeCount + 20);
          } else {
            assertThat(statistics.getTickerCount(TickerType.BLOCK_CHECKSUM_COMPUTE_COUNT))
                .isEqualTo(beforeChecksumComputeCount);
          }
        }
      }
    }
  }
}
