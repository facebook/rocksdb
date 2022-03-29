// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

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

  /**
   * Class to factor out the specific DB operations within the test
   */
  abstract static class Operations {
    final int kv_count;
    final List<String> elements = new ArrayList<>();
    final List<String> sortedElements = new ArrayList<>();

    Operations(final int kv_count) {
      this.kv_count = kv_count;
      for (int i = 0; i < kv_count; i++) elements.add(MessageFormat.format("{0,number,#}", i));
      sortedElements.addAll(elements);
      Collections.sort(sortedElements);
    }

    void fill(final RocksDB db) throws RocksDBException {
      for (int i = 0; i < kv_count; i++) {
        final String key = MessageFormat.format("key{0}", elements.get(i));
        final String value = MessageFormat.format("value{0}", elements.get(i));
        // noinspection ObjectAllocationInLoop
        db.put(key.getBytes(), value.getBytes());
      }
      db.flush(new FlushOptions());
    }

    @SuppressWarnings("ObjectAllocationInLoop")
    void get(final RocksDB db, final boolean verifyFlag) throws RocksDBException {
      try (final ReadOptions readOptions = new ReadOptions()) {
        readOptions.setReadaheadSize(32 * 1024);
        readOptions.setFillCache(false);
        readOptions.setVerifyChecksums(verifyFlag);

        for (int i = 0; i < kv_count / 10; i++) {
          @SuppressWarnings("UnsecureRandomNumberGeneration")
          final int index = Double.valueOf(Math.random() * kv_count).intValue();
          final String key = MessageFormat.format("key{0}", sortedElements.get(index));
          final String expectedValue = MessageFormat.format("value{0}", sortedElements.get(index));

          final byte[] value = db.get(readOptions, key.getBytes());
          assertThat(value).isEqualTo(expectedValue.getBytes());
        }
      }
    }

    @SuppressWarnings("ObjectAllocationInLoop")
    void multiGet(final RocksDB db, final boolean verifyFlag) throws RocksDBException {
      try (final ReadOptions readOptions = new ReadOptions()) {
        readOptions.setReadaheadSize(32 * 1024);
        readOptions.setFillCache(false);
        readOptions.setVerifyChecksums(verifyFlag);

        final List<byte[]> keys = new ArrayList<>();
        final List<String> expectedValues = new ArrayList<>();

        for (int i = 0; i < kv_count / 10; i++) {
          @SuppressWarnings("UnsecureRandomNumberGeneration")
          final int index = Double.valueOf(Math.random() * kv_count).intValue();
          keys.add(MessageFormat.format("key{0}", sortedElements.get(index)).getBytes());

          expectedValues.add(MessageFormat.format("value{0}", sortedElements.get(index)));
        }

        final List<byte[]> values = db.multiGetAsList(readOptions, keys);
        for (int i = 0; i < keys.size(); i++) {
          assertThat(values.get(i)).isEqualTo(expectedValues.get(i).getBytes());
        }
      }
    }

    void iterate(final RocksDB db, final boolean verifyFlag) throws RocksDBException {
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
          // noinspection ObjectAllocationInLoop
          assertThat(key).isEqualTo(
              (MessageFormat.format("key{0}", sortedElements.get(i))).getBytes());
          // noinspection ObjectAllocationInLoop
          assertThat(value).isEqualTo(
              (MessageFormat.format("value{0}", sortedElements.get(i))).getBytes());
          rocksIterator.next();
          rocksIterator.status();
          i++;
        }
      }
      assertThat(i).isEqualTo(kv_count);
    }

    abstract void performOperations(final RocksDB db, final boolean verifyFlag)
        throws RocksDBException;
  }

  private static final int KV_COUNT = 10000;

  /**
   * Run some operations and count the TickerType.BLOCK_CHECKSUM_COMPUTE_COUNT before and after
   * It should GO UP when the read options have checksum verification turned on.
   * It shoulld REMAIN UNCHANGED when the read options have checksum verification turned off.
   * As the read options refer only to the read operations, there are still a few checksums
   * performed outside this (blocks are getting loaded for lots of reasons, not aways directly due
   * to reads) but this test provides a good enough proxy for whether the flag is being noticed.
   *
   * @param operations the DB reading operations to perform which affect the checksum stats
   *
   * @throws RocksDBException
   */
  private void verifyChecksums(final Operations operations) throws RocksDBException {
    final String dbPath = dbFolder.getRoot().getAbsolutePath();

    // noinspection SingleStatementInBlock
    try (final Statistics statistics = new Statistics();
         final Options options = new Options().setCreateIfMissing(true).setStatistics(statistics)) {
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        // 0
        System.out.println(MessageFormat.format(
            "newly open {0}", statistics.getTickerCount(TickerType.BLOCK_CHECKSUM_COMPUTE_COUNT)));
        operations.fill(db);
        //
        System.out.println(MessageFormat.format(
            "flushed {0}", statistics.getTickerCount(TickerType.BLOCK_CHECKSUM_COMPUTE_COUNT)));
      }

      // 2
      System.out.println(MessageFormat.format("closed-after-write {0}",
          statistics.getTickerCount(TickerType.BLOCK_CHECKSUM_COMPUTE_COUNT)));

      for (final boolean verifyFlag : new boolean[] {false, true, false, true}) {
        try (final RocksDB db = RocksDB.open(options, dbPath)) {
          final long beforeOperationsCount =
              statistics.getTickerCount(TickerType.BLOCK_CHECKSUM_COMPUTE_COUNT);
          System.out.println(MessageFormat.format("re-opened {0}", beforeOperationsCount));
          operations.performOperations(db, verifyFlag);
          final long afterOperationsCount =
              statistics.getTickerCount(TickerType.BLOCK_CHECKSUM_COMPUTE_COUNT);
          if (verifyFlag) {
            // We don't need to be exact - we are checking that the checksums happen
            // exactly how many depends on block size etc etc, so may not be entirely stable
            System.out.println(MessageFormat.format("verify=true {0}", afterOperationsCount));
            assertThat(afterOperationsCount).isGreaterThan(beforeOperationsCount + 20);
          } else {
            System.out.println(MessageFormat.format("verify=false {0}", afterOperationsCount));
            assertThat(afterOperationsCount).isEqualTo(beforeOperationsCount);
          }
        }
      }
    }
  }

  @Test
  public void verifyChecksumsInIteration() throws RocksDBException {
    // noinspection AnonymousInnerClassMayBeStatic
    verifyChecksums(new Operations(KV_COUNT) {
      @Override
      void performOperations(final RocksDB db, final boolean verifyFlag) throws RocksDBException {
        iterate(db, verifyFlag);
      }
    });
  }

  @Test
  public void verifyChecksumsGet() throws RocksDBException {
    // noinspection AnonymousInnerClassMayBeStatic
    verifyChecksums(new Operations(KV_COUNT) {
      @Override
      void performOperations(final RocksDB db, final boolean verifyFlag) throws RocksDBException {
        get(db, verifyFlag);
      }
    });
  }

  @Test
  public void verifyChecksumsMultiGet() throws RocksDBException {
    // noinspection AnonymousInnerClassMayBeStatic
    verifyChecksums(new Operations(KV_COUNT) {
      @Override
      void performOperations(final RocksDB db, final boolean verifyFlag) throws RocksDBException {
        multiGet(db, verifyFlag);
      }
    });
  }
}
