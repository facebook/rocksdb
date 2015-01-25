package org.rocksdb.test;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.*;

import static org.assertj.core.api.Assertions.assertThat;

public class TransactionLogIteratorTest {
  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void transactionLogIterator() throws RocksDBException {
    RocksDB db = null;
    Options options = null;
    TransactionLogIterator transactionLogIterator = null;
    try {
      options = new Options().
          setCreateIfMissing(true);
      db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
      transactionLogIterator = db.getUpdatesSince(0);
    } finally {
      if (transactionLogIterator != null) {
        transactionLogIterator.dispose();
      }
      if (db != null) {
        db.close();
      }
      if (options != null) {
        options.dispose();
      }
    }
  }

  @Test
  public void getBatch() throws RocksDBException {
    RocksDB db = null;
    Options options = null;
    TransactionLogIterator transactionLogIterator = null;
    try {
      options = new Options().
          setCreateIfMissing(true).
          setWalTtlSeconds(1000).
          setWalSizeLimitMB(10);

      db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());

      for (int i = 0; i < 250; i++){
        db.put(String.valueOf(i).getBytes(),
            String.valueOf(i).getBytes());
      }
      db.flush(new FlushOptions().setWaitForFlush(true));
      transactionLogIterator = db.getUpdatesSince(0);
      assertThat(transactionLogIterator.isValid()).isTrue();
      transactionLogIterator.status();

      TransactionLogIterator.BatchResult batchResult =
          transactionLogIterator.getBatch();
      assertThat(batchResult.sequenceNumber()).isEqualTo(1);
    } finally {
      if (transactionLogIterator != null) {
        transactionLogIterator.dispose();
      }
      if (db != null) {
        db.close();
      }
      if (options != null) {
        options.dispose();
      }
    }
  }
}
