// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SequenceNumberAllocationTest {
  @Parameters(name = "{0}")
  public static Iterable<Boolean> parameters() {
    return Arrays.asList(true, false);
  }

  @Parameter(0) public boolean disableWAL;

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void testSeqnoFlushAndClose() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final WriteOptions writeOptions = new WriteOptions().setDisableWAL(disableWAL);
         final FlushOptions flushOptions = new FlushOptions();
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      int count = fillDatabase(db, writeOptions, 0, 15);
      assertEquals("Database values count mismatch", 15, count);
      db.flush(flushOptions);
      db.closeE();
    }

    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      assertTrue("Database should not be empty", db.getLatestSequenceNumber() != 0);
      assertEquals("Sequence number changed between closing and reopening the database", 15,
          db.getLatestSequenceNumber());
    }
  }

  @Test
  public void testSeqnoCloseWithoutFlush() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final WriteOptions writeOptions = new WriteOptions().setDisableWAL(disableWAL);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      int count = fillDatabase(db, writeOptions, 0, 30);
      assertEquals("Database values count mismatch", 30, count);
      db.closeE();
    }

    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      assertTrue("Database should not be empty", db.getLatestSequenceNumber() != 0);
      assertEquals("Sequence number changed between closing and reopening the database", 30,
          db.getLatestSequenceNumber());
    }
  }

  @Test
  public void testSeqnoThreadInterruptAfterFlush() throws RocksDBException {
    ExecutorService executor = Executors.newFixedThreadPool(1);
    AtomicLong sequenceNumberAfterFlush = new AtomicLong(0);
    CountDownLatch waitingSignal = new CountDownLatch(1);
    CountDownLatch dbOperationsSignal = new CountDownLatch(1);

    Future<?> task = executor.submit(() -> {
      try (final Options options = new Options().setCreateIfMissing(true);
           final WriteOptions writeOptions = new WriteOptions().setDisableWAL(disableWAL);
           final FlushOptions flushOptions = new FlushOptions();
           final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
        int count = fillDatabase(db, writeOptions, 0, 10);
        assertEquals("Database values count mismatch", 10, count);
        db.flush(flushOptions);
        sequenceNumberAfterFlush.set(db.getLatestSequenceNumber());
        assertEquals("Incorrect sequence number after flush", 10, sequenceNumberAfterFlush.get());
        dbOperationsSignal.countDown();
        waitingSignal.await(60, TimeUnit.SECONDS);
      } catch (RocksDBException e) {
        fail("Encountered exception:" + e);
      } catch (InterruptedException e) {
      } finally {
        dbOperationsSignal.countDown();
        waitingSignal.countDown();
      }
    });

    try {
      dbOperationsSignal.await(5, TimeUnit.SECONDS);
      task.cancel(true);
      assertTrue("Failed to cancel task", task.isCancelled());
    } catch (InterruptedException e) {
    } finally {
      executor.shutdownNow();
      assertTrue("Executor did not shutdown", executor.isShutdown());
    }

    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.openReadOnly(options, dbFolder.getRoot().getAbsolutePath())) {
      assertTrue("Database should not be empty", db.getLatestSequenceNumber() != 0);
      assertEquals("Lastest sequence number is not the one expected",
          sequenceNumberAfterFlush.get(), db.getLatestSequenceNumber());
    }
  }

  @Test
  public void testSeqnoDataWrittenAfterFlush() throws RocksDBException {
    ExecutorService executor = Executors.newFixedThreadPool(1);
    AtomicLong sequenceNumberAfterFlush = new AtomicLong(0);
    CountDownLatch waitingSignal = new CountDownLatch(1);
    CountDownLatch dbOperationsSignal = new CountDownLatch(1);

    Future<?> task = executor.submit(() -> {
      try (final Options options = new Options().setCreateIfMissing(true);
           final WriteOptions writeOptions = new WriteOptions().setDisableWAL(disableWAL);
           final FlushOptions flushOptions = new FlushOptions();
           final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
        int count = fillDatabase(db, writeOptions, 0, 10);
        assertEquals("Database values count mismatch", 10, count);
        db.flush(flushOptions);
        sequenceNumberAfterFlush.set(db.getLatestSequenceNumber());
        assertEquals("Incorrect sequence number after flush", 10, sequenceNumberAfterFlush.get());
        count = fillDatabase(db, writeOptions, 10, 20);
        assertEquals("Database values count mismatch", 20, count);
        assertEquals(
            "Unexpected value for latest DB sequence number", 20, db.getLatestSequenceNumber());
        dbOperationsSignal.countDown();
        waitingSignal.await(60, TimeUnit.SECONDS);
      } catch (RocksDBException e) {
        fail("Encountered exception:" + e);
      } catch (InterruptedException e) {
      } finally {
        dbOperationsSignal.countDown();
        waitingSignal.countDown();
        executor.shutdownNow();
        assertTrue("Executor did not shutdown", executor.isShutdown());
      }
    });

    try {
      dbOperationsSignal.await(5, TimeUnit.SECONDS);
      task.cancel(true);
      assertTrue("Failed to cancel task", task.isCancelled());
    } catch (InterruptedException e) {
    } finally {
      executor.shutdownNow();
      assertTrue("Executor did not shutdown", executor.isShutdown());
    }

    int expectedSequenceNumber = disableWAL ? 10 : 20;
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.openReadOnly(options, dbFolder.getRoot().getAbsolutePath())) {
      assertTrue("Database should not be empty", db.getLatestSequenceNumber() != 0);
      assertEquals("Lastest sequence number is not the one expected", expectedSequenceNumber,
          db.getLatestSequenceNumber());
    }
  }

  private int fillDatabase(RocksDB db, WriteOptions writeOptions, int start, int end)
      throws RocksDBException {
    assertEquals("Database does not have expected sequence number " + start, start,
        db.getLatestSequenceNumber());
    int count;
    for (count = start; count < end; ++count) {
      byte[] key = ("key" + count).getBytes(StandardCharsets.UTF_8);
      byte[] value = ("value" + count).getBytes(StandardCharsets.UTF_8);
      db.put(writeOptions, key, value);
      assertEquals("Sequence number is not updated", count + 1, db.getLatestSequenceNumber());
    }

    return count;
  }
}
