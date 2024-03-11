// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

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

public class SequenceNumberAllocationTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void seqnoFlushAndCloseWithWal() throws RocksDBException {
    seqnoFlushAndClose(false);
  }

  @Test
  public void seqnoFlushAndCloseWithoutWal() throws RocksDBException {
    seqnoFlushAndClose(true);
  }

  private void seqnoFlushAndClose(final boolean disableWal) throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final WriteOptions writeOptions = new WriteOptions().setDisableWAL(disableWal);
         final FlushOptions flushOptions = new FlushOptions();
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      final int count = fillDatabase(db, writeOptions, 0, 15);
      assertEquals("Database values count mismatch", 15, count);
      db.flush(flushOptions);
    }

    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      assertNotEquals("Database should not be empty", 0, db.getLatestSequenceNumber());
      assertEquals("Sequence number changed between closing and reopening the database", 15,
          db.getLatestSequenceNumber());
    }
  }

  @Test
  public void seqnoCloseWithoutFlushWithWal() throws RocksDBException {
    seqnoCloseWithoutFlush(false);
  }

  @Test
  public void seqnoCloseWithoutFlushWithoutWal() throws RocksDBException {
    seqnoCloseWithoutFlush(true);
  }

  private void seqnoCloseWithoutFlush(final boolean disableWal) throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final WriteOptions writeOptions = new WriteOptions().setDisableWAL(disableWal);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      final int count = fillDatabase(db, writeOptions, 0, 30);
      assertEquals("Database values count mismatch", 30, count);
    }

    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      assertNotEquals("Database should not be empty", 0, db.getLatestSequenceNumber());
      assertEquals("Sequence number changed between closing and reopening the database", 30,
          db.getLatestSequenceNumber());
    }
  }

  @Test
  public void seqnoThreadInterruptAfterFlushWithWal() throws RocksDBException {
    seqnoThreadInterruptAfterFlush(false);
  }

  @Test
  public void seqnoThreadInterruptAfterFlushWithoutWal() throws RocksDBException {
    seqnoThreadInterruptAfterFlush(true);
  }

  private void seqnoThreadInterruptAfterFlush(final boolean disableWal) throws RocksDBException {
    final ExecutorService executor = Executors.newFixedThreadPool(1);
    final AtomicLong sequenceNumberAfterFlush = new AtomicLong(0);
    final CountDownLatch waitingSignal = new CountDownLatch(1);
    final CountDownLatch dbOperationsSignal = new CountDownLatch(1);

    final Future<?> task = executor.submit(() -> {
      try (final Options options = new Options().setCreateIfMissing(true);
           final WriteOptions writeOptions = new WriteOptions().setDisableWAL(disableWal);
           final FlushOptions flushOptions = new FlushOptions();
           final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
        final int count = fillDatabase(db, writeOptions, 0, 10);
        assertEquals("Database values count mismatch", 10, count);
        db.flush(flushOptions);
        sequenceNumberAfterFlush.set(db.getLatestSequenceNumber());
        assertEquals("Incorrect sequence number after flush", 10, sequenceNumberAfterFlush.get());
        dbOperationsSignal.countDown();
        waitingSignal.await(60, TimeUnit.SECONDS);
      } catch (final RocksDBException | InterruptedException e) {
        if (e instanceof InterruptedException) {
          Thread.interrupted();
        }
        fail("Encountered exception:" + e.getMessage());
      } finally {
        dbOperationsSignal.countDown();
        waitingSignal.countDown();
      }
    });

    try {
      dbOperationsSignal.await(5, TimeUnit.SECONDS);
      task.cancel(true);
      assertTrue("Failed to cancel task", task.isCancelled());
    } catch (final InterruptedException e) {
      Thread.interrupted();
      fail("Encountered exception:" + e.getMessage());
    } finally {
      executor.shutdownNow();
      assertTrue("Executor did not shutdown", executor.isShutdown());
    }

    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.openReadOnly(options, dbFolder.getRoot().getAbsolutePath())) {
      assertNotEquals("Database should not be empty", 0, db.getLatestSequenceNumber());
      assertEquals("Lastest sequence number is not the one expected",
          sequenceNumberAfterFlush.get(), db.getLatestSequenceNumber());
    }
  }

  @Test
  public void seqnoDataWrittenAfterFlushWithWal() throws RocksDBException {
    seqnoDataWrittenAfterFlush(false);
  }

  @Test
  public void seqnoDataWrittenAfterFlushWithoutWal() throws RocksDBException {
    seqnoDataWrittenAfterFlush(true);
  }

  private void seqnoDataWrittenAfterFlush(final boolean disableWal) throws RocksDBException {
    final ExecutorService executor = Executors.newFixedThreadPool(1);
    final AtomicLong sequenceNumberAfterFlush = new AtomicLong(0);
    final CountDownLatch waitingSignal = new CountDownLatch(1);
    final CountDownLatch dbOperationsSignal = new CountDownLatch(1);

    final Future<?> task = executor.submit(() -> {
      try (final Options options = new Options().setCreateIfMissing(true);
           final WriteOptions writeOptions = new WriteOptions().setDisableWAL(disableWal);
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
      } catch (final RocksDBException | InterruptedException e) {
        if (e instanceof InterruptedException) {
          Thread.interrupted();
        }
        fail("Encountered exception:" + e.getMessage());
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
    } catch (final InterruptedException e) {
      Thread.interrupted();
      fail("Encountered exception:" + e.getMessage());
    } finally {
      executor.shutdownNow();
      assertTrue("Executor did not shutdown", executor.isShutdown());
    }

    final int expectedSequenceNumber = disableWal ? 10 : 20;
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.openReadOnly(options, dbFolder.getRoot().getAbsolutePath())) {
      assertNotEquals("Database should not be empty", 0, db.getLatestSequenceNumber());
      assertEquals("Lastest sequence number is not the one expected", expectedSequenceNumber,
          db.getLatestSequenceNumber());
    }
  }

  private int fillDatabase(final RocksDB db, final WriteOptions writeOptions, final int start,
      final int end) throws RocksDBException {
    assertEquals("Database does not have expected sequence number " + start, start,
        db.getLatestSequenceNumber());
    int count;
    for (count = start; count < end; ++count) {
      final byte[] key = ("key" + count).getBytes(UTF_8);
      final byte[] value = ("value" + count).getBytes(UTF_8);
      db.put(writeOptions, key, value);
      assertEquals("Sequence number is not updated", count + 1, db.getLatestSequenceNumber());
    }
    return count;
  }
}
