package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class EventListenerTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  public static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  void flushDb(AbstractEventListener el, AtomicBoolean wasCbCalled) throws RocksDBException {
    try (final Options opt = new Options()
        .setCreateIfMissing(true)
        .setListeners(el);
         final RocksDB db =
             RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
      byte[] value = new byte[24];
      rand.nextBytes(value);
      db.put("testKey".getBytes(), value);
      db.flush(new FlushOptions());
      assertTrue(wasCbCalled.get());
    }
  }

  @Test
  public void onFlushCompleted() throws RocksDBException {
    // Callback is synchronous, but we need mutable container to update boolean value in other method
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    AbstractEventListener onFlushCompletedListener = new AbstractEventListener() {
      @Override
      public void onFlushCompleted(final RocksDB rocksDb,
                                   final FlushJobInfo flushJobInfo) {
        // TODO(TP): add more asserts ?
        assertNotNull(flushJobInfo.getColumnFamilyName());
        assertEquals(FlushReason.MANUAL_FLUSH, flushJobInfo.getFlushReason());
        wasCbCalled.set(true);
      }
    };
    flushDb(onFlushCompletedListener, wasCbCalled);
  }

  @Ignore("Not implemented")
  @Test
  public void onFlushBegin() throws RocksDBException {
    // Callback is synchronous, but we need mutable container to update boolean value in other method
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    AbstractEventListener onFlushBeginListener = new AbstractEventListener() {
      @Override
      public void onFlushBegin(final RocksDB rocksDb,
                               final FlushJobInfo flushJobInfo) {
        // TODO(TP): add more asserts ?
        assertNotNull(flushJobInfo.getColumnFamilyName());
        assertEquals(FlushReason.MANUAL_FLUSH, flushJobInfo.getFlushReason());
        wasCbCalled.set(true);
      }
    };
    flushDb(onFlushBeginListener, wasCbCalled);
  }

  void deleteTableFile(AbstractEventListener el, AtomicBoolean wasCbCalled) throws RocksDBException, InterruptedException {
    try (final Options opt = new Options()
        .setCreateIfMissing(true)
        .setListeners(el);
         final RocksDB db =
             RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
      byte[] value = new byte[24];
      rand.nextBytes(value);
      db.put("testKey".getBytes(), value);
      RocksDB.LiveFiles liveFiles = db.getLiveFiles();
      assertNotNull(liveFiles);
      assertNotNull(liveFiles.files);
      assertFalse(liveFiles.files.isEmpty());
      db.deleteFile(liveFiles.files.get(0));
      assertTrue(wasCbCalled.get());
    }
  }

  @Test
  public void onTableFileDeleted() throws RocksDBException, InterruptedException {
    // Callback is synchronous, but we need mutable container to update boolean value in other method
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    AbstractEventListener onTableFileDeletedListener = new AbstractEventListener() {
      @Override
      public void onTableFileDeleted(final TableFileDeletionInfo tableFileDeletionInfo) {
        // TODO(TP): add more asserts ?
        assertNotNull(tableFileDeletionInfo.getDbName());
        wasCbCalled.set(true);
      }
    };
    deleteTableFile(onTableFileDeletedListener, wasCbCalled);
  }
}
