package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.test.TestableEventListener;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
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
        // TODO(TP): add more asserts
        assertNotNull(flushJobInfo.getColumnFamilyName());
        assertEquals(FlushReason.MANUAL_FLUSH, flushJobInfo.getFlushReason());
        wasCbCalled.set(true);
      }
    };
    flushDb(onFlushCompletedListener, wasCbCalled);
  }

  @Test
  public void onFlushBegin() throws RocksDBException {
    // Callback is synchronous, but we need mutable container to update boolean value in other method
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    AbstractEventListener onFlushBeginListener = new AbstractEventListener() {
      @Override
      public void onFlushBegin(final RocksDB rocksDb,
                               final FlushJobInfo flushJobInfo) {
        // TODO(TP): add more asserts
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
        // TODO(TP): add more asserts
        assertNotNull(tableFileDeletionInfo.getDbName());
        wasCbCalled.set(true);
      }
    };
    deleteTableFile(onTableFileDeletedListener, wasCbCalled);
  }

  void compactRange(AbstractEventListener el, AtomicBoolean wasCbCalled) throws RocksDBException {
    try (final Options opt = new Options()
        .setCreateIfMissing(true)
        .setListeners(el);
         final RocksDB db =
             RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
      byte[] value = new byte[24];
      rand.nextBytes(value);
      db.put("testKey".getBytes(), value);
      db.compactRange();
      assertTrue(wasCbCalled.get());
    }
  }

  @Test
  public void onCompactionBegin() throws RocksDBException {
    // Callback is synchronous, but we need mutable container to update boolean value in other method
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    AbstractEventListener onCompactionBeginListener = new AbstractEventListener() {
      @Override
      public void onCompactionBegin(final RocksDB db,
                                    final CompactionJobInfo compactionJobInfo) {
        // TODO(TP): add more asserts
        assertEquals(CompactionReason.kManualCompaction, compactionJobInfo.compactionReason());
        wasCbCalled.set(true);
      }
    };
    compactRange(onCompactionBeginListener, wasCbCalled);
  }

  @Test
  public void onCompactionCompleted() throws RocksDBException {
    // Callback is synchronous, but we need mutable container to update boolean value in other method
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    AbstractEventListener onCompactionCompletedListener = new AbstractEventListener() {
      @Override
      public void onCompactionCompleted(final RocksDB db,
                                        final CompactionJobInfo compactionJobInfo) {
        // TODO(TP): add more asserts
        assertEquals(CompactionReason.kManualCompaction, compactionJobInfo.compactionReason());
        wasCbCalled.set(true);
      }
    };
    compactRange(onCompactionCompletedListener, wasCbCalled);
  }

  @Test
  public void onTableFileCreated() throws RocksDBException {
    // Callback is synchronous, but we need mutable container to update boolean value in other method
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    AbstractEventListener onTableFileCreatedListener = new AbstractEventListener() {
      @Override
      public void onTableFileCreated(final TableFileCreationInfo tableFileCreationInfo) {
        // TODO(TP): add more asserts
        assertEquals(TableFileCreationReason.FLUSH, tableFileCreationInfo.getReason());
        wasCbCalled.set(true);
      }
    };
    flushDb(onTableFileCreatedListener, wasCbCalled);
  }

  @Test
  public void onTableFileCreationStarted() throws RocksDBException {
    // Callback is synchronous, but we need mutable container to update boolean value in other method
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    AbstractEventListener onTableFileCreationStartedListener = new AbstractEventListener() {
      @Override
      public void onTableFileCreationStarted(final TableFileCreationBriefInfo tableFileCreationBriefInfo) {
        // TODO(TP): add more asserts
        assertEquals(TableFileCreationReason.FLUSH, tableFileCreationBriefInfo.getReason());
        wasCbCalled.set(true);
      }
    };
    flushDb(onTableFileCreationStartedListener, wasCbCalled);
  }

  @Test
  public void onMemTableSealed() {
    // TODO
  }

  void deleteColumnFamilyHandle(AbstractEventListener el, AtomicBoolean wasCbCalled) throws RocksDBException {
    try (final Options opt = new Options()
        .setCreateIfMissing(true)
        .setListeners(el);
         final RocksDB db =
             RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
      byte[] value = new byte[24];
      rand.nextBytes(value);
      db.put("testKey".getBytes(), value);
      ColumnFamilyHandle columnFamilyHandle = db.getDefaultColumnFamily();
      columnFamilyHandle.close();
      assertTrue(wasCbCalled.get());
    }
  }

  @Test
  public void onColumnFamilyHandleDeletionStarted() throws RocksDBException {
    // Callback is synchronous, but we need mutable container to update boolean value in other method
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    AbstractEventListener onColumnFamilyHandleDeletionStartedListener = new AbstractEventListener() {
      @Override
      public void onColumnFamilyHandleDeletionStarted(final ColumnFamilyHandle columnFamilyHandle) {
        // TODO(TP): add more asserts
        assertNotNull(columnFamilyHandle);
        wasCbCalled.set(true);
      }
    };
    deleteColumnFamilyHandle(onColumnFamilyHandleDeletionStartedListener, wasCbCalled);
  }

  void ingestExternalFile(AbstractEventListener el, AtomicBoolean wasCbCalled) throws RocksDBException {
    try (final Options opt = new Options()
        .setCreateIfMissing(true)
        .setListeners(el);
         final RocksDB db =
             RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
      String uuid = UUID.randomUUID().toString();
      SstFileWriter sstFileWriter = new SstFileWriter(new EnvOptions(), opt);
      Path externalFilePath = Paths.get(db.getName(), uuid);
      sstFileWriter.open(externalFilePath.toString());
      sstFileWriter.put("testKey".getBytes(), uuid.getBytes());
      sstFileWriter.finish();
      db.ingestExternalFile(Collections.singletonList(externalFilePath.toString()),
          new IngestExternalFileOptions());
      assertTrue(wasCbCalled.get());
    }
  }

  @Test
  public void onExternalFileIngested() throws RocksDBException, InterruptedException {
    // Callback is synchronous, but we need mutable container to update boolean value in other method
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    AbstractEventListener onExternalFileIngestedListener = new AbstractEventListener() {
      @Override
      public void onExternalFileIngested(final RocksDB db,
                                         final ExternalFileIngestionInfo externalFileIngestionInfo) {
        // TODO(TP): add more asserts
        assertNotNull(db);
        wasCbCalled.set(true);
      }
    };
    ingestExternalFile(onExternalFileIngestedListener, wasCbCalled);
  }

  @Test
  public void onBackgroundError() {
    // TODO
  }

  @Test
  public void onStallConditionsChanged() {
    // TODO
  }

  @Test
  public void onFileReadFinish() {
    // TODO
  }

  @Test
  public void onFileWriteFinish() {
    // TODO
  }

  @Test
  public void shouldBeNotifiedOnFileIO() {
    // TODO
  }

  @Test
  public void onErrorRecoveryBegin() {
    // TODO
  }

  @Test
  public void onErrorRecoveryCompleted() {
    // TODO
  }

  @Test
  public void testAllCallbacksInvocation() {
    final int CALLBACKS_COUNT = 17;
    final AtomicBoolean[] wasCalled = new AtomicBoolean[CALLBACKS_COUNT];
    for (int i = 0; i < CALLBACKS_COUNT; ++i) {
      wasCalled[i] = new AtomicBoolean();
    }
    TestableEventListener listener = new TestableEventListener() {
      @Override
      public void onFlushCompleted(final RocksDB db,
                                   final FlushJobInfo flushJobInfo) {
        wasCalled[0].set(true);
      }

      @Override
      public void onFlushBegin(final RocksDB db, final FlushJobInfo flushJobInfo) {
        wasCalled[1].set(true);
      }

      @Override
      public void onTableFileDeleted(
          final TableFileDeletionInfo tableFileDeletionInfo) {
        wasCalled[2].set(true);
      }

      @Override
      public void onCompactionBegin(final RocksDB db,
                                    final CompactionJobInfo compactionJobInfo) {
        wasCalled[3].set(true);
      }

      @Override
      public void onCompactionCompleted(final RocksDB db,
                                        final CompactionJobInfo compactionJobInfo) {
        wasCalled[4].set(true);
      }

      @Override
      public void onTableFileCreated(
          final TableFileCreationInfo tableFileCreationInfo) {
        wasCalled[5].set(true);
      }

      @Override
      public void onTableFileCreationStarted(
          final TableFileCreationBriefInfo tableFileCreationBriefInfo) {
        wasCalled[6].set(true);
      }

      @Override
      public void onMemTableSealed(final MemTableInfo memTableInfo) {
        wasCalled[7].set(true);
      }

      @Override
      public void onColumnFamilyHandleDeletionStarted(
          final ColumnFamilyHandle columnFamilyHandle) {
        wasCalled[8].set(true);
      }

      @Override
      public void onExternalFileIngested(final RocksDB db,
                                         final ExternalFileIngestionInfo externalFileIngestionInfo) {
        wasCalled[9].set(true);
      }

      @Override
      public void onBackgroundError(
          final BackgroundErrorReason backgroundErrorReason,
          final Status backgroundError) {
        wasCalled[10].set(true);
      }

      @Override
      public void onStallConditionsChanged(final WriteStallInfo writeStallInfo) {
        wasCalled[11].set(true);
      }

      @Override
      public void onFileReadFinish(final FileOperationInfo fileOperationInfo) {
        wasCalled[12].set(true);
      }

      @Override
      public void onFileWriteFinish(final FileOperationInfo fileOperationInfo) {
        wasCalled[13].set(true);
      }

      @Override
      public boolean shouldBeNotifiedOnFileIO() {
        wasCalled[14].set(true);
        return false;
      }

      @Override
      public boolean onErrorRecoveryBegin(
          final BackgroundErrorReason backgroundErrorReason,
          final Status backgroundError) {
        wasCalled[15].set(true);
        return true;
      }

      @Override
      public void onErrorRecoveryCompleted(final Status oldBackgroundError) {
        wasCalled[16].set(true);
      }
    };
    listener.invokeAllCallbacks();
    for (int i = 0; i < CALLBACKS_COUNT; ++i) {
      assertTrue("Callback method " + i + " was not called", wasCalled[i].get());
    }
  }
}
