//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.AbstractEventListener.EnabledEventCallback;
import org.rocksdb.test.TestableEventListener;

public class EventListenerTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  public static final Random rand = PlatformRandomHelper.getPlatformSpecificRandomFactory();

  void flushDb(final AbstractEventListener el, final AtomicBoolean wasCbCalled)
      throws RocksDBException {
    try (final Options opt =
             new Options().setCreateIfMissing(true).setListeners(Collections.singletonList(el));
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
      final byte[] value = new byte[24];
      rand.nextBytes(value);
      db.put("testKey".getBytes(), value);
      db.flush(new FlushOptions());
      assertTrue(wasCbCalled.get());
    }
  }

  @Test
  public void onFlushCompleted() throws RocksDBException {
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    final AbstractEventListener onFlushCompletedListener = new AbstractEventListener() {
      @Override
      public void onFlushCompleted(final RocksDB rocksDb, final FlushJobInfo flushJobInfo) {
        assertNotNull(flushJobInfo.getColumnFamilyName());
        assertEquals(FlushReason.MANUAL_FLUSH, flushJobInfo.getFlushReason());
        wasCbCalled.set(true);
      }
    };
    flushDb(onFlushCompletedListener, wasCbCalled);
  }

  @Test
  public void onFlushBegin() throws RocksDBException {
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    final AbstractEventListener onFlushBeginListener = new AbstractEventListener() {
      @Override
      public void onFlushBegin(final RocksDB rocksDb, final FlushJobInfo flushJobInfo) {
        assertNotNull(flushJobInfo.getColumnFamilyName());
        assertEquals(FlushReason.MANUAL_FLUSH, flushJobInfo.getFlushReason());
        wasCbCalled.set(true);
      }
    };
    flushDb(onFlushBeginListener, wasCbCalled);
  }

  void deleteTableFile(final AbstractEventListener el, final AtomicBoolean wasCbCalled)
      throws RocksDBException {
    try (final Options opt =
             new Options().setCreateIfMissing(true).setListeners(Collections.singletonList(el));
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
      final byte[] value = new byte[24];
      rand.nextBytes(value);
      db.put("testKey".getBytes(), value);
      final RocksDB.LiveFiles liveFiles = db.getLiveFiles();
      assertNotNull(liveFiles);
      assertNotNull(liveFiles.files);
      assertFalse(liveFiles.files.isEmpty());
      db.deleteFile(liveFiles.files.get(0));
      assertTrue(wasCbCalled.get());
    }
  }

  @Test
  public void onTableFileDeleted() throws RocksDBException, InterruptedException {
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    final AbstractEventListener onTableFileDeletedListener = new AbstractEventListener() {
      @Override
      public void onTableFileDeleted(final TableFileDeletionInfo tableFileDeletionInfo) {
        assertNotNull(tableFileDeletionInfo.getDbName());
        wasCbCalled.set(true);
      }
    };
    deleteTableFile(onTableFileDeletedListener, wasCbCalled);
  }

  void compactRange(final AbstractEventListener el, final AtomicBoolean wasCbCalled)
      throws RocksDBException {
    try (final Options opt =
             new Options().setCreateIfMissing(true).setListeners(Collections.singletonList(el));
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
      final byte[] value = new byte[24];
      rand.nextBytes(value);
      db.put("testKey".getBytes(), value);
      db.compactRange();
      assertTrue(wasCbCalled.get());
    }
  }

  @Test
  public void onCompactionBegin() throws RocksDBException {
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    final AbstractEventListener onCompactionBeginListener = new AbstractEventListener() {
      @Override
      public void onCompactionBegin(final RocksDB db, final CompactionJobInfo compactionJobInfo) {
        assertEquals(CompactionReason.kManualCompaction, compactionJobInfo.compactionReason());
        wasCbCalled.set(true);
      }
    };
    compactRange(onCompactionBeginListener, wasCbCalled);
  }

  @Test
  public void onCompactionCompleted() throws RocksDBException {
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    final AbstractEventListener onCompactionCompletedListener = new AbstractEventListener() {
      @Override
      public void onCompactionCompleted(
          final RocksDB db, final CompactionJobInfo compactionJobInfo) {
        assertEquals(CompactionReason.kManualCompaction, compactionJobInfo.compactionReason());
        wasCbCalled.set(true);
      }
    };
    compactRange(onCompactionCompletedListener, wasCbCalled);
  }

  @Test
  public void onTableFileCreated() throws RocksDBException {
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    final AbstractEventListener onTableFileCreatedListener = new AbstractEventListener() {
      @Override
      public void onTableFileCreated(final TableFileCreationInfo tableFileCreationInfo) {
        assertEquals(TableFileCreationReason.FLUSH, tableFileCreationInfo.getReason());
        wasCbCalled.set(true);
      }
    };
    flushDb(onTableFileCreatedListener, wasCbCalled);
  }

  @Test
  public void onTableFileCreationStarted() throws RocksDBException {
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    final AbstractEventListener onTableFileCreationStartedListener = new AbstractEventListener() {
      @Override
      public void onTableFileCreationStarted(
          final TableFileCreationBriefInfo tableFileCreationBriefInfo) {
        assertEquals(TableFileCreationReason.FLUSH, tableFileCreationBriefInfo.getReason());
        wasCbCalled.set(true);
      }
    };
    flushDb(onTableFileCreationStartedListener, wasCbCalled);
  }

  void deleteColumnFamilyHandle(final AbstractEventListener el, final AtomicBoolean wasCbCalled)
      throws RocksDBException {
    try (final Options opt =
             new Options().setCreateIfMissing(true).setListeners(Collections.singletonList(el));
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
      final byte[] value = new byte[24];
      rand.nextBytes(value);
      db.put("testKey".getBytes(), value);
      ColumnFamilyHandle columnFamilyHandle = db.getDefaultColumnFamily();
      columnFamilyHandle.close();
      assertTrue(wasCbCalled.get());
    }
  }

  @Test
  public void onColumnFamilyHandleDeletionStarted() throws RocksDBException {
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    final AbstractEventListener onColumnFamilyHandleDeletionStartedListener =
        new AbstractEventListener() {
          @Override
          public void onColumnFamilyHandleDeletionStarted(
              final ColumnFamilyHandle columnFamilyHandle) {
            assertNotNull(columnFamilyHandle);
            wasCbCalled.set(true);
          }
        };
    deleteColumnFamilyHandle(onColumnFamilyHandleDeletionStartedListener, wasCbCalled);
  }

  void ingestExternalFile(final AbstractEventListener el, final AtomicBoolean wasCbCalled)
      throws RocksDBException {
    try (final Options opt =
             new Options().setCreateIfMissing(true).setListeners(Collections.singletonList(el));
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
      final String uuid = UUID.randomUUID().toString();
      final SstFileWriter sstFileWriter = new SstFileWriter(new EnvOptions(), opt);
      final Path externalFilePath = Paths.get(db.getName(), uuid);
      sstFileWriter.open(externalFilePath.toString());
      sstFileWriter.put("testKey".getBytes(), uuid.getBytes());
      sstFileWriter.finish();
      db.ingestExternalFile(
          Collections.singletonList(externalFilePath.toString()), new IngestExternalFileOptions());
      assertTrue(wasCbCalled.get());
    }
  }

  @Test
  public void onExternalFileIngested() throws RocksDBException {
    final AtomicBoolean wasCbCalled = new AtomicBoolean();
    final AbstractEventListener onExternalFileIngestedListener = new AbstractEventListener() {
      @Override
      public void onExternalFileIngested(
          final RocksDB db, final ExternalFileIngestionInfo externalFileIngestionInfo) {
        assertNotNull(db);
        wasCbCalled.set(true);
      }
    };
    ingestExternalFile(onExternalFileIngestedListener, wasCbCalled);
  }

  @Test
  public void testAllCallbacksInvocation() {
    final int TEST_INT_VAL = -1;
    final long TEST_LONG_VAL = -1;
    // Expected test data objects
    final Map<String, String> userCollectedPropertiesTestData =
        Collections.singletonMap("key", "value");
    final Map<String, String> readablePropertiesTestData = Collections.singletonMap("key", "value");
    final Map<String, Long> propertiesOffsetsTestData =
        Collections.singletonMap("key", TEST_LONG_VAL);
    final TableProperties tablePropertiesTestData = new TableProperties(TEST_LONG_VAL,
        TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL,
        TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL,
        TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL,
        TEST_LONG_VAL, TEST_LONG_VAL, "columnFamilyName".getBytes(), "filterPolicyName",
        "comparatorName", "mergeOperatorName", "prefixExtractorName", "propertyCollectorsNames",
        "compressionName", userCollectedPropertiesTestData, readablePropertiesTestData,
        propertiesOffsetsTestData);
    final FlushJobInfo flushJobInfoTestData = new FlushJobInfo(Integer.MAX_VALUE,
        "testColumnFamily", "/file/path", TEST_LONG_VAL, Integer.MAX_VALUE, true, true,
        TEST_LONG_VAL, TEST_LONG_VAL, tablePropertiesTestData, (byte) 0x0a);
    final Status statusTestData = new Status(Status.Code.Incomplete, Status.SubCode.NoSpace, null);
    final TableFileDeletionInfo tableFileDeletionInfoTestData =
        new TableFileDeletionInfo("dbName", "/file/path", Integer.MAX_VALUE, statusTestData);
    final TableFileCreationInfo tableFileCreationInfoTestData =
        new TableFileCreationInfo(TEST_LONG_VAL, tablePropertiesTestData, statusTestData, "dbName",
            "columnFamilyName", "/file/path", Integer.MAX_VALUE, (byte) 0x03);
    final TableFileCreationBriefInfo tableFileCreationBriefInfoTestData =
        new TableFileCreationBriefInfo(
            "dbName", "columnFamilyName", "/file/path", Integer.MAX_VALUE, (byte) 0x03);
    final MemTableInfo memTableInfoTestData = new MemTableInfo(
        "columnFamilyName", TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL, TEST_LONG_VAL);
    final FileOperationInfo fileOperationInfoTestData = new FileOperationInfo("/file/path",
        TEST_LONG_VAL, TEST_LONG_VAL, 1_600_699_420_000_000_000L, 5_000_000_000L, statusTestData);
    final WriteStallInfo writeStallInfoTestData =
        new WriteStallInfo("columnFamilyName", (byte) 0x1, (byte) 0x2);
    final ExternalFileIngestionInfo externalFileIngestionInfoTestData =
        new ExternalFileIngestionInfo("columnFamilyName", "/external/file/path",
            "/internal/file/path", TEST_LONG_VAL, tablePropertiesTestData);

    final CapturingTestableEventListener listener = new CapturingTestableEventListener() {
      @Override
      public void onFlushCompleted(final RocksDB db, final FlushJobInfo flushJobInfo) {
        super.onFlushCompleted(db, flushJobInfo);
        assertEquals(flushJobInfoTestData, flushJobInfo);
      }

      @Override
      public void onFlushBegin(final RocksDB db, final FlushJobInfo flushJobInfo) {
        super.onFlushBegin(db, flushJobInfo);
        assertEquals(flushJobInfoTestData, flushJobInfo);
      }

      @Override
      public void onTableFileDeleted(final TableFileDeletionInfo tableFileDeletionInfo) {
        super.onTableFileDeleted(tableFileDeletionInfo);
        assertEquals(tableFileDeletionInfoTestData, tableFileDeletionInfo);
      }

      @Override
      public void onCompactionBegin(final RocksDB db, final CompactionJobInfo compactionJobInfo) {
        super.onCompactionBegin(db, compactionJobInfo);
        assertArrayEquals(
            "compactionColumnFamily".getBytes(), compactionJobInfo.columnFamilyName());
        assertEquals(statusTestData, compactionJobInfo.status());
        assertEquals(TEST_LONG_VAL, compactionJobInfo.threadId());
        assertEquals(Integer.MAX_VALUE, compactionJobInfo.jobId());
        assertEquals(Integer.MAX_VALUE, compactionJobInfo.baseInputLevel());
        assertEquals(Integer.MAX_VALUE, compactionJobInfo.outputLevel());
        assertEquals(Collections.singletonList("inputFile.sst"), compactionJobInfo.inputFiles());
        assertEquals(Collections.singletonList("outputFile.sst"), compactionJobInfo.outputFiles());
        assertEquals(Collections.singletonMap("tableProperties", tablePropertiesTestData),
            compactionJobInfo.tableProperties());
        assertEquals(CompactionReason.kFlush, compactionJobInfo.compactionReason());
        assertEquals(CompressionType.SNAPPY_COMPRESSION, compactionJobInfo.compression());
      }

      @Override
      public void onCompactionCompleted(
          final RocksDB db, final CompactionJobInfo compactionJobInfo) {
        super.onCompactionCompleted(db, compactionJobInfo);
        assertArrayEquals(
            "compactionColumnFamily".getBytes(), compactionJobInfo.columnFamilyName());
        assertEquals(statusTestData, compactionJobInfo.status());
        assertEquals(TEST_LONG_VAL, compactionJobInfo.threadId());
        assertEquals(Integer.MAX_VALUE, compactionJobInfo.jobId());
        assertEquals(Integer.MAX_VALUE, compactionJobInfo.baseInputLevel());
        assertEquals(Integer.MAX_VALUE, compactionJobInfo.outputLevel());
        assertEquals(Collections.singletonList("inputFile.sst"), compactionJobInfo.inputFiles());
        assertEquals(Collections.singletonList("outputFile.sst"), compactionJobInfo.outputFiles());
        assertEquals(Collections.singletonMap("tableProperties", tablePropertiesTestData),
            compactionJobInfo.tableProperties());
        assertEquals(CompactionReason.kFlush, compactionJobInfo.compactionReason());
        assertEquals(CompressionType.SNAPPY_COMPRESSION, compactionJobInfo.compression());
      }

      @Override
      public void onTableFileCreated(final TableFileCreationInfo tableFileCreationInfo) {
        super.onTableFileCreated(tableFileCreationInfo);
        assertEquals(tableFileCreationInfoTestData, tableFileCreationInfo);
      }

      @Override
      public void onTableFileCreationStarted(
          final TableFileCreationBriefInfo tableFileCreationBriefInfo) {
        super.onTableFileCreationStarted(tableFileCreationBriefInfo);
        assertEquals(tableFileCreationBriefInfoTestData, tableFileCreationBriefInfo);
      }

      @Override
      public void onMemTableSealed(final MemTableInfo memTableInfo) {
        super.onMemTableSealed(memTableInfo);
        assertEquals(memTableInfoTestData, memTableInfo);
      }

      @Override
      public void onColumnFamilyHandleDeletionStarted(final ColumnFamilyHandle columnFamilyHandle) {
        super.onColumnFamilyHandleDeletionStarted(columnFamilyHandle);
      }

      @Override
      public void onExternalFileIngested(
          final RocksDB db, final ExternalFileIngestionInfo externalFileIngestionInfo) {
        super.onExternalFileIngested(db, externalFileIngestionInfo);
        assertEquals(externalFileIngestionInfoTestData, externalFileIngestionInfo);
      }

      @Override
      public void onBackgroundError(
          final BackgroundErrorReason backgroundErrorReason, final Status backgroundError) {
        super.onBackgroundError(backgroundErrorReason, backgroundError);
      }

      @Override
      public void onStallConditionsChanged(final WriteStallInfo writeStallInfo) {
        super.onStallConditionsChanged(writeStallInfo);
        assertEquals(writeStallInfoTestData, writeStallInfo);
      }

      @Override
      public void onFileReadFinish(final FileOperationInfo fileOperationInfo) {
        super.onFileReadFinish(fileOperationInfo);
        assertEquals(fileOperationInfoTestData, fileOperationInfo);
      }

      @Override
      public void onFileWriteFinish(final FileOperationInfo fileOperationInfo) {
        super.onFileWriteFinish(fileOperationInfo);
        assertEquals(fileOperationInfoTestData, fileOperationInfo);
      }

      @Override
      public void onFileFlushFinish(final FileOperationInfo fileOperationInfo) {
        super.onFileFlushFinish(fileOperationInfo);
        assertEquals(fileOperationInfoTestData, fileOperationInfo);
      }

      @Override
      public void onFileSyncFinish(final FileOperationInfo fileOperationInfo) {
        super.onFileSyncFinish(fileOperationInfo);
        assertEquals(fileOperationInfoTestData, fileOperationInfo);
      }

      @Override
      public void onFileRangeSyncFinish(final FileOperationInfo fileOperationInfo) {
        super.onFileRangeSyncFinish(fileOperationInfo);
        assertEquals(fileOperationInfoTestData, fileOperationInfo);
      }

      @Override
      public void onFileTruncateFinish(final FileOperationInfo fileOperationInfo) {
        assertEquals(fileOperationInfoTestData, fileOperationInfo);
        super.onFileTruncateFinish(fileOperationInfo);
      }

      @Override
      public void onFileCloseFinish(final FileOperationInfo fileOperationInfo) {
        super.onFileCloseFinish(fileOperationInfo);
        assertEquals(fileOperationInfoTestData, fileOperationInfo);
      }

      @Override
      public boolean shouldBeNotifiedOnFileIO() {
        super.shouldBeNotifiedOnFileIO();
        return false;
      }

      @Override
      public boolean onErrorRecoveryBegin(
          final BackgroundErrorReason backgroundErrorReason, final Status backgroundError) {
        super.onErrorRecoveryBegin(backgroundErrorReason, backgroundError);
        assertEquals(BackgroundErrorReason.FLUSH, backgroundErrorReason);
        assertEquals(statusTestData, backgroundError);
        return true;
      }

      @Override
      public void onErrorRecoveryCompleted(final Status oldBackgroundError) {
        super.onErrorRecoveryCompleted(oldBackgroundError);
        assertEquals(statusTestData, oldBackgroundError);
      }
    };

    // test action
    listener.invokeAllCallbacks();

    // assert
    assertAllEventsCalled(listener);
  }

  @Test
  public void testEnabledCallbacks() {
    final EnabledEventCallback enabledEvents[] = {
        EnabledEventCallback.ON_MEMTABLE_SEALED, EnabledEventCallback.ON_ERROR_RECOVERY_COMPLETED};

    final CapturingTestableEventListener listener =
        new CapturingTestableEventListener(enabledEvents);

    // test action
    listener.invokeAllCallbacks();

    // assert
    assertEventsCalled(listener, enabledEvents);
  }

  private static void assertAllEventsCalled(
      final CapturingTestableEventListener capturingTestableEventListener) {
    assertEventsCalled(capturingTestableEventListener, EnumSet.allOf(EnabledEventCallback.class));
  }

  private static void assertEventsCalled(
      final CapturingTestableEventListener capturingTestableEventListener,
      final EnabledEventCallback[] expected) {
    assertEventsCalled(capturingTestableEventListener, EnumSet.copyOf(Arrays.asList(expected)));
  }

  private static void assertEventsCalled(
      final CapturingTestableEventListener capturingTestableEventListener,
      final EnumSet<EnabledEventCallback> expected) {
    final ListenerEvents capturedEvents = capturingTestableEventListener.capturedListenerEvents;

    if (expected.contains(EnabledEventCallback.ON_FLUSH_COMPLETED)) {
      assertTrue("onFlushCompleted was not called", capturedEvents.flushCompleted);
    } else {
      assertFalse("onFlushCompleted was not called", capturedEvents.flushCompleted);
    }

    if (expected.contains(EnabledEventCallback.ON_FLUSH_BEGIN)) {
      assertTrue("onFlushBegin was not called", capturedEvents.flushBegin);
    } else {
      assertFalse("onFlushBegin was called", capturedEvents.flushBegin);
    }

    if (expected.contains(EnabledEventCallback.ON_TABLE_FILE_DELETED)) {
      assertTrue("onTableFileDeleted was not called", capturedEvents.tableFileDeleted);
    } else {
      assertFalse("onTableFileDeleted was called", capturedEvents.tableFileDeleted);
    }

    if (expected.contains(EnabledEventCallback.ON_COMPACTION_BEGIN)) {
      assertTrue("onCompactionBegin was not called", capturedEvents.compactionBegin);
    } else {
      assertFalse("onCompactionBegin was called", capturedEvents.compactionBegin);
    }

    if (expected.contains(EnabledEventCallback.ON_COMPACTION_COMPLETED)) {
      assertTrue("onCompactionCompleted was not called", capturedEvents.compactionCompleted);
    } else {
      assertFalse("onCompactionCompleted was called", capturedEvents.compactionCompleted);
    }

    if (expected.contains(EnabledEventCallback.ON_TABLE_FILE_CREATED)) {
      assertTrue("onTableFileCreated was not called", capturedEvents.tableFileCreated);
    } else {
      assertFalse("onTableFileCreated was called", capturedEvents.tableFileCreated);
    }

    if (expected.contains(EnabledEventCallback.ON_TABLE_FILE_CREATION_STARTED)) {
      assertTrue(
          "onTableFileCreationStarted was not called", capturedEvents.tableFileCreationStarted);
    } else {
      assertFalse("onTableFileCreationStarted was called", capturedEvents.tableFileCreationStarted);
    }

    if (expected.contains(EnabledEventCallback.ON_MEMTABLE_SEALED)) {
      assertTrue("onMemTableSealed was not called", capturedEvents.memTableSealed);
    } else {
      assertFalse("onMemTableSealed was called", capturedEvents.memTableSealed);
    }

    if (expected.contains(EnabledEventCallback.ON_COLUMN_FAMILY_HANDLE_DELETION_STARTED)) {
      assertTrue("onColumnFamilyHandleDeletionStarted was not called",
          capturedEvents.columnFamilyHandleDeletionStarted);
    } else {
      assertFalse("onColumnFamilyHandleDeletionStarted was called",
          capturedEvents.columnFamilyHandleDeletionStarted);
    }

    if (expected.contains(EnabledEventCallback.ON_EXTERNAL_FILE_INGESTED)) {
      assertTrue("onExternalFileIngested was not called", capturedEvents.externalFileIngested);
    } else {
      assertFalse("onExternalFileIngested was called", capturedEvents.externalFileIngested);
    }

    if (expected.contains(EnabledEventCallback.ON_BACKGROUND_ERROR)) {
      assertTrue("onBackgroundError was not called", capturedEvents.backgroundError);
    } else {
      assertFalse("onBackgroundError was called", capturedEvents.backgroundError);
    }

    if (expected.contains(EnabledEventCallback.ON_STALL_CONDITIONS_CHANGED)) {
      assertTrue("onStallConditionsChanged was not called", capturedEvents.stallConditionsChanged);
    } else {
      assertFalse("onStallConditionsChanged was called", capturedEvents.stallConditionsChanged);
    }

    if (expected.contains(EnabledEventCallback.ON_FILE_READ_FINISH)) {
      assertTrue("onFileReadFinish was not called", capturedEvents.fileReadFinish);
    } else {
      assertFalse("onFileReadFinish was called", capturedEvents.fileReadFinish);
    }

    if (expected.contains(EnabledEventCallback.ON_FILE_WRITE_FINISH)) {
      assertTrue("onFileWriteFinish was not called", capturedEvents.fileWriteFinish);
    } else {
      assertFalse("onFileWriteFinish was called", capturedEvents.fileWriteFinish);
    }

    if (expected.contains(EnabledEventCallback.ON_FILE_FLUSH_FINISH)) {
      assertTrue("onFileFlushFinish was not called", capturedEvents.fileFlushFinish);
    } else {
      assertFalse("onFileFlushFinish was called", capturedEvents.fileFlushFinish);
    }

    if (expected.contains(EnabledEventCallback.ON_FILE_SYNC_FINISH)) {
      assertTrue("onFileSyncFinish was not called", capturedEvents.fileSyncFinish);
    } else {
      assertFalse("onFileSyncFinish was called", capturedEvents.fileSyncFinish);
    }

    if (expected.contains(EnabledEventCallback.ON_FILE_RANGE_SYNC_FINISH)) {
      assertTrue("onFileRangeSyncFinish was not called", capturedEvents.fileRangeSyncFinish);
    } else {
      assertFalse("onFileRangeSyncFinish was called", capturedEvents.fileRangeSyncFinish);
    }

    if (expected.contains(EnabledEventCallback.ON_FILE_TRUNCATE_FINISH)) {
      assertTrue("onFileTruncateFinish was not called", capturedEvents.fileTruncateFinish);
    } else {
      assertFalse("onFileTruncateFinish was called", capturedEvents.fileTruncateFinish);
    }

    if (expected.contains(EnabledEventCallback.ON_FILE_CLOSE_FINISH)) {
      assertTrue("onFileCloseFinish was not called", capturedEvents.fileCloseFinish);
    } else {
      assertFalse("onFileCloseFinish was called", capturedEvents.fileCloseFinish);
    }

    if (expected.contains(EnabledEventCallback.SHOULD_BE_NOTIFIED_ON_FILE_IO)) {
      assertTrue(
          "shouldBeNotifiedOnFileIO was not called", capturedEvents.shouldBeNotifiedOnFileIO);
    } else {
      assertFalse("shouldBeNotifiedOnFileIO was called", capturedEvents.shouldBeNotifiedOnFileIO);
    }

    if (expected.contains(EnabledEventCallback.ON_ERROR_RECOVERY_BEGIN)) {
      assertTrue("onErrorRecoveryBegin was not called", capturedEvents.errorRecoveryBegin);
    } else {
      assertFalse("onErrorRecoveryBegin was called", capturedEvents.errorRecoveryBegin);
    }

    if (expected.contains(EnabledEventCallback.ON_ERROR_RECOVERY_COMPLETED)) {
      assertTrue("onErrorRecoveryCompleted was not called", capturedEvents.errorRecoveryCompleted);
    } else {
      assertFalse("onErrorRecoveryCompleted was called", capturedEvents.errorRecoveryCompleted);
    }
  }

  /**
   * Members are volatile as they may be written
   * and read by different threads.
   */
  private static class ListenerEvents {
    volatile boolean flushCompleted;
    volatile boolean flushBegin;
    volatile boolean tableFileDeleted;
    volatile boolean compactionBegin;
    volatile boolean compactionCompleted;
    volatile boolean tableFileCreated;
    volatile boolean tableFileCreationStarted;
    volatile boolean memTableSealed;
    volatile boolean columnFamilyHandleDeletionStarted;
    volatile boolean externalFileIngested;
    volatile boolean backgroundError;
    volatile boolean stallConditionsChanged;
    volatile boolean fileReadFinish;
    volatile boolean fileWriteFinish;
    volatile boolean fileFlushFinish;
    volatile boolean fileSyncFinish;
    volatile boolean fileRangeSyncFinish;
    volatile boolean fileTruncateFinish;
    volatile boolean fileCloseFinish;
    volatile boolean shouldBeNotifiedOnFileIO;
    volatile boolean errorRecoveryBegin;
    volatile boolean errorRecoveryCompleted;
  }

  private static class CapturingTestableEventListener extends TestableEventListener {
    final ListenerEvents capturedListenerEvents = new ListenerEvents();

    public CapturingTestableEventListener() {}

    public CapturingTestableEventListener(final EnabledEventCallback... enabledEventCallbacks) {
      super(enabledEventCallbacks);
    }

    @Override
    public void onFlushCompleted(final RocksDB db, final FlushJobInfo flushJobInfo) {
      capturedListenerEvents.flushCompleted = true;
    }

    @Override
    public void onFlushBegin(final RocksDB db, final FlushJobInfo flushJobInfo) {
      capturedListenerEvents.flushBegin = true;
    }

    @Override
    public void onTableFileDeleted(final TableFileDeletionInfo tableFileDeletionInfo) {
      capturedListenerEvents.tableFileDeleted = true;
    }

    @Override
    public void onCompactionBegin(final RocksDB db, final CompactionJobInfo compactionJobInfo) {
      capturedListenerEvents.compactionBegin = true;
    }

    @Override
    public void onCompactionCompleted(final RocksDB db, final CompactionJobInfo compactionJobInfo) {
      capturedListenerEvents.compactionCompleted = true;
    }

    @Override
    public void onTableFileCreated(final TableFileCreationInfo tableFileCreationInfo) {
      capturedListenerEvents.tableFileCreated = true;
    }

    @Override
    public void onTableFileCreationStarted(
        final TableFileCreationBriefInfo tableFileCreationBriefInfo) {
      capturedListenerEvents.tableFileCreationStarted = true;
    }

    @Override
    public void onMemTableSealed(final MemTableInfo memTableInfo) {
      capturedListenerEvents.memTableSealed = true;
    }

    @Override
    public void onColumnFamilyHandleDeletionStarted(final ColumnFamilyHandle columnFamilyHandle) {
      capturedListenerEvents.columnFamilyHandleDeletionStarted = true;
    }

    @Override
    public void onExternalFileIngested(
        final RocksDB db, final ExternalFileIngestionInfo externalFileIngestionInfo) {
      capturedListenerEvents.externalFileIngested = true;
    }

    @Override
    public void onBackgroundError(
        final BackgroundErrorReason backgroundErrorReason, final Status backgroundError) {
      capturedListenerEvents.backgroundError = true;
    }

    @Override
    public void onStallConditionsChanged(final WriteStallInfo writeStallInfo) {
      capturedListenerEvents.stallConditionsChanged = true;
    }

    @Override
    public void onFileReadFinish(final FileOperationInfo fileOperationInfo) {
      capturedListenerEvents.fileReadFinish = true;
    }

    @Override
    public void onFileWriteFinish(final FileOperationInfo fileOperationInfo) {
      capturedListenerEvents.fileWriteFinish = true;
    }

    @Override
    public void onFileFlushFinish(final FileOperationInfo fileOperationInfo) {
      capturedListenerEvents.fileFlushFinish = true;
    }

    @Override
    public void onFileSyncFinish(final FileOperationInfo fileOperationInfo) {
      capturedListenerEvents.fileSyncFinish = true;
    }

    @Override
    public void onFileRangeSyncFinish(final FileOperationInfo fileOperationInfo) {
      capturedListenerEvents.fileRangeSyncFinish = true;
    }

    @Override
    public void onFileTruncateFinish(final FileOperationInfo fileOperationInfo) {
      capturedListenerEvents.fileTruncateFinish = true;
    }

    @Override
    public void onFileCloseFinish(final FileOperationInfo fileOperationInfo) {
      capturedListenerEvents.fileCloseFinish = true;
    }

    @Override
    public boolean shouldBeNotifiedOnFileIO() {
      capturedListenerEvents.shouldBeNotifiedOnFileIO = true;
      return false;
    }

    @Override
    public boolean onErrorRecoveryBegin(
        final BackgroundErrorReason backgroundErrorReason, final Status backgroundError) {
      capturedListenerEvents.errorRecoveryBegin = true;
      return true;
    }

    @Override
    public void onErrorRecoveryCompleted(final Status oldBackgroundError) {
      capturedListenerEvents.errorRecoveryCompleted = true;
    }
  }
}
