// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.List;

/**
 * EventListener class contains a set of callback functions that will
 * be called when specific RocksDB event happens such as flush.  It can
 * be used as a building block for developing custom features such as
 * stats-collector or external compaction algorithm.
 *
 * Note that callback functions should not run for an extended period of
 * time before the function returns, otherwise RocksDB may be blocked.
 * For example, it is not suggested to do
 * {@link RocksDB#compactFiles(CompactionOptions, ColumnFamilyHandle, List, int, int,
 * CompactionJobInfo)} (as it may run for a long while) or issue many of
 * {@link RocksDB#put(ColumnFamilyHandle, WriteOptions, byte[], byte[])}
 * (as Put may be blocked in certain cases) in the same thread in the
 * EventListener callback.
 *
 * However, doing
 * {@link RocksDB#compactFiles(CompactionOptions, ColumnFamilyHandle, List, int, int,
 * CompactionJobInfo)} and {@link RocksDB#put(ColumnFamilyHandle, WriteOptions, byte[], byte[])} in
 * another thread is considered safe.
 *
 * [Threading] All EventListener callback will be called using the
 * actual thread that involves in that specific event. For example, it
 * is the RocksDB background flush thread that does the actual flush to
 * call {@link #onFlushCompleted(RocksDB, FlushJobInfo)}.
 *
 * [Locking] All EventListener callbacks are designed to be called without
 * the current thread holding any DB mutex. This is to prevent potential
 * deadlock and performance issue when using EventListener callback
 * in a complex way.
 */
public interface EventListener {
  /**
   * A callback function to RocksDB which will be called before a
   * RocksDB starts to flush memtables.
   *
   * Note that the this function must be implemented in a way such that
   * it should not run for an extended period of time before the function
   * returns. Otherwise, RocksDB may be blocked.
   *
   * @param db the database
   * @param flushJobInfo the flush job info, contains data copied from
   *     respective native structure.
   */
  void onFlushBegin(final RocksDB db, final FlushJobInfo flushJobInfo);

  /**
   * callback function to RocksDB which will be called whenever a
   * registered RocksDB flushes a file.
   *
   * Note that the this function must be implemented in a way such that
   * it should not run for an extended period of time before the function
   * returns. Otherwise, RocksDB may be blocked.
   *
   * @param db the database
   * @param flushJobInfo the flush job info, contains data copied from
   *     respective native structure.
   */
  void onFlushCompleted(final RocksDB db, final FlushJobInfo flushJobInfo);

  /**
   * A callback function for RocksDB which will be called whenever
   * a SST file is deleted. Different from
   * {@link #onCompactionCompleted(RocksDB, CompactionJobInfo)} and
   * {@link #onFlushCompleted(RocksDB, FlushJobInfo)},
   * this callback is designed for external logging
   * service and thus only provide string parameters instead
   * of a pointer to DB.  Applications that build logic basic based
   * on file creations and deletions is suggested to implement
   * {@link #onFlushCompleted(RocksDB, FlushJobInfo)} and
   * {@link #onCompactionCompleted(RocksDB, CompactionJobInfo)}.
   *
   * Note that if applications would like to use the passed reference
   * outside this function call, they should make copies from the
   * returned value.
   *
   * @param tableFileDeletionInfo the table file deletion info,
   *     contains data copied from respective native structure.
   */
  void onTableFileDeleted(final TableFileDeletionInfo tableFileDeletionInfo);

  /**
   * A callback function to RocksDB which will be called before a
   * RocksDB starts to compact. The default implementation is
   * no-op.
   *
   * Note that the this function must be implemented in a way such that
   * it should not run for an extended period of time before the function
   * returns. Otherwise, RocksDB may be blocked.
   *
   * @param db a pointer to the rocksdb instance which just compacted
   *     a file.
   * @param compactionJobInfo a reference to a native CompactionJobInfo struct,
   *     which is released after this function is returned, and must be copied
   *     if it is needed outside of this function.
   */
  void onCompactionBegin(final RocksDB db, final CompactionJobInfo compactionJobInfo);

  /**
   * A callback function for RocksDB which will be called whenever
   * a registered RocksDB compacts a file. The default implementation
   * is a no-op.
   *
   * Note that this function must be implemented in a way such that
   * it should not run for an extended period of time before the function
   * returns. Otherwise, RocksDB may be blocked.
   *
   * @param db a pointer to the rocksdb instance which just compacted
   *     a file.
   * @param compactionJobInfo a reference to a native CompactionJobInfo struct,
   *     which is released after this function is returned, and must be copied
   *     if it is needed outside of this function.
   */
  void onCompactionCompleted(final RocksDB db, final CompactionJobInfo compactionJobInfo);

  /**
   * A callback function for RocksDB which will be called whenever
   * a SST file is created.  Different from OnCompactionCompleted and
   * OnFlushCompleted, this callback is designed for external logging
   * service and thus only provide string parameters instead
   * of a pointer to DB.  Applications that build logic basic based
   * on file creations and deletions is suggested to implement
   * OnFlushCompleted and OnCompactionCompleted.
   *
   * Historically it will only be called if the file is successfully created.
   * Now it will also be called on failure case. User can check info.status
   * to see if it succeeded or not.
   *
   * Note that if applications would like to use the passed reference
   * outside this function call, they should make copies from these
   * returned value.
   *
   * @param tableFileCreationInfo the table file creation info,
   *     contains data copied from respective native structure.
   */
  void onTableFileCreated(final TableFileCreationInfo tableFileCreationInfo);

  /**
   * A callback function for RocksDB which will be called before
   * a SST file is being created. It will follow by OnTableFileCreated after
   * the creation finishes.
   *
   * Note that if applications would like to use the passed reference
   * outside this function call, they should make copies from these
   * returned value.
   *
   * @param tableFileCreationBriefInfo the table file creation brief info,
   *     contains data copied from respective native structure.
   */
  void onTableFileCreationStarted(final TableFileCreationBriefInfo tableFileCreationBriefInfo);

  /**
   * A callback function for RocksDB which will be called before
   * a memtable is made immutable.
   *
   * Note that the this function must be implemented in a way such that
   * it should not run for an extended period of time before the function
   * returns.  Otherwise, RocksDB may be blocked.
   *
   * Note that if applications would like to use the passed reference
   * outside this function call, they should make copies from these
   * returned value.
   *
   * @param memTableInfo the mem table info, contains data
   *     copied from respective native structure.
   */
  void onMemTableSealed(final MemTableInfo memTableInfo);

  /**
   * A callback function for RocksDB which will be called before
   * a column family handle is deleted.
   *
   * Note that the this function must be implemented in a way such that
   * it should not run for an extended period of time before the function
   * returns.  Otherwise, RocksDB may be blocked.
   *
   * @param columnFamilyHandle is a pointer to the column family handle to be
   *     deleted which will become a dangling pointer after the deletion.
   */
  void onColumnFamilyHandleDeletionStarted(final ColumnFamilyHandle columnFamilyHandle);

  /**
   * A callback function for RocksDB which will be called after an external
   * file is ingested using IngestExternalFile.
   *
   * Note that the this function will run on the same thread as
   * IngestExternalFile(), if this function is blocked, IngestExternalFile()
   * will be blocked from finishing.
   *
   * @param db the database
   * @param externalFileIngestionInfo the external file ingestion info,
   *     contains data copied from respective native structure.
   */
  void onExternalFileIngested(
      final RocksDB db, final ExternalFileIngestionInfo externalFileIngestionInfo);

  /**
   * A callback function for RocksDB which will be called before setting the
   * background error status to a non-OK value. The new background error status
   * is provided in `bg_error` and can be modified by the callback. E.g., a
   * callback can suppress errors by resetting it to Status::OK(), thus
   * preventing the database from entering read-only mode. We do not provide any
   * guarantee when failed flushes/compactions will be rescheduled if the user
   * suppresses an error.
   *
   * Note that this function can run on the same threads as flush, compaction,
   * and user writes. So, it is extremely important not to perform heavy
   * computations or blocking calls in this function.
   *
   * @param backgroundErrorReason background error reason code
   * @param backgroundError background error codes
   */
  void onBackgroundError(
      final BackgroundErrorReason backgroundErrorReason, final Status backgroundError);

  /**
   * A callback function for RocksDB which will be called whenever a change
   * of superversion triggers a change of the stall conditions.
   *
   * Note that the this function must be implemented in a way such that
   * it should not run for an extended period of time before the function
   * returns. Otherwise, RocksDB may be blocked.
   *
   * @param writeStallInfo write stall info,
   *     contains data copied from respective native structure.
   */
  void onStallConditionsChanged(final WriteStallInfo writeStallInfo);

  /**
   * A callback function for RocksDB which will be called whenever a file read
   * operation finishes.
   *
   * @param fileOperationInfo file operation info,
   *     contains data copied from respective native structure.
   */
  void onFileReadFinish(final FileOperationInfo fileOperationInfo);

  /**
   * A callback function for RocksDB which will be called whenever a file write
   * operation finishes.
   *
   * @param fileOperationInfo file operation info,
   *     contains data copied from respective native structure.
   */
  void onFileWriteFinish(final FileOperationInfo fileOperationInfo);

  /**
   * A callback function for RocksDB which will be called whenever a file flush
   * operation finishes.
   *
   * @param fileOperationInfo file operation info,
   *     contains data copied from respective native structure.
   */
  void onFileFlushFinish(final FileOperationInfo fileOperationInfo);

  /**
   * A callback function for RocksDB which will be called whenever a file sync
   * operation finishes.
   *
   * @param fileOperationInfo file operation info,
   *     contains data copied from respective native structure.
   */
  void onFileSyncFinish(final FileOperationInfo fileOperationInfo);

  /**
   * A callback function for RocksDB which will be called whenever a file
   * rangeSync operation finishes.
   *
   * @param fileOperationInfo file operation info,
   *     contains data copied from respective native structure.
   */
  void onFileRangeSyncFinish(final FileOperationInfo fileOperationInfo);

  /**
   * A callback function for RocksDB which will be called whenever a file
   * truncate operation finishes.
   *
   * @param fileOperationInfo file operation info,
   *     contains data copied from respective native structure.
   */
  void onFileTruncateFinish(final FileOperationInfo fileOperationInfo);

  /**
   * A callback function for RocksDB which will be called whenever a file close
   * operation finishes.
   *
   * @param fileOperationInfo file operation info,
   *     contains data copied from respective native structure.
   */
  void onFileCloseFinish(final FileOperationInfo fileOperationInfo);

  /**
   * If true, the {@link #onFileReadFinish(FileOperationInfo)}
   * and {@link #onFileWriteFinish(FileOperationInfo)} will be called. If
   * false, then they won't be called.
   *
   * Default: false
   */
  boolean shouldBeNotifiedOnFileIO();

  /**
   * A callback function for RocksDB which will be called just before
   * starting the automatic recovery process for recoverable background
   * errors, such as NoSpace(). The callback can suppress the automatic
   * recovery by setting returning false. The database will then
   * have to be transitioned out of read-only mode by calling
   * RocksDB#resume().
   *
   * @param backgroundErrorReason background error reason code
   * @param backgroundError background error codes
   */
  boolean onErrorRecoveryBegin(
      final BackgroundErrorReason backgroundErrorReason, final Status backgroundError);

  /**
   * A callback function for RocksDB which will be called once the database
   * is recovered from read-only mode after an error. When this is called, it
   * means normal writes to the database can be issued and the user can
   * initiate any further recovery actions needed
   *
   * @param oldBackgroundError old background error codes
   */
  void onErrorRecoveryCompleted(final Status oldBackgroundError);
}
