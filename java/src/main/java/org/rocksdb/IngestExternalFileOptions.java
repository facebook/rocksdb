package org.rocksdb;
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

import java.util.List;

/**
 * IngestExternalFileOptions is used by {@link RocksDB#ingestExternalFile(ColumnFamilyHandle, List, IngestExternalFileOptions)}
 */
public class IngestExternalFileOptions extends RocksObject {

  public IngestExternalFileOptions() {
    super(newIngestExternalFileOptions());
  }

  /**
   * @param moveFiles {@link #setMoveFiles(boolean)}
   * @param snapshotConsistency {@link #setSnapshotConsistency(boolean)}
   * @param allowGlobalSeqNo {@link #setAllowGlobalSeqNo(boolean)}
   * @param allowBlockingFlush {@link #setAllowBlockingFlush(boolean)}
   */
  public IngestExternalFileOptions(final boolean moveFiles,
      final boolean snapshotConsistency, final boolean allowGlobalSeqNo,
      final boolean allowBlockingFlush) {
    super(newIngestExternalFileOptions(moveFiles, snapshotConsistency,
        allowGlobalSeqNo, allowBlockingFlush));
  }

  /**
   * Can be set to true to move the files instead of copying them.
   *
   * @return true if files will be moved
   */
  public boolean moveFiles() {
    return moveFiles(nativeHandle_);
  }

  /**
   * Can be set to true to move the files instead of copying them.
   *
   * @param moveFiles true if files should be moved instead of copied
   */
  public void setMoveFiles(final boolean moveFiles) {
    setMoveFiles(nativeHandle_, moveFiles);
  }

  /**
   * If set to false, an ingested file keys could appear in existing snapshots
   * that where created before the file was ingested.
   *
   * @return true if snapshot consistency is assured
   */
  public boolean snapshotConsistency() {
    return snapshotConsistency(nativeHandle_);
  }

  /**
   * If set to false, an ingested file keys could appear in existing snapshots
   * that where created before the file was ingested.
   *
   * @param snapshotConsistency true if snapshot consistency is required
   */
  public void setSnapshotConsistency(final boolean snapshotConsistency) {
    setSnapshotConsistency(nativeHandle_, snapshotConsistency);
  }

  /**
   * If set to false, {@link RocksDB#ingestExternalFile(ColumnFamilyHandle, List, IngestExternalFileOptions)}
   * will fail if the file key range overlaps with existing keys or tombstones in the DB.
   *
   * @return true if global seq numbers are assured
   */
  public boolean allowGlobalSeqNo() {
    return allowGlobalSeqNo(nativeHandle_);
  }

  /**
   * If set to false, {@link RocksDB#ingestExternalFile(ColumnFamilyHandle, List, IngestExternalFileOptions)}
   * will fail if the file key range overlaps with existing keys or tombstones in the DB.
   *
   * @param allowGlobalSeqNo true if global seq numbers are required
   */
  public void setAllowGlobalSeqNo(final boolean allowGlobalSeqNo) {
    setAllowGlobalSeqNo(nativeHandle_, allowGlobalSeqNo);
  }

  /**
   * If set to false and the file key range overlaps with the memtable key range
   * (memtable flush required), IngestExternalFile will fail.
   *
   * @return true if blocking flushes may occur
   */
  public boolean allowBlockingFlush() {
    return allowBlockingFlush(nativeHandle_);
  }

  /**
   * If set to false and the file key range overlaps with the memtable key range
   * (memtable flush required), IngestExternalFile will fail.
   *
   * @param allowBlockingFlush true if blocking flushes are allowed
   */
  public void setAllowBlockingFlush(final boolean allowBlockingFlush) {
    setAllowBlockingFlush(nativeHandle_, allowBlockingFlush);
  }

  private native static long newIngestExternalFileOptions();
  private native static long newIngestExternalFileOptions(
      final boolean moveFiles, final boolean snapshotConsistency,
      final boolean allowGlobalSeqNo, final boolean allowBlockingFlush);
  private native boolean moveFiles(final long handle);
  private native void setMoveFiles(final long handle, final boolean move_files);
  private native boolean snapshotConsistency(final long handle);
  private native void setSnapshotConsistency(final long handle,
      final boolean snapshotConsistency);
  private native boolean allowGlobalSeqNo(final long handle);
  private native void setAllowGlobalSeqNo(final long handle,
      final boolean allowGloablSeqNo);
  private native boolean allowBlockingFlush(final long handle);
  private native void setAllowBlockingFlush(final long handle,
      final boolean allowBlockingFlush);
  @Override protected final native void disposeInternal(final long handle);
}
