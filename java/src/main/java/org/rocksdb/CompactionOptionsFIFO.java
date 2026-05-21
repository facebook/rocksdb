// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Options for FIFO Compaction
 */
public class CompactionOptionsFIFO extends RocksObject {

  public CompactionOptionsFIFO() {
    super(newCompactionOptionsFIFO());
  }

  /**
   * Once the total sum of table files reaches this, we will delete the oldest
   * table file
   * <p>
   * Default: 1GB
   *
   * @param maxTableFilesSize The maximum size of the table files
   *
   * @return the reference to the current options.
   */
  public CompactionOptionsFIFO setMaxTableFilesSize(
      final long maxTableFilesSize) {
    setMaxTableFilesSize(nativeHandle_, maxTableFilesSize);
    return this;
  }

  /**
   * Once the total sum of table files reaches this, we will delete the oldest
   * table file
   * <p>
   * Default: 1GB
   *
   * @return max table file size in bytes
   */
  public long maxTableFilesSize() {
    return maxTableFilesSize(nativeHandle_);
  }

  /**
   * If true, try to do compaction to compact smaller files into larger ones.
   * Minimum files to compact follows options.level0_file_num_compaction_trigger
   * and compaction won't trigger if average compact bytes per del file is
   * larger than options.write_buffer_size. This is to protect large files
   * from being compacted again.
   * <p>
   * Default: false
   *
   * @param allowCompaction true to allow intra-L0 compaction
   *
   * @return the reference to the current options.
   */
  public CompactionOptionsFIFO setAllowCompaction(
      final boolean allowCompaction) {
    setAllowCompaction(nativeHandle_, allowCompaction);
    return this;
  }

  /**
   * Check if intra-L0 compaction is enabled.
   * When enabled, we try to compact smaller files into larger ones.
   * <p>
   * See {@link #setAllowCompaction(boolean)}.
   * <p>
   * Default: false
   *
   * @return true if intra-L0 compaction is enabled, false otherwise.
   */
  public boolean allowCompaction() {
    return allowCompaction(nativeHandle_);
  }

  /**
   * Combined SST + blob file size limit for FIFO compaction trimming.
   * When non-zero, FIFO uses total_sst + total_blob for size-based dropping.
   * When zero (default), uses max_table_files_size (SST-only).
   *
   * @param maxDataFilesSize the combined size limit in bytes
   *
   * @return the reference to the current options.
   */
  public CompactionOptionsFIFO setMaxDataFilesSize(final long maxDataFilesSize) {
    setMaxDataFilesSize(nativeHandle_, maxDataFilesSize);
    return this;
  }

  /**
   * Get the combined SST + blob file size limit.
   *
   * @return max data files size in bytes, 0 means disabled
   */
  public long maxDataFilesSize() {
    return maxDataFilesSize(nativeHandle_);
  }

  /**
   * Enable capacity-derived intra-L0 compaction using the observed key/value
   * size ratio. Requires maxDataFilesSize &gt; 0.
   *
   * @param useKvRatioCompaction true to enable
   *
   * @return the reference to the current options.
   */
  public CompactionOptionsFIFO setUseKvRatioCompaction(final boolean useKvRatioCompaction) {
    setUseKvRatioCompaction(nativeHandle_, useKvRatioCompaction);
    return this;
  }

  /**
   * Check if capacity-derived intra-L0 compaction is enabled.
   *
   * @return true if enabled
   */
  public boolean useKvRatioCompaction() {
    return useKvRatioCompaction(nativeHandle_);
  }

  private static native long newCompactionOptionsFIFO();
  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }
  private static native void disposeInternalJni(final long handle);

  private static native void setMaxTableFilesSize(final long handle, final long maxTableFilesSize);
  private static native long maxTableFilesSize(final long handle);
  private static native void setAllowCompaction(final long handle, final boolean allowCompaction);
  private static native boolean allowCompaction(final long handle);
  private static native void setMaxDataFilesSize(final long handle, final long maxDataFilesSize);
  private static native long maxDataFilesSize(final long handle);
  private static native void setUseKvRatioCompaction(
      final long handle, final boolean useKvRatioCompaction);
  private static native boolean useKvRatioCompaction(final long handle);
}
