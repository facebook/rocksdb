// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Options for Universal Compaction
 */
public class CompactionOptionsUniversal extends RocksObject {

  public CompactionOptionsUniversal() {
    super(newCompactionOptionsUniversal());
  }

  /**
   * Percentage flexibility while comparing file size. If the candidate file(s)
   * size is 1% smaller than the next file's size, then include next file into
   * this candidate set.
   * <p>
   * Default: 1
   *
   * @param sizeRatio The size ratio to use
   *
   * @return the reference to the current options.
   */
  public CompactionOptionsUniversal setSizeRatio(final int sizeRatio) {
    setSizeRatio(nativeHandle_, sizeRatio);
    return this;
  }

  /**
   * Percentage flexibility while comparing file size. If the candidate file(s)
   * size is 1% smaller than the next file's size, then include next file into
   * this candidate set.
   * <p>
   * Default: 1
   *
   * @return The size ratio in use
   */
  public int sizeRatio() {
    return sizeRatio(nativeHandle_);
  }

  /**
   * The minimum number of files in a single compaction run.
   * <p>
   * Default: 2
   *
   * @param minMergeWidth minimum number of files in a single compaction run
   *
   * @return the reference to the current options.
   */
  public CompactionOptionsUniversal setMinMergeWidth(final int minMergeWidth) {
    setMinMergeWidth(nativeHandle_, minMergeWidth);
    return this;
  }

  /**
   * The minimum number of files in a single compaction run.
   * <p>
   * Default: 2
   *
   * @return minimum number of files in a single compaction run
   */
  public int minMergeWidth() {
    return minMergeWidth(nativeHandle_);
  }

  /**
   * The maximum number of files in a single compaction run.
   * <p>
   * Default: {@link Long#MAX_VALUE}
   *
   * @param maxMergeWidth maximum number of files in a single compaction run
   *
   * @return the reference to the current options.
   */
  public CompactionOptionsUniversal setMaxMergeWidth(final int maxMergeWidth) {
    setMaxMergeWidth(nativeHandle_, maxMergeWidth);
    return this;
  }

  /**
   * The maximum number of files in a single compaction run.
   * <p>
   * Default: {@link Long#MAX_VALUE}
   *
   * @return maximum number of files in a single compaction run
   */
  public int maxMergeWidth() {
    return maxMergeWidth(nativeHandle_);
  }

  /**
   * The size amplification is defined as the amount (in percentage) of
   * additional storage needed to store a single byte of data in the database.
   * For example, a size amplification of 2% means that a database that
   * contains 100 bytes of user-data may occupy upto 102 bytes of
   * physical storage. By this definition, a fully compacted database has
   * a size amplification of 0%. Rocksdb uses the following heuristic
   * to calculate size amplification: it assumes that all files excluding
   * the earliest file contribute to the size amplification.
   * <p>
   * Default: 200, which means that a 100 byte database could require upto
   * 300 bytes of storage.
   *
   * @param maxSizeAmplificationPercent the amount of additional storage needed
   *     (as a percentage) to store a single byte in the database
   *
   * @return the reference to the current options.
   */
  public CompactionOptionsUniversal setMaxSizeAmplificationPercent(
      final int maxSizeAmplificationPercent) {
    setMaxSizeAmplificationPercent(nativeHandle_, maxSizeAmplificationPercent);
    return this;
  }

  /**
   * The size amplification is defined as the amount (in percentage) of
   * additional storage needed to store a single byte of data in the database.
   * For example, a size amplification of 2% means that a database that
   * contains 100 bytes of user-data may occupy upto 102 bytes of
   * physical storage. By this definition, a fully compacted database has
   * a size amplification of 0%. Rocksdb uses the following heuristic
   * to calculate size amplification: it assumes that all files excluding
   * the earliest file contribute to the size amplification.
   * <p>
   * Default: 200, which means that a 100 byte database could require upto
   * 300 bytes of storage.
   *
   * @return the amount of additional storage needed (as a percentage) to store
   *     a single byte in the database
   */
  public int maxSizeAmplificationPercent() {
    return maxSizeAmplificationPercent(nativeHandle_);
  }

  /**
   * If this option is set to be -1 (the default value), all the output files
   * will follow compression type specified.
   * <p>
   * If this option is not negative, we will try to make sure compressed
   * size is just above this value. In normal cases, at least this percentage
   * of data will be compressed.
   * <p>
   * When we are compacting to a new file, here is the criteria whether
   * it needs to be compressed: assuming here are the list of files sorted
   * by generation time:
   *    A1...An B1...Bm C1...Ct
   * where A1 is the newest and Ct is the oldest, and we are going to compact
   * B1...Bm, we calculate the total size of all the files as total_size, as
   * well as  the total size of C1...Ct as total_C, the compaction output file
   * will be compressed iff
   *    total_C / total_size &lt; this percentage
   * <p>
   * Default: -1
   *
   * @param compressionSizePercent percentage of size for compression
   *
   * @return the reference to the current options.
   */
  public CompactionOptionsUniversal setCompressionSizePercent(
      final int compressionSizePercent) {
    setCompressionSizePercent(nativeHandle_, compressionSizePercent);
    return this;
  }

  /**
   * If this option is set to be -1 (the default value), all the output files
   * will follow compression type specified.
   * <p>
   * If this option is not negative, we will try to make sure compressed
   * size is just above this value. In normal cases, at least this percentage
   * of data will be compressed.
   * <p>
   * When we are compacting to a new file, here is the criteria whether
   * it needs to be compressed: assuming here are the list of files sorted
   * by generation time:
   *    A1...An B1...Bm C1...Ct
   * where A1 is the newest and Ct is the oldest, and we are going to compact
   * B1...Bm, we calculate the total size of all the files as total_size, as
   * well as  the total size of C1...Ct as total_C, the compaction output file
   * will be compressed iff
   *    total_C / total_size &lt; this percentage
   * <p>
   * Default: -1
   *
   * @return percentage of size for compression
   */
  public int compressionSizePercent() {
    return compressionSizePercent(nativeHandle_);
  }

  /**
   * The algorithm used to stop picking files into a single compaction run
   * <p>
   * Default: {@link CompactionStopStyle#CompactionStopStyleTotalSize}
   *
   * @param compactionStopStyle The compaction algorithm
   *
   * @return the reference to the current options.
   */
  public CompactionOptionsUniversal setStopStyle(
      final CompactionStopStyle compactionStopStyle) {
    setStopStyle(nativeHandle_, compactionStopStyle.getValue());
    return this;
  }

  /**
   * The algorithm used to stop picking files into a single compaction run
   * <p>
   * Default: {@link CompactionStopStyle#CompactionStopStyleTotalSize}
   *
   * @return The compaction algorithm
   */
  public CompactionStopStyle stopStyle() {
    return CompactionStopStyle.getCompactionStopStyle(stopStyle(nativeHandle_));
  }

  /**
   * Option to optimize the universal multi level compaction by enabling
   * trivial move for non overlapping files.
   * <p>
   * Default: false
   *
   * @param allowTrivialMove true if trivial move is allowed
   *
   * @return the reference to the current options.
   */
  public CompactionOptionsUniversal setAllowTrivialMove(
      final boolean allowTrivialMove) {
    setAllowTrivialMove(nativeHandle_, allowTrivialMove);
    return this;
  }

  /**
   * Option to optimize the universal multi level compaction by enabling
   * trivial move for non overlapping files.
   * <p>
   * Default: false
   *
   * @return true if trivial move is allowed
   */
  public boolean allowTrivialMove() {
    return allowTrivialMove(nativeHandle_);
  }

  private static native long newCompactionOptionsUniversal();
  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }
  private static native void disposeInternalJni(final long handle);

  private static native void setSizeRatio(final long handle, final int sizeRatio);
  private static native int sizeRatio(final long handle);
  private static native void setMinMergeWidth(final long handle, final int minMergeWidth);
  private static native int minMergeWidth(final long handle);
  private static native void setMaxMergeWidth(final long handle, final int maxMergeWidth);
  private static native int maxMergeWidth(final long handle);
  private static native void setMaxSizeAmplificationPercent(
      final long handle, final int maxSizeAmplificationPercent);
  private static native int maxSizeAmplificationPercent(final long handle);
  private static native void setCompressionSizePercent(
      final long handle, final int compressionSizePercent);
  private static native int compressionSizePercent(final long handle);
  private static native void setStopStyle(final long handle, final byte stopStyle);
  private static native byte stopStyle(final long handle);
  private static native void setAllowTrivialMove(final long handle, final boolean allowTrivialMove);
  private static native boolean allowTrivialMove(final long handle);
}
