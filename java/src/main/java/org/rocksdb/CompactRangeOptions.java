// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * CompactRangeOptions is used by CompactRange() call. In the documentation of the methods "the compaction" refers to
 * any compaction that is using this CompactRangeOptions.
 */
public class CompactRangeOptions extends RocksObject {

  // For level based compaction, we can configure if we want to skip/force bottommost level compaction.
  // The order of this neum MUST follow the C++ layer. See BottommostLevelCompaction in db/options.h
  public enum BottommostLevelCompaction {
    /**
     * Skip bottommost level compaction
     */
    kSkip,
    /**
     * Only compact bottommost level if there is a compaction filter. This is the default option
     */
    kIfHaveCompactionFilter,
    /**
     * Always compact bottommost level
     */
    kForce,;

    /**
     * Returns the native C++ rocks enum value
     * @return the native C++ rocks enum value
     */
    public int rocksId() {
      return this.ordinal(); // identical with the C++ layer
    }

    /**
     * Returns the BottommostLevelCompaction for the given C++ rocks enum value.
     * @param bottommostLevelCompaction
     * @return BottommostLevelCompaction instance, or null if none matches
     */
    public static BottommostLevelCompaction fromRocksId(int bottommostLevelCompaction) {
      switch (bottommostLevelCompaction) {
        case 0: return kSkip;
        case 1: return kIfHaveCompactionFilter;
        case 2: return kForce;
        default: return null;
      }
    }
  }

  ;


  /**
   * Construct CompactRangeOptions.
   */
  public CompactRangeOptions() {
    super(newCompactRangeOptions());
  }

  /**
   * Returns whether the compaction is exclusive or other compactions may run concurrently at the same time.
   *
   * @return true if exclusive, false if concurrent
   */
  public boolean exclusiveManualCompaction() {
    return exclusiveManualCompaction(nativeHandle_);
  }

  /**
   * Sets whether the compaction is exclusive or other compaction are allowed run concurrently at the same time.
   *
   * @param exclusiveCompaction true if compaction should be exclusive
   */
  public CompactRangeOptions setExclusiveManualCompaction(boolean exclusiveCompaction) {
    setExclusiveManualCompaction(nativeHandle_, exclusiveCompaction);
    return this;
  }


  public BottommostLevelCompaction bottommostLevelCompaction() {
    return BottommostLevelCompaction.fromRocksId(bottommostLevelCompaction(nativeHandle_));
  }

  public CompactRangeOptions setBottommostLevelCompaction(BottommostLevelCompaction bottommostLevelCompaction) {
    setBottommostLevelCompaction(nativeHandle_, bottommostLevelCompaction.rocksId());
    return this;
  }

  /**
   * Returns whether compacted files will be moved to the minimum level capable of holding the data or given level
   * (specified non-negative target_level).
   * @return true, if compacted files will be moved to the minimum level
   */
  public boolean changeLevel() {
    return changeLevel(nativeHandle_);
  }

  /**
   * Whether compacted files will be moved to the minimum level capable of holding the data or given level
   * (specified non-negative target_level).
   * @param changeLevel If true, compacted files will be moved to the minimum level
   */
  public void setChangeLevel(boolean changeLevel) {
    setChangeLevel(nativeHandle_, changeLevel);
  }

  /**
   * If change_level is true and target_level have non-negative value, compacted files will be moved to target_level.
   */
  public int targetLevel() {
    return targetLevel(nativeHandle_);
  }


  /**
   * If change_level is true and target_level have non-negative value, compacted files will be moved to target_level.
   */
  public void setTargetLevel(int targetLevel) {
    setTargetLevel(nativeHandle_, targetLevel);
  }

  /**
   * target_path_id for compaction output. Compaction outputs will be placed in options.db_paths[target_path_id].
   *
   * @return target_path_id
   */
  public int targetPathId() {
    return targetPathId(nativeHandle_);
  }

  /**
   * Compaction outputs will be placed in options.db_paths[target_path_id]. Behavior is undefined if target_path_id is
   * out of range.
   *
   * @param targetPathId target path id
   */
  public void setTargetPathId(int targetPathId) {
    setTargetPathId(nativeHandle_, targetPathId);
  }

  /**
   * If true, compaction will execute immediately even if doing so would cause the DB to
   // enter write stall mode. Otherwise, it'll sleep until load is low enough.
   * @return true if compaction will execute immediately
   */
  public boolean allowWriteStall() {
    return allowWriteStall(nativeHandle_);
  }


  /**
   * If true, compaction will execute immediately even if doing so would cause the DB to
   // enter write stall mode. Otherwise, it'll sleep until load is low enough.

   * @param allowWriteStall true if compaction should execute immediately
   */
  public void setAllowWriteStall(boolean allowWriteStall) {
    setAllowWriteStall(nativeHandle_, allowWriteStall);
  }

  /**
   * If &gt; 0, it will replace the option in the DBOptions for this compaction
   * @return number of subcompactions
   */
  public int maxSubcompactions() {
    return maxSubcompactions(nativeHandle_);
  }

  /**
   * If &gt; 0, it will replace the option in the DBOptions for this compaction
   * @param maxSubcompactions number of subcompactions
   */
  public void setMaxSubcompactions(int maxSubcompactions) {
    setMaxSubcompactions(nativeHandle_, maxSubcompactions);
  }

  private native static long newCompactRangeOptions();
  private native boolean exclusiveManualCompaction(final long handle);
  private native void setExclusiveManualCompaction(final long handle, boolean exclusive_manual_compaction);
  private native int bottommostLevelCompaction(final long handle);
  private native void setBottommostLevelCompaction(final long handle, int bottommostLevelCompaction);
  private native boolean changeLevel(final long handle);
  private native void setChangeLevel(final long handle, boolean changeLevel);
  private native int targetLevel(long nativeHandle_);
  private native void setTargetLevel(long nativeHandle_, int targetLevel);
  private native int targetPathId(long nativeHandle_);
  private native void setTargetPathId(long nativeHandle_, int /* uint32_t */ targetPathId);
  private native boolean allowWriteStall(long nativeHandle_);
  private native void setAllowWriteStall(long nativeHandle_, boolean allowWriteStall);
  private native void setMaxSubcompactions(long nativeHandle_, int /* uint32_t */ maxSubcompactions);
  private native int maxSubcompactions(long nativeHandle_);

  @Override
  protected final native void disposeInternal(final long handle);

}
