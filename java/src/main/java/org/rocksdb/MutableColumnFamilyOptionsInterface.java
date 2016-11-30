// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

public interface MutableColumnFamilyOptionsInterface {

  /**
   * Amount of data to build up in memory (backed by an unsorted log
   * on disk) before converting to a sorted on-disk file.
   *
   * Larger values increase performance, especially during bulk loads.
   * Up to {@code max_write_buffer_number} write buffers may be held in memory
   * at the same time, so you may wish to adjust this parameter
   * to control memory usage.
   *
   * Also, a larger write buffer will result in a longer recovery time
   * the next time the database is opened.
   *
   * Default: 4MB
   * @param writeBufferSize the size of write buffer.
   * @return the instance of the current Object.
   * @throws java.lang.IllegalArgumentException thrown on 32-Bit platforms
   *   while overflowing the underlying platform specific value.
   */
  MutableColumnFamilyOptionsInterface setWriteBufferSize(long writeBufferSize);

  /**
   * Return size of write buffer size.
   *
   * @return size of write buffer.
   * @see #setWriteBufferSize(long)
   */
  long writeBufferSize();

  /**
   * The size of one block in arena memory allocation.
   * If &le; 0, a proper value is automatically calculated (usually 1/10 of
   * writer_buffer_size).
   *
   * There are two additional restriction of the The specified size:
   * (1) size should be in the range of [4096, 2 &lt;&lt; 30] and
   * (2) be the multiple of the CPU word (which helps with the memory
   * alignment).
   *
   * We'll automatically check and adjust the size number to make sure it
   * conforms to the restrictions.
   * Default: 0
   *
   * @param arenaBlockSize the size of an arena block
   * @return the reference to the current option.
   * @throws java.lang.IllegalArgumentException thrown on 32-Bit platforms
   *   while overflowing the underlying platform specific value.
   */
  MutableColumnFamilyOptionsInterface setArenaBlockSize(long arenaBlockSize);

  /**
   * The size of one block in arena memory allocation.
   * If &le; 0, a proper value is automatically calculated (usually 1/10 of
   * writer_buffer_size).
   *
   * There are two additional restriction of the The specified size:
   * (1) size should be in the range of [4096, 2 &lt;&lt; 30] and
   * (2) be the multiple of the CPU word (which helps with the memory
   * alignment).
   *
   * We'll automatically check and adjust the size number to make sure it
   * conforms to the restrictions.
   * Default: 0
   *
   * @return the size of an arena block
   */
  long arenaBlockSize();

  /**
   * if prefix_extractor is set and memtable_prefix_bloom_size_ratio is not 0,
   * create prefix bloom for memtable with the size of
   * write_buffer_size * memtable_prefix_bloom_size_ratio.
   * If it is larger than 0.25, it is santinized to 0.25.
   *
   * Default: 0 (disable)
   *
   * @param memtablePrefixBloomSizeRatio The ratio
   * @return the reference to the current option.
   */
  MutableColumnFamilyOptionsInterface setMemtablePrefixBloomSizeRatio(
      double memtablePrefixBloomSizeRatio);

  /**
   * if prefix_extractor is set and memtable_prefix_bloom_size_ratio is not 0,
   * create prefix bloom for memtable with the size of
   * write_buffer_size * memtable_prefix_bloom_size_ratio.
   * If it is larger than 0.25, it is santinized to 0.25.
   *
   * Default: 0 (disable)
   *
   * @return the ratio
   */
  double memtablePrefixBloomSizeRatio();

  /**
   * Page size for huge page TLB for bloom in memtable. If &le; 0, not allocate
   * from huge page TLB but from malloc.
   * Need to reserve huge pages for it to be allocated. For example:
   *     sysctl -w vm.nr_hugepages=20
   * See linux doc Documentation/vm/hugetlbpage.txt
   *
   * @param memtableHugePageSize The page size of the huge
   *     page tlb
   * @return the reference to the current option.
   */
  MutableColumnFamilyOptionsInterface setMemtableHugePageSize(
      long memtableHugePageSize);

  /**
   * Page size for huge page TLB for bloom in memtable. If &le; 0, not allocate
   * from huge page TLB but from malloc.
   * Need to reserve huge pages for it to be allocated. For example:
   *     sysctl -w vm.nr_hugepages=20
   * See linux doc Documentation/vm/hugetlbpage.txt
   *
   * @return The page size of the huge page tlb
   */
  long memtableHugePageSize();

  /**
   * Maximum number of successive merge operations on a key in the memtable.
   *
   * When a merge operation is added to the memtable and the maximum number of
   * successive merges is reached, the value of the key will be calculated and
   * inserted into the memtable instead of the merge operation. This will
   * ensure that there are never more than max_successive_merges merge
   * operations in the memtable.
   *
   * Default: 0 (disabled)
   *
   * @param maxSuccessiveMerges the maximum number of successive merges.
   * @return the reference to the current option.
   * @throws java.lang.IllegalArgumentException thrown on 32-Bit platforms
   *   while overflowing the underlying platform specific value.
   */
  MutableColumnFamilyOptionsInterface setMaxSuccessiveMerges(
      long maxSuccessiveMerges);

  /**
   * Maximum number of successive merge operations on a key in the memtable.
   *
   * When a merge operation is added to the memtable and the maximum number of
   * successive merges is reached, the value of the key will be calculated and
   * inserted into the memtable instead of the merge operation. This will
   * ensure that there are never more than max_successive_merges merge
   * operations in the memtable.
   *
   * Default: 0 (disabled)
   *
   * @return the maximum number of successive merges.
   */
  long maxSuccessiveMerges();

  /**
   * The maximum number of write buffers that are built up in memory.
   * The default is 2, so that when 1 write buffer is being flushed to
   * storage, new writes can continue to the other write buffer.
   * Default: 2
   *
   * @param maxWriteBufferNumber maximum number of write buffers.
   * @return the instance of the current Object.
   */
  MutableColumnFamilyOptionsInterface setMaxWriteBufferNumber(
      int maxWriteBufferNumber);

  /**
   * Returns maximum number of write buffers.
   *
   * @return maximum number of write buffers.
   * @see #setMaxWriteBufferNumber(int)
   */
  int maxWriteBufferNumber();

  /**
   * Number of locks used for inplace update
   * Default: 10000, if inplace_update_support = true, else 0.
   *
   * @param inplaceUpdateNumLocks the number of locks used for
   *     inplace updates.
   * @return the reference to the current option.
   * @throws java.lang.IllegalArgumentException thrown on 32-Bit platforms
   *   while overflowing the underlying platform specific value.
   */
  MutableColumnFamilyOptionsInterface setInplaceUpdateNumLocks(
      long inplaceUpdateNumLocks);

  /**
   * Number of locks used for inplace update
   * Default: 10000, if inplace_update_support = true, else 0.
   *
   * @return the number of locks used for inplace update.
   */
  long inplaceUpdateNumLocks();

  /**
   * Disable automatic compactions. Manual compactions can still
   * be issued on this column family
   *
   * @param disableAutoCompactions true if auto-compactions are disabled.
   * @return the reference to the current option.
   */
  MutableColumnFamilyOptionsInterface setDisableAutoCompactions(
      boolean disableAutoCompactions);

  /**
   * Disable automatic compactions. Manual compactions can still
   * be issued on this column family
   *
   * @return true if auto-compactions are disabled.
   */
  boolean disableAutoCompactions();

  /**
   * Puts are delayed 0-1 ms when any level has a compaction score that exceeds
   * soft_rate_limit. This is ignored when == 0.0.
   * CONSTRAINT: soft_rate_limit &le; hard_rate_limit. If this constraint does
   * not hold, RocksDB will set soft_rate_limit = hard_rate_limit
   * Default: 0 (disabled)
   *
   * @param softRateLimit the soft-rate-limit of a compaction score
   *     for put delay.
   * @return the reference to the current option.
   *
   * @deprecated Instead use {@link #setSoftPendingCompactionBytesLimit(long)}
   */
  @Deprecated
  MutableColumnFamilyOptionsInterface setSoftRateLimit(double softRateLimit);

  /**
   * Puts are delayed 0-1 ms when any level has a compaction score that exceeds
   * soft_rate_limit. This is ignored when == 0.0.
   * CONSTRAINT: soft_rate_limit &le; hard_rate_limit. If this constraint does
   * not hold, RocksDB will set soft_rate_limit = hard_rate_limit
   * Default: 0 (disabled)
   *
   * @return soft-rate-limit for put delay.
   *
   * @deprecated Instead use {@link #softPendingCompactionBytesLimit()}
   */
  @Deprecated
  double softRateLimit();

  /**
   * All writes will be slowed down to at least delayed_write_rate if estimated
   * bytes needed to be compaction exceed this threshold.
   *
   * Default: 64GB
   *
   * @param softPendingCompactionBytesLimit The soft limit to impose on
   *   compaction
   * @return the reference to the current option.
   */
  MutableColumnFamilyOptionsInterface setSoftPendingCompactionBytesLimit(
      long softPendingCompactionBytesLimit);

  /**
   * All writes will be slowed down to at least delayed_write_rate if estimated
   * bytes needed to be compaction exceed this threshold.
   *
   * Default: 64GB
   *
   * @return The soft limit to impose on compaction
   */
  long softPendingCompactionBytesLimit();

  /**
   * Puts are delayed 1ms at a time when any level has a compaction score that
   * exceeds hard_rate_limit. This is ignored when &le; 1.0.
   * Default: 0 (disabled)
   *
   * @param hardRateLimit the hard-rate-limit of a compaction score for put
   *     delay.
   * @return the reference to the current option.
   *
   * @deprecated Instead use {@link #setHardPendingCompactionBytesLimit(long)}
   */
  @Deprecated
  MutableColumnFamilyOptionsInterface setHardRateLimit(double hardRateLimit);

  /**
   * Puts are delayed 1ms at a time when any level has a compaction score that
   * exceeds hard_rate_limit. This is ignored when &le; 1.0.
   * Default: 0 (disabled)
   *
   * @return the hard-rate-limit of a compaction score for put delay.
   *
   * @deprecated Instead use {@link #hardPendingCompactionBytesLimit()}
   */
  @Deprecated
  double hardRateLimit();

  /**
   * All writes are stopped if estimated bytes needed to be compaction exceed
   * this threshold.
   *
   * Default: 256GB
   *
   * @param hardPendingCompactionBytesLimit The hard limit to impose on
   *   compaction
   * @return the reference to the current option.
   */
  MutableColumnFamilyOptionsInterface setHardPendingCompactionBytesLimit(
      long hardPendingCompactionBytesLimit);

  /**
   * All writes are stopped if estimated bytes needed to be compaction exceed
   * this threshold.
   *
   * Default: 256GB
   *
   * @return The hard limit to impose on compaction
   */
  long hardPendingCompactionBytesLimit();

  /**
   * Number of files to trigger level-0 compaction. A value &lt; 0 means that
   * level-0 compaction will not be triggered by number of files at all.
   *
   * Default: 4
   *
   * @param level0FileNumCompactionTrigger The number of files to trigger
   *   level-0 compaction
   * @return the reference to the current option.
   */
  MutableColumnFamilyOptionsInterface setLevel0FileNumCompactionTrigger(
      int level0FileNumCompactionTrigger);

  /**
   * Number of files to trigger level-0 compaction. A value &lt; 0 means that
   * level-0 compaction will not be triggered by number of files at all.
   *
   * Default: 4
   *
   * @return The number of files to trigger
   */
  int level0FileNumCompactionTrigger();

  /**
   * Soft limit on number of level-0 files. We start slowing down writes at this
   * point. A value &lt; 0 means that no writing slow down will be triggered by
   * number of files in level-0.
   *
   * @param level0SlowdownWritesTrigger The soft limit on the number of
   *   level-0 files
   * @return the reference to the current option.
   */
  MutableColumnFamilyOptionsInterface setLevel0SlowdownWritesTrigger(
      int level0SlowdownWritesTrigger);

  /**
   * Soft limit on number of level-0 files. We start slowing down writes at this
   * point. A value &lt; 0 means that no writing slow down will be triggered by
   * number of files in level-0.
   *
   * @return The soft limit on the number of
   *   level-0 files
   */
  int level0SlowdownWritesTrigger();

  /**
   * Maximum number of level-0 files.  We stop writes at this point.
   *
   * @param level0StopWritesTrigger The maximum number of level-0 files
   * @return the reference to the current option.
   */
  MutableColumnFamilyOptionsInterface setLevel0StopWritesTrigger(
      int level0StopWritesTrigger);

  /**
   * Maximum number of level-0 files.  We stop writes at this point.
   *
   * @return The maximum number of level-0 files
   */
  int level0StopWritesTrigger();

  /**
   * We try to limit number of bytes in one compaction to be lower than this
   * threshold. But it's not guaranteed.
   * Value 0 will be sanitized.
   *
   * @param maxCompactionBytes max bytes in a compaction
   * @return the reference to the current option.
   * @see #maxCompactionBytes()
   */
  MutableColumnFamilyOptionsInterface setMaxCompactionBytes(final long maxCompactionBytes);

  /**
   * We try to limit number of bytes in one compaction to be lower than this
   * threshold. But it's not guaranteed.
   * Value 0 will be sanitized.
   *
   * @return the maximum number of bytes in for a compaction.
   * @see #setMaxCompactionBytes(long)
   */
  long maxCompactionBytes();

  /**
   * The target file size for compaction.
   * This targetFileSizeBase determines a level-1 file size.
   * Target file size for level L can be calculated by
   * targetFileSizeBase * (targetFileSizeMultiplier ^ (L-1))
   * For example, if targetFileSizeBase is 2MB and
   * target_file_size_multiplier is 10, then each file on level-1 will
   * be 2MB, and each file on level 2 will be 20MB,
   * and each file on level-3 will be 200MB.
   * by default targetFileSizeBase is 2MB.
   *
   * @param targetFileSizeBase the target size of a level-0 file.
   * @return the reference to the current option.
   *
   * @see #setTargetFileSizeMultiplier(int)
   */
  MutableColumnFamilyOptionsInterface setTargetFileSizeBase(
      long targetFileSizeBase);

  /**
   * The target file size for compaction.
   * This targetFileSizeBase determines a level-1 file size.
   * Target file size for level L can be calculated by
   * targetFileSizeBase * (targetFileSizeMultiplier ^ (L-1))
   * For example, if targetFileSizeBase is 2MB and
   * target_file_size_multiplier is 10, then each file on level-1 will
   * be 2MB, and each file on level 2 will be 20MB,
   * and each file on level-3 will be 200MB.
   * by default targetFileSizeBase is 2MB.
   *
   * @return the target size of a level-0 file.
   *
   * @see #targetFileSizeMultiplier()
   */
  long targetFileSizeBase();

  /**
   * targetFileSizeMultiplier defines the size ratio between a
   * level-L file and level-(L+1) file.
   * By default target_file_size_multiplier is 1, meaning
   * files in different levels have the same target.
   *
   * @param multiplier the size ratio between a level-(L+1) file
   *     and level-L file.
   * @return the reference to the current option.
   */
  MutableColumnFamilyOptionsInterface setTargetFileSizeMultiplier(
      int multiplier);

  /**
   * targetFileSizeMultiplier defines the size ratio between a
   * level-(L+1) file and level-L file.
   * By default targetFileSizeMultiplier is 1, meaning
   * files in different levels have the same target.
   *
   * @return the size ratio between a level-(L+1) file and level-L file.
   */
  int targetFileSizeMultiplier();

  /**
   * The upper-bound of the total size of level-1 files in bytes.
   * Maximum number of bytes for level L can be calculated as
   * (maxBytesForLevelBase) * (maxBytesForLevelMultiplier ^ (L-1))
   * For example, if maxBytesForLevelBase is 20MB, and if
   * max_bytes_for_level_multiplier is 10, total data size for level-1
   * will be 20MB, total file size for level-2 will be 200MB,
   * and total file size for level-3 will be 2GB.
   * by default 'maxBytesForLevelBase' is 10MB.
   *
   * @param maxBytesForLevelBase maximum bytes for level base.
   *
   * @return the reference to the current option.
   * @see #setMaxBytesForLevelMultiplier(double)
   */
  MutableColumnFamilyOptionsInterface setMaxBytesForLevelBase(
      long maxBytesForLevelBase);

  /**
   * The upper-bound of the total size of level-1 files in bytes.
   * Maximum number of bytes for level L can be calculated as
   * (maxBytesForLevelBase) * (maxBytesForLevelMultiplier ^ (L-1))
   * For example, if maxBytesForLevelBase is 20MB, and if
   * max_bytes_for_level_multiplier is 10, total data size for level-1
   * will be 20MB, total file size for level-2 will be 200MB,
   * and total file size for level-3 will be 2GB.
   * by default 'maxBytesForLevelBase' is 10MB.
   *
   * @return the upper-bound of the total size of level-1 files
   *     in bytes.
   * @see #maxBytesForLevelMultiplier()
   */
  long maxBytesForLevelBase();

  /**
   * The ratio between the total size of level-(L+1) files and the total
   * size of level-L files for all L.
   * DEFAULT: 10
   *
   * @param multiplier the ratio between the total size of level-(L+1)
   *     files and the total size of level-L files for all L.
   * @return the reference to the current option.
   * @see #setMaxBytesForLevelBase(long)
   */
  MutableColumnFamilyOptionsInterface setMaxBytesForLevelMultiplier(double multiplier);

  /**
   * The ratio between the total size of level-(L+1) files and the total
   * size of level-L files for all L.
   * DEFAULT: 10
   *
   * @return the ratio between the total size of level-(L+1) files and
   *     the total size of level-L files for all L.
   * @see #maxBytesForLevelBase()
   */
  double maxBytesForLevelMultiplier();

  /**
   * Different max-size multipliers for different levels.
   * These are multiplied by max_bytes_for_level_multiplier to arrive
   * at the max-size of each level.
   *
   * Default: 1
   *
   * @param maxBytesForLevelMultiplierAdditional The max-size multipliers
   *   for each level
   * @return the reference to the current option.
   */
  MutableColumnFamilyOptionsInterface setMaxBytesForLevelMultiplierAdditional(
      int[] maxBytesForLevelMultiplierAdditional);

  /**
   * Different max-size multipliers for different levels.
   * These are multiplied by max_bytes_for_level_multiplier to arrive
   * at the max-size of each level.
   *
   * Default: 1
   *
   * @return The max-size multipliers for each level
   */
  int[] maxBytesForLevelMultiplierAdditional();

  /**
   * If true, compaction will verify checksum on every read that happens
   * as part of compaction
   * Default: true
   *
   * @param verifyChecksumsInCompaction true if compaction verifies
   *     checksum on every read.
   * @return the reference to the current option.
   */
  MutableColumnFamilyOptionsInterface setVerifyChecksumsInCompaction(
      boolean verifyChecksumsInCompaction);

  /**
   * If true, compaction will verify checksum on every read that happens
   * as part of compaction
   * Default: true
   *
   * @return true if compaction verifies checksum on every read.
   */
  boolean verifyChecksumsInCompaction();

  /**
   * An iteration-&gt;Next() sequentially skips over keys with the same
   * user-key unless this option is set. This number specifies the number
   * of keys (with the same userkey) that will be sequentially
   * skipped before a reseek is issued.
   * Default: 8
   *
   * @param maxSequentialSkipInIterations the number of keys could
   *     be skipped in a iteration.
   * @return the reference to the current option.
   */
  MutableColumnFamilyOptionsInterface setMaxSequentialSkipInIterations(
      long maxSequentialSkipInIterations);

  /**
   * An iteration-&gt;Next() sequentially skips over keys with the same
   * user-key unless this option is set. This number specifies the number
   * of keys (with the same userkey) that will be sequentially
   * skipped before a reseek is issued.
   * Default: 8
   *
   * @return the number of keys could be skipped in a iteration.
   */
  long maxSequentialSkipInIterations();

  /**
   * After writing every SST file, reopen it and read all the keys.
   *
   * Default: false
   *
   * @param paranoidFileChecks true to enable paranoid file checks
   * @return the reference to the current option.
   */
  MutableColumnFamilyOptionsInterface setParanoidFileChecks(
      boolean paranoidFileChecks);

  /**
   * After writing every SST file, reopen it and read all the keys.
   *
   * Default: false
   *
   * @return true if paranoid file checks are enabled
   */
  boolean paranoidFileChecks();
}
