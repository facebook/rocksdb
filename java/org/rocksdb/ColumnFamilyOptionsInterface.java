// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

public interface ColumnFamilyOptionsInterface {

  /**
   * Use this if you don't need to keep the data sorted, i.e. you'll never use
   * an iterator, only Put() and Get() API calls
   *
   * @param blockCacheSizeMb Block cache size in MB
   * @return the instance of the current Object.
   */
  Object optimizeForPointLookup(long blockCacheSizeMb);

  /**
   * <p>Default values for some parameters in ColumnFamilyOptions are not
   * optimized for heavy workloads and big datasets, which means you might
   * observe write stalls under some conditions. As a starting point for tuning
   * RocksDB options, use the following for level style compaction.</p>
   *
   * <p>Make sure to also call IncreaseParallelism(), which will provide the
   * biggest performance gains.</p>
   * <p>Note: we might use more memory than memtable_memory_budget during high
   * write rate period</p>
   *
   * @return the instance of the current Object.
   */
  Object optimizeLevelStyleCompaction();

  /**
   * <p>Default values for some parameters in ColumnFamilyOptions are not
   * optimized for heavy workloads and big datasets, which means you might
   * observe write stalls under some conditions. As a starting point for tuning
   * RocksDB options, use the following for level style compaction.</p>
   *
   * <p>Make sure to also call IncreaseParallelism(), which will provide the
   * biggest performance gains.</p>
   * <p>Note: we might use more memory than memtable_memory_budget during high
   * write rate period</p>
   *
   * @param memtableMemoryBudget memory budget in bytes
   * @return the instance of the current Object.
   */
  Object optimizeLevelStyleCompaction(long memtableMemoryBudget);

  /**
   * <p>Default values for some parameters in ColumnFamilyOptions are not
   * optimized for heavy workloads and big datasets, which means you might
   * observe write stalls under some conditions. As a starting point for tuning
   * RocksDB options, use the following for universal style compaction.</p>
   *
   * <p>Universal style compaction is focused on reducing Write Amplification
   * Factor for big data sets, but increases Space Amplification.</p>
   *
   * <p>Make sure to also call IncreaseParallelism(), which will provide the
   * biggest performance gains.</p>
   *
   * <p>Note: we might use more memory than memtable_memory_budget during high
   * write rate period</p>
   *
   * @return the instance of the current Object.
   */
  Object optimizeUniversalStyleCompaction();

  /**
   * <p>Default values for some parameters in ColumnFamilyOptions are not
   * optimized for heavy workloads and big datasets, which means you might
   * observe write stalls under some conditions. As a starting point for tuning
   * RocksDB options, use the following for universal style compaction.</p>
   *
   * <p>Universal style compaction is focused on reducing Write Amplification
   * Factor for big data sets, but increases Space Amplification.</p>
   *
   * <p>Make sure to also call IncreaseParallelism(), which will provide the
   * biggest performance gains.</p>
   *
   * <p>Note: we might use more memory than memtable_memory_budget during high
   * write rate period</p>
   *
   * @param memtableMemoryBudget memory budget in bytes
   * @return the instance of the current Object.
   */
  Object optimizeUniversalStyleCompaction(long memtableMemoryBudget);

  /**
   * Set {@link BuiltinComparator} to be used with RocksDB.
   *
   * Note: Comparator can be set once upon database creation.
   *
   * Default: BytewiseComparator.
   * @param builtinComparator a {@link BuiltinComparator} type.
   * @return the instance of the current Object.
   */
  Object setComparator(BuiltinComparator builtinComparator);

  /**
   * Use the specified comparator for key ordering.
   *
   * Comparator should not be disposed before options instances using this comparator is
   * disposed. If dispose() function is not called, then comparator object will be
   * GC'd automatically.
   *
   * Comparator instance can be re-used in multiple options instances.
   *
   * @param comparator java instance.
   * @return the instance of the current Object.
   */
  Object setComparator(AbstractComparator comparator);

  /**
   * <p>Set the merge operator to be used for merging two merge operands
   * of the same key. The merge function is invoked during
   * compaction and at lookup time, if multiple key/value pairs belonging
   * to the same key are found in the database.</p>
   *
   * @param name the name of the merge function, as defined by
   * the MergeOperators factory (see utilities/MergeOperators.h)
   * The merge function is specified by name and must be one of the
   * standard merge operators provided by RocksDB. The available
   * operators are "put", "uint64add", "stringappend" and "stringappendtest".
   * @return the instance of the current Object.
   */
  public Object setMergeOperatorName(String name);

  /**
   * <p>Set the merge operator to be used for merging two different key/value
   * pairs that share the same key. The merge function is invoked during
   * compaction and at lookup time, if multiple key/value pairs belonging
   * to the same key are found in the database.</p>
   *
   * @param mergeOperator {@link MergeOperator} instance.
   * @return the instance of the current Object.
   */
  public Object setMergeOperator(MergeOperator mergeOperator);

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
   * @throws org.rocksdb.RocksDBException thrown on 32-Bit platforms while
   *    overflowing the underlying platform specific value.
   */
  Object setWriteBufferSize(long writeBufferSize)
      throws RocksDBException;

  /**
   * Return size of write buffer size.
   *
   * @return size of write buffer.
   * @see #setWriteBufferSize(long)
   */
  long writeBufferSize();

  /**
   * The maximum number of write buffers that are built up in memory.
   * The default is 2, so that when 1 write buffer is being flushed to
   * storage, new writes can continue to the other write buffer.
   * Default: 2
   *
   * @param maxWriteBufferNumber maximum number of write buffers.
   * @return the instance of the current Object.
   */
  Object setMaxWriteBufferNumber(
      int maxWriteBufferNumber);

  /**
   * Returns maximum number of write buffers.
   *
   * @return maximum number of write buffers.
   * @see #setMaxWriteBufferNumber(int)
   */
  int maxWriteBufferNumber();

  /**
   * The minimum number of write buffers that will be merged together
   * before writing to storage.  If set to 1, then
   * all write buffers are flushed to L0 as individual files and this increases
   * read amplification because a get request has to check in all of these
   * files. Also, an in-memory merge may result in writing lesser
   * data to storage if there are duplicate records in each of these
   * individual write buffers.  Default: 1
   *
   * @param minWriteBufferNumberToMerge the minimum number of write buffers
   *     that will be merged together.
   * @return the reference to the current option.
   */
  Object setMinWriteBufferNumberToMerge(
      int minWriteBufferNumberToMerge);

  /**
   * The minimum number of write buffers that will be merged together
   * before writing to storage.  If set to 1, then
   * all write buffers are flushed to L0 as individual files and this increases
   * read amplification because a get request has to check in all of these
   * files. Also, an in-memory merge may result in writing lesser
   * data to storage if there are duplicate records in each of these
   * individual write buffers.  Default: 1
   *
   * @return the minimum number of write buffers that will be merged together.
   */
  int minWriteBufferNumberToMerge();

  /**
   * This prefix-extractor uses the first n bytes of a key as its prefix.
   *
   * In some hash-based memtable representation such as HashLinkedList
   * and HashSkipList, prefixes are used to partition the keys into
   * several buckets.  Prefix extractor is used to specify how to
   * extract the prefix given a key.
   *
   * @param n use the first n bytes of a key as its prefix.
   * @return the reference to the current option.
   */
  Object useFixedLengthPrefixExtractor(int n);

  /**
   * Compress blocks using the specified compression algorithm.  This
   * parameter can be changed dynamically.
   *
   * Default: SNAPPY_COMPRESSION, which gives lightweight but fast compression.
   *
   * @param compressionType Compression Type.
   * @return the reference to the current option.
   */
  Object setCompressionType(CompressionType compressionType);

  /**
   * Compress blocks using the specified compression algorithm.  This
   * parameter can be changed dynamically.
   *
   * Default: SNAPPY_COMPRESSION, which gives lightweight but fast compression.
   *
   * @return Compression type.
   */
  CompressionType compressionType();

  /**
   * Set the number of levels for this database
   * If level-styled compaction is used, then this number determines
   * the total number of levels.
   *
   * @param numLevels the number of levels.
   * @return the reference to the current option.
   */
  Object setNumLevels(int numLevels);

  /**
   * If level-styled compaction is used, then this number determines
   * the total number of levels.
   *
   * @return the number of levels.
   */
  int numLevels();

  /**
   * Number of files to trigger level-0 compaction. A value &lt; 0 means that
   * level-0 compaction will not be triggered by number of files at all.
   * Default: 4
   *
   * @param numFiles the number of files in level-0 to trigger compaction.
   * @return the reference to the current option.
   */
  Object setLevelZeroFileNumCompactionTrigger(
      int numFiles);

  /**
   * The number of files in level 0 to trigger compaction from level-0 to
   * level-1.  A value &lt; 0 means that level-0 compaction will not be
   * triggered by number of files at all.
   * Default: 4
   *
   * @return the number of files in level 0 to trigger compaction.
   */
  int levelZeroFileNumCompactionTrigger();

  /**
   * Soft limit on number of level-0 files. We start slowing down writes at this
   * point. A value &lt; 0 means that no writing slow down will be triggered by
   * number of files in level-0.
   *
   * @param numFiles soft limit on number of level-0 files.
   * @return the reference to the current option.
   */
  Object setLevelZeroSlowdownWritesTrigger(
      int numFiles);

  /**
   * Soft limit on the number of level-0 files. We start slowing down writes
   * at this point. A value &lt; 0 means that no writing slow down will be
   * triggered by number of files in level-0.
   *
   * @return the soft limit on the number of level-0 files.
   */
  int levelZeroSlowdownWritesTrigger();

  /**
   * Maximum number of level-0 files.  We stop writes at this point.
   *
   * @param numFiles the hard limit of the number of level-0 files.
   * @return the reference to the current option.
   */
  Object setLevelZeroStopWritesTrigger(int numFiles);

  /**
   * Maximum number of level-0 files.  We stop writes at this point.
   *
   * @return the hard limit of the number of level-0 file.
   */
  int levelZeroStopWritesTrigger();

  /**
   * The highest level to which a new compacted memtable is pushed if it
   * does not create overlap.  We try to push to level 2 to avoid the
   * relatively expensive level 0&ge;1 compactions and to avoid some
   * expensive manifest file operations.  We do not push all the way to
   * the largest level since that can generate a lot of wasted disk
   * space if the same key space is being repeatedly overwritten.
   *
   * @param maxMemCompactionLevel the highest level to which a new compacted
   *     mem-table will be pushed.
   * @return the reference to the current option.
   */
  Object setMaxMemCompactionLevel(
      int maxMemCompactionLevel);

  /**
   * The highest level to which a new compacted memtable is pushed if it
   * does not create overlap.  We try to push to level 2 to avoid the
   * relatively expensive level 0&ge;1 compactions and to avoid some
   * expensive manifest file operations.  We do not push all the way to
   * the largest level since that can generate a lot of wasted disk
   * space if the same key space is being repeatedly overwritten.
   *
   * @return the highest level where a new compacted memtable will be pushed.
   */
  int maxMemCompactionLevel();

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
  Object setTargetFileSizeBase(long targetFileSizeBase);

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
  Object setTargetFileSizeMultiplier(int multiplier);

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
   * @see #setMaxBytesForLevelMultiplier(int)
   */
  Object setMaxBytesForLevelBase(
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
   * @return the upper-bound of the total size of leve-1 files in bytes.
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
  Object setMaxBytesForLevelMultiplier(int multiplier);

  /**
   * The ratio between the total size of level-(L+1) files and the total
   * size of level-L files for all L.
   * DEFAULT: 10
   *
   * @return the ratio between the total size of level-(L+1) files and
   *     the total size of level-L files for all L.
   * @see #maxBytesForLevelBase()
   */
  int maxBytesForLevelMultiplier();

  /**
   * Maximum number of bytes in all compacted files.  We avoid expanding
   * the lower level file set of a compaction if it would make the
   * total compaction cover more than
   * (expanded_compaction_factor * targetFileSizeLevel()) many bytes.
   *
   * @param expandedCompactionFactor the maximum number of bytes in all
   *     compacted files.
   * @return the reference to the current option.
   * @see #setSourceCompactionFactor(int)
   */
  Object setExpandedCompactionFactor(int expandedCompactionFactor);

  /**
   * Maximum number of bytes in all compacted files.  We avoid expanding
   * the lower level file set of a compaction if it would make the
   * total compaction cover more than
   * (expanded_compaction_factor * targetFileSizeLevel()) many bytes.
   *
   * @return the maximum number of bytes in all compacted files.
   * @see #sourceCompactionFactor()
   */
  int expandedCompactionFactor();

  /**
   * Maximum number of bytes in all source files to be compacted in a
   * single compaction run. We avoid picking too many files in the
   * source level so that we do not exceed the total source bytes
   * for compaction to exceed
   * (source_compaction_factor * targetFileSizeLevel()) many bytes.
   * Default:1, i.e. pick maxfilesize amount of data as the source of
   * a compaction.
   *
   * @param sourceCompactionFactor the maximum number of bytes in all
   *     source files to be compacted in a single compaction run.
   * @return the reference to the current option.
   * @see #setExpandedCompactionFactor(int)
   */
  Object setSourceCompactionFactor(int sourceCompactionFactor);

  /**
   * Maximum number of bytes in all source files to be compacted in a
   * single compaction run. We avoid picking too many files in the
   * source level so that we do not exceed the total source bytes
   * for compaction to exceed
   * (source_compaction_factor * targetFileSizeLevel()) many bytes.
   * Default:1, i.e. pick maxfilesize amount of data as the source of
   * a compaction.
   *
   * @return the maximum number of bytes in all source files to be compactedo.
   * @see #expandedCompactionFactor()
   */
  int sourceCompactionFactor();

  /**
   * Control maximum bytes of overlaps in grandparent (i.e., level+2) before we
   * stop building a single file in a level-&gt;level+1 compaction.
   *
   * @param maxGrandparentOverlapFactor maximum bytes of overlaps in
   *     "grandparent" level.
   * @return the reference to the current option.
   */
  Object setMaxGrandparentOverlapFactor(
      int maxGrandparentOverlapFactor);

  /**
   * Control maximum bytes of overlaps in grandparent (i.e., level+2) before we
   * stop building a single file in a level-&gt;level+1 compaction.
   *
   * @return maximum bytes of overlaps in "grandparent" level.
   */
  int maxGrandparentOverlapFactor();

  /**
   * Puts are delayed 0-1 ms when any level has a compaction score that exceeds
   * soft_rate_limit. This is ignored when == 0.0.
   * CONSTRAINT: soft_rate_limit &le; hard_rate_limit. If this constraint does not
   * hold, RocksDB will set soft_rate_limit = hard_rate_limit
   * Default: 0 (disabled)
   *
   * @param softRateLimit the soft-rate-limit of a compaction score
   *     for put delay.
   * @return the reference to the current option.
   */
  Object setSoftRateLimit(double softRateLimit);

  /**
   * Puts are delayed 0-1 ms when any level has a compaction score that exceeds
   * soft_rate_limit. This is ignored when == 0.0.
   * CONSTRAINT: soft_rate_limit &le; hard_rate_limit. If this constraint does not
   * hold, RocksDB will set soft_rate_limit = hard_rate_limit
   * Default: 0 (disabled)
   *
   * @return soft-rate-limit for put delay.
   */
  double softRateLimit();

  /**
   * Puts are delayed 1ms at a time when any level has a compaction score that
   * exceeds hard_rate_limit. This is ignored when &le; 1.0.
   * Default: 0 (disabled)
   *
   * @param hardRateLimit the hard-rate-limit of a compaction score for put
   *     delay.
   * @return the reference to the current option.
   */
  Object setHardRateLimit(double hardRateLimit);

  /**
   * Puts are delayed 1ms at a time when any level has a compaction score that
   * exceeds hard_rate_limit. This is ignored when &le; 1.0.
   * Default: 0 (disabled)
   *
   * @return the hard-rate-limit of a compaction score for put delay.
   */
  double hardRateLimit();

  /**
   * The maximum time interval a put will be stalled when hard_rate_limit
   * is enforced. If 0, then there is no limit.
   * Default: 1000
   *
   * @param rateLimitDelayMaxMilliseconds the maximum time interval a put
   *     will be stalled.
   * @return the reference to the current option.
   */
  Object setRateLimitDelayMaxMilliseconds(
      int rateLimitDelayMaxMilliseconds);

  /**
   * The maximum time interval a put will be stalled when hard_rate_limit
   * is enforced.  If 0, then there is no limit.
   * Default: 1000
   *
   * @return the maximum time interval a put will be stalled when
   *     hard_rate_limit is enforced.
   */
  int rateLimitDelayMaxMilliseconds();

  /**
   * The size of one block in arena memory allocation.
   * If &le; 0, a proper value is automatically calculated (usually 1/10 of
   * writer_buffer_size).
   *
   * There are two additonal restriction of the The specified size:
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
   * @throws org.rocksdb.RocksDBException thrown on 32-Bit platforms while
   *    overflowing the underlying platform specific value.
   */
  Object setArenaBlockSize(long arenaBlockSize)
      throws RocksDBException;

  /**
   * The size of one block in arena memory allocation.
   * If &le; 0, a proper value is automatically calculated (usually 1/10 of
   * writer_buffer_size).
   *
   * There are two additonal restriction of the The specified size:
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
   * Disable automatic compactions. Manual compactions can still
   * be issued on this column family
   *
   * @param disableAutoCompactions true if auto-compactions are disabled.
   * @return the reference to the current option.
   */
  Object setDisableAutoCompactions(boolean disableAutoCompactions);

  /**
   * Disable automatic compactions. Manual compactions can still
   * be issued on this column family
   *
   * @return true if auto-compactions are disabled.
   */
  boolean disableAutoCompactions();

  /**
   * Purge duplicate/deleted keys when a memtable is flushed to storage.
   * Default: true
   *
   * @param purgeRedundantKvsWhileFlush true if purging keys is disabled.
   * @return the reference to the current option.
   */
  Object setPurgeRedundantKvsWhileFlush(
      boolean purgeRedundantKvsWhileFlush);

  /**
   * Purge duplicate/deleted keys when a memtable is flushed to storage.
   * Default: true
   *
   * @return true if purging keys is disabled.
   */
  boolean purgeRedundantKvsWhileFlush();

  /**
   * Set compaction style for DB.
   *
   * Default: LEVEL.
   *
   * @param compactionStyle Compaction style.
   * @return the reference to the current option.
   */
  Object setCompactionStyle(CompactionStyle compactionStyle);

  /**
   * Compaction style for DB.
   *
   * @return Compaction style.
   */
  CompactionStyle compactionStyle();

  /**
   * If true, compaction will verify checksum on every read that happens
   * as part of compaction
   * Default: true
   *
   * @param verifyChecksumsInCompaction true if compaction verifies
   *     checksum on every read.
   * @return the reference to the current option.
   */
  Object setVerifyChecksumsInCompaction(
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
   * Use KeyMayExist API to filter deletes when this is true.
   * If KeyMayExist returns false, i.e. the key definitely does not exist, then
   * the delete is a noop. KeyMayExist only incurs in-memory look up.
   * This optimization avoids writing the delete to storage when appropriate.
   * Default: false
   *
   * @param filterDeletes true if filter-deletes behavior is on.
   * @return the reference to the current option.
   */
  Object setFilterDeletes(boolean filterDeletes);

  /**
   * Use KeyMayExist API to filter deletes when this is true.
   * If KeyMayExist returns false, i.e. the key definitely does not exist, then
   * the delete is a noop. KeyMayExist only incurs in-memory look up.
   * This optimization avoids writing the delete to storage when appropriate.
   * Default: false
   *
   * @return true if filter-deletes behavior is on.
   */
  boolean filterDeletes();

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
  Object setMaxSequentialSkipInIterations(long maxSequentialSkipInIterations);

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
   * Set the config for mem-table.
   *
   * @param config the mem-table config.
   * @return the instance of the current Object.
   * @throws org.rocksdb.RocksDBException thrown on 32-Bit platforms while
   *    overflowing the underlying platform specific value.
   */
  Object setMemTableConfig(MemTableConfig config)
      throws RocksDBException;

  /**
   * Returns the name of the current mem table representation.
   * Memtable format can be set using setTableFormatConfig.
   *
   * @return the name of the currently-used memtable factory.
   * @see #setTableFormatConfig(org.rocksdb.TableFormatConfig)
   */
  String memTableFactoryName();

  /**
   * Set the config for table format.
   *
   * @param config the table format config.
   * @return the reference of the current Options.
   */
  Object setTableFormatConfig(TableFormatConfig config);

  /**
   * @return the name of the currently used table factory.
   */
  String tableFactoryName();

  /**
   * Allows thread-safe inplace updates.
   * If inplace_callback function is not set,
   *   Put(key, new_value) will update inplace the existing_value iff
   *   * key exists in current memtable
   *   * new sizeof(new_value) &le; sizeof(existing_value)
   *   * existing_value for that key is a put i.e. kTypeValue
   * If inplace_callback function is set, check doc for inplace_callback.
   * Default: false.
   *
   * @param inplaceUpdateSupport true if thread-safe inplace updates
   *     are allowed.
   * @return the reference to the current option.
   */
  Object setInplaceUpdateSupport(boolean inplaceUpdateSupport);

  /**
   * Allows thread-safe inplace updates.
   * If inplace_callback function is not set,
   *   Put(key, new_value) will update inplace the existing_value iff
   *   * key exists in current memtable
   *   * new sizeof(new_value) &le; sizeof(existing_value)
   *   * existing_value for that key is a put i.e. kTypeValue
   * If inplace_callback function is set, check doc for inplace_callback.
   * Default: false.
   *
   * @return true if thread-safe inplace updates are allowed.
   */
  boolean inplaceUpdateSupport();

  /**
   * Number of locks used for inplace update
   * Default: 10000, if inplace_update_support = true, else 0.
   *
   * @param inplaceUpdateNumLocks the number of locks used for
   *     inplace updates.
   * @return the reference to the current option.
   * @throws org.rocksdb.RocksDBException thrown on 32-Bit platforms while
   *    overflowing the underlying platform specific value.
   */
  Object setInplaceUpdateNumLocks(long inplaceUpdateNumLocks)
      throws RocksDBException;

  /**
   * Number of locks used for inplace update
   * Default: 10000, if inplace_update_support = true, else 0.
   *
   * @return the number of locks used for inplace update.
   */
  long inplaceUpdateNumLocks();

  /**
   * Sets the number of bits used in the prefix bloom filter.
   *
   * This value will be used only when a prefix-extractor is specified.
   *
   * @param memtablePrefixBloomBits the number of bits used in the
   *     prefix bloom filter.
   * @return the reference to the current option.
   */
  Object setMemtablePrefixBloomBits(int memtablePrefixBloomBits);

  /**
   * Returns the number of bits used in the prefix bloom filter.
   *
   * This value will be used only when a prefix-extractor is specified.
   *
   * @return the number of bloom-bits.
   * @see #useFixedLengthPrefixExtractor(int)
   */
  int memtablePrefixBloomBits();

  /**
   * The number of hash probes per key used in the mem-table.
   *
   * @param memtablePrefixBloomProbes the number of hash probes per key.
   * @return the reference to the current option.
   */
  Object setMemtablePrefixBloomProbes(int memtablePrefixBloomProbes);

  /**
   * The number of hash probes per key used in the mem-table.
   *
   * @return the number of hash probes per key.
   */
  int memtablePrefixBloomProbes();

  /**
   * Control locality of bloom filter probes to improve cache miss rate.
   * This option only applies to memtable prefix bloom and plaintable
   * prefix bloom. It essentially limits the max number of cache lines each
   * bloom filter check can touch.
   * This optimization is turned off when set to 0. The number should never
   * be greater than number of probes. This option can boost performance
   * for in-memory workload but should use with care since it can cause
   * higher false positive rate.
   * Default: 0
   *
   * @param bloomLocality the level of locality of bloom-filter probes.
   * @return the reference to the current option.
   */
  Object setBloomLocality(int bloomLocality);

  /**
   * Control locality of bloom filter probes to improve cache miss rate.
   * This option only applies to memtable prefix bloom and plaintable
   * prefix bloom. It essentially limits the max number of cache lines each
   * bloom filter check can touch.
   * This optimization is turned off when set to 0. The number should never
   * be greater than number of probes. This option can boost performance
   * for in-memory workload but should use with care since it can cause
   * higher false positive rate.
   * Default: 0
   *
   * @return the level of locality of bloom-filter probes.
   * @see #setMemtablePrefixBloomProbes(int)
   */
  int bloomLocality();

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
   * @throws org.rocksdb.RocksDBException thrown on 32-Bit platforms while
   *    overflowing the underlying platform specific value.
   */
  Object setMaxSuccessiveMerges(long maxSuccessiveMerges)
      throws RocksDBException;

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
   * The number of partial merge operands to accumulate before partial
   * merge will be performed. Partial merge will not be called
   * if the list of values to merge is less than min_partial_merge_operands.
   *
   * If min_partial_merge_operands &lt; 2, then it will be treated as 2.
   *
   * Default: 2
   *
   * @param minPartialMergeOperands min partial merge operands
   * @return the reference to the current option.
   */
  Object setMinPartialMergeOperands(int minPartialMergeOperands);

  /**
   * The number of partial merge operands to accumulate before partial
   * merge will be performed. Partial merge will not be called
   * if the list of values to merge is less than min_partial_merge_operands.
   *
   * If min_partial_merge_operands &lt; 2, then it will be treated as 2.
   *
   * Default: 2
   *
   * @return min partial merge operands
   */
  int minPartialMergeOperands();

  /**
   * Default memtable memory budget used with the following methods:
   *
   * <ol>
   *   <li>{@link #optimizeLevelStyleCompaction()}</li>
   *   <li>{@link #optimizeUniversalStyleCompaction()}</li>
   * </ol>
   */
  long DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET = 512 * 1024 * 1024;
}
