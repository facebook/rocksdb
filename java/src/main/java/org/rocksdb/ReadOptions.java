// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * The class that controls the get behavior.
 *
 * Note that dispose() must be called before an Options instance
 * become out-of-scope to release the allocated memory in c++.
 */
public class ReadOptions extends RocksObject {
  public ReadOptions() {
    super(newReadOptions());
  }

  /**
   * @param verifyChecksums verification will be performed on every read
   *     when set to true
   * @param fillCache if true, then fill-cache behavior will be performed.
   */
  public ReadOptions(final boolean verifyChecksums, final boolean fillCache) {
    super(newReadOptions(verifyChecksums, fillCache));
  }

  /**
   * Copy constructor.
   *
   * NOTE: This does a shallow copy, which means snapshot, iterate_upper_bound
   * and other pointers will be cloned!
   *
   * @param other The ReadOptions to copy.
   */
  public ReadOptions(ReadOptions other) {
    super(copyReadOptions(other.nativeHandle_));
    this.iterateLowerBoundSlice_ = other.iterateLowerBoundSlice_;
    this.iterateUpperBoundSlice_ = other.iterateUpperBoundSlice_;
    this.timestampSlice_ = other.timestampSlice_;
    this.iterStartTs_ = other.iterStartTs_;
  }

  /**
   * If true, all data read from underlying storage will be
   * verified against corresponding checksums.
   * Default: true
   *
   * @return true if checksum verification is on.
   */
  public boolean verifyChecksums() {
    assert(isOwningHandle());
    return verifyChecksums(nativeHandle_);
  }

  /**
   * If true, all data read from underlying storage will be
   * verified against corresponding checksums.
   * Default: true
   *
   * @param verifyChecksums if true, then checksum verification
   *     will be performed on every read.
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setVerifyChecksums(
      final boolean verifyChecksums) {
    assert(isOwningHandle());
    setVerifyChecksums(nativeHandle_, verifyChecksums);
    return this;
  }

  // TODO(yhchiang): this option seems to be block-based table only.
  //                 move this to a better place?
  /**
   * Fill the cache when loading the block-based sst formated db.
   * Callers may wish to set this field to false for bulk scans.
   * Default: true
   *
   * @return true if the fill-cache behavior is on.
   */
  public boolean fillCache() {
    assert(isOwningHandle());
    return fillCache(nativeHandle_);
  }

  /**
   * Fill the cache when loading the block-based sst formatted db.
   * Callers may wish to set this field to false for bulk scans.
   * Default: true
   *
   * @param fillCache if true, then fill-cache behavior will be
   *     performed.
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setFillCache(final boolean fillCache) {
    assert(isOwningHandle());
    setFillCache(nativeHandle_, fillCache);
    return this;
  }

  /**
   * Returns the currently assigned Snapshot instance.
   *
   * @return the Snapshot assigned to this instance. If no Snapshot
   *     is assigned null.
   */
  public Snapshot snapshot() {
    assert(isOwningHandle());
    long snapshotHandle = snapshot(nativeHandle_);
    if (snapshotHandle != 0) {
      return new Snapshot(snapshotHandle);
    }
    return null;
  }

  /**
   * <p>If "snapshot" is non-nullptr, read as of the supplied snapshot
   * (which must belong to the DB that is being read and which must
   * not have been released).  If "snapshot" is nullptr, use an implicit
   * snapshot of the state at the beginning of this read operation.</p>
   * <p>Default: null</p>
   *
   * @param snapshot {@link Snapshot} instance
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setSnapshot(final Snapshot snapshot) {
    assert(isOwningHandle());
    if (snapshot != null) {
      setSnapshot(nativeHandle_, snapshot.nativeHandle_);
    } else {
      setSnapshot(nativeHandle_, 0l);
    }
    return this;
  }

  /**
   * Returns the current read tier.
   *
   * @return the read tier in use, by default {@link ReadTier#READ_ALL_TIER}
   */
  public ReadTier readTier() {
    assert(isOwningHandle());
    return ReadTier.getReadTier(readTier(nativeHandle_));
  }

  /**
   * Specify if this read request should process data that ALREADY
   * resides on a particular cache. If the required data is not
   * found at the specified cache, then {@link RocksDBException} is thrown.
   *
   * @param readTier {@link ReadTier} instance
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setReadTier(final ReadTier readTier) {
    assert(isOwningHandle());
    setReadTier(nativeHandle_, readTier.getValue());
    return this;
  }

  /**
   * Specify to create a tailing iterator -- a special iterator that has a
   * view of the complete database (i.e. it can also be used to read newly
   * added data) and is optimized for sequential reads. It will return records
   * that were inserted into the database after the creation of the iterator.
   * Default: false
   *
   * Not supported in {@code ROCKSDB_LITE} mode!
   *
   * @return true if tailing iterator is enabled.
   */
  public boolean tailing() {
    assert(isOwningHandle());
    return tailing(nativeHandle_);
  }

  /**
   * Specify to create a tailing iterator -- a special iterator that has a
   * view of the complete database (i.e. it can also be used to read newly
   * added data) and is optimized for sequential reads. It will return records
   * that were inserted into the database after the creation of the iterator.
   * Default: false
   * Not supported in ROCKSDB_LITE mode!
   *
   * @param tailing if true, then tailing iterator will be enabled.
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setTailing(final boolean tailing) {
    assert(isOwningHandle());
    setTailing(nativeHandle_, tailing);
    return this;
  }

  /**
   * Returns whether managed iterators will be used.
   *
   * @return the setting of whether managed iterators will be used,
   *     by default false
   *
   * @deprecated This options is not used anymore.
   */
  @Deprecated
  public boolean managed() {
    assert(isOwningHandle());
    return managed(nativeHandle_);
  }

  /**
   * Specify to create a managed iterator -- a special iterator that
   * uses less resources by having the ability to free its underlying
   * resources on request.
   *
   * @param managed if true, then managed iterators will be enabled.
   * @return the reference to the current ReadOptions.
   *
   * @deprecated This options is not used anymore.
   */
  @Deprecated
  public ReadOptions setManaged(final boolean managed) {
    assert(isOwningHandle());
    setManaged(nativeHandle_, managed);
    return this;
  }

  /**
   * Returns whether a total seek order will be used
   *
   * @return the setting of whether a total seek order will be used
   */
  public boolean totalOrderSeek() {
    assert(isOwningHandle());
    return totalOrderSeek(nativeHandle_);
  }

  /**
   * Enable a total order seek regardless of index format (e.g. hash index)
   * used in the table. Some table format (e.g. plain table) may not support
   * this option.
   *
   * @param totalOrderSeek if true, then total order seek will be enabled.
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setTotalOrderSeek(final boolean totalOrderSeek) {
    assert(isOwningHandle());
    setTotalOrderSeek(nativeHandle_, totalOrderSeek);
    return this;
  }

  /**
   * Returns whether the iterator only iterates over the same prefix as the seek
   *
   * @return the setting of whether the iterator only iterates over the same
   *   prefix as the seek, default is false
   */
  public boolean prefixSameAsStart() {
    assert(isOwningHandle());
    return prefixSameAsStart(nativeHandle_);
  }

  /**
   * Enforce that the iterator only iterates over the same prefix as the seek.
   * This option is effective only for prefix seeks, i.e. prefix_extractor is
   * non-null for the column family and {@link #totalOrderSeek()} is false.
   * Unlike iterate_upper_bound, {@link #setPrefixSameAsStart(boolean)} only
   * works within a prefix but in both directions.
   *
   * @param prefixSameAsStart if true, then the iterator only iterates over the
   *   same prefix as the seek
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setPrefixSameAsStart(final boolean prefixSameAsStart) {
    assert(isOwningHandle());
    setPrefixSameAsStart(nativeHandle_, prefixSameAsStart);
    return this;
  }

  /**
   * Returns whether the blocks loaded by the iterator will be pinned in memory
   *
   * @return the setting of whether the blocks loaded by the iterator will be
   *   pinned in memory
   */
  public boolean pinData() {
    assert(isOwningHandle());
    return pinData(nativeHandle_);
  }

  /**
   * Keep the blocks loaded by the iterator pinned in memory as long as the
   * iterator is not deleted, If used when reading from tables created with
   * BlockBasedTableOptions::use_delta_encoding = false,
   * Iterator's property "rocksdb.iterator.is-key-pinned" is guaranteed to
   * return 1.
   *
   * @param pinData if true, the blocks loaded by the iterator will be pinned
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setPinData(final boolean pinData) {
    assert(isOwningHandle());
    setPinData(nativeHandle_, pinData);
    return this;
  }

  /**
   * If true, when PurgeObsoleteFile is called in CleanupIteratorState, we
   * schedule a background job in the flush job queue and delete obsolete files
   * in background.
   *
   * Default: false
   *
   * @return true when PurgeObsoleteFile is called in CleanupIteratorState
   */
  public boolean backgroundPurgeOnIteratorCleanup() {
    assert(isOwningHandle());
    return backgroundPurgeOnIteratorCleanup(nativeHandle_);
  }

  /**
   * If true, when PurgeObsoleteFile is called in CleanupIteratorState, we
   * schedule a background job in the flush job queue and delete obsolete files
   * in background.
   *
   * Default: false
   *
   * @param backgroundPurgeOnIteratorCleanup true when PurgeObsoleteFile is
   *     called in CleanupIteratorState
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setBackgroundPurgeOnIteratorCleanup(
      final boolean backgroundPurgeOnIteratorCleanup) {
    assert(isOwningHandle());
    setBackgroundPurgeOnIteratorCleanup(nativeHandle_,
        backgroundPurgeOnIteratorCleanup);
    return this;
  }

  /**
   * If non-zero, NewIterator will create a new table reader which
   * performs reads of the given size. Using a large size (&gt; 2MB) can
   * improve the performance of forward iteration on spinning disks.
   *
   * Default: 0
   *
   * @return The readahead size is bytes
   */
  public long readaheadSize() {
    assert(isOwningHandle());
    return readaheadSize(nativeHandle_);
  }

  /**
   * If non-zero, NewIterator will create a new table reader which
   * performs reads of the given size. Using a large size (&gt; 2MB) can
   * improve the performance of forward iteration on spinning disks.
   *
   * Default: 0
   *
   * @param readaheadSize The readahead size is bytes
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setReadaheadSize(final long readaheadSize) {
    assert(isOwningHandle());
    setReadaheadSize(nativeHandle_, readaheadSize);
    return this;
  }

  /**
   * A threshold for the number of keys that can be skipped before failing an
   * iterator seek as incomplete.
   *
   * @return the number of keys that can be skipped
   *     before failing an iterator seek as incomplete.
   */
  public long maxSkippableInternalKeys() {
    assert(isOwningHandle());
    return maxSkippableInternalKeys(nativeHandle_);
  }

  /**
   * A threshold for the number of keys that can be skipped before failing an
   * iterator seek as incomplete. The default value of 0 should be used to
   * never fail a request as incomplete, even on skipping too many keys.
   *
   * Default: 0
   *
   * @param maxSkippableInternalKeys the number of keys that can be skipped
   *     before failing an iterator seek as incomplete.
   *
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setMaxSkippableInternalKeys(
      final long maxSkippableInternalKeys) {
    assert(isOwningHandle());
    setMaxSkippableInternalKeys(nativeHandle_, maxSkippableInternalKeys);
    return this;
  }

  /**
   * If true, keys deleted using the DeleteRange() API will be visible to
   * readers until they are naturally deleted during compaction. This improves
   * read performance in DBs with many range deletions.
   *
   * Default: false
   *
   * @return true if keys deleted using the DeleteRange() API will be visible
   */
  public boolean ignoreRangeDeletions() {
    assert(isOwningHandle());
    return ignoreRangeDeletions(nativeHandle_);
  }

  /**
   * If true, keys deleted using the DeleteRange() API will be visible to
   * readers until they are naturally deleted during compaction. This improves
   * read performance in DBs with many range deletions.
   *
   * Default: false
   *
   * @param ignoreRangeDeletions true if keys deleted using the DeleteRange()
   *     API should be visible
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setIgnoreRangeDeletions(final boolean ignoreRangeDeletions) {
    assert(isOwningHandle());
    setIgnoreRangeDeletions(nativeHandle_, ignoreRangeDeletions);
    return this;
  }

  /**
   * Defines the smallest key at which the backward
   * iterator can return an entry. Once the bound is passed,
   * {@link RocksIterator#isValid()} will be false.
   *
   * The lower bound is inclusive i.e. the bound value is a valid
   * entry.
   *
   * If prefix_extractor is not null, the Seek target and `iterate_lower_bound`
   * need to have the same prefix. This is because ordering is not guaranteed
   * outside of prefix domain.
   *
   * Default: null
   *
   * @param iterateLowerBound Slice representing the lower bound
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setIterateLowerBound(final AbstractSlice<?> iterateLowerBound) {
    assert(isOwningHandle());
    setIterateLowerBound(
        nativeHandle_, iterateLowerBound == null ? 0 : iterateLowerBound.getNativeHandle());
    // Hold onto a reference so it doesn't get garbage collected out from under us.
    iterateLowerBoundSlice_ = iterateLowerBound;
    return this;
  }

  /**
   * Returns the smallest key at which the backward
   * iterator can return an entry.
   *
   * The lower bound is inclusive i.e. the bound value is a valid entry.
   *
   * @return the smallest key, or null if there is no lower bound defined.
   */
  public Slice iterateLowerBound() {
    assert(isOwningHandle());
    final long lowerBoundSliceHandle = iterateLowerBound(nativeHandle_);
    if (lowerBoundSliceHandle != 0) {
      // Disown the new slice - it's owned by the C++ side of the JNI boundary
      // from the perspective of this method.
      return new Slice(lowerBoundSliceHandle, false);
    }
    return null;
  }

  /**
   * Defines the extent up to which the forward iterator
   * can returns entries. Once the bound is reached,
   * {@link RocksIterator#isValid()} will be false.
   *
   * The upper bound is exclusive i.e. the bound value is not a valid entry.
   *
   * If prefix_extractor is not null, the Seek target and iterate_upper_bound
   * need to have the same prefix. This is because ordering is not guaranteed
   * outside of prefix domain.
   *
   * Default: null
   *
   * @param iterateUpperBound Slice representing the upper bound
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setIterateUpperBound(final AbstractSlice<?> iterateUpperBound) {
    assert(isOwningHandle());
    setIterateUpperBound(
        nativeHandle_, iterateUpperBound == null ? 0 : iterateUpperBound.getNativeHandle());
    // Hold onto a reference so it doesn't get garbage collected out from under us.
    iterateUpperBoundSlice_ = iterateUpperBound;
    return this;
  }

  /**
   * Returns the largest key at which the forward
   * iterator can return an entry.
   *
   * The upper bound is exclusive i.e. the bound value is not a valid entry.
   *
   * @return the largest key, or null if there is no upper bound defined.
   */
  public Slice iterateUpperBound() {
    assert(isOwningHandle());
    final long upperBoundSliceHandle = iterateUpperBound(nativeHandle_);
    if (upperBoundSliceHandle != 0) {
      // Disown the new slice - it's owned by the C++ side of the JNI boundary
      // from the perspective of this method.
      return new Slice(upperBoundSliceHandle, false);
    }
    return null;
  }

  /**
   * A callback to determine whether relevant keys for this scan exist in a
   * given table based on the table's properties. The callback is passed the
   * properties of each table during iteration. If the callback returns false,
   * the table will not be scanned. This option only affects Iterators and has
   * no impact on point lookups.
   *
   * Default: null (every table will be scanned)
   *
   * @param tableFilter the table filter for the callback.
   *
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setTableFilter(final AbstractTableFilter tableFilter) {
    assert(isOwningHandle());
    setTableFilter(nativeHandle_, tableFilter.nativeHandle_);
    return this;
  }

  /**
   * When true, by default use total_order_seek = true, and RocksDB can
   * selectively enable prefix seek mode if won't generate a different result
   * from total_order_seek, based on seek key, and iterator upper bound.
   * Not supported in ROCKSDB_LITE mode, in the way that even with value true
   * prefix mode is not used.
   * Default: false
   *
   * @return true if auto prefix mode is set.
   *
   */
  public boolean autoPrefixMode() {
    assert (isOwningHandle());
    return autoPrefixMode(nativeHandle_);
  }

  /**
   * When true, by default use total_order_seek = true, and RocksDB can
   * selectively enable prefix seek mode if won't generate a different result
   * from total_order_seek, based on seek key, and iterator upper bound.
   * Not supported in ROCKSDB_LITE mode, in the way that even with value true
   * prefix mode is not used.
   * Default: false
   * @param mode auto prefix mode
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setAutoPrefixMode(final boolean mode) {
    assert (isOwningHandle());
    setAutoPrefixMode(nativeHandle_, mode);
    return this;
  }

  /**
   * Timestamp of operation. Read should return the latest data visible to the
   * specified timestamp. All timestamps of the same database must be of the
   * same length and format. The user is responsible for providing a customized
   * compare function via Comparator to order &gt;key, timestamp&gt; tuples.
   * For iterator, iter_start_ts is the lower bound (older) and timestamp
   * serves as the upper bound. Versions of the same record that fall in
   * the timestamp range will be returned. If iter_start_ts is nullptr,
   * only the most recent version visible to timestamp is returned.
   * The user-specified timestamp feature is still under active development,
   * and the API is subject to change.
   *
   * Default: null
   * @see #iterStartTs()
   * @return Reference to timestamp or null if there is no timestamp defined.
   */
  public Slice timestamp() {
    assert (isOwningHandle());
    final long timestampSliceHandle = timestamp(nativeHandle_);
    if (timestampSliceHandle != 0) {
      return new Slice(timestampSliceHandle);
    } else {
      return null;
    }
  }

  /**
   * Timestamp of operation. Read should return the latest data visible to the
   * specified timestamp. All timestamps of the same database must be of the
   * same length and format. The user is responsible for providing a customized
   * compare function via Comparator to order {@code <key, timestamp>} tuples.
   * For iterator, {@code iter_start_ts} is the lower bound (older) and timestamp
   * serves as the upper bound. Versions of the same record that fall in
   * the timestamp range will be returned. If iter_start_ts is nullptr,
   * only the most recent version visible to timestamp is returned.
   * The user-specified timestamp feature is still under active development,
   * and the API is subject to change.
   *
   * Default: null
   * @see #setIterStartTs(AbstractSlice)
   * @param timestamp Slice representing the timestamp
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setTimestamp(final AbstractSlice<?> timestamp) {
    assert (isOwningHandle());
    setTimestamp(nativeHandle_, timestamp == null ? 0 : timestamp.getNativeHandle());
    timestampSlice_ = timestamp;
    return this;
  }

  /**
   * Timestamp of operation. Read should return the latest data visible to the
   * specified timestamp. All timestamps of the same database must be of the
   * same length and format. The user is responsible for providing a customized
   * compare function via Comparator to order {@code <key, timestamp>} tuples.
   * For iterator, {@code iter_start_ts} is the lower bound (older) and timestamp
   * serves as the upper bound. Versions of the same record that fall in
   * the timestamp range will be returned. If iter_start_ts is nullptr,
   * only the most recent version visible to timestamp is returned.
   * The user-specified timestamp feature is still under active development,
   * and the API is subject to change.
   *
   * Default: null
   * @return Reference to lower bound timestamp or null if there is no lower bound timestamp
   *     defined.
   */
  public Slice iterStartTs() {
    assert (isOwningHandle());
    final long iterStartTsHandle = iterStartTs(nativeHandle_);
    if (iterStartTsHandle != 0) {
      return new Slice(iterStartTsHandle);
    } else {
      return null;
    }
  }

  /**
   * Timestamp of operation. Read should return the latest data visible to the
   * specified timestamp. All timestamps of the same database must be of the
   * same length and format. The user is responsible for providing a customized
   * compare function via Comparator to order {@code <key, timestamp>} tuples.
   * For iterator, {@code iter_start_ts} is the lower bound (older) and timestamp
   * serves as the upper bound. Versions of the same record that fall in
   * the timestamp range will be returned. If iter_start_ts is nullptr,
   * only the most recent version visible to timestamp is returned.
   * The user-specified timestamp feature is still under active development,
   * and the API is subject to change.
   *
   * Default: null
   *
   * @param iterStartTs Reference to lower bound timestamp or null if there is no lower bound
   *     timestamp defined
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setIterStartTs(final AbstractSlice<?> iterStartTs) {
    assert (isOwningHandle());
    setIterStartTs(nativeHandle_, iterStartTs == null ? 0 : iterStartTs.getNativeHandle());
    iterStartTs_ = iterStartTs;
    return this;
  }

  /**
   * Deadline for completing an API call (Get/MultiGet/Seek/Next for now)
   * in microseconds.
   * It should be set to microseconds since epoch, i.e, {@code gettimeofday} or
   * equivalent plus allowed duration in microseconds. The best way is to use
   * {@code env->NowMicros() + some timeout}.
   * This is best efforts. The call may exceed the deadline if there is IO
   * involved and the file system doesn't support deadlines, or due to
   * checking for deadline periodically rather than for every key if
   * processing a batch
   *
   * @return deadline time in microseconds
   */
  public long deadline() {
    assert (isOwningHandle());
    return deadline(nativeHandle_);
  }

  /**
   * Deadline for completing an API call (Get/MultiGet/Seek/Next for now)
   * in microseconds.
   * It should be set to microseconds since epoch, i.e, {@code gettimeofday} or
   * equivalent plus allowed duration in microseconds. The best way is to use
   * {@code env->NowMicros() + some timeout}.
   * This is best efforts. The call may exceed the deadline if there is IO
   * involved and the file system doesn't support deadlines, or due to
   * checking for deadline periodically rather than for every key if
   * processing a batch
   *
   * @param deadlineTime deadline time in microseconds.
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setDeadline(final long deadlineTime) {
    assert (isOwningHandle());
    setDeadline(nativeHandle_, deadlineTime);
    return this;
  }

  /**
   * A timeout in microseconds to be passed to the underlying FileSystem for
   * reads. As opposed to deadline, this determines the timeout for each
   * individual file read request. If a MultiGet/Get/Seek/Next etc call
   * results in multiple reads, each read can last up to io_timeout us.
   * @return ioTimeout time in microseconds
   */
  public long ioTimeout() {
    assert (isOwningHandle());
    return ioTimeout(nativeHandle_);
  }

  /**
   * A timeout in microseconds to be passed to the underlying FileSystem for
   * reads. As opposed to deadline, this determines the timeout for each
   * individual file read request. If a MultiGet/Get/Seek/Next etc call
   * results in multiple reads, each read can last up to io_timeout us.
   *
   * @param ioTimeout time in microseconds.
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setIoTimeout(final long ioTimeout) {
    assert (isOwningHandle());
    setIoTimeout(nativeHandle_, ioTimeout);
    return this;
  }

  /**
   * It limits the maximum cumulative value size of the keys in batch while
   * reading through MultiGet. Once the cumulative value size exceeds this
   * soft limit then all the remaining keys are returned with status Aborted.
   *
   * Default: {@code std::numeric_limits<uint64_t>::max()}
   * @return actual valueSizeSofLimit
   */
  public long valueSizeSoftLimit() {
    assert (isOwningHandle());
    return valueSizeSoftLimit(nativeHandle_);
  }

  /**
   * It limits the maximum cumulative value size of the keys in batch while
   * reading through MultiGet. Once the cumulative value size exceeds this
   * soft limit then all the remaining keys are returned with status Aborted.
   *
   * Default: {@code std::numeric_limits<uint64_t>::max()}
   *
   * @param valueSizeSoftLimit the maximum cumulative value size of the keys
   * @return the reference to the current ReadOptions
   */
  public ReadOptions setValueSizeSoftLimit(final long valueSizeSoftLimit) {
    assert (isOwningHandle());
    setValueSizeSoftLimit(nativeHandle_, valueSizeSoftLimit);
    return this;
  }

  // instance variables
  // NOTE: If you add new member variables, please update the copy constructor above!
  //
  // Hold a reference to any iterate lower or upper bound that was set on this
  // object until we're destroyed or it's overwritten. That way the caller can
  // freely leave scope without us losing the Java Slice object, which during
  // close() would also reap its associated rocksdb::Slice native object since
  // it's possibly (likely) to be an owning handle.
  private AbstractSlice<?> iterateLowerBoundSlice_;
  private AbstractSlice<?> iterateUpperBoundSlice_;
  private AbstractSlice<?> timestampSlice_;
  private AbstractSlice<?> iterStartTs_;

  private native static long newReadOptions();
  private native static long newReadOptions(final boolean verifyChecksums,
    final boolean fillCache);
  private native static long copyReadOptions(long handle);
  @Override protected final native void disposeInternal(final long handle);

  private native boolean verifyChecksums(long handle);
  private native void setVerifyChecksums(long handle, boolean verifyChecksums);
  private native boolean fillCache(long handle);
  private native void setFillCache(long handle, boolean fillCache);
  private native long snapshot(long handle);
  private native void setSnapshot(long handle, long snapshotHandle);
  private native byte readTier(long handle);
  private native void setReadTier(long handle, byte readTierValue);
  private native boolean tailing(long handle);
  private native void setTailing(long handle, boolean tailing);
  private native boolean managed(long handle);
  private native void setManaged(long handle, boolean managed);
  private native boolean totalOrderSeek(long handle);
  private native void setTotalOrderSeek(long handle, boolean totalOrderSeek);
  private native boolean prefixSameAsStart(long handle);
  private native void setPrefixSameAsStart(long handle, boolean prefixSameAsStart);
  private native boolean pinData(long handle);
  private native void setPinData(long handle, boolean pinData);
  private native boolean backgroundPurgeOnIteratorCleanup(final long handle);
  private native void setBackgroundPurgeOnIteratorCleanup(final long handle,
      final boolean backgroundPurgeOnIteratorCleanup);
  private native long readaheadSize(final long handle);
  private native void setReadaheadSize(final long handle,
      final long readaheadSize);
  private native long maxSkippableInternalKeys(final long handle);
  private native void setMaxSkippableInternalKeys(final long handle,
      final long maxSkippableInternalKeys);
  private native boolean ignoreRangeDeletions(final long handle);
  private native void setIgnoreRangeDeletions(final long handle,
      final boolean ignoreRangeDeletions);
  private native void setIterateUpperBound(final long handle,
      final long upperBoundSliceHandle);
  private native long iterateUpperBound(final long handle);
  private native void setIterateLowerBound(final long handle,
      final long lowerBoundSliceHandle);
  private native long iterateLowerBound(final long handle);
  private native void setTableFilter(final long handle, final long tableFilterHandle);
  private native boolean autoPrefixMode(final long handle);
  private native void setAutoPrefixMode(final long handle, final boolean autoPrefixMode);
  private native long timestamp(final long handle);
  private native void setTimestamp(final long handle, final long timestampSliceHandle);
  private native long iterStartTs(final long handle);
  private native void setIterStartTs(final long handle, final long iterStartTsHandle);
  private native long deadline(final long handle);
  private native void setDeadline(final long handle, final long deadlineTime);
  private native long ioTimeout(final long handle);
  private native void setIoTimeout(final long handle, final long ioTimeout);
  private native long valueSizeSoftLimit(final long handle);
  private native void setValueSizeSoftLimit(final long handle, final long softLimit);
}
