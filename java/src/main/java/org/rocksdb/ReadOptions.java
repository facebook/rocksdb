// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

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
   * @return the setting of whether managed iterators will be used, by default false
   */
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
   */
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

  private native static long newReadOptions();
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

  @Override protected final native void disposeInternal(final long handle);

}
