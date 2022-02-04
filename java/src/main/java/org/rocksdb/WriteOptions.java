// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Options that control write operations.
 *
 * Note that developers should call WriteOptions.dispose() to release the
 * c++ side memory before a WriteOptions instance runs out of scope.
 */
public class WriteOptions extends RocksObject {
  /**
   * Construct WriteOptions instance.
   */
  public WriteOptions() {
    super(newWriteOptions());

  }

  // TODO(AR) consider ownership
  WriteOptions(final long nativeHandle) {
    super(nativeHandle);
    disOwnNativeHandle();
  }

  /**
   * Copy constructor for WriteOptions.
   *
   * NOTE: This does a shallow copy, which means comparator, merge_operator, compaction_filter,
   * compaction_filter_factory and other pointers will be cloned!
   *
   * @param other The ColumnFamilyOptions to copy.
   */
  public WriteOptions(WriteOptions other) {
    super(copyWriteOptions(other.nativeHandle_));
    this.timestampSlice_ = other.timestampSlice_;
  }


  /**
   * If true, the write will be flushed from the operating system
   * buffer cache (by calling WritableFile::Sync()) before the write
   * is considered complete.  If this flag is true, writes will be
   * slower.
   *
   * If this flag is false, and the machine crashes, some recent
   * writes may be lost.  Note that if it is just the process that
   * crashes (i.e., the machine does not reboot), no writes will be
   * lost even if sync==false.
   *
   * In other words, a DB write with sync==false has similar
   * crash semantics as the "write()" system call.  A DB write
   * with sync==true has similar crash semantics to a "write()"
   * system call followed by "fdatasync()".
   *
   * Default: false
   *
   * @param flag a boolean flag to indicate whether a write
   *     should be synchronized.
   * @return the instance of the current WriteOptions.
   */
  public WriteOptions setSync(final boolean flag) {
    setSync(nativeHandle_, flag);
    return this;
  }

  /**
   * If true, the write will be flushed from the operating system
   * buffer cache (by calling WritableFile::Sync()) before the write
   * is considered complete.  If this flag is true, writes will be
   * slower.
   *
   * If this flag is false, and the machine crashes, some recent
   * writes may be lost.  Note that if it is just the process that
   * crashes (i.e., the machine does not reboot), no writes will be
   * lost even if sync==false.
   *
   * In other words, a DB write with sync==false has similar
   * crash semantics as the "write()" system call.  A DB write
   * with sync==true has similar crash semantics to a "write()"
   * system call followed by "fdatasync()".
   *
   * @return boolean value indicating if sync is active.
   */
  public boolean sync() {
    return sync(nativeHandle_);
  }

  /**
   * If true, writes will not first go to the write ahead log,
   * and the write may got lost after a crash. The backup engine
   * relies on write-ahead logs to back up the memtable, so if
   * you disable write-ahead logs, you must create backups with
   * flush_before_backup=true to avoid losing unflushed memtable data.
   *
   * @param flag a boolean flag to specify whether to disable
   *     write-ahead-log on writes.
   * @return the instance of the current WriteOptions.
   */
  public WriteOptions setDisableWAL(final boolean flag) {
    setDisableWAL(nativeHandle_, flag);
    return this;
  }

  /**
   * If true, writes will not first go to the write ahead log,
   * and the write may got lost after a crash. The backup engine
   * relies on write-ahead logs to back up the memtable, so if
   * you disable write-ahead logs, you must create backups with
   * flush_before_backup=true to avoid losing unflushed memtable data.
   *
   * @return boolean value indicating if WAL is disabled.
   */
  public boolean disableWAL() {
    return disableWAL(nativeHandle_);
  }

  /**
   * If true and if user is trying to write to column families that don't exist
   * (they were dropped), ignore the write (don't return an error). If there
   * are multiple writes in a WriteBatch, other writes will succeed.
   *
   * Default: false
   *
   * @param ignoreMissingColumnFamilies true to ignore writes to column families
   *     which don't exist
   * @return the instance of the current WriteOptions.
   */
  public WriteOptions setIgnoreMissingColumnFamilies(
      final boolean ignoreMissingColumnFamilies) {
    setIgnoreMissingColumnFamilies(nativeHandle_, ignoreMissingColumnFamilies);
    return this;
  }

  /**
   * If true and if user is trying to write to column families that don't exist
   * (they were dropped), ignore the write (don't return an error). If there
   * are multiple writes in a WriteBatch, other writes will succeed.
   *
   * Default: false
   *
   * @return true if writes to column families which don't exist are ignored
   */
  public boolean ignoreMissingColumnFamilies() {
    return ignoreMissingColumnFamilies(nativeHandle_);
  }

  /**
   * If true and we need to wait or sleep for the write request, fails
   * immediately with {@link Status.Code#Incomplete}.
   *
   * @param noSlowdown true to fail write requests if we need to wait or sleep
   * @return the instance of the current WriteOptions.
   */
  public WriteOptions setNoSlowdown(final boolean noSlowdown) {
    setNoSlowdown(nativeHandle_, noSlowdown);
    return this;
  }

  /**
   * If true and we need to wait or sleep for the write request, fails
   * immediately with {@link Status.Code#Incomplete}.
   *
   * @return true when write requests are failed if we need to wait or sleep
   */
  public boolean noSlowdown() {
    return noSlowdown(nativeHandle_);
  }

  /**
   * If true, this write request is of lower priority if compaction is
   * behind. In the case that, {@link #noSlowdown()} == true, the request
   * will be cancelled immediately with {@link Status.Code#Incomplete} returned.
   * Otherwise, it will be slowed down. The slowdown value is determined by
   * RocksDB to guarantee it introduces minimum impacts to high priority writes.
   *
   * Default: false
   *
   * @param lowPri true if the write request should be of lower priority than
   *     compactions which are behind.
   *
   * @return the instance of the current WriteOptions.
   */
  public WriteOptions setLowPri(final boolean lowPri) {
    setLowPri(nativeHandle_, lowPri);
    return this;
  }

  /**
   * Returns true if this write request is of lower priority if compaction is
   * behind.
   *
   * See {@link #setLowPri(boolean)}.
   *
   * @return true if this write request is of lower priority, false otherwise.
   */
  public boolean lowPri() {
    return lowPri(nativeHandle_);
  }

  /**
   * Timestamp of operation. Write should set specified timestamp for the data.
   * All timestamps of the same database must be of the same length and format.
   * The user is responsible for providing a customized
   * compare function via Comparator to order {@code <key, timestamp>} tuples.
   * For iterator, {@code iter_start_ts} is the lower bound (older) and timestamp
   * serves as the upper bound. Versions of the same record that fall in
   * the timestamp range will be returned. If iter_start_ts is nullptr,
   * only the most recent version visible to timestamp is returned.
   * The user-specified timestamp feature is still under active development,
   * and the API is subject to change.
   *
   * Default: null
   * @param timestamp Slice representing the timestamp
   * @return the reference to the current WriteOptions.
   */
  public WriteOptions setTimestamp(final AbstractSlice<?> timestamp) {
    assert (isOwningHandle());
    setTimestamp(nativeHandle_, timestamp == null ? 0 : timestamp.getNativeHandle());
    timestampSlice_ = timestamp;
    return this;
  }

  /**
   * Timestamp of operation. Write should set specified timestamp for the data.
   * All timestamps of the same database must be of the
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

  private native static long newWriteOptions();
  private native static long copyWriteOptions(long handle);
  private AbstractSlice<?> timestampSlice_;

  @Override protected final native void disposeInternal(final long handle);

  private native void setSync(long handle, boolean flag);
  private native boolean sync(long handle);
  private native void setDisableWAL(long handle, boolean flag);
  private native boolean disableWAL(long handle);
  private native void setIgnoreMissingColumnFamilies(final long handle,
      final boolean ignoreMissingColumnFamilies);
  private native boolean ignoreMissingColumnFamilies(final long handle);
  private native void setNoSlowdown(final long handle,
      final boolean noSlowdown);
  private native boolean noSlowdown(final long handle);
  private native void setLowPri(final long handle, final boolean lowPri);
  private native boolean lowPri(final long handle);
  private native long timestamp(final long handle);
  private native void setTimestamp(final long handle, final long timestampSliceHandle);
}
