// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.List;

/**
 * SizeApproximationOptions to be passed to
 * {@link RocksDB#getApproximateSizes(ColumnFamilyHandle, List, SizeApproximationOptions)}.
 */
public class SizeApproximationOptions extends RocksObject {
  static {
    RocksDB.loadLibrary();
  }

  /**
   * Construct a new instance of SizeApproximationOptions.
   */
  public SizeApproximationOptions() {
    super(newSizeApproximationOptions());
  }

  /**
   * Set whether the returned size should include the recently written
   * data in the memtables.
   *
   * Defaults to {@code false}.
   *
   * @param includeMemtables boolean value indicating whether the returned
   *     size should include data in memtables. If set to {@code false},
   *     then {@code includeFiles} must be {@code true}.
   *
   * @return this {@code SizeApproximationOptions} object
   */
  public SizeApproximationOptions setIncludeMemtables(final boolean includeMemtables) {
    assert (isOwningHandle());
    setIncludeMemtables(nativeHandle_, includeMemtables);
    return this;
  }

  /**
   * Include size of data in memtables.
   *
   * @return boolean value indicating if the returned size will include data
   *     in memtables
   */
  public boolean includeMemtables() {
    assert (isOwningHandle());
    return includeMemtables(nativeHandle_);
  }

  /**
   * Set whether the returned size should include data serialized to disk.
   *
   * Defaults to {@code true}.
   *
   * @param includeFiles boolean value indicating whether the returned size
   *     should include data serialized to disk. If set to {@code false}, then
   *     {@code includeMemtables} must be {@code true}.
   *
   * @return this {@code SizeApproximationOptions} object
   */
  public SizeApproximationOptions setIncludeFiles(final boolean includeFiles) {
    assert (isOwningHandle());
    setIncludeFiles(nativeHandle_, includeFiles);
    return this;
  }

  /**
   * Include size data in files.
   *
   * @return boolean value indicating if the returned size will include data
   *     serialized to disk.
   */
  public boolean includeFiles() {
    assert (isOwningHandle());
    return includeFiles(nativeHandle_);
  }

  /**
   * Sets the maximum error margin for the estimated total file size.
   *
   * Defaults to {@code -1.0}.
   *
   * When approximating the files' size that is used to store a key range
   * using {@link RocksDB#getApproximateSizes(ColumnFamilyHandle, List, SizeApproximationOptions)},
   * allow approximation with an error margin of up to
   * {@code totalFilesSize * filesSizeErrorMargin}. This allows some
   * shortcuts in files size approximation, resulting in better performance,
   * while guaranteeing the resulting error is within the requested margin.
   *
   * For example, if the value is 0.1, then the error margin of the returned
   * files size approximation will be within 10%. If the value is non-positive,
   * a more precise yet more CPU intensive estimation is performed.
   *
   * @param filesSizeErrorMargin the maximum error margin for the total
   *     file size estimate
   *
   * @return this {@code SizeApproximationOptions} object
   */
  public SizeApproximationOptions setFilesSizeErrorMargin(final double filesSizeErrorMargin) {
    assert (isOwningHandle());
    setFilesSizeErrorMargin(nativeHandle_, filesSizeErrorMargin);
    return this;
  }

  /**
   * Maximum error margin for the estimated total file size. Negative
   * values mean that the system must perform a more precise estimate at
   * the cost of additional CPU.
   *
   * @return maximum error margin for the estimated file size
   */
  public double filesSizeErrorMargin() {
    assert (isOwningHandle());
    return filesSizeErrorMargin(nativeHandle_);
  }

  private native static long newSizeApproximationOptions();
  @Override protected final native void disposeInternal(final long handle);

  private native void setIncludeMemtables(final long handle, final boolean includeMemtables);
  private native boolean includeMemtables(final long handle);
  private native void setIncludeFiles(final long handle, final boolean includeFiles);
  private native boolean includeFiles(final long handle);
  private native void setFilesSizeErrorMargin(final long handle, final double filesSizeErrorMargin);
  private native double filesSizeErrorMargin(final long handle);
}
