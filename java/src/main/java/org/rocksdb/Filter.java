// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Filters are stored in rocksdb and are consulted automatically
 * by rocksdb to decide whether or not to read some
 * information from disk. In many cases, a filter can cut down the
 * number of disk seeks form a handful to a single disk seek per
 * DB::Get() call.
 */
//TODO(AR) should be renamed FilterPolicy
public class Filter extends RocksObject {
  /**
   * Creates a new FilterPolicy based on the input value string and returns the
   * result. The value might be an ID, and ID with properties, or an old-style
   * policy string. The value describes the FilterPolicy being created.
   * For BloomFilters, value may be a ":"-delimited value of the form:
   * "bloomfilter:[bits_per_key]", e.g. ""bloomfilter:4"
   * The above string is equivalent to calling NewBloomFilterPolicy(4).
   * Creates a new Filter based on the input opts string
   * @param opts The input string stating the name of the policy and its parameters
   */
  public static Filter createFromString(final String opts) throws RocksDBException {
    return new Filter(createFilterFromString(opts));
  }

  /**
   * Creates a new FilterPolicy based on the input value string and returns the
   * result.
   * @param cfgOpts Controls how the filter is created
   * @param opts The input string stating the name of the policy and its parameters
   */
  public static Filter createFromString(final ConfigOptions cfgOpts, final String opts)
      throws RocksDBException {
    return new Filter(createFilterFromString(cfgOpts.nativeHandle_, opts));
  }

  protected Filter(final long nativeHandle) {
    super(nativeHandle);
  }

  /**
   * Deletes underlying C++ filter pointer.
   *
   * Note that this function should be called only after all
   * RocksDB instances referencing the filter are closed.
   * Otherwise an undefined behavior will occur.
   */
  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  public String getId() {
    assert (isOwningHandle());
    return getId(nativeHandle_);
  }

  public boolean isInstanceOf(String name) {
    assert (isOwningHandle());
    return isInstanceOf(nativeHandle_, name);
  }

  @Override
  protected final native void disposeInternal(final long handle);
  protected native static long createFilterFromString(final String opts) throws RocksDBException;
  protected native static long createFilterFromString(final long cfgHandle, final String opts)
      throws RocksDBException;
  private native String getId(long handle);
  private native boolean isInstanceOf(long handle, String name);
}
