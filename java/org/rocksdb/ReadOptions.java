// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
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
    super();
    newReadOptions();
  }
  private native void newReadOptions();

  /**
   * If true, all data read from underlying storage will be
   * verified against corresponding checksums.
   * Default: true
   *
   * @return true if checksum verification is on.
   */
  public boolean verifyChecksums() {
    assert(isInitialized());
    return verifyChecksums(nativeHandle_);
  }
  private native boolean verifyChecksums(long handle);

  /**
   * If true, all data read from underlying storage will be
   * verified against corresponding checksums.
   * Default: true
   *
   * @param verifyChecksums if true, then checksum verification
   *     will be performed on every read.
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setVerifyChecksums(boolean verifyChecksums) {
    assert(isInitialized());
    setVerifyChecksums(nativeHandle_, verifyChecksums);
    return this;
  }
  private native void setVerifyChecksums(
      long handle, boolean verifyChecksums);

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
    assert(isInitialized());
    return fillCache(nativeHandle_);
  }
  private native boolean fillCache(long handle);

  /**
   * Fill the cache when loading the block-based sst formated db.
   * Callers may wish to set this field to false for bulk scans.
   * Default: true
   *
   * @param fillCache if true, then fill-cache behavior will be
   *     performed.
   * @return the reference to the current ReadOptions.
   */
  public ReadOptions setFillCache(boolean fillCache) {
    assert(isInitialized());
    setFillCache(nativeHandle_, fillCache);
    return this;
  }
  private native void setFillCache(
      long handle, boolean fillCache);

  /**
   * Specify to create a tailing iterator -- a special iterator that has a
   * view of the complete database (i.e. it can also be used to read newly
   * added data) and is optimized for sequential reads. It will return records
   * that were inserted into the database after the creation of the iterator.
   * Default: false
   * Not supported in ROCKSDB_LITE mode!
   *
   * @return true if tailing iterator is enabled.
   */
  public boolean tailing() {
    assert(isInitialized());
    return tailing(nativeHandle_);
  }
  private native boolean tailing(long handle);

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
  public ReadOptions setTailing(boolean tailing) {
    assert(isInitialized());
    setTailing(nativeHandle_, tailing);
    return this;
  }
  private native void setTailing(
      long handle, boolean tailing);


  @Override protected void disposeInternal() {
    assert(isInitialized());
    disposeInternal(nativeHandle_);
  }
  private native void disposeInternal(long handle);

}
