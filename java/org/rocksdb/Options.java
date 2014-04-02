// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * Options to control the behavior of a database.  It will be used
 * during the creation of a RocksDB (i.e., RocksDB::Open()).
 *
 * Note that dispose() must be called before an Options instance
 * become out-of-scope to release the allocated memory in c++.
 */
public class Options {
  /**
   * Construct options for opening a RocksDB.
   *
   * This constructor will create (by allocating a block of memory)
   * an rocksdb::Options in the c++ side.
   */
  public Options() {
    nativeHandle_ = 0;
    newOptions();
  }

  /**
   * If this value is set to true, then the database will be created
   * if it is missing during RocksDB::Open().
   * Default: false
   *
   * @param flag a flag indicating whether to create a database the
   *     specified database in RocksDB::Open() operation is missing.
   * @see RocksDB::Open()
   */
  public void setCreateIfMissing(boolean flag) {
    assert(nativeHandle_ != 0);
    setCreateIfMissing(nativeHandle_, flag);
  }

  /**
   * Return true if the create_if_missing flag is set to true.
   * If true, the database will be created if it is missing.
   *
   * @return return true if the create_if_missing flag is set to true.
   * @see setCreateIfMissing()
   */
  public boolean craeteIfMissing() {
    assert(nativeHandle_ != 0);
    return createIfMissing(nativeHandle_);
  }

  /**
   * Release the memory allocated for the current instance
   * in the c++ side.
   */
  public synchronized void dispose() {
    if (nativeHandle_ != 0) {
      dispose0();
    }
  }

  private native void newOptions();
  private native void dispose0();
  private native void setCreateIfMissing(long handle, boolean flag);
  private native boolean createIfMissing(long handle);

  long nativeHandle_;
}
