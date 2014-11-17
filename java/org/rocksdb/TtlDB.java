// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.List;


public class TtlDB extends RocksDB {

  //static Status Open(const Options& options, const std::string& dbname,
  //    DBWithTTL** dbptr, int32_t ttl = 0,
  //    bool read_only = false);
  public static TtlDB open(Options options, String db_path, int ttl,
      boolean readOnly) throws RocksDBException {
    TtlDB ttldb = new TtlDB();
    ttldb.open(options.nativeHandle_, db_path, ttl, readOnly);

    // Prevent the RocksDB object from attempting to delete
    // the underly C++ DB object.
    //ttldb.disOwnNativeHandle();
    return ttldb;
  }

  //static Status Open(const DBOptions& db_options, const std::string& dbname,
  //                   const std::vector<ColumnFamilyDescriptor>& column_families,
  //                   std::vector<ColumnFamilyHandle*>* handles,
  //                   DBWithTTL** dbptr, std::vector<int32_t> ttls,
  //                   bool read_only = false);
  public static TtlDB open(DBOptions options, String db_path,
      List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      List<ColumnFamilyHandle> columnFamilyHandles,
      List<Integer> ttlValues, boolean readOnly){


    return null;
  }

  public ColumnFamilyHandle createColumnFamilyWithTtl(
      ColumnFamilyDescriptor columnFamilyDescriptor, int ttl) {
    return null;
  }

  /**
   * Close the TtlDB instance and release resource.
   *
   * Internally, TtlDB owns the {@code rocksdb::DB} pointer to its associated
   * {@link org.rocksdb.RocksDB}. The release of that RocksDB pointer is handled in the destructor
   * of the c++ {@code rocksdb::TtlDB} and should be transparent to Java developers.
   */
  @Override public synchronized void close() {
    if (isInitialized()) {
      super.close();
    }
  }

  /**
   * A protected construction that will be used in the static factory
   * method {@link #open(DBOptions, String, java.util.List, java.util.List)} and
   * {@link #open(DBOptions, String, java.util.List, java.util.List, java.util.List, boolean)}.
   */
  protected TtlDB() {
    super();
  }

  @Override protected void finalize() throws Throwable {
    close();
    super.finalize();
  }

  private native void open(long optionsHandle, String db_path, int ttl,
      boolean readOnly) throws RocksDBException;
}
