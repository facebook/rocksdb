// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
package org.rocksdb.test;

import org.rocksdb.*;

public class FlushTest {

  static final String db_path = "/tmp/rocksdbjni_flush_test";
  static {
    RocksDB.loadLibrary();
  }

  public static void main(String[] args) {
    RocksDB db = null;
    Options options = new Options();
    WriteOptions wOpt = new WriteOptions();
    FlushOptions flushOptions = new FlushOptions();

    try {
      // Setup options
      options.setCreateIfMissing(true);
      options.setMaxWriteBufferNumber(10);
      options.setMinWriteBufferNumberToMerge(10);
      flushOptions.setWaitForFlush(true);
      wOpt.setDisableWAL(true);
      db = RocksDB.open(options, db_path);

      db.put(wOpt, "key1".getBytes(), "value1".getBytes());
      db.put(wOpt, "key2".getBytes(), "value2".getBytes());
      db.put(wOpt, "key3".getBytes(), "value3".getBytes());
      db.put(wOpt, "key4".getBytes(), "value4".getBytes());
      assert(db.getProperty("rocksdb.num-entries-active-mem-table").equals("4"));
      db.flush(flushOptions);
      assert(db.getProperty("rocksdb.num-entries-active-mem-table").equals("0"));
    } catch (RocksDBException e) {
      assert(false);
    }

    db.close();
    options.dispose();
    wOpt.dispose();
    flushOptions.dispose();
  }
}
