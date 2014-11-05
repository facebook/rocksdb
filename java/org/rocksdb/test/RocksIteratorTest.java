// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
package org.rocksdb.test;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public class RocksIteratorTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void rocksIteratorGc()
      throws RocksDBException {
    RocksDB db;
    Options options = new Options();
    options.setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
    db = RocksDB.open(options,
        dbFolder.getRoot().getAbsolutePath());
    db.put("key".getBytes(), "value".getBytes());
    RocksIterator iter = db.newIterator();
    RocksIterator iter2 = db.newIterator();
    RocksIterator iter3 = db.newIterator();
    iter = null;
    db.close();
    db = null;
    iter2 = null;
    System.gc();
    System.runFinalization();
    iter3.dispose();
    System.gc();
    System.runFinalization();
  }
}
