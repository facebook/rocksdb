// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
package org.rocksdb.test;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.ArrayList;
import java.util.List;

public class RocksIteratorTest {
  static final String DB_PATH = "/tmp/rocksdbjni_iterator_test";
  static {
    RocksDB.loadLibrary();
  }

  public static void main(String[] args){
    RocksDB db;
    Options options = new Options();
    options.setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
    try {
      db = RocksDB.open(options, DB_PATH);
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
      System.out.println("Passed RocksIterator Test");
      iter3.dispose();
      System.gc();
      System.runFinalization();
    }catch (RocksDBException e){
      e.printStackTrace();
      assert(false);
    }
  }
}
