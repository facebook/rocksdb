// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
package org.rocksdb.test;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.List;

public class KeyMayExistTest {
  static final String DB_PATH = "/tmp/rocksdbjni_keymayexit_test";
  static {
    RocksDB.loadLibrary();
  }

  public static void main(String[] args){
    RocksDB db;
    Options options = new Options();
    options.setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
    try {
      // open database using cf names
      List<String> cfNames = new ArrayList<String>();
      List<ColumnFamilyHandle> columnFamilyHandleList =
          new ArrayList<ColumnFamilyHandle>();
      cfNames.add("default");
      cfNames.add("new_cf");
      db = RocksDB.open(options, DB_PATH, cfNames, columnFamilyHandleList);
      assert(columnFamilyHandleList.size()==2);

      db.put("key".getBytes(), "value".getBytes());
      StringBuffer retValue = new StringBuffer();
      if (db.keyMayExist(columnFamilyHandleList.get(0), "key".getBytes(),
          retValue)) {
        assert(retValue.toString().equals("value"));
      } else {
        assert(false);
      }
      assert(db.keyMayExist(columnFamilyHandleList.get(1), "key".getBytes(),
          retValue) == false);
      System.out.println("Passed KeyMayExistTest");
    }catch (RocksDBException e){
      e.printStackTrace();
      assert(false);
    }
  }
}
