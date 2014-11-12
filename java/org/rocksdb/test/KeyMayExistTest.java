// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
package org.rocksdb.test;

import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;

public class KeyMayExistTest {
  static final String DB_PATH = "/tmp/rocksdbjni_keymayexit_test";
  static {
    RocksDB.loadLibrary();
  }

  public static void main(String[] args){
    RocksDB db;
    DBOptions options = new DBOptions();
    options.setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
    try {
      // open database using cf names
      List<ColumnFamilyDescriptor> cfNames =
          new ArrayList<ColumnFamilyDescriptor>();
      List<ColumnFamilyHandle> columnFamilyHandleList =
          new ArrayList<ColumnFamilyHandle>();
      cfNames.add(new ColumnFamilyDescriptor("default"));
      cfNames.add(new ColumnFamilyDescriptor("new_cf"));
      db = RocksDB.open(options, DB_PATH, cfNames, columnFamilyHandleList);
      assert(columnFamilyHandleList.size()==2);

      db.put("key".getBytes(), "value".getBytes());
      // Test without column family
      StringBuffer retValue = new StringBuffer();
      if (db.keyMayExist("key".getBytes(), retValue)) {
        assert(retValue.toString().equals("value"));
      } else {
        assert(false);
      }
      // Test without column family but with readOptions
      retValue = new StringBuffer();
      if (db.keyMayExist(new ReadOptions(), "key".getBytes(),
          retValue)) {
        assert(retValue.toString().equals("value"));
      } else {
        assert(false);
      }
      // Test with column family
      retValue = new StringBuffer();
      if (db.keyMayExist(columnFamilyHandleList.get(0), "key".getBytes(),
          retValue)) {
        assert(retValue.toString().equals("value"));
      } else {
        assert(false);
      }
      // Test with column family and readOptions
      retValue = new StringBuffer();
      if (db.keyMayExist(new ReadOptions(),
          columnFamilyHandleList.get(0), "key".getBytes(),
          retValue)) {
        assert(retValue.toString().equals("value"));
      } else {
        assert(false);
      }
      // KeyMayExist in CF1 must return false
      assert(db.keyMayExist(columnFamilyHandleList.get(1), "key".getBytes(),
          retValue) == false);
      System.out.println("Passed KeyMayExistTest");
    }catch (RocksDBException e){
      e.printStackTrace();
      assert(false);
    }
  }
}
