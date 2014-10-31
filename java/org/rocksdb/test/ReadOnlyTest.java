// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
package org.rocksdb.test;

import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;

public class ReadOnlyTest {
  static final String DB_PATH = "/tmp/rocksdbjni_readonly_test";
  static {
    RocksDB.loadLibrary();
  }

  public static void main(String[] args){
    RocksDB db = null, db2 = null, db3 = null;
    List<ColumnFamilyHandle> columnFamilyHandleList =
        new ArrayList<ColumnFamilyHandle>();
    List<ColumnFamilyHandle> db2ColumnFamilyHandleList =
        new ArrayList<ColumnFamilyHandle>();
    List<ColumnFamilyHandle> db3ColumnFamilyHandleList =
        new ArrayList<ColumnFamilyHandle>();
    Options options = new Options();
    options.setCreateIfMissing(true);
    try {
      db = RocksDB.open(options, DB_PATH);
      db.put("key".getBytes(), "value".getBytes());
      db2 = RocksDB.openReadOnly(DB_PATH);
      assert("value".equals(new String(db2.get("key".getBytes()))));
      db.close();
      db2.close();


      List<ColumnFamilyDescriptor> cfNames =
          new ArrayList<ColumnFamilyDescriptor>();
      cfNames.add(new ColumnFamilyDescriptor("default"));

      db = RocksDB.open(DB_PATH, cfNames, columnFamilyHandleList);
      columnFamilyHandleList.add(db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf", new ColumnFamilyOptions())));
      columnFamilyHandleList.add(db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf2", new ColumnFamilyOptions())));
      db.put(columnFamilyHandleList.get(2), "key2".getBytes(),
          "value2".getBytes());

      db2 = RocksDB.openReadOnly(DB_PATH, cfNames, db2ColumnFamilyHandleList);
      assert(db2.get("key2".getBytes())==null);
      assert(db2.get(columnFamilyHandleList.get(0), "key2".getBytes())==null);

      List<ColumnFamilyDescriptor> cfNewName =
          new ArrayList<ColumnFamilyDescriptor>();
      cfNewName.add(new ColumnFamilyDescriptor("default"));
      cfNewName.add(new ColumnFamilyDescriptor("new_cf2"));
      db3 = RocksDB.openReadOnly(DB_PATH, cfNewName, db3ColumnFamilyHandleList);
      assert(new String(db3.get(db3ColumnFamilyHandleList.get(1),
          "key2".getBytes())).equals("value2"));
    }catch (RocksDBException e){
      e.printStackTrace();
      assert(false);
    }
    // test that put fails in readonly mode
    try {
      db2.put("key".getBytes(), "value".getBytes());
      assert(false);
    } catch (RocksDBException e) {
      assert(true);
    }
    try {
      db3.put(db3ColumnFamilyHandleList.get(1),
          "key".getBytes(), "value".getBytes());
      assert(false);
    } catch (RocksDBException e) {
      assert(true);
    }
    // test that remove fails in readonly mode
    try {
      db2.remove("key".getBytes());
      assert(false);
    } catch (RocksDBException e) {
      assert(true);
    }
    try {
      db3.remove(db3ColumnFamilyHandleList.get(1),
          "key".getBytes());
      assert(false);
    } catch (RocksDBException e) {
      assert(true);
    }
    // test that write fails in readonly mode
    WriteBatch wb = new WriteBatch();
    wb.put("key".getBytes(), "value".getBytes());
    try {
      db2.write(new WriteOptions(), wb);
      assert(false);
    } catch (RocksDBException e) {
      assert(true);
    }
    wb.dispose();
    wb = new WriteBatch();
    wb.put(db3ColumnFamilyHandleList.get(1),
        "key".getBytes(), "value".getBytes());
    try {
      db3.write(new WriteOptions(), wb);
      assert(false);
    } catch (RocksDBException e) {
      assert(true);
    }
    wb.dispose();
    // cleanup c++ pointers
    for (ColumnFamilyHandle columnFamilyHandle :
        columnFamilyHandleList) {
      columnFamilyHandle.dispose();
    }
    db.close();
    for (ColumnFamilyHandle columnFamilyHandle :
        db2ColumnFamilyHandleList) {
      columnFamilyHandle.dispose();
    }
    db2.close();
    for (ColumnFamilyHandle columnFamilyHandle :
        db3ColumnFamilyHandleList) {
      columnFamilyHandle.dispose();
    }
    db3.close();
    System.out.println("Passed ReadOnlyTest");
  }
}
