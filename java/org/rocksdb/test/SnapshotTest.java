// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
package org.rocksdb.test;

import java.util.ArrayList;
import java.util.List;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;


public class SnapshotTest
{
  static final String DB_PATH = "/tmp/rocksdbjni_snapshot_test";
  static {
    RocksDB.loadLibrary();
  }

  public static void main(String[] args){
    RocksDB db = null;
    Options options = new Options();
    options.setCreateIfMissing(true);
    try {
      db = RocksDB.open(options, DB_PATH);
      db.put("key".getBytes(), "value".getBytes());
      // Get new Snapshot of database
      Snapshot snapshot = db.getSnapshot();
      ReadOptions readOptions = new ReadOptions();
      // set snapshot in ReadOptions
      readOptions.setSnapshot(snapshot);
      // retrieve key value pair
      assert(new String(db.get("key".getBytes()))
          .equals("value"));
      // retrieve key value pair created before
      // the snapshot was made
      assert(new String(db.get(readOptions,
          "key".getBytes())).equals("value"));
      // add new key/value pair
      db.put("newkey".getBytes(), "newvalue".getBytes());
      // using no snapshot the latest db entries
      // will be taken into account
      assert(new String(db.get("newkey".getBytes()))
          .equals("newvalue"));
      // snapshopot was created before newkey
      assert(db.get(readOptions, "newkey".getBytes())
          == null);
      // Retrieve snapshot from read options
      Snapshot sameSnapshot = readOptions.snapshot();
      readOptions.setSnapshot(sameSnapshot);
      // results must be the same with new Snapshot
      // instance using the same native pointer
      assert(new String(db.get(readOptions,
          "key".getBytes())).equals("value"));
      // update key value pair to newvalue
      db.put("key".getBytes(), "newvalue".getBytes());
      // read with previously created snapshot will
      // read previous version of key value pair
      assert(new String(db.get(readOptions,
          "key".getBytes())).equals("value"));
      // read for newkey using the snapshot must be
      // null
      assert(db.get(readOptions, "newkey".getBytes())
          == null);
      // setting null to snapshot in ReadOptions leads
      // to no Snapshot being used.
      readOptions.setSnapshot(null);
      assert(new String(db.get(readOptions,
          "newkey".getBytes())).equals("newvalue"));
      // release Snapshot
      db.releaseSnapshot(snapshot);
      // Close database
      db.close();
    }catch (RocksDBException e){
      e.printStackTrace();
      assert(false);
    }
    System.out.println("Passed SnapshotTest");
  }
}
