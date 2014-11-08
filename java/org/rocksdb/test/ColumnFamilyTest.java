// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import org.rocksdb.*;

public class ColumnFamilyTest {
  static final String db_path = "/tmp/rocksdbjni_columnfamily_test";
  static {
    RocksDB.loadLibrary();
  }

  public static void main(String[] args) {

    RocksDB db = null;
    Options options = new Options();
    options.setCreateIfMissing(true);

    DBOptions dbOptions = new DBOptions();
    dbOptions.setCreateIfMissing(true);

    try {
        db = RocksDB.open(options, db_path);
    } catch (RocksDBException e) {
      assert(false);
    }
    // Test listColumnFamilies
    List<byte[]> columnFamilyNames;
    try {
      columnFamilyNames =  RocksDB.listColumnFamilies(options, db_path);
      if (columnFamilyNames != null && columnFamilyNames.size() > 0) {
        assert(columnFamilyNames.size() == 1);
        assert(new String(columnFamilyNames.get(0)).equals("default"));
      } else {
        assert(false);
      }
    } catch (RocksDBException e) {
      assert(false);
    }

    // Test createColumnFamily
    try {
      db.createColumnFamily(new ColumnFamilyDescriptor("new_cf",
          new ColumnFamilyOptions()));
    } catch (RocksDBException e) {
      assert(false);
    }

    if (db != null) {
      db.close();
    }

    // Test listColumnFamilies after create "new_cf"
    try {
      columnFamilyNames =  RocksDB.listColumnFamilies(options, db_path);
      if (columnFamilyNames != null && columnFamilyNames.size() > 0) {
        assert(columnFamilyNames.size() == 2);
        assert(new String(columnFamilyNames.get(0)).equals("default"));
        assert(new String(columnFamilyNames.get(1)).equals("new_cf"));
      } else {
        assert(false);
      }
    } catch (RocksDBException e) {
      assert(false);
    }

    // Test open database with column family names
    List<ColumnFamilyDescriptor> cfNames =
        new ArrayList<>();
    List<ColumnFamilyHandle> columnFamilyHandleList =
        new ArrayList<>();
    cfNames.add(new ColumnFamilyDescriptor("default"));
    cfNames.add(new ColumnFamilyDescriptor("new_cf"));

    try {
      db = RocksDB.open(dbOptions, db_path, cfNames, columnFamilyHandleList);
      assert(columnFamilyHandleList.size() == 2);
      db.put("dfkey1".getBytes(), "dfvalue".getBytes());
      db.put(columnFamilyHandleList.get(0), "dfkey2".getBytes(),
          "dfvalue".getBytes());
      db.put(columnFamilyHandleList.get(1), "newcfkey1".getBytes(),
          "newcfvalue".getBytes());

      String retVal = new String(db.get(columnFamilyHandleList.get(1),
          "newcfkey1".getBytes()));
      assert(retVal.equals("newcfvalue"));
      assert( (db.get(columnFamilyHandleList.get(1),
          "dfkey1".getBytes())) == null);
      db.remove(columnFamilyHandleList.get(1), "newcfkey1".getBytes());
      assert( (db.get(columnFamilyHandleList.get(1),
          "newcfkey1".getBytes())) == null);
      db.remove("dfkey2".getBytes());
      assert( (db.get(columnFamilyHandleList.get(0),
          "dfkey2".getBytes())) == null);
    } catch (RocksDBException e) {
      assert(false);
    }

    // Test create write to and drop ColumnFamily
    ColumnFamilyHandle tmpColumnFamilyHandle = null;
    try {
      tmpColumnFamilyHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("tmpCF", new ColumnFamilyOptions()));
      db.put(tmpColumnFamilyHandle, "key".getBytes(), "value".getBytes());
      db.dropColumnFamily(tmpColumnFamilyHandle);
      tmpColumnFamilyHandle.dispose();
    } catch (Exception e) {
      assert(false);
    }

    // Put to disposed column family tmpColumnFamilyHandle must fail
    try {
      db.put(tmpColumnFamilyHandle, "key".getBytes(), "value".getBytes());
      assert(false);
    } catch (RocksDBException e) {
      assert(true);
    }

    // Remove to disposed column family tmpColumnFamilyHandle must fail
    try {
      db.remove(tmpColumnFamilyHandle, "key".getBytes());
      assert(false);
    } catch (RocksDBException e) {
      assert(true);
    }

    // Get on a disposed column family tmpColumnFamilyHandle must fail
    try {
      db.get(tmpColumnFamilyHandle, "key".getBytes());
      assert(false);
    } catch (RocksDBException e) {
      assert(true);
    }

    // Test WriteBatch
    try {
      WriteBatch writeBatch = new WriteBatch();
      WriteOptions writeOpt = new WriteOptions();
      writeBatch.put("key".getBytes(), "value".getBytes());
      writeBatch.put(columnFamilyHandleList.get(1), "newcfkey".getBytes(),
          "value".getBytes());
      writeBatch.put(columnFamilyHandleList.get(1), "newcfkey2".getBytes(),
          "value2".getBytes());
      writeBatch.remove("xyz".getBytes());
      writeBatch.remove(columnFamilyHandleList.get(1), "xyz".getBytes());
      db.write(writeOpt, writeBatch);
      writeBatch.dispose();
      assert(db.get(columnFamilyHandleList.get(1),
          "xyz".getBytes()) == null);
      assert(new String(db.get(columnFamilyHandleList.get(1),
          "newcfkey".getBytes())).equals("value"));
      assert(new String(db.get(columnFamilyHandleList.get(1),
          "newcfkey2".getBytes())).equals("value2"));
      assert(new String(db.get("key".getBytes())).equals("value"));
    } catch (Exception e) {
      e.printStackTrace();
      assert(false);
    }

    // Test iterator on column family
    try {
      RocksIterator rocksIterator = db.newIterator(
          columnFamilyHandleList.get(1));
      rocksIterator.seekToFirst();
      Map<String, String> refMap = new HashMap<String, String>();
      refMap.put("newcfkey", "value");
      refMap.put("newcfkey2", "value2");
      int i = 0;
      while(rocksIterator.isValid()) {
        i++;
        refMap.get(new String(rocksIterator.key())).equals(
            new String(rocksIterator.value()));
        rocksIterator.next();
      }
      assert(i == 2);
      rocksIterator.dispose();
    } catch(Exception e) {
      assert(false);
    }

    // Test property handling on column families
    try {
      assert(db.getProperty("rocksdb.estimate-num-keys") != null);
      assert(db.getProperty("rocksdb.stats") != null);
      assert(db.getProperty(columnFamilyHandleList.get(0),
          "rocksdb.sstables") != null);
      assert(db.getProperty(columnFamilyHandleList.get(1),
          "rocksdb.estimate-num-keys") != null);
      assert(db.getProperty(columnFamilyHandleList.get(1),
          "rocksdb.stats") != null);
      assert(db.getProperty(columnFamilyHandleList.get(1),
          "rocksdb.sstables") != null);
    } catch(Exception e) {
      assert(false);
    }

    // MultiGet test
    List<ColumnFamilyHandle> cfCustomList = new ArrayList<ColumnFamilyHandle>();
    try {
      List<byte[]> keys = new ArrayList<byte[]>();
      keys.add("key".getBytes());
      keys.add("newcfkey".getBytes());
      Map<byte[], byte[]> retValues = db.multiGet(columnFamilyHandleList,keys);
      assert(retValues.size() == 2);
      assert(new String(retValues.get(keys.get(0)))
          .equals("value"));
      assert(new String(retValues.get(keys.get(1)))
          .equals("value"));

      cfCustomList.add(columnFamilyHandleList.get(0));
      cfCustomList.add(columnFamilyHandleList.get(0));
      retValues = db.multiGet(cfCustomList, keys);
      assert(retValues.size() == 1);
      assert(new String(retValues.get(keys.get(0)))
          .equals("value"));
    } catch (RocksDBException e) {
      assert(false);
    }

    // Test multiget without correct number of column
    // families
    try {
      List<byte[]> keys = new ArrayList<byte[]>();
      keys.add("key".getBytes());
      keys.add("newcfkey".getBytes());
      cfCustomList.remove(1);
      db.multiGet(cfCustomList, keys);
      assert(false);
    } catch (RocksDBException e) {
      assert(false);
    } catch (IllegalArgumentException e) {
      assert(true);
    }

    try {
      // iterate over default key/value pairs
      List<RocksIterator> iterators =
          db.newIterators(columnFamilyHandleList);
      assert(iterators.size() == 2);
      RocksIterator iter = iterators.get(0);
      iter.seekToFirst();
      Map<String,String> defRefMap = new HashMap<String, String>();
      defRefMap.put("dfkey1", "dfvalue");
      defRefMap.put("key", "value");
      while (iter.isValid()) {
        defRefMap.get(new String(iter.key())).equals(
            new String(iter.value()));
        iter.next();
      }
      // iterate over new_cf key/value pairs
      Map<String,String> cfRefMap = new HashMap<String, String>();
      cfRefMap.put("newcfkey", "value");
      cfRefMap.put("newcfkey2", "value2");
      iter = iterators.get(1);
      iter.seekToFirst();
      while (iter.isValid()) {
        cfRefMap.get(new String(iter.key())).equals(
            new String(iter.value()));
        iter.next();
      }
      // free iterators
      for (RocksIterator iterator : iterators) {
        iterator.dispose();
      }
      assert(true);
    } catch (RocksDBException e) {
      assert(false);
    }

    System.out.println("Passed ColumnFamilyTest");
    // free cf handles before database close
    for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
      columnFamilyHandle.dispose();
    }
    // close database
    db.close();
    // be sure to dispose c++ pointers
    options.dispose();
  }
}
