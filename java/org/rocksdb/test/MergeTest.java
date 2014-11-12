// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import java.util.List;
import java.util.ArrayList;
import org.rocksdb.*;

public class MergeTest {
  static final String db_path_string = "/tmp/rocksdbjni_mergestring_db";
  static final String db_cf_path_string = "/tmp/rocksdbjni_mergecfstring_db";
  static final String db_path_operator = "/tmp/rocksdbjni_mergeoperator_db";

  static {
    RocksDB.loadLibrary();
  }

  public static void testStringOption()
      throws InterruptedException, RocksDBException {
    Options opt = new Options();
    opt.setCreateIfMissing(true);
    opt.setMergeOperatorName("stringappend");

    RocksDB db = RocksDB.open(opt, db_path_string);
    // writing aa under key
    db.put("key".getBytes(), "aa".getBytes());
    // merge bb under key
    db.merge("key".getBytes(), "bb".getBytes());

    byte[] value = db.get("key".getBytes());
    String strValue = new String(value);

    db.close();
    opt.dispose();
    assert(strValue.equals("aa,bb"));
  }

  public static void testCFStringOption()
      throws InterruptedException, RocksDBException {
    DBOptions opt = new DBOptions();
    opt.setCreateIfMissing(true);
    opt.setCreateMissingColumnFamilies(true);

    List<ColumnFamilyDescriptor> cfDescr =
        new ArrayList<ColumnFamilyDescriptor>();
    List<ColumnFamilyHandle> columnFamilyHandleList =
    new ArrayList<ColumnFamilyHandle>();
    cfDescr.add(new ColumnFamilyDescriptor("default",
        new ColumnFamilyOptions().setMergeOperatorName(
            "stringappend")));
    cfDescr.add(new ColumnFamilyDescriptor("default",
        new ColumnFamilyOptions().setMergeOperatorName(
            "stringappend")));
    RocksDB db = RocksDB.open(opt, db_cf_path_string,
        cfDescr, columnFamilyHandleList);

    // writing aa under key
    db.put(columnFamilyHandleList.get(1),
        "cfkey".getBytes(), "aa".getBytes());
    // merge bb under key
    db.merge(columnFamilyHandleList.get(1),
        "cfkey".getBytes(), "bb".getBytes());

    byte[] value = db.get(columnFamilyHandleList.get(1), "cfkey".getBytes());
    String strValue = new String(value);

    for (ColumnFamilyHandle handle : columnFamilyHandleList) {
      handle.dispose();
    }
    db.close();
    opt.dispose();
    assert(strValue.equals("aa,bb"));
  }

  public static void testOperatorOption()
      throws InterruptedException, RocksDBException {
    Options opt = new Options();
    opt.setCreateIfMissing(true);

    StringAppendOperator stringAppendOperator = new StringAppendOperator();
    opt.setMergeOperator(stringAppendOperator);

    RocksDB db = RocksDB.open(opt, db_path_string);
    // Writing aa under key
    db.put("key".getBytes(), "aa".getBytes());

    // Writing bb under key
    db.merge("key".getBytes(), "bb".getBytes());

    byte[] value = db.get("key".getBytes());
    String strValue = new String(value);

    db.close();
    opt.dispose();
    assert(strValue.equals("aa,bb"));
  }

  public static void testCFOperatorOption()
      throws InterruptedException, RocksDBException {
    DBOptions opt = new DBOptions();
    opt.setCreateIfMissing(true);
    opt.setCreateMissingColumnFamilies(true);
    StringAppendOperator stringAppendOperator = new StringAppendOperator();

    List<ColumnFamilyDescriptor> cfDescr =
        new ArrayList<ColumnFamilyDescriptor>();
    List<ColumnFamilyHandle> columnFamilyHandleList =
    new ArrayList<ColumnFamilyHandle>();
    cfDescr.add(new ColumnFamilyDescriptor("default",
        new ColumnFamilyOptions().setMergeOperator(
            stringAppendOperator)));
    cfDescr.add(new ColumnFamilyDescriptor("new_cf",
        new ColumnFamilyOptions().setMergeOperator(
            stringAppendOperator)));
    RocksDB db = RocksDB.open(opt, db_path_operator,
        cfDescr, columnFamilyHandleList);

    // writing aa under key
    db.put(columnFamilyHandleList.get(1),
        "cfkey".getBytes(), "aa".getBytes());
    // merge bb under key
    db.merge(columnFamilyHandleList.get(1),
        "cfkey".getBytes(), "bb".getBytes());
    byte[] value = db.get(columnFamilyHandleList.get(1), "cfkey".getBytes());
    String strValue = new String(value);

    // Test also with createColumnFamily
    ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(
        new ColumnFamilyDescriptor("new_cf2",
            new ColumnFamilyOptions().setMergeOperator(
                new StringAppendOperator())));
    // writing xx under cfkey2
    db.put(columnFamilyHandle, "cfkey2".getBytes(), "xx".getBytes());
    // merge yy under cfkey2
    db.merge(columnFamilyHandle, "cfkey2".getBytes(), "yy".getBytes());
    value = db.get(columnFamilyHandle, "cfkey2".getBytes());
    String strValueTmpCf = new String(value);

    db.close();
    opt.dispose();
    assert(strValue.equals("aa,bb"));
    assert(strValueTmpCf.equals("xx,yy"));
  }

  public static void testOperatorGcBehaviour()
      throws RocksDBException {
    Options opt = new Options();
    opt.setCreateIfMissing(true);
    StringAppendOperator stringAppendOperator = new StringAppendOperator();
    opt.setMergeOperator(stringAppendOperator);
    RocksDB db = RocksDB.open(opt, db_path_string);
    db.close();
    opt.dispose();
    System.gc();
    System.runFinalization();
    // test reuse
    opt = new Options();
    opt.setMergeOperator(stringAppendOperator);
    db = RocksDB.open(opt, db_path_string);
    db.close();
    opt.dispose();
    System.gc();
    System.runFinalization();
    // test param init
    opt = new Options();
    opt.setMergeOperator(new StringAppendOperator());
    db = RocksDB.open(opt, db_path_string);
    db.close();
    opt.dispose();
    System.gc();
    System.runFinalization();
    // test replace one with another merge operator instance
    opt = new Options();
    opt.setMergeOperator(stringAppendOperator);
    StringAppendOperator newStringAppendOperator = new StringAppendOperator();
    opt.setMergeOperator(newStringAppendOperator);
    db = RocksDB.open(opt, db_path_string);
    db.close();
    opt.dispose();
    stringAppendOperator = null;
    newStringAppendOperator = null;
    System.gc();
    System.runFinalization();
  }

  public static void main(String[] args)
      throws InterruptedException, RocksDBException {
    testStringOption();
    testCFStringOption();
    testOperatorOption();
    testCFOperatorOption();
    testOperatorGcBehaviour();
    System.out.println("Passed MergeTest.");
  }
}
