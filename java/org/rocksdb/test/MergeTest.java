// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import java.util.List;
import java.util.ArrayList;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.*;

import static org.assertj.core.api.Assertions.assertThat;

public class MergeTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void stringOption()
      throws InterruptedException, RocksDBException {
    String db_path_string =
        dbFolder.getRoot().getAbsolutePath();
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
    assertThat(strValue).isEqualTo("aa,bb");
  }

  @Test
  public void cFStringOption()
      throws InterruptedException, RocksDBException {
    DBOptions opt = new DBOptions();
    String db_path_string =
        dbFolder.getRoot().getAbsolutePath();
    opt.setCreateIfMissing(true);
    opt.setCreateMissingColumnFamilies(true);

    List<ColumnFamilyDescriptor> cfDescr =
        new ArrayList<>();
    List<ColumnFamilyHandle> columnFamilyHandleList =
    new ArrayList<>();
    cfDescr.add(new ColumnFamilyDescriptor("default",
        new ColumnFamilyOptions().setMergeOperatorName(
            "stringappend")));
    cfDescr.add(new ColumnFamilyDescriptor("default",
        new ColumnFamilyOptions().setMergeOperatorName(
            "stringappend")));
    RocksDB db = RocksDB.open(opt, db_path_string,
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
    assertThat(strValue).isEqualTo("aa,bb");
  }

  @Test
  public void operatorOption()
      throws InterruptedException, RocksDBException {
    String db_path_string =
        dbFolder.getRoot().getAbsolutePath();
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
    assertThat(strValue).isEqualTo("aa,bb");
  }

  @Test
  public void cFOperatorOption()
      throws InterruptedException, RocksDBException {
    DBOptions opt = new DBOptions();
    String db_path_string =
        dbFolder.getRoot().getAbsolutePath();

    opt.setCreateIfMissing(true);
    opt.setCreateMissingColumnFamilies(true);
    StringAppendOperator stringAppendOperator = new StringAppendOperator();

    List<ColumnFamilyDescriptor> cfDescr =
        new ArrayList<>();
    List<ColumnFamilyHandle> columnFamilyHandleList =
    new ArrayList<>();
    cfDescr.add(new ColumnFamilyDescriptor("default",
        new ColumnFamilyOptions().setMergeOperator(
            stringAppendOperator)));
    cfDescr.add(new ColumnFamilyDescriptor("new_cf",
        new ColumnFamilyOptions().setMergeOperator(
            stringAppendOperator)));
    RocksDB db = RocksDB.open(opt, db_path_string,
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

    columnFamilyHandle.dispose();
    db.close();
    opt.dispose();
    assertThat(strValue).isEqualTo("aa,bb");
    assertThat(strValueTmpCf).isEqualTo("xx,yy");
  }

  @Test
  public void operatorGcBehaviour()
      throws RocksDBException {
    String db_path_string =
        dbFolder.getRoot().getAbsolutePath();
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
  }
}
