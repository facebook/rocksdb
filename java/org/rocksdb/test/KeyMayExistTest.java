// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
package org.rocksdb.test;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class KeyMayExistTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void keyMayExist() throws RocksDBException {
<<<<<<< HEAD
    RocksDB db;
    DBOptions options = new DBOptions();
    options.setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
    // open database using cf names
    List<ColumnFamilyDescriptor> cfDescriptors =
        new ArrayList<ColumnFamilyDescriptor>();
    List<ColumnFamilyHandle> columnFamilyHandleList =
        new ArrayList<>();
    cfDescriptors.add(new ColumnFamilyDescriptor("default"));
    cfDescriptors.add(new ColumnFamilyDescriptor("new_cf"));
    db = RocksDB.open(options,
        dbFolder.getRoot().getAbsolutePath(),
        cfDescriptors, columnFamilyHandleList);
    assertThat(columnFamilyHandleList.size()).
        isEqualTo(2);
    db.put("key".getBytes(), "value".getBytes());
    // Test without column family
    StringBuffer retValue = new StringBuffer();
    boolean exists = db.keyMayExist("key".getBytes(), retValue);
    assertThat(exists).isTrue();
    assertThat(retValue.toString()).
        isEqualTo("value");
=======
    RocksDB db = null;
    Options options = null;
    try {
      options = new Options();
      options.setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true);
      // open database using cf names
      List<String> cfNames = new ArrayList<>();
      List<ColumnFamilyHandle> columnFamilyHandleList =
          new ArrayList<>();
      cfNames.add("default");
      cfNames.add("new_cf");
      db = RocksDB.open(options,
          dbFolder.getRoot().getAbsolutePath(),
          cfNames, columnFamilyHandleList);
      assertThat(columnFamilyHandleList.size()).
          isEqualTo(2);
      db.put("key".getBytes(), "value".getBytes());
      // Test without column family
      StringBuffer retValue = new StringBuffer();
      boolean exists = db.keyMayExist("key".getBytes(), retValue);
      assertThat(exists).isTrue();
      assertThat(retValue.toString()).
          isEqualTo("value");
>>>>>>> [RocksJava] Integrated review comments from D28209

      // Test without column family but with readOptions
      retValue = new StringBuffer();
      exists = db.keyMayExist(new ReadOptions(), "key".getBytes(),
          retValue);
      assertThat(exists).isTrue();
      assertThat(retValue.toString()).
          isEqualTo("value");

      // Test with column family
      retValue = new StringBuffer();
      exists = db.keyMayExist(columnFamilyHandleList.get(0), "key".getBytes(),
          retValue);
      assertThat(exists).isTrue();
      assertThat(retValue.toString()).
          isEqualTo("value");

<<<<<<< HEAD
    // Test with column family and readOptions
    retValue = new StringBuffer();
    exists = db.keyMayExist(new ReadOptions(),
        columnFamilyHandleList.get(0), "key".getBytes(),
        retValue);
    assertThat(exists).isTrue();
    assertThat(retValue.toString()).
        isEqualTo("value");
=======
      // Test with column family and readOptions
      retValue = new StringBuffer();
      exists = db.keyMayExist(new ReadOptions(),
          columnFamilyHandleList.get(0), "key".getBytes(),
          retValue);
      assertThat(exists).isTrue();
      assertThat(retValue.toString()).
          isEqualTo("value");
>>>>>>> [RocksJava] Integrated review comments from D28209

      // KeyMayExist in CF1 must return false
      assertThat(db.keyMayExist(columnFamilyHandleList.get(1),
          "key".getBytes(), retValue)).isFalse();
    } finally {
      if (db != null) {
        db.close();
      }
      if (options != null) {
        options.dispose();
      }
    }
  }
}
