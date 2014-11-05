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

public class ReadOnlyTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void readOnlyOpen() throws RocksDBException {
    RocksDB db, db2, db3;
    List<ColumnFamilyHandle> columnFamilyHandleList =
        new ArrayList<>();
    List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
        new ArrayList<>();
    List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList2 =
        new ArrayList<>();

    Options options = new Options();
    options.setCreateIfMissing(true);

    db = RocksDB.open(options,
        dbFolder.getRoot().getAbsolutePath());
    db.put("key".getBytes(), "value".getBytes());
    db2 = RocksDB.openReadOnly(
        dbFolder.getRoot().getAbsolutePath());
    assertThat("value").
        isEqualTo(new String(db2.get("key".getBytes())));
    db.close();
    db2.close();

    List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    cfDescriptors.add(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
            new ColumnFamilyOptions()));

    db = RocksDB.open(
        dbFolder.getRoot().getAbsolutePath(), cfDescriptors, columnFamilyHandleList);
    columnFamilyHandleList.add(db.createColumnFamily(
        new ColumnFamilyDescriptor("new_cf", new ColumnFamilyOptions())));
    columnFamilyHandleList.add(db.createColumnFamily(
        new ColumnFamilyDescriptor("new_cf2", new ColumnFamilyOptions())));
    db.put(columnFamilyHandleList.get(2), "key2".getBytes(),
        "value2".getBytes());

    db2 = RocksDB.openReadOnly(
        dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
        readOnlyColumnFamilyHandleList);
    assertThat(db2.get("key2".getBytes())).isNull();
    assertThat(db2.get(readOnlyColumnFamilyHandleList.get(0), "key2".getBytes())).
        isNull();

    cfDescriptors.clear();
    cfDescriptors.add(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
            new ColumnFamilyOptions()));
    cfDescriptors.add(
        new ColumnFamilyDescriptor("new_cf2", new ColumnFamilyOptions()));
    db3 = RocksDB.openReadOnly(
        dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
        readOnlyColumnFamilyHandleList2);
    assertThat(new String(db3.get(readOnlyColumnFamilyHandleList2.get(1),
        "key2".getBytes()))).isEqualTo("value2");
    db.close();
    db2.close();
    db3.close();
    options.dispose();
  }

  @Test(expected = RocksDBException.class)
  public void failToWriteInReadOnly() throws RocksDBException {
    List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    cfDescriptors.add(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
            new ColumnFamilyOptions()));
    List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
        new ArrayList<>();
    Options options = new Options();
    options.setCreateIfMissing(true);

    RocksDB db = RocksDB.open(options,
        dbFolder.getRoot().getAbsolutePath());
    db.close();
    RocksDB rDb = RocksDB.openReadOnly(
        dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
        readOnlyColumnFamilyHandleList);
    // test that put fails in readonly mode
    rDb.put("key".getBytes(), "value".getBytes());
  }

  @Test(expected = RocksDBException.class)
  public void failToCFWriteInReadOnly() throws RocksDBException {
    List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    cfDescriptors.add(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
            new ColumnFamilyOptions()));
    List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
        new ArrayList<>();

    Options options = new Options();
    options.setCreateIfMissing(true);

    RocksDB db = RocksDB.open(options,
        dbFolder.getRoot().getAbsolutePath());
    db.close();
    RocksDB rDb = RocksDB.openReadOnly(
        dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
        readOnlyColumnFamilyHandleList);

    rDb.put(readOnlyColumnFamilyHandleList.get(0),
        "key".getBytes(), "value".getBytes());
  }

  @Test(expected = RocksDBException.class)
  public void failToRemoveInReadOnly() throws RocksDBException {
    List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    cfDescriptors.add(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
            new ColumnFamilyOptions()));
    List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
        new ArrayList<>();

    Options options = new Options();
    options.setCreateIfMissing(true);

    RocksDB db = RocksDB.open(options,
        dbFolder.getRoot().getAbsolutePath());
    db.close();
    RocksDB rDb = RocksDB.openReadOnly(
        dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
        readOnlyColumnFamilyHandleList);

    rDb.remove("key".getBytes());
  }

  @Test(expected = RocksDBException.class)
  public void failToCFRemoveInReadOnly() throws RocksDBException {
    List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    cfDescriptors.add(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
            new ColumnFamilyOptions()));
    List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
        new ArrayList<>();

    Options options = new Options();
    options.setCreateIfMissing(true);

    RocksDB db = RocksDB.open(options,
        dbFolder.getRoot().getAbsolutePath());
    db.close();

    RocksDB rDb = RocksDB.openReadOnly(
        dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
        readOnlyColumnFamilyHandleList);

    rDb.remove(readOnlyColumnFamilyHandleList.get(0),
        "key".getBytes());
  }

  @Test(expected = RocksDBException.class)
  public void failToWriteBatchReadOnly() throws RocksDBException {
    List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    cfDescriptors.add(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
            new ColumnFamilyOptions()));
    List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
        new ArrayList<>();

    Options options = new Options();
    options.setCreateIfMissing(true);

    RocksDB db = RocksDB.open(options,
        dbFolder.getRoot().getAbsolutePath());
    db.close();

    RocksDB rDb = RocksDB.openReadOnly(
        dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
        readOnlyColumnFamilyHandleList);

    WriteBatch wb = new WriteBatch();
    wb.put("key".getBytes(), "value".getBytes());
    rDb.write(new WriteOptions(), wb);
  }

  @Test(expected = RocksDBException.class)
  public void failToCFWriteBatchReadOnly() throws RocksDBException {
    List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    cfDescriptors.add(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
            new ColumnFamilyOptions()));
    List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
        new ArrayList<>();

    Options options = new Options();
    options.setCreateIfMissing(true);

    RocksDB db = RocksDB.open(options,
        dbFolder.getRoot().getAbsolutePath());
    db.close();

    RocksDB rDb = RocksDB.openReadOnly(
        dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
        readOnlyColumnFamilyHandleList);

    WriteBatch wb = new WriteBatch();
    wb.put(readOnlyColumnFamilyHandleList.get(0),
        "key".getBytes(), "value".getBytes());
    rDb.write(new WriteOptions(), wb);
  }
}
