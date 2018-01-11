// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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
    try (final Options opt = new Options()
        .setCreateIfMissing(true)
        .setMergeOperatorName("stringappend");
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {
      // writing aa under key
      db.put("key".getBytes(), "aa".getBytes());
      // merge bb under key
      db.merge("key".getBytes(), "bb".getBytes());

      final byte[] value = db.get("key".getBytes());
      final String strValue = new String(value);
      assertThat(strValue).isEqualTo("aa,bb");
    }
  }

  @Test
  public void cFStringOption()
      throws InterruptedException, RocksDBException {

    try (final ColumnFamilyOptions cfOpt1 = new ColumnFamilyOptions()
        .setMergeOperatorName("stringappend");
         final ColumnFamilyOptions cfOpt2 = new ColumnFamilyOptions()
             .setMergeOperatorName("stringappend")
    ) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpt1),
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpt2)
      );

      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions opt = new DBOptions()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true);
           final RocksDB db = RocksDB.open(opt,
               dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
               columnFamilyHandleList)) {
        try {
          // writing aa under key
          db.put(columnFamilyHandleList.get(1),
              "cfkey".getBytes(), "aa".getBytes());
          // merge bb under key
          db.merge(columnFamilyHandleList.get(1),
              "cfkey".getBytes(), "bb".getBytes());

          byte[] value = db.get(columnFamilyHandleList.get(1),
              "cfkey".getBytes());
          String strValue = new String(value);
          assertThat(strValue).isEqualTo("aa,bb");
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandleList) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void operatorOption()
      throws InterruptedException, RocksDBException {
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator();
         final Options opt = new Options()
            .setCreateIfMissing(true)
            .setMergeOperator(stringAppendOperator);
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {
      // Writing aa under key
      db.put("key".getBytes(), "aa".getBytes());

      // Writing bb under key
      db.merge("key".getBytes(), "bb".getBytes());

      final byte[] value = db.get("key".getBytes());
      final String strValue = new String(value);

      assertThat(strValue).isEqualTo("aa,bb");
    }
  }

  @Test
  public void cFOperatorOption()
      throws InterruptedException, RocksDBException {
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator();
         final ColumnFamilyOptions cfOpt1 = new ColumnFamilyOptions()
             .setMergeOperator(stringAppendOperator);
         final ColumnFamilyOptions cfOpt2 = new ColumnFamilyOptions()
             .setMergeOperator(stringAppendOperator)
    ) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpt1),
          new ColumnFamilyDescriptor("new_cf".getBytes(), cfOpt2)
      );
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions opt = new DBOptions()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true);
           final RocksDB db = RocksDB.open(opt,
               dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
               columnFamilyHandleList)
      ) {
        try {
          // writing aa under key
          db.put(columnFamilyHandleList.get(1),
              "cfkey".getBytes(), "aa".getBytes());
          // merge bb under key
          db.merge(columnFamilyHandleList.get(1),
              "cfkey".getBytes(), "bb".getBytes());
          byte[] value = db.get(columnFamilyHandleList.get(1),
              "cfkey".getBytes());
          String strValue = new String(value);

          // Test also with createColumnFamily
          try (final ColumnFamilyOptions cfHandleOpts =
                   new ColumnFamilyOptions()
                       .setMergeOperator(stringAppendOperator);
               final ColumnFamilyHandle cfHandle =
                   db.createColumnFamily(
                       new ColumnFamilyDescriptor("new_cf2".getBytes(),
                           cfHandleOpts))
          ) {
            // writing xx under cfkey2
            db.put(cfHandle, "cfkey2".getBytes(), "xx".getBytes());
            // merge yy under cfkey2
            db.merge(cfHandle, new WriteOptions(), "cfkey2".getBytes(),
                "yy".getBytes());
            value = db.get(cfHandle, "cfkey2".getBytes());
            String strValueTmpCf = new String(value);

            assertThat(strValue).isEqualTo("aa,bb");
            assertThat(strValueTmpCf).isEqualTo("xx,yy");
          }
        } finally {
          for (final ColumnFamilyHandle columnFamilyHandle :
              columnFamilyHandleList) {
            columnFamilyHandle.close();
          }
        }
      }
    }
  }

  @Test
  public void operatorGcBehaviour()
      throws RocksDBException {
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator()) {
      try (final Options opt = new Options()
              .setCreateIfMissing(true)
              .setMergeOperator(stringAppendOperator);
           final RocksDB db = RocksDB.open(opt,
                   dbFolder.getRoot().getAbsolutePath())) {
        //no-op
      }


      // test reuse
      try (final Options opt = new Options()
              .setMergeOperator(stringAppendOperator);
           final RocksDB db = RocksDB.open(opt,
                   dbFolder.getRoot().getAbsolutePath())) {
        //no-op
      }

      // test param init
      try (final StringAppendOperator stringAppendOperator2 = new StringAppendOperator();
           final Options opt = new Options()
              .setMergeOperator(stringAppendOperator2);
           final RocksDB db = RocksDB.open(opt,
                   dbFolder.getRoot().getAbsolutePath())) {
        //no-op
      }

      // test replace one with another merge operator instance
      try (final Options opt = new Options()
              .setMergeOperator(stringAppendOperator);
           final StringAppendOperator newStringAppendOperator = new StringAppendOperator()) {
        opt.setMergeOperator(newStringAppendOperator);
        try (final RocksDB db = RocksDB.open(opt,
                dbFolder.getRoot().getAbsolutePath())) {
          //no-op
        }
      }
    }
  }

  @Test
  public void emptyStringInSetMergeOperatorByName() {
    try (final Options opt = new Options()
        .setMergeOperatorName("");
         final ColumnFamilyOptions cOpt = new ColumnFamilyOptions()
             .setMergeOperatorName("")) {
      //no-op
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullStringInSetMergeOperatorByNameOptions() {
    try (final Options opt = new Options()) {
      opt.setMergeOperatorName(null);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void
  nullStringInSetMergeOperatorByNameColumnFamilyOptions() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      opt.setMergeOperatorName(null);
    }
  }
}
