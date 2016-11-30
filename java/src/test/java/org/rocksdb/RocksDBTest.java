// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class RocksDBTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  public static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Test
  public void open() throws RocksDBException {
    try (final RocksDB db =
             RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
    }
  }

  @Test
  public void open_opt() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
    }
  }

  @Test
  public void put() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
         final WriteOptions opt = new WriteOptions()) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put(opt, "key2".getBytes(), "12345678".getBytes());
      assertThat(db.get("key1".getBytes())).isEqualTo(
          "value".getBytes());
      assertThat(db.get("key2".getBytes())).isEqualTo(
          "12345678".getBytes());
    }
  }

  @Test
  public void write() throws RocksDBException {
    try (final Options options = new Options().setMergeOperator(
        new StringAppendOperator()).setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath());
         final WriteOptions opts = new WriteOptions()) {

      try (final WriteBatch wb1 = new WriteBatch()) {
        wb1.put("key1".getBytes(), "aa".getBytes());
        wb1.merge("key1".getBytes(), "bb".getBytes());

        try (final WriteBatch wb2 = new WriteBatch()) {
          wb2.put("key2".getBytes(), "xx".getBytes());
          wb2.merge("key2".getBytes(), "yy".getBytes());
          db.write(opts, wb1);
          db.write(opts, wb2);
        }
      }

      assertThat(db.get("key1".getBytes())).isEqualTo(
          "aa,bb".getBytes());
      assertThat(db.get("key2".getBytes())).isEqualTo(
          "xx,yy".getBytes());
    }
  }

  @Test
  public void getWithOutValue() throws RocksDBException {
    try (final RocksDB db =
             RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      byte[] outValue = new byte[5];
      // not found value
      int getResult = db.get("keyNotFound".getBytes(), outValue);
      assertThat(getResult).isEqualTo(RocksDB.NOT_FOUND);
      // found value which fits in outValue
      getResult = db.get("key1".getBytes(), outValue);
      assertThat(getResult).isNotEqualTo(RocksDB.NOT_FOUND);
      assertThat(outValue).isEqualTo("value".getBytes());
      // found value which fits partially
      getResult = db.get("key2".getBytes(), outValue);
      assertThat(getResult).isNotEqualTo(RocksDB.NOT_FOUND);
      assertThat(outValue).isEqualTo("12345".getBytes());
    }
  }

  @Test
  public void getWithOutValueReadOptions() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
         final ReadOptions rOpt = new ReadOptions()) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      byte[] outValue = new byte[5];
      // not found value
      int getResult = db.get(rOpt, "keyNotFound".getBytes(),
          outValue);
      assertThat(getResult).isEqualTo(RocksDB.NOT_FOUND);
      // found value which fits in outValue
      getResult = db.get(rOpt, "key1".getBytes(), outValue);
      assertThat(getResult).isNotEqualTo(RocksDB.NOT_FOUND);
      assertThat(outValue).isEqualTo("value".getBytes());
      // found value which fits partially
      getResult = db.get(rOpt, "key2".getBytes(), outValue);
      assertThat(getResult).isNotEqualTo(RocksDB.NOT_FOUND);
      assertThat(outValue).isEqualTo("12345".getBytes());
    }
  }

  @Test
  public void multiGet() throws RocksDBException, InterruptedException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
         final ReadOptions rOpt = new ReadOptions()) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      List<byte[]> lookupKeys = new ArrayList<>();
      lookupKeys.add("key1".getBytes());
      lookupKeys.add("key2".getBytes());
      Map<byte[], byte[]> results = db.multiGet(lookupKeys);
      assertThat(results).isNotNull();
      assertThat(results.values()).isNotNull();
      assertThat(results.values()).
          contains("value".getBytes(), "12345678".getBytes());
      // test same method with ReadOptions
      results = db.multiGet(rOpt, lookupKeys);
      assertThat(results).isNotNull();
      assertThat(results.values()).isNotNull();
      assertThat(results.values()).
          contains("value".getBytes(), "12345678".getBytes());

      // remove existing key
      lookupKeys.remove("key2".getBytes());
      // add non existing key
      lookupKeys.add("key3".getBytes());
      results = db.multiGet(lookupKeys);
      assertThat(results).isNotNull();
      assertThat(results.values()).isNotNull();
      assertThat(results.values()).
          contains("value".getBytes());
      // test same call with readOptions
      results = db.multiGet(rOpt, lookupKeys);
      assertThat(results).isNotNull();
      assertThat(results.values()).isNotNull();
      assertThat(results.values()).
          contains("value".getBytes());
    }
  }

  @Test
  public void merge() throws RocksDBException {
    try (final Options opt = new Options()
        .setCreateIfMissing(true)
        .setMergeOperator(new StringAppendOperator());
         final WriteOptions wOpt = new WriteOptions();
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())
    ) {
      db.put("key1".getBytes(), "value".getBytes());
      assertThat(db.get("key1".getBytes())).isEqualTo(
          "value".getBytes());
      // merge key1 with another value portion
      db.merge("key1".getBytes(), "value2".getBytes());
      assertThat(db.get("key1".getBytes())).isEqualTo(
          "value,value2".getBytes());
      // merge key1 with another value portion
      db.merge(wOpt, "key1".getBytes(), "value3".getBytes());
      assertThat(db.get("key1".getBytes())).isEqualTo(
          "value,value2,value3".getBytes());
      // merge on non existent key shall insert the value
      db.merge(wOpt, "key2".getBytes(), "xxxx".getBytes());
      assertThat(db.get("key2".getBytes())).isEqualTo(
          "xxxx".getBytes());
    }
  }

  @Test
  public void delete() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
         final WriteOptions wOpt = new WriteOptions()) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      assertThat(db.get("key1".getBytes())).isEqualTo(
          "value".getBytes());
      assertThat(db.get("key2".getBytes())).isEqualTo(
          "12345678".getBytes());
      db.delete("key1".getBytes());
      db.delete(wOpt, "key2".getBytes());
      assertThat(db.get("key1".getBytes())).isNull();
      assertThat(db.get("key2".getBytes())).isNull();
    }
  }

  @Test
  public void singleDelete() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
         final WriteOptions wOpt = new WriteOptions()) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      assertThat(db.get("key1".getBytes())).isEqualTo(
          "value".getBytes());
      assertThat(db.get("key2".getBytes())).isEqualTo(
          "12345678".getBytes());
      db.singleDelete("key1".getBytes());
      db.singleDelete(wOpt, "key2".getBytes());
      assertThat(db.get("key1".getBytes())).isNull();
      assertThat(db.get("key2".getBytes())).isNull();
    }
  }

  @Test
  public void singleDelete_nonExisting() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
         final WriteOptions wOpt = new WriteOptions()) {
      db.singleDelete("key1".getBytes());
      db.singleDelete(wOpt, "key2".getBytes());
      assertThat(db.get("key1".getBytes())).isNull();
      assertThat(db.get("key2".getBytes())).isNull();
    }
  }

  @Test
  public void getIntProperty() throws RocksDBException {
    try (
        final Options options = new Options()
            .setCreateIfMissing(true)
            .setMaxWriteBufferNumber(10)
            .setMinWriteBufferNumberToMerge(10);
        final RocksDB db = RocksDB.open(options,
            dbFolder.getRoot().getAbsolutePath());
        final WriteOptions wOpt = new WriteOptions().setDisableWAL(true)
    ) {
      db.put(wOpt, "key1".getBytes(), "value1".getBytes());
      db.put(wOpt, "key2".getBytes(), "value2".getBytes());
      db.put(wOpt, "key3".getBytes(), "value3".getBytes());
      db.put(wOpt, "key4".getBytes(), "value4".getBytes());
      assertThat(db.getLongProperty("rocksdb.num-entries-active-mem-table"))
          .isGreaterThan(0);
      assertThat(db.getLongProperty("rocksdb.cur-size-active-mem-table"))
          .isGreaterThan(0);
    }
  }

  @Test
  public void fullCompactRange() throws RocksDBException {
    try (final Options opt = new Options().
        setCreateIfMissing(true).
        setDisableAutoCompactions(true).
        setCompactionStyle(CompactionStyle.LEVEL).
        setNumLevels(4).
        setWriteBufferSize(100 << 10).
        setLevelZeroFileNumCompactionTrigger(3).
        setTargetFileSizeBase(200 << 10).
        setTargetFileSizeMultiplier(1).
        setMaxBytesForLevelBase(500 << 10).
        setMaxBytesForLevelMultiplier(1).
        setDisableAutoCompactions(false);
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {
      // fill database with key/value pairs
      byte[] b = new byte[10000];
      for (int i = 0; i < 200; i++) {
        rand.nextBytes(b);
        db.put((String.valueOf(i)).getBytes(), b);
      }
      db.compactRange();
    }
  }

  @Test
  public void fullCompactRangeColumnFamily()
      throws RocksDBException {
    try (
        final DBOptions opt = new DBOptions().
            setCreateIfMissing(true).
            setCreateMissingColumnFamilies(true);
        final ColumnFamilyOptions new_cf_opts = new ColumnFamilyOptions().
            setDisableAutoCompactions(true).
            setCompactionStyle(CompactionStyle.LEVEL).
            setNumLevels(4).
            setWriteBufferSize(100 << 10).
            setLevelZeroFileNumCompactionTrigger(3).
            setTargetFileSizeBase(200 << 10).
            setTargetFileSizeMultiplier(1).
            setMaxBytesForLevelBase(500 << 10).
            setMaxBytesForLevelMultiplier(1).
            setDisableAutoCompactions(false)
    ) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts));

      // open database
      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      try (final RocksDB db = RocksDB.open(opt,
          dbFolder.getRoot().getAbsolutePath(),
          columnFamilyDescriptors,
          columnFamilyHandles)) {
        try {
          // fill database with key/value pairs
          byte[] b = new byte[10000];
          for (int i = 0; i < 200; i++) {
            rand.nextBytes(b);
            db.put(columnFamilyHandles.get(1),
                String.valueOf(i).getBytes(), b);
          }
          db.compactRange(columnFamilyHandles.get(1));
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void compactRangeWithKeys()
      throws RocksDBException {
    try (final Options opt = new Options().
        setCreateIfMissing(true).
        setDisableAutoCompactions(true).
        setCompactionStyle(CompactionStyle.LEVEL).
        setNumLevels(4).
        setWriteBufferSize(100 << 10).
        setLevelZeroFileNumCompactionTrigger(3).
        setTargetFileSizeBase(200 << 10).
        setTargetFileSizeMultiplier(1).
        setMaxBytesForLevelBase(500 << 10).
        setMaxBytesForLevelMultiplier(1).
        setDisableAutoCompactions(false);
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {
      // fill database with key/value pairs
      byte[] b = new byte[10000];
      for (int i = 0; i < 200; i++) {
        rand.nextBytes(b);
        db.put((String.valueOf(i)).getBytes(), b);
      }
      db.compactRange("0".getBytes(), "201".getBytes());
    }
  }

  @Test
  public void compactRangeWithKeysReduce()
      throws RocksDBException {
    try (
        final Options opt = new Options().
            setCreateIfMissing(true).
            setDisableAutoCompactions(true).
            setCompactionStyle(CompactionStyle.LEVEL).
            setNumLevels(4).
            setWriteBufferSize(100 << 10).
            setLevelZeroFileNumCompactionTrigger(3).
            setTargetFileSizeBase(200 << 10).
            setTargetFileSizeMultiplier(1).
            setMaxBytesForLevelBase(500 << 10).
            setMaxBytesForLevelMultiplier(1).
            setDisableAutoCompactions(false);
        final RocksDB db = RocksDB.open(opt,
            dbFolder.getRoot().getAbsolutePath())) {
      // fill database with key/value pairs
      byte[] b = new byte[10000];
      for (int i = 0; i < 200; i++) {
        rand.nextBytes(b);
        db.put((String.valueOf(i)).getBytes(), b);
      }
      db.flush(new FlushOptions().setWaitForFlush(true));
      db.compactRange("0".getBytes(), "201".getBytes(),
          true, -1, 0);
    }
  }

  @Test
  public void compactRangeWithKeysColumnFamily()
      throws RocksDBException {
    try (final DBOptions opt = new DBOptions().
        setCreateIfMissing(true).
        setCreateMissingColumnFamilies(true);
         final ColumnFamilyOptions new_cf_opts = new ColumnFamilyOptions().
             setDisableAutoCompactions(true).
             setCompactionStyle(CompactionStyle.LEVEL).
             setNumLevels(4).
             setWriteBufferSize(100 << 10).
             setLevelZeroFileNumCompactionTrigger(3).
             setTargetFileSizeBase(200 << 10).
             setTargetFileSizeMultiplier(1).
             setMaxBytesForLevelBase(500 << 10).
             setMaxBytesForLevelMultiplier(1).
             setDisableAutoCompactions(false)
    ) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts)
          );

      // open database
      final List<ColumnFamilyHandle> columnFamilyHandles =
          new ArrayList<>();
      try (final RocksDB db = RocksDB.open(opt,
          dbFolder.getRoot().getAbsolutePath(),
          columnFamilyDescriptors,
          columnFamilyHandles)) {
        try {
          // fill database with key/value pairs
          byte[] b = new byte[10000];
          for (int i = 0; i < 200; i++) {
            rand.nextBytes(b);
            db.put(columnFamilyHandles.get(1),
                String.valueOf(i).getBytes(), b);
          }
          db.compactRange(columnFamilyHandles.get(1),
              "0".getBytes(), "201".getBytes());
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void compactRangeWithKeysReduceColumnFamily()
      throws RocksDBException {
    try (final DBOptions opt = new DBOptions().
        setCreateIfMissing(true).
        setCreateMissingColumnFamilies(true);
         final ColumnFamilyOptions new_cf_opts = new ColumnFamilyOptions().
             setDisableAutoCompactions(true).
             setCompactionStyle(CompactionStyle.LEVEL).
             setNumLevels(4).
             setWriteBufferSize(100 << 10).
             setLevelZeroFileNumCompactionTrigger(3).
             setTargetFileSizeBase(200 << 10).
             setTargetFileSizeMultiplier(1).
             setMaxBytesForLevelBase(500 << 10).
             setMaxBytesForLevelMultiplier(1).
             setDisableAutoCompactions(false)
    ) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts)
          );

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      // open database
      try (final RocksDB db = RocksDB.open(opt,
          dbFolder.getRoot().getAbsolutePath(),
          columnFamilyDescriptors,
          columnFamilyHandles)) {
        try {
          // fill database with key/value pairs
          byte[] b = new byte[10000];
          for (int i = 0; i < 200; i++) {
            rand.nextBytes(b);
            db.put(columnFamilyHandles.get(1),
                String.valueOf(i).getBytes(), b);
          }
          db.compactRange(columnFamilyHandles.get(1), "0".getBytes(),
              "201".getBytes(), true, -1, 0);
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void compactRangeToLevel()
      throws RocksDBException, InterruptedException {
    final int NUM_KEYS_PER_L0_FILE = 100;
    final int KEY_SIZE = 20;
    final int VALUE_SIZE = 300;
    final int L0_FILE_SIZE =
        NUM_KEYS_PER_L0_FILE * (KEY_SIZE + VALUE_SIZE);
    final int NUM_L0_FILES = 10;
    final int TEST_SCALE = 5;
    final int KEY_INTERVAL = 100;
    try (final Options opt = new Options().
        setCreateIfMissing(true).
        setCompactionStyle(CompactionStyle.LEVEL).
        setNumLevels(5).
        // a slightly bigger write buffer than L0 file
        // so that we can ensure manual flush always
        // go before background flush happens.
            setWriteBufferSize(L0_FILE_SIZE * 2).
        // Disable auto L0 -> L1 compaction
            setLevelZeroFileNumCompactionTrigger(20).
            setTargetFileSizeBase(L0_FILE_SIZE * 100).
            setTargetFileSizeMultiplier(1).
        // To disable auto compaction
            setMaxBytesForLevelBase(NUM_L0_FILES * L0_FILE_SIZE * 100).
            setMaxBytesForLevelMultiplier(2).
            setDisableAutoCompactions(true);
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())
    ) {
      // fill database with key/value pairs
      byte[] value = new byte[VALUE_SIZE];
      int int_key = 0;
      for (int round = 0; round < 5; ++round) {
        int initial_key = int_key;
        for (int f = 1; f <= NUM_L0_FILES; ++f) {
          for (int i = 0; i < NUM_KEYS_PER_L0_FILE; ++i) {
            int_key += KEY_INTERVAL;
            rand.nextBytes(value);

            db.put(String.format("%020d", int_key).getBytes(),
                value);
          }
          db.flush(new FlushOptions().setWaitForFlush(true));
          // Make sure we do create one more L0 files.
          assertThat(
              db.getProperty("rocksdb.num-files-at-level0")).
              isEqualTo("" + f);
        }

        // Compact all L0 files we just created
        db.compactRange(
            String.format("%020d", initial_key).getBytes(),
            String.format("%020d", int_key - 1).getBytes());
        // Making sure there isn't any L0 files.
        assertThat(
            db.getProperty("rocksdb.num-files-at-level0")).
            isEqualTo("0");
        // Making sure there are some L1 files.
        // Here we only use != 0 instead of a specific number
        // as we don't want the test make any assumption on
        // how compaction works.
        assertThat(
            db.getProperty("rocksdb.num-files-at-level1")).
            isNotEqualTo("0");
        // Because we only compacted those keys we issued
        // in this round, there shouldn't be any L1 -> L2
        // compaction.  So we expect zero L2 files here.
        assertThat(
            db.getProperty("rocksdb.num-files-at-level2")).
            isEqualTo("0");
      }
    }
  }

  @Test
  public void compactRangeToLevelColumnFamily()
      throws RocksDBException {
    final int NUM_KEYS_PER_L0_FILE = 100;
    final int KEY_SIZE = 20;
    final int VALUE_SIZE = 300;
    final int L0_FILE_SIZE =
        NUM_KEYS_PER_L0_FILE * (KEY_SIZE + VALUE_SIZE);
    final int NUM_L0_FILES = 10;
    final int TEST_SCALE = 5;
    final int KEY_INTERVAL = 100;

    try (final DBOptions opt = new DBOptions().
        setCreateIfMissing(true).
        setCreateMissingColumnFamilies(true);
         final ColumnFamilyOptions new_cf_opts = new ColumnFamilyOptions().
             setCompactionStyle(CompactionStyle.LEVEL).
             setNumLevels(5).
             // a slightly bigger write buffer than L0 file
             // so that we can ensure manual flush always
             // go before background flush happens.
                 setWriteBufferSize(L0_FILE_SIZE * 2).
             // Disable auto L0 -> L1 compaction
                 setLevelZeroFileNumCompactionTrigger(20).
                 setTargetFileSizeBase(L0_FILE_SIZE * 100).
                 setTargetFileSizeMultiplier(1).
             // To disable auto compaction
                 setMaxBytesForLevelBase(NUM_L0_FILES * L0_FILE_SIZE * 100).
                 setMaxBytesForLevelMultiplier(2).
                 setDisableAutoCompactions(true)
    ) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts)
          );

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      // open database
      try (final RocksDB db = RocksDB.open(opt,
          dbFolder.getRoot().getAbsolutePath(),
          columnFamilyDescriptors,
          columnFamilyHandles)) {
        try {
          // fill database with key/value pairs
          byte[] value = new byte[VALUE_SIZE];
          int int_key = 0;
          for (int round = 0; round < 5; ++round) {
            int initial_key = int_key;
            for (int f = 1; f <= NUM_L0_FILES; ++f) {
              for (int i = 0; i < NUM_KEYS_PER_L0_FILE; ++i) {
                int_key += KEY_INTERVAL;
                rand.nextBytes(value);

                db.put(columnFamilyHandles.get(1),
                    String.format("%020d", int_key).getBytes(),
                    value);
              }
              db.flush(new FlushOptions().setWaitForFlush(true),
                  columnFamilyHandles.get(1));
              // Make sure we do create one more L0 files.
              assertThat(
                  db.getProperty(columnFamilyHandles.get(1),
                      "rocksdb.num-files-at-level0")).
                  isEqualTo("" + f);
            }

            // Compact all L0 files we just created
            db.compactRange(
                columnFamilyHandles.get(1),
                String.format("%020d", initial_key).getBytes(),
                String.format("%020d", int_key - 1).getBytes());
            // Making sure there isn't any L0 files.
            assertThat(
                db.getProperty(columnFamilyHandles.get(1),
                    "rocksdb.num-files-at-level0")).
                isEqualTo("0");
            // Making sure there are some L1 files.
            // Here we only use != 0 instead of a specific number
            // as we don't want the test make any assumption on
            // how compaction works.
            assertThat(
                db.getProperty(columnFamilyHandles.get(1),
                    "rocksdb.num-files-at-level1")).
                isNotEqualTo("0");
            // Because we only compacted those keys we issued
            // in this round, there shouldn't be any L1 -> L2
            // compaction.  So we expect zero L2 files here.
            assertThat(
                db.getProperty(columnFamilyHandles.get(1),
                    "rocksdb.num-files-at-level2")).
                isEqualTo("0");
          }
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void pauseContinueBackgroundWork() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())
    ) {
      db.pauseBackgroundWork();
      db.continueBackgroundWork();
      db.pauseBackgroundWork();
      db.continueBackgroundWork();
    }
  }

  @Test
  public void enableDisableFileDeletions() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())
    ) {
      db.disableFileDeletions();
      db.enableFileDeletions(false);
      db.disableFileDeletions();
      db.enableFileDeletions(true);
    }
  }

  @Test
  public void setOptions() throws RocksDBException {
    try (final DBOptions options = new DBOptions()
             .setCreateIfMissing(true)
             .setCreateMissingColumnFamilies(true);
         final ColumnFamilyOptions new_cf_opts = new ColumnFamilyOptions()
             .setWriteBufferSize(4096)) {

      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts));

      // open database
      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      try (final RocksDB db = RocksDB.open(options,
          dbFolder.getRoot().getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles)) {
        try {
          final MutableColumnFamilyOptions mutableOptions =
              MutableColumnFamilyOptions.builder()
                  .setWriteBufferSize(2048)
                  .build();

          db.setOptions(columnFamilyHandles.get(1), mutableOptions);

        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }
}
