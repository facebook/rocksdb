// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.util.TestUtil;

public class MultiGetTest {
  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void putNThenMultiGet() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1ForKey1".getBytes());
      db.put("key2".getBytes(), "value2ForKey2".getBytes());
      db.put("key3".getBytes(), "value3ForKey3".getBytes());
      final List<byte[]> keys =
          Arrays.asList("key1".getBytes(), "key2".getBytes(), "key3".getBytes());
      final List<byte[]> values = db.multiGetAsList(keys);
      assertThat(values.size()).isEqualTo(keys.size());
      assertThat(values.get(0)).isEqualTo("value1ForKey1".getBytes());
      assertThat(values.get(1)).isEqualTo("value2ForKey2".getBytes());
      assertThat(values.get(2)).isEqualTo("value3ForKey3".getBytes());
    }
  }

  @Test
  public void putNThenMultiGetDirect() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1ForKey1".getBytes());
      db.put("key2".getBytes(), "value2ForKey2".getBytes());
      db.put("key3".getBytes(), "value3ForKey3".getBytes());

      final List<ByteBuffer> keys = new ArrayList<>();
      keys.add(ByteBuffer.allocateDirect(12).put("key1".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key2".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
      // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
      for (final ByteBuffer key : keys) {
        key.flip();
      }
      final List<ByteBuffer> values = new ArrayList<>();
      for (int i = 0; i < keys.size(); i++) {
        values.add(ByteBuffer.allocateDirect(24));
      }

      {
        final List<ByteBufferGetStatus> results = db.multiGetByteBuffers(keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);
        assertThat(results.get(1).requiredSize).isEqualTo("value2ForKey2".getBytes().length);
        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value1ForKey1".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(1).value))
            .isEqualTo("value2ForKey2".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value3ForKey3".getBytes());
      }

      {
        final List<ByteBufferGetStatus> results =
            db.multiGetByteBuffers(new ReadOptions(), keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);
        assertThat(results.get(1).requiredSize).isEqualTo("value2ForKey2".getBytes().length);
        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value1ForKey1".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(1).value))
            .isEqualTo("value2ForKey2".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value3ForKey3".getBytes());
      }
    }
  }

  @Test
  public void putNThenMultiGetDirectSliced() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1ForKey1".getBytes());
      db.put("key2".getBytes(), "value2ForKey2".getBytes());
      db.put("key3".getBytes(), "value3ForKey3".getBytes());

      final List<ByteBuffer> keys = new ArrayList<>();
      keys.add(ByteBuffer.allocateDirect(12).put("key2".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
      keys.add(
          ByteBuffer.allocateDirect(12).put("prefix1".getBytes()).slice().put("key1".getBytes()));
      // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
      for (final ByteBuffer key : keys) {
        key.flip();
      }
      final List<ByteBuffer> values = new ArrayList<>();
      for (int i = 0; i < keys.size(); i++) {
        values.add(ByteBuffer.allocateDirect(24));
      }

      {
        final List<ByteBufferGetStatus> results = db.multiGetByteBuffers(keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(1).requiredSize).isEqualTo("value2ForKey2".getBytes().length);
        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);
        assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value2ForKey2".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(1).value))
            .isEqualTo("value3ForKey3".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value1ForKey1".getBytes());
      }
    }
  }

  @Test
  public void putNThenMultiGetDirectBadValuesArray() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1ForKey1".getBytes());
      db.put("key2".getBytes(), "value2ForKey2".getBytes());
      db.put("key3".getBytes(), "value3ForKey3".getBytes());

      final List<ByteBuffer> keys = new ArrayList<>();
      keys.add(ByteBuffer.allocateDirect(12).put("key1".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key2".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
      // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
      for (final ByteBuffer key : keys) {
        key.flip();
      }

      {
        final List<ByteBuffer> values = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
          values.add(ByteBuffer.allocateDirect(24));
        }

        values.remove(0);

        try {
          db.multiGetByteBuffers(keys, values);
          fail("Expected exception when not enough value ByteBuffers supplied");
        } catch (final IllegalArgumentException e) {
          assertThat(e.getMessage()).contains("For each key there must be a corresponding value");
        }
      }

      {
        final List<ByteBuffer> values = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
          values.add(ByteBuffer.allocateDirect(24));
        }

        values.add(ByteBuffer.allocateDirect(24));

        try {
          db.multiGetByteBuffers(keys, values);
          fail("Expected exception when too many value ByteBuffers supplied");
        } catch (final IllegalArgumentException e) {
          assertThat(e.getMessage()).contains("For each key there must be a corresponding value");
        }
      }
    }
  }

  @Test
  public void putNThenMultiGetDirectShortValueBuffers() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1ForKey1".getBytes());
      db.put("key2".getBytes(), "value2ForKey2".getBytes());
      db.put("key3".getBytes(), "value3ForKey3".getBytes());

      final List<ByteBuffer> keys = new ArrayList<>();
      keys.add(ByteBuffer.allocateDirect(12).put("key1".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key2".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
      // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
      for (final ByteBuffer key : keys) {
        key.flip();
      }

      {
        final List<ByteBuffer> values = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
          values.add(ByteBuffer.allocateDirect(4));
        }

        final List<ByteBufferGetStatus> statii = db.multiGetByteBuffers(keys, values);
        assertThat(statii.size()).isEqualTo(values.size());
        for (final ByteBufferGetStatus status : statii) {
          assertThat(status.status.getCode()).isEqualTo(Status.Code.Ok);
          assertThat(status.requiredSize).isEqualTo("value3ForKey3".getBytes().length);
          final ByteBuffer expected =
              ByteBuffer.allocateDirect(24).put(Arrays.copyOf("valueX".getBytes(), 4));
          expected.flip();
          assertThat(status.value).isEqualTo(expected);
        }
      }
    }
  }

  @Test
  public void putNThenMultiGetDirectNondefaultCF() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>(0);
      cfDescriptors.add(new ColumnFamilyDescriptor("cf0".getBytes()));
      cfDescriptors.add(new ColumnFamilyDescriptor("cf1".getBytes()));
      cfDescriptors.add(new ColumnFamilyDescriptor("cf2".getBytes()));

      final List<ColumnFamilyHandle> cf = db.createColumnFamilies(cfDescriptors);

      db.put(cf.get(0), "key1".getBytes(), "value1ForKey1".getBytes());
      db.put(cf.get(0), "key2".getBytes(), "value2ForKey2".getBytes());
      db.put(cf.get(0), "key3".getBytes(), "value3ForKey3".getBytes());

      final List<ByteBuffer> keys = new ArrayList<>();
      keys.add(ByteBuffer.allocateDirect(12).put("key1".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key2".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
      // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
      for (final ByteBuffer key : keys) {
        key.flip();
      }
      final List<ByteBuffer> values = new ArrayList<>();
      for (int i = 0; i < keys.size(); i++) {
        values.add(ByteBuffer.allocateDirect(24));
      }

      {
        final List<ByteBufferGetStatus> results = db.multiGetByteBuffers(keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.NotFound);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.NotFound);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.NotFound);
      }

      {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        columnFamilyHandles.add(cf.get(0));
        final List<ByteBufferGetStatus> results =
            db.multiGetByteBuffers(columnFamilyHandles, keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);
        assertThat(results.get(1).requiredSize).isEqualTo("value2ForKey2".getBytes().length);
        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value1ForKey1".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(1).value))
            .isEqualTo("value2ForKey2".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value3ForKey3".getBytes());
      }

      {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        columnFamilyHandles.add(cf.get(0));
        columnFamilyHandles.add(cf.get(0));
        columnFamilyHandles.add(cf.get(0));
        final List<ByteBufferGetStatus> results =
            db.multiGetByteBuffers(columnFamilyHandles, keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);
        assertThat(results.get(1).requiredSize).isEqualTo("value2ForKey2".getBytes().length);
        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value1ForKey1".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(1).value))
            .isEqualTo("value2ForKey2".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value3ForKey3".getBytes());
      }
    }
  }

  @Test
  public void putNThenMultiGetDirectCFParams() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1ForKey1".getBytes());
      db.put("key2".getBytes(), "value2ForKey2".getBytes());
      db.put("key3".getBytes(), "value3ForKey3".getBytes());

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      columnFamilyHandles.add(db.getDefaultColumnFamily());
      columnFamilyHandles.add(db.getDefaultColumnFamily());

      final List<ByteBuffer> keys = new ArrayList<>();
      keys.add(ByteBuffer.allocateDirect(12).put("key1".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key2".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
      // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
      for (final ByteBuffer key : keys) {
        key.flip();
      }
      final List<ByteBuffer> values = new ArrayList<>();
      for (int i = 0; i < keys.size(); i++) {
        values.add(ByteBuffer.allocateDirect(24));
      }
      try {
        db.multiGetByteBuffers(columnFamilyHandles, keys, values);
        fail("Expected exception when 2 column families supplied");
      } catch (final IllegalArgumentException e) {
        assertThat(e.getMessage()).contains("Wrong number of ColumnFamilyHandle(s) supplied");
      }

      columnFamilyHandles.clear();
      columnFamilyHandles.add(db.getDefaultColumnFamily());
      final List<ByteBufferGetStatus> results =
          db.multiGetByteBuffers(columnFamilyHandles, keys, values);

      assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
      assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
      assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

      assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);
      assertThat(results.get(1).requiredSize).isEqualTo("value2ForKey2".getBytes().length);
      assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

      assertThat(TestUtil.bufferBytes(results.get(0).value)).isEqualTo("value1ForKey1".getBytes());
      assertThat(TestUtil.bufferBytes(results.get(1).value)).isEqualTo("value2ForKey2".getBytes());
      assertThat(TestUtil.bufferBytes(results.get(2).value)).isEqualTo("value3ForKey3".getBytes());
    }
  }

  @Test
  public void putNThenMultiGetDirectMixedCF() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
      cfDescriptors.add(new ColumnFamilyDescriptor("cf0".getBytes()));
      cfDescriptors.add(new ColumnFamilyDescriptor("cf1".getBytes()));
      cfDescriptors.add(new ColumnFamilyDescriptor("cf2".getBytes()));
      cfDescriptors.add(new ColumnFamilyDescriptor("cf3".getBytes()));

      final List<ColumnFamilyHandle> cf = db.createColumnFamilies(cfDescriptors);

      db.put(cf.get(1), "key1".getBytes(), "value1ForKey1".getBytes());
      db.put("key2".getBytes(), "value2ForKey2".getBytes());
      db.put(cf.get(3), "key3".getBytes(), "value3ForKey3".getBytes());

      final List<ByteBuffer> keys = new ArrayList<>();
      keys.add(ByteBuffer.allocateDirect(12).put("key1".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key2".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
      // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
      for (final ByteBuffer key : keys) {
        key.flip();
      }
      final List<ByteBuffer> values = new ArrayList<>();
      for (int i = 0; i < keys.size(); i++) {
        values.add(ByteBuffer.allocateDirect(24));
      }

      {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        columnFamilyHandles.add(db.getDefaultColumnFamily());

        final List<ByteBufferGetStatus> results =
            db.multiGetByteBuffers(columnFamilyHandles, keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.NotFound);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.NotFound);

        assertThat(results.get(1).requiredSize).isEqualTo("value2ForKey2".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(1).value))
            .isEqualTo("value2ForKey2".getBytes());
      }

      {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        columnFamilyHandles.add(cf.get(1));

        final List<ByteBufferGetStatus> results =
            db.multiGetByteBuffers(columnFamilyHandles, keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.NotFound);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.NotFound);

        assertThat(results.get(0).requiredSize).isEqualTo("value2ForKey2".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value1ForKey1".getBytes());
      }

      {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        columnFamilyHandles.add(cf.get(1));
        columnFamilyHandles.add(db.getDefaultColumnFamily());
        columnFamilyHandles.add(cf.get(3));

        final List<ByteBufferGetStatus> results =
            db.multiGetByteBuffers(columnFamilyHandles, keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);
        assertThat(results.get(1).requiredSize).isEqualTo("value2ForKey2".getBytes().length);
        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value1ForKey1".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(1).value))
            .isEqualTo("value2ForKey2".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value3ForKey3".getBytes());
      }

      {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        columnFamilyHandles.add(db.getDefaultColumnFamily());
        columnFamilyHandles.add(cf.get(1));
        columnFamilyHandles.add(cf.get(3));

        final List<ByteBufferGetStatus> results =
            db.multiGetByteBuffers(columnFamilyHandles, keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.NotFound);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.NotFound);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value3ForKey3".getBytes());
      }
    }
  }

  @Test
  public void putNThenMultiGetDirectTruncateCF() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
      cfDescriptors.add(new ColumnFamilyDescriptor("cf0".getBytes()));

      final List<ColumnFamilyHandle> cf = db.createColumnFamilies(cfDescriptors);

      db.put(cf.get(0), "key1".getBytes(), "value1ForKey1".getBytes());
      db.put(cf.get(0), "key2".getBytes(), "value2ForKey2WithLotsOfTrailingGarbage".getBytes());
      db.put(cf.get(0), "key3".getBytes(), "value3ForKey3".getBytes());

      final List<ByteBuffer> keys = new ArrayList<>();
      keys.add(ByteBuffer.allocateDirect(12).put("key1".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key2".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
      // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
      for (final ByteBuffer key : keys) {
        key.flip();
      }
      final List<ByteBuffer> values = new ArrayList<>();
      for (int i = 0; i < keys.size(); i++) {
        values.add(ByteBuffer.allocateDirect(24));
      }

      {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        columnFamilyHandles.add(cf.get(0));
        final List<ByteBufferGetStatus> results =
            db.multiGetByteBuffers(columnFamilyHandles, keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);
        assertThat(results.get(1).requiredSize)
            .isEqualTo("value2ForKey2WithLotsOfTrailingGarbage".getBytes().length);
        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value1ForKey1".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(1).value))
            .isEqualTo("valu e2Fo rKey 2Wit hLot sOfT".replace(" ", "").getBytes());
        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value3ForKey3".getBytes());
      }
    }
  }
}
