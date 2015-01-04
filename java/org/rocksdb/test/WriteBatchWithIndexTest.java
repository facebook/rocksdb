//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

package org.rocksdb.test;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.WriteBatchWithIndex;
import org.rocksdb.DirectSlice;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.rocksdb.WBWIRocksIterator;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

import static org.assertj.core.api.Assertions.assertThat;


public class WriteBatchWithIndexTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void readYourOwnWrites() throws RocksDBException {
    RocksDB db = null;
    Options options = null;
    try {
      options = new Options();
      // Setup options
      options.setCreateIfMissing(true);
      db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());

      final byte[] k1 = "key1".getBytes();
      final byte[] v1 = "value1".getBytes();
      final byte[] k2 = "key2".getBytes();
      final byte[] v2 = "value2".getBytes();

      db.put(k1, v1);
      db.put(k2, v2);

      final WriteBatchWithIndex wbwi = new WriteBatchWithIndex(true);

      RocksIterator base = null;
      RocksIterator it = null;
      try {
        base = db.newIterator();
        it = wbwi.newIteratorWithBase(base);

        it.seek(k1);
        assertThat(it.isValid()).isTrue();
        assertThat(it.key()).isEqualTo(k1);
        assertThat(it.value()).isEqualTo(v1);

        it.seek(k2);
        assertThat(it.isValid()).isTrue();
        assertThat(it.key()).isEqualTo(k2);
        assertThat(it.value()).isEqualTo(v2);

        //put data to the write batch and make sure we can read it.
        final byte[] k3 = "key3".getBytes();
        final byte[] v3 = "value3".getBytes();
        wbwi.put(k3, v3);
        it.seek(k3);
        assertThat(it.isValid()).isTrue();
        assertThat(it.key()).isEqualTo(k3);
        assertThat(it.value()).isEqualTo(v3);

        //update k2 in the write batch and check the value
        final byte[] v2Other = "otherValue2".getBytes();
        wbwi.put(k2, v2Other);
        it.seek(k2);
        assertThat(it.isValid()).isTrue();
        assertThat(it.key()).isEqualTo(k2);
        assertThat(it.value()).isEqualTo(v2Other);

        //remove k1 and make sure we can read back the write
        wbwi.remove(k1);
        it.seek(k1);
        assertThat(it.key()).isNotEqualTo(k1);

        //reinsert k1 and make sure we see the new value
        final byte[] v1Other = "otherValue1".getBytes();
        wbwi.put(k1, v1Other);
        it.seek(k1);
        assertThat(it.isValid()).isTrue();
        assertThat(it.key()).isEqualTo(k1);
        assertThat(it.value()).isEqualTo(v1Other);
      } finally {
        if (it != null) {
          it.dispose();
        }
        if (base != null) {
          base.dispose();
        }
      }

    } finally {
      if (db != null) {
        db.close();
      }
      if (options != null) {
        options.dispose();
      }
    }
  }

  @Test
  public void write_writeBatchWithIndex() throws RocksDBException {
    RocksDB db = null;
    Options options = null;
    try {
      options = new Options();
      // Setup options
      options.setCreateIfMissing(true);
      db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());

      final byte[] k1 = "key1".getBytes();
      final byte[] v1 = "value1".getBytes();
      final byte[] k2 = "key2".getBytes();
      final byte[] v2 = "value2".getBytes();

      WriteBatchWithIndex wbwi = null;

      try {
        wbwi = new WriteBatchWithIndex();


        wbwi.put(k1, v1);
        wbwi.put(k2, v2);

        db.write(new WriteOptions(), wbwi);
      } finally {
        if(wbwi != null) {
          wbwi.dispose();
        }
      }

      assertThat(db.get(k1)).isEqualTo(v1);
      assertThat(db.get(k2)).isEqualTo(v2);

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

