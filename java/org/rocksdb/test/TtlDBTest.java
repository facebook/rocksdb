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

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TtlDBTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void ttlDBOpen() throws RocksDBException,
      InterruptedException {
    Options options = null;
    TtlDB ttlDB = null;
    try {
      options = new Options().
          setCreateIfMissing(true).
          setMaxGrandparentOverlapFactor(0).
          setMaxMemCompactionLevel(0);
      ttlDB = TtlDB.open(options,
          dbFolder.getRoot().getAbsolutePath());
      ttlDB.put("key".getBytes(), "value".getBytes());
      assertThat(ttlDB.get("key".getBytes())).
          isEqualTo("value".getBytes());
      assertThat(ttlDB.get("key".getBytes())).isNotNull();
    } finally {
      if (ttlDB != null) {
        ttlDB.close();
      }
      if (options != null) {
        options.dispose();
      }
    }
  }

  @Test
  public void ttlDBOpenWithTtl() throws RocksDBException,
      InterruptedException {
    Options options = null;
    TtlDB ttlDB = null;
    try {
      options = new Options().
          setCreateIfMissing(true).
          setMaxGrandparentOverlapFactor(0).
          setMaxMemCompactionLevel(0);
      ttlDB = TtlDB.open(options, dbFolder.getRoot().getAbsolutePath(),
          1, false);
      ttlDB.put("key".getBytes(), "value".getBytes());
      assertThat(ttlDB.get("key".getBytes())).
          isEqualTo("value".getBytes());
      TimeUnit.SECONDS.sleep(2);

      ttlDB.compactRange();
      assertThat(ttlDB.get("key".getBytes())).isNull();
    } finally {
      if (ttlDB != null) {
        ttlDB.close();
      }
      if (options != null) {
        options.dispose();
      }
    }
  }

  @Test
  public void createTtlColumnFamily() throws RocksDBException,
      InterruptedException {
    Options options = null;
    TtlDB ttlDB = null;
    ColumnFamilyHandle columnFamilyHandle = null;
    try {
      options = new Options().setCreateIfMissing(true);
      ttlDB = TtlDB.open(options,
          dbFolder.getRoot().getAbsolutePath());
      columnFamilyHandle = ttlDB.createColumnFamilyWithTtl(
          new ColumnFamilyDescriptor("new_cf"), 1);
      ttlDB.put(columnFamilyHandle, "key".getBytes(),
          "value".getBytes());
      assertThat(ttlDB.get(columnFamilyHandle, "key".getBytes())).
          isEqualTo("value".getBytes());
      Thread.sleep(2500);
      ttlDB.compactRange(columnFamilyHandle);
      assertThat(ttlDB.get(columnFamilyHandle, "key".getBytes())).isNull();
    } finally {
      if (columnFamilyHandle != null) {
        columnFamilyHandle.dispose();
      }
      if (ttlDB != null) {
        ttlDB.close();
      }
      if (options != null) {
        options.dispose();
      }
    }
  }
}
