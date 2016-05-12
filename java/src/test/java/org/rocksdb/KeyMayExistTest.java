// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
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
    final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes())
    );

    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath(),
             cfDescriptors, columnFamilyHandleList)) {
      try {
        assertThat(columnFamilyHandleList.size()).
            isEqualTo(2);
        db.put("key".getBytes(), "value".getBytes());
        // Test without column family
        StringBuffer retValue = new StringBuffer();
        boolean exists = db.keyMayExist("key".getBytes(), retValue);
        assertThat(exists).isTrue();
        assertThat(retValue.toString()).isEqualTo("value");

        // Test without column family but with readOptions
        try (final ReadOptions readOptions = new ReadOptions()) {
          retValue = new StringBuffer();
          exists = db.keyMayExist(readOptions, "key".getBytes(), retValue);
          assertThat(exists).isTrue();
          assertThat(retValue.toString()).isEqualTo("value");
        }

        // Test with column family
        retValue = new StringBuffer();
        exists = db.keyMayExist(columnFamilyHandleList.get(0), "key".getBytes(),
            retValue);
        assertThat(exists).isTrue();
        assertThat(retValue.toString()).isEqualTo("value");

        // Test with column family and readOptions
        try (final ReadOptions readOptions = new ReadOptions()) {
          retValue = new StringBuffer();
          exists = db.keyMayExist(readOptions,
              columnFamilyHandleList.get(0), "key".getBytes(),
              retValue);
          assertThat(exists).isTrue();
          assertThat(retValue.toString()).isEqualTo("value");
        }

        // KeyMayExist in CF1 must return false
        assertThat(db.keyMayExist(columnFamilyHandleList.get(1),
            "key".getBytes(), retValue)).isFalse();
      } finally {
        for (final ColumnFamilyHandle columnFamilyHandle :
            columnFamilyHandleList) {
          columnFamilyHandle.close();
        }
      }
    }
  }
}
