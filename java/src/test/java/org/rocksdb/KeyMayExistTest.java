// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
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
        db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));
        // Test without column family
        KeyMayExistResult exists = db.keyMayExist("key".getBytes(UTF_8), true);
        assertThat(exists).isNotNull();
        assertThat(exists.value).isNotNull();
        assertThat(new String(exists.value, UTF_8)).isEqualTo("value");

        exists = db.keyMayExist("key".getBytes(UTF_8), false);
        assertThat(exists).isSameAs(KeyMayExistResult.VALUE_NOT_SET);

        // Slice key
        final StringBuilder builder = new StringBuilder("prefix");
        final int offset = builder.toString().length();
        builder.append("slice key 0");
        final int len = builder.toString().length() - offset;
        builder.append("suffix");

        final byte[] sliceKey = builder.toString().getBytes(UTF_8);
        final byte[] sliceValue = "slice value 0".getBytes(UTF_8);
        db.put(sliceKey, offset, len, sliceValue, 0, sliceValue.length);

        exists = db.keyMayExist(sliceKey, offset, len, true);
        assertThat(exists).isNotNull();
        assertThat(exists.value).isNotNull();
        assertThat(exists.value).isEqualTo(sliceValue);

        exists = db.keyMayExist(sliceKey, offset, len, false);
        assertThat(exists).isSameAs(KeyMayExistResult.VALUE_NOT_SET);

        // Test without column family but with readOptions
        try (final ReadOptions readOptions = new ReadOptions()) {
          exists = db.keyMayExist(readOptions, "key".getBytes(UTF_8), true);
          assertThat(exists).isNotNull();
          assertThat(exists.value).isNotNull();
          assertThat(new String(exists.value, UTF_8)).isEqualTo("value");

          exists = db.keyMayExist(readOptions, "key".getBytes(UTF_8), false);
          assertThat(exists).isSameAs(KeyMayExistResult.VALUE_NOT_SET);

          exists = db.keyMayExist(readOptions, sliceKey, offset, len, true);
          assertThat(exists).isNotNull();
          assertThat(exists.value).isNotNull();
          assertThat(exists.value).isEqualTo(sliceValue);

          exists = db.keyMayExist(readOptions, sliceKey, offset, len, false);
          assertThat(exists).isSameAs(KeyMayExistResult.VALUE_NOT_SET);
        }

        // Test with column family
        exists = db.keyMayExist(columnFamilyHandleList.get(0), "key".getBytes(UTF_8),
            true);
        assertThat(exists).isNotNull();
        assertThat(exists.value).isNotNull();
        assertThat(new String(exists.value, UTF_8)).isEqualTo("value");

        exists = db.keyMayExist(columnFamilyHandleList.get(0), "key".getBytes(UTF_8),
            false);
        assertThat(exists).isSameAs(KeyMayExistResult.VALUE_NOT_SET);

        // Test slice sky with column family
        exists = db.keyMayExist(columnFamilyHandleList.get(0), sliceKey, offset, len,
            true);
        assertThat(exists).isNotNull();
        assertThat(exists.value).isNotNull();
        assertThat(exists.value).isEqualTo(sliceValue);

        exists = db.keyMayExist(columnFamilyHandleList.get(0), sliceKey, offset, len,
            false);
        assertThat(exists).isSameAs(KeyMayExistResult.VALUE_NOT_SET);

        // Test with column family and readOptions
        try (final ReadOptions readOptions = new ReadOptions()) {
          exists = db.keyMayExist(columnFamilyHandleList.get(0), readOptions,
              "key".getBytes(UTF_8), true);
          assertThat(exists).isNotNull();
          assertThat(exists.value).isNotNull();
          assertThat(new String(exists.value, UTF_8)).isEqualTo("value");

          exists = db.keyMayExist(columnFamilyHandleList.get(0), readOptions,
              "key".getBytes(UTF_8), false);
          assertThat(exists).isSameAs(KeyMayExistResult.VALUE_NOT_SET);

          // Test slice key with column family and read options
          exists = db.keyMayExist(columnFamilyHandleList.get(0), readOptions,
              sliceKey, offset, len, true);
          assertThat(exists).isNotNull();
          assertThat(exists.value).isNotNull();
          assertThat(exists.value).isEqualTo(sliceValue);

          exists = db.keyMayExist(columnFamilyHandleList.get(0), readOptions,
              sliceKey, offset, len, false);
          assertThat(exists).isSameAs(KeyMayExistResult.VALUE_NOT_SET);
        }

        // KeyMayExist in CF1 must return null value
        exists = db.keyMayExist(columnFamilyHandleList.get(1),
            "key".getBytes(UTF_8), true);
        assertThat(exists).isNull();
        exists = db.keyMayExist(columnFamilyHandleList.get(1),
            "key".getBytes(UTF_8), false);
        assertThat(exists).isNull();

        // slice key
        exists = db.keyMayExist(columnFamilyHandleList.get(1),
            sliceKey, 1, 3, true);
        assertThat(exists).isNull();
        exists = db.keyMayExist(columnFamilyHandleList.get(1),
            sliceKey, 1, 3, false);
        assertThat(exists).isNull();
      } finally {
        for (final ColumnFamilyHandle columnFamilyHandle :
            columnFamilyHandleList) {
          columnFamilyHandle.close();
        }
      }
    }
  }

  @Test
  public void keyMayExistNonUnicodeString() throws RocksDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {
      final byte key[] = "key".getBytes(UTF_8);
      final byte value[] = { (byte)0x80 };  // invalid unicode code-point
      db.put(key, value);

      final byte buf[] = new byte[10];
      final int read = db.get(key, buf);
      assertThat(read).isEqualTo(1);
      assertThat(buf).startsWith(value);

      KeyMayExistResult exists = db.keyMayExist("key".getBytes(UTF_8), true);
      assertThat(exists).isNotNull();
      assertThat(exists.value).isNotNull();
      assertThat(exists.value).isEqualTo(value);

      exists = db.keyMayExist("key".getBytes(UTF_8), false);
      assertThat(exists).isSameAs(KeyMayExistResult.VALUE_NOT_SET);
    }
  }
}
