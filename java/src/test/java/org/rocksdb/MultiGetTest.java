// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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
}
