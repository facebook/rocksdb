// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MultiGetManyKeysTest {
  @Parameterized.Parameters
  public static List<Integer> data() {
    return Arrays.asList(3, 250, 60000, 70000, 150000, 750000);
  }

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  private final int keySize;

  public MultiGetManyKeysTest(final Integer keySize) {
    this.keySize = keySize;
  }

  /**
   * Test for https://github.com/facebook/rocksdb/issues/8039
   */
  @Test
  public void multiGetAsListLarge() throws RocksDBException {
    final Random rand = new Random();
    final List<byte[]> keys = new ArrayList<>();
    for (int i = 0; i < keySize; i++) {
      final byte[] key = new byte[4];
      rand.nextBytes(key);
      keys.add(key);
    }

    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      final List<byte[]> values = db.multiGetAsList(keys);
      assertThat(values.size()).isEqualTo(keys.size());
    }
  }

  @Test
  public void multiGetAsListCheckResults() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      final List<byte[]> keys = new ArrayList<>();
      for (int i = 0; i < keySize; i++) {
        byte[] key = ("key" + i + ":").getBytes();
        keys.add(key);
        db.put(key, ("value" + i + ":").getBytes());
      }

      final List<byte[]> values = db.multiGetAsList(keys);
      assertThat(values.size()).isEqualTo(keys.size());
      for (int i = 0; i < keySize; i++) {
        assertThat(values.get(i)).isEqualTo(("value" + i + ":").getBytes());
      }
    }
  }
}
