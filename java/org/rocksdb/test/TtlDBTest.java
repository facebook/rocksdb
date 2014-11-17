// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.TtlDB;

import static org.assertj.core.api.Assertions.assertThat;

public class TtlDBTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void ttlDBOpen() throws RocksDBException, InterruptedException {
    Options options = null;
    TtlDB ttlDB = null;
    try {
      options = new Options().setCreateIfMissing(true);
      ttlDB = TtlDB.open(options, dbFolder.getRoot().getAbsolutePath(),
          1, false);
      ttlDB.put("key".getBytes(), "value".getBytes());
      assertThat(ttlDB.get("key".getBytes())).
          isEqualTo("value".getBytes());
      Thread.sleep(1250);
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
}
