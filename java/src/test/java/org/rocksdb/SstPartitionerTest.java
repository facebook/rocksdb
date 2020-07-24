// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SstPartitionerTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void sstFixedPrefix() throws InterruptedException, RocksDBException {
    try (SstPartitionerFixedPrefixFactory factory = new SstPartitionerFixedPrefixFactory(4);
         final Options opt =
             new Options().setCreateIfMissing(true).setSstPartitionerFactory(factory);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      // writing (long)100 under key
      db.put("aaaa1".getBytes(), "A".getBytes());
      db.put("bbbb1".getBytes(), "B".getBytes());
      db.flush(new FlushOptions());

      db.put("aaaa1".getBytes(), "A2".getBytes());
      db.flush(new FlushOptions());

      db.compactRange();

      List<LiveFileMetaData> metadata = db.getLiveFilesMetaData();
      assertThat(metadata.size()).isEqualTo(2);
    }
  }
}
