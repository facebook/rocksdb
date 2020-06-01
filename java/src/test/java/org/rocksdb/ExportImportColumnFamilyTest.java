// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ExportImportColumnFamilyTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Rule public TemporaryFolder exportFolder = new TemporaryFolder();

  @Test
  public void exportColumnFamily() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key".getBytes(), "value".getBytes());

      try (
          final Checkpoint checkpoint = Checkpoint.create(db);
          final ExportImportFilesMetaData metadata = checkpoint.exportColumnFamily(
              db.getDefaultColumnFamily(), exportFolder.getRoot().getAbsolutePath() + "/export1")) {
        assertThat(metadata).isNotNull();
        assertThat(metadata.files().size()).isEqualTo(1);
      }
    }
  }
}
