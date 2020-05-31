// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ExportImportColumnFamilyTest {
  static {
    RocksDB.loadLibrary();
  }

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Rule public TemporaryFolder exportFolder = new TemporaryFolder();

  @Rule public TemporaryFolder sstFolder = new TemporaryFolder();

  public static final Random rand = PlatformRandomHelper.getPlatformSpecificRandomFactory();

  @Test(expected = RocksDBException.class)
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

  @Test
  public void importColumnFamily() throws RocksDBException, UnsupportedEncodingException {
    try (final EnvOptions envOptions = new EnvOptions(); Options options = new Options();
         final SstFileWriter sstFileWriter = new SstFileWriter(envOptions, options)) {
      final String sstName = "test.sst";
      final String sstPath = sstFolder.getRoot().getAbsolutePath() + "/" + sstName;
      sstFileWriter.open(sstPath);

      assertThat(sstFileWriter.fileSize()).isEqualTo(0);

      for (int i = 1; i <= 2; i++) {
        sstFileWriter.put(("key" + i).getBytes(), ("value" + i).getBytes());
      }
      sstFileWriter.finish();

      List<LiveFileMetaData> files = new ArrayList<>();
      files.add(new LiveFileMetaData("sourceColumnFamilyName".getBytes("UTF-8"), 0, sstName,
          sstFolder.getRoot().getAbsolutePath(), 0, 10, 19, "sourceSmallestKey".getBytes("UTF-8"),
          "sourcelargestKey".getBytes("UTF-8"), 0, false, 0, 0));

      try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
           final ExportImportFilesMetaData metadata =
               new ExportImportFilesMetaData("comparatorName", files);
           final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
           final ImportColumnFamilyOptions importOpts = new ImportColumnFamilyOptions();
           final ColumnFamilyHandle cfHandle =
               db.createColumnFamilyWithImport(cfOpts, "toto", importOpts, metadata)) {
        assertThat(cfHandle).isNotNull();
        assertThat(db.get(cfHandle, "key1".getBytes())).isEqualTo("value1".getBytes());
        assertThat(db.get(cfHandle, "key2".getBytes())).isEqualTo("value2".getBytes());
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void exportImportColumnFamily() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath() + "/db1")) {
      db.put("key".getBytes(), "value".getBytes());

      try (
          final Checkpoint checkpoint = Checkpoint.create(db);
          final ExportImportFilesMetaData metadata = checkpoint.exportColumnFamily(
              db.getDefaultColumnFamily(), exportFolder.getRoot().getAbsolutePath() + "/export1")) {
        assertThat(metadata).isNotNull();

        try (final RocksDB dbNew =
                 RocksDB.open(options, dbFolder.getRoot().getAbsolutePath() + "/db2");
             final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
             final ImportColumnFamilyOptions importOpts = new ImportColumnFamilyOptions();
             final ColumnFamilyHandle cfHandle =
                 dbNew.createColumnFamilyWithImport(cfOpts, "toto", importOpts, metadata)) {
          assertThat(cfHandle).isNotNull();
          assertThat(db.get(cfHandle, "key".getBytes())).isEqualTo("value".getBytes());
        }
      }
    }
  }
}
