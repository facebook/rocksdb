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
    RocksDB db = null;
    Options options = null;
    Checkpoint checkpoint = null;
    ExportImportFilesMetaData metadata = null;
    try {
      options = new Options().setCreateIfMissing(true);
      db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
      db.put("key".getBytes(), "value".getBytes());
      checkpoint = Checkpoint.create(db);
      metadata = checkpoint.exportColumnFamily(
          db.getDefaultColumnFamily(), exportFolder.getRoot().getAbsolutePath() + "/export1");
      assertThat(metadata).isNotNull();
      assertThat(metadata.files().size()).isEqualTo(1);
    } finally {
      if (db != null) {
        db.close();
      }

      if (options != null) {
        options.close();
      }

      if (checkpoint != null) {
        checkpoint.close();
      }

      if (metadata != null) {
        metadata.close();
      }
    }
  }

  @Test
  public void importColumnFamily() throws RocksDBException, UnsupportedEncodingException {
    EnvOptions envOptions = null;
    Options options = null;
    SstFileWriter sstFileWriter = null;
    ExportImportFilesMetaData metadata = null;
    RocksDB db = null;
    ColumnFamilyHandle cfHandle = null;
    ColumnFamilyOptions cfOpts = null;
    ImportColumnFamilyOptions importOpts = null;
    try {
      envOptions = new EnvOptions();
      options = new Options();
      sstFileWriter = new SstFileWriter(envOptions, options);

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

      db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
      metadata = new ExportImportFilesMetaData("comparatorName", files);
      cfOpts = new ColumnFamilyOptions();
      importOpts = new ImportColumnFamilyOptions();
      cfHandle = db.createColumnFamilyWithImport(cfOpts, "toto", importOpts, metadata);

      assertThat(cfHandle).isNotNull();
      assertThat(db.get(cfHandle, "key1".getBytes())).isEqualTo("value1".getBytes());
      assertThat(db.get(cfHandle, "key2".getBytes())).isEqualTo("value2".getBytes());
    } finally {
      if (envOptions != null) {
        envOptions.close();
      }

      if (options != null) {
        options.close();
      }

      if (sstFileWriter != null) {
        sstFileWriter.close();
      }

      if (metadata != null) {
        metadata.close();
      }

      if (db != null) {
        db.close();
      }

      if (cfHandle != null) {
        cfHandle.close();
      }

      if (cfOpts != null) {
        cfOpts.close();
      }

      if (importOpts != null) {
        importOpts.close();
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void exportImportColumnFamily() throws RocksDBException {
    RocksDB db = null;
    Options options = null;
    Checkpoint checkpoint = null;
    ExportImportFilesMetaData metadata = null;
    try {
      options = new Options().setCreateIfMissing(true);
      db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath() + "/db1");
      db.put("key".getBytes(), "value".getBytes());
      checkpoint = Checkpoint.create(db);
      metadata = checkpoint.exportColumnFamily(
          db.getDefaultColumnFamily(), exportFolder.getRoot().getAbsolutePath() + "/export1");
    } finally {
      if (db != null) {
        db.close();
      }

      if (checkpoint != null) {
        checkpoint.close();
      }
    }

    assertThat(options).isNotNull();
    assertThat(metadata).isNotNull();

    ColumnFamilyHandle cfHandle = null;
    ColumnFamilyOptions cfOpts = null;
    ImportColumnFamilyOptions importOpts = null;
    try {
      db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath() + "/db2");
      cfOpts = new ColumnFamilyOptions();
      importOpts = new ImportColumnFamilyOptions();
      cfHandle = db.createColumnFamilyWithImport(cfOpts, "toto", importOpts, metadata);
      assertThat(cfHandle).isNotNull();
      assertThat(db.get(cfHandle, "key".getBytes())).isEqualTo("value".getBytes());
    } finally {
      if (db != null) {
        db.close();
      }

      if (options != null) {
        options.close();
      }

      if (metadata != null) {
        metadata.close();
      }

      if (cfHandle != null) {
        cfHandle.close();
      }

      if (cfOpts != null) {
        cfOpts.close();
      }

      if (importOpts != null) {
        importOpts.close();
      }
    }
  }
}
