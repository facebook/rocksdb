// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.util.BytewiseComparator;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

public class SstFileWriterTest {
  private static final String SST_FILE_NAME = "test.sst";
  private static final String DB_DIRECTORY_NAME = "test_db";

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource = new RocksMemoryResource();

  @Rule public TemporaryFolder parentFolder = new TemporaryFolder();

  private File newSstFile(final TreeMap<String, String> keyValues)
      throws IOException, RocksDBException {
    final EnvOptions envOptions = new EnvOptions();
    final ComparatorOptions comparatorOptions = new ComparatorOptions();
    final BytewiseComparator comparator = new BytewiseComparator(comparatorOptions);
    final Options options = new Options().setComparator(comparator);
    final SstFileWriter sstFileWriter = new SstFileWriter(envOptions, options, comparator);
    final File sstFile = parentFolder.newFile(SST_FILE_NAME);
    try {
      sstFileWriter.open(sstFile.getAbsolutePath());
      for (Map.Entry<String, String> keyValue : keyValues.entrySet()) {
        Slice keySlice = new Slice(keyValue.getKey());
        Slice valueSlice = new Slice(keyValue.getValue());
        sstFileWriter.add(keySlice, valueSlice);
        keySlice.close();
        valueSlice.close();
      }
      sstFileWriter.finish();
    } finally {
      assertThat(sstFileWriter).isNotNull();
      sstFileWriter.close();
      options.close();
      envOptions.close();
      comparatorOptions.close();
      comparator.close();
    }
    return sstFile;
  }

  @Test
  public void generateSstFile() throws RocksDBException, IOException {
    final TreeMap<String, String> keyValues = new TreeMap<>();
    keyValues.put("key1", "value1");
    keyValues.put("key2", "value2");
    newSstFile(keyValues);
  }

  @Test
  public void ingestSstFile() throws RocksDBException, IOException {
    final TreeMap<String, String> keyValues = new TreeMap<>();
    keyValues.put("key1", "value1");
    keyValues.put("key2", "value2");
    final File sstFile = newSstFile(keyValues);
    final File dbFolder = parentFolder.newFolder(DB_DIRECTORY_NAME);
    final Options options = new Options().setCreateIfMissing(true);
    final RocksDB db = RocksDB.open(options, dbFolder.getAbsolutePath());
    db.addFileWithFilePath(sstFile.getAbsolutePath());

    assertThat(db.get("key1".getBytes())).isEqualTo("value1".getBytes());
    assertThat(db.get("key2".getBytes())).isEqualTo("value2".getBytes());

    options.close();
    db.close();
  }
}
