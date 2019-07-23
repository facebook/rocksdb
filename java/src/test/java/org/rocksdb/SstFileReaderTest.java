// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.util.BytewiseComparator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SstFileReaderTest {
  private static final String SST_FILE_NAME = "test.sst";

  class KeyValueWithOp {
    KeyValueWithOp(String key, String value, OpType opType) {
      this.key = key;
      this.value = value;
      this.opType = opType;
    }

    String getKey() {
      return key;
    }

    String getValue() {
      return value;
    }

    OpType getOpType() {
      return opType;
    }

    private String key;
    private String value;
    private OpType opType;
  }

  @Rule public TemporaryFolder parentFolder = new TemporaryFolder();

  enum OpType { PUT, PUT_BYTES, MERGE, MERGE_BYTES, DELETE, DELETE_BYTES}

  private File newSstFile(final List<KeyValueWithOp> keyValues) throws IOException, RocksDBException {
    final EnvOptions envOptions = new EnvOptions();
    final StringAppendOperator stringAppendOperator = new StringAppendOperator();
    final Options options = new Options().setMergeOperator(stringAppendOperator);
    SstFileWriter sstFileWriter;
    sstFileWriter = new SstFileWriter(envOptions, options);

    final File sstFile = parentFolder.newFile(SST_FILE_NAME);
    try {
      sstFileWriter.open(sstFile.getAbsolutePath());
      for (KeyValueWithOp keyValue : keyValues) {
        Slice keySlice = new Slice(keyValue.getKey());
        Slice valueSlice = new Slice(keyValue.getValue());
        byte[] keyBytes = keyValue.getKey().getBytes();
        byte[] valueBytes = keyValue.getValue().getBytes();
        switch (keyValue.getOpType()) {
          case PUT:
            sstFileWriter.put(keySlice, valueSlice);
            break;
          case PUT_BYTES:
            sstFileWriter.put(keyBytes, valueBytes);
            break;
          case MERGE:
            sstFileWriter.merge(keySlice, valueSlice);
            break;
          case MERGE_BYTES:
            sstFileWriter.merge(keyBytes, valueBytes);
            break;
          case DELETE:
            sstFileWriter.delete(keySlice);
            break;
          case DELETE_BYTES:
            sstFileWriter.delete(keyBytes);
            break;
          default:
            fail("Unsupported op type");
        }
        keySlice.close();
        valueSlice.close();
      }
      sstFileWriter.finish();
    } finally {
      assertThat(sstFileWriter).isNotNull();
      sstFileWriter.close();
      options.close();
      envOptions.close();
    }
    return sstFile;
  }

  @Test
  public void readSstFile() throws RocksDBException, IOException {
    final List<KeyValueWithOp> keyValues = new ArrayList<>();
    keyValues.add(new KeyValueWithOp("key1", "value1", OpType.PUT));


    final File sstFile = newSstFile(keyValues);
    try(final StringAppendOperator stringAppendOperator =
            new StringAppendOperator();
        final Options options = new Options()
            .setCreateIfMissing(true)
            .setMergeOperator(stringAppendOperator);
        final SstFileReader reader = new SstFileReader(options)
    ) {
      // Open the sst file and iterator
      reader.open(sstFile.getAbsolutePath());
      final ReadOptions readOptions = new ReadOptions();
      final SstFileReaderIterator iterator = reader.newIterator(readOptions);

      // Use the iterator to read sst file
      iterator.seekToFirst();

      // Verify Checksum
      reader.verifyChecksum();

      // Verify Table Properties
      assertEquals(reader.getTableProperties().getNumEntries(), 1);

      // Check key and value
      assertThat(iterator.key()).isEqualTo("key1".getBytes());
      assertThat(iterator.value()).isEqualTo("value1".getBytes());
    }
  }

}
