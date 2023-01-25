// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.rocksdb.util.FFIUtil.usingFFI;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.MemorySession;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.util.FFIUtil;

public class FFIDBBulkTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  static final int repeatCount = 1000;
  static final int keyCount = 10000;
  static final int keySize = 128;
  static final int valueSize = 32768;

  @Before
  public void setup() throws RocksDBException {
    final FFIUtil.ByteArray keysArray = new FFIUtil.ByteArray(keySize, "key");
    final FFIUtil.ByteArray valuesArray = new FFIUtil.ByteArray(valueSize, "value");

    usingFFI(dbFolder, dbFFI -> {
      try {
        final RocksDB db = dbFFI.getRocksDB();
        for (final ColumnFamilyHandle columnFamilyHandle : dbFFI.getColumnFamilies()) {
          for (int j = 0; j < keyCount; j++) {
            db.put(columnFamilyHandle, keysArray.get(j), valuesArray.get(j));
          }
        }
        try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
          db.flush(flushOptions);
        }
      } catch (final RocksDBException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
  }

  @Test
  public void getBulk() throws RocksDBException {
    final int SIGNIFICANT_KEY_BYTES = 12;
    final FFIUtil.ByteArray keysArray = new FFIUtil.ByteArray(SIGNIFICANT_KEY_BYTES, "key");
    final FFIUtil.ByteArray valuesArray = new FFIUtil.ByteArray(valueSize, "value");

    final byte[] value = new byte[valueSize];

    usingFFI(dbFolder, dbFFI -> {
      try (final FFIDB.GetParams getParams = FFIDB.GetParams.create(dbFFI)) {
        final MemorySegment keySegment = dbFFI.allocateSegment(keySize);
        keySegment.fill((byte) 0x30);

        for (int repeat = 0; repeat < repeatCount; repeat++) {
          for (final ColumnFamilyHandle columnFamilyHandle : dbFFI.getColumnFamilies()) {
            for (int keyIndex = keyCount - 1; keyIndex >= 0; keyIndex--) {
              final byte[] key = keysArray.get(keyIndex);
              keySegment.copyFrom(MemorySegment.ofArray(key));
              dbFFI.get(columnFamilyHandle, keySegment, getParams, value);
              assertThat(value[5]).isEqualTo(valuesArray.get(keyIndex)[5]);
              assertThat(value[6]).isEqualTo(valuesArray.get(keyIndex)[6]);
            }
          }
        }
      } catch (final RocksDBException | IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
  }
}
