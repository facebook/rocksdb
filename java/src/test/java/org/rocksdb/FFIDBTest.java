// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.rocksdb.util.FFIUtil.usingFFI;

import java.lang.foreign.ValueLayout;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FFIDBTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void getPinnableSlice() throws RocksDBException {
    usingFFI(dbFolder, dbFFI -> {
      try {
        final RocksDB db = dbFFI.getRocksDB();
        db.put(db.getDefaultColumnFamily(), "key1".getBytes(), "value1".getBytes());
        var getPinnableSlice = dbFFI.getPinnableSlice("key2".getBytes());
        assertThat(getPinnableSlice.code()).isEqualTo(Status.Code.NotFound);
        getPinnableSlice = dbFFI.getPinnableSlice("key1".getBytes());
        assertThat(getPinnableSlice.code()).isEqualTo(Status.Code.Ok);
        assertThat(getPinnableSlice.pinnableSlice().get().data()).isNotNull();
        final byte[] bytes =
            getPinnableSlice.pinnableSlice().get().data().toArray(ValueLayout.JAVA_BYTE);
        getPinnableSlice.pinnableSlice().get().reset();
        assertThat(new String(bytes, UTF_8)).isEqualTo("value1");
      } catch (final RocksDBException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
  }

  @Test
  public void get() throws RocksDBException {
    usingFFI(dbFolder, dbFFI -> {
      final RocksDB db = dbFFI.getRocksDB();
      try {
        db.put(db.getDefaultColumnFamily(), "key1".getBytes(), "value1".getBytes());
        var getBytes = dbFFI.get("key2".getBytes());
        assertThat(getBytes.code()).isEqualTo(Status.Code.NotFound);
        getBytes = dbFFI.get("key1".getBytes());
        assertThat(getBytes.code()).isEqualTo(Status.Code.Ok);
        assertThat(new String(getBytes.value(), UTF_8)).isEqualTo("value1");
      } catch (final RocksDBException e) {
        throw new RuntimeException(e);
      }

      return null;
    });
  }
}
