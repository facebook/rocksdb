// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.rocksdb.util.FFIUtil.usingFFI;

import java.lang.foreign.MemorySegment;
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
        var getPinnableSlice =
            dbFFI.getPinnableSlice(dbFFI.copy("key2"), FFIDB.GetParams.create(dbFFI));
        assertThat(getPinnableSlice.code()).isEqualTo(Status.Code.NotFound);
        getPinnableSlice =
            dbFFI.getPinnableSlice(dbFFI.copy("key1"), FFIDB.GetParams.create(dbFFI));
        assertThat(getPinnableSlice.code()).isEqualTo(Status.Code.Ok);
        assertThat(getPinnableSlice.pinnableSlice().get().data()).isNotNull();
        final byte[] bytes = getPinnableSlice.pinnableSlice().get().data().toArray(JAVA_BYTE);
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
        var getBytes = dbFFI.get(dbFFI.copy("key2"), FFIDB.GetParams.create(dbFFI));
        assertThat(getBytes.code()).isEqualTo(Status.Code.NotFound);
        getBytes = dbFFI.get(dbFFI.copy("key1"), FFIDB.GetParams.create(dbFFI));
        assertThat(getBytes.code()).isEqualTo(Status.Code.Ok);
        assertThat(new String(getBytes.value(), UTF_8)).isEqualTo("value1");
      } catch (final RocksDBException e) {
        throw new RuntimeException(e);
      }

      return null;
    });
  }

  @Test
  public void getOutputSlice() throws RocksDBException {
    usingFFI(dbFolder, dbFFI -> {
      try {
        final RocksDB db = dbFFI.getRocksDB();
        db.put(db.getDefaultColumnFamily(), "key1".getBytes(), "value1".getBytes());
        final MemorySegment outputSegment = dbFFI.allocateSegment(32);

        var getOutputSlice = dbFFI.getOutputSlice(outputSegment, dbFFI.copy("key2"));
        assertThat(getOutputSlice.code()).isEqualTo(Status.Code.NotFound);
        assertThat(getOutputSlice.outputSlice().isPresent()).isFalse();

        getOutputSlice = dbFFI.getOutputSlice(outputSegment, dbFFI.copy("key1"));
        assertThat(getOutputSlice.code()).isEqualTo(Status.Code.Ok);
        assertThat(getOutputSlice.outputSlice().isPresent()).isTrue();
        var outputSlice = getOutputSlice.outputSlice().get();
        assertThat(outputSlice.outputSize()).isEqualTo("value1".getBytes().length);
        var value = outputSlice.outputSegment().toArray(JAVA_BYTE);
        assertThat(value).startsWith("value1".getBytes(UTF_8));

        final MemorySegment shortOutputSegment = dbFFI.allocateSegment(5);
        getOutputSlice = dbFFI.getOutputSlice(shortOutputSegment, dbFFI.copy("key1"));
        assertThat(getOutputSlice.code()).isEqualTo(Status.Code.Ok);
        assertThat(getOutputSlice.outputSlice().isPresent()).isTrue();
        outputSlice = getOutputSlice.outputSlice().get();
        assertThat(outputSlice.outputSize()).isEqualTo("value1".getBytes().length);
        value = outputSlice.outputSegment().toArray(JAVA_BYTE);
        assertThat(value).startsWith("value".getBytes(UTF_8));
      } catch (final RocksDBException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
  }

  @Test
  public void identity() throws RocksDBException {
    usingFFI(dbFolder, dbFFI -> {
      try {
        final int seven = dbFFI.identity(7);
        assertThat(seven).isEqualTo(8);
      } catch (final RocksDBException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
  }
}
