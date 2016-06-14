//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This class mimics the db/write_batch_test.cc
 * in the c++ rocksdb library.
 * <p/>
 * Not ported yet:
 * <p/>
 * Continue();
 * PutGatherSlices();
 */
public class WriteBatchTest {
  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void emptyWriteBatch() {
    try (final WriteBatch batch = new WriteBatch()) {
      assertThat(batch.count()).isEqualTo(0);
    }
  }

  @Test
  public void multipleBatchOperations()
      throws UnsupportedEncodingException {
    try (WriteBatch batch = new WriteBatch()) {
      batch.put("foo".getBytes("US-ASCII"), "bar".getBytes("US-ASCII"));
      batch.remove("box".getBytes("US-ASCII"));
      batch.put("baz".getBytes("US-ASCII"), "boo".getBytes("US-ASCII"));

      WriteBatchTestInternalHelper.setSequence(batch, 100);
      assertThat(WriteBatchTestInternalHelper.sequence(batch)).
          isNotNull().
          isEqualTo(100);
      assertThat(batch.count()).isEqualTo(3);
      assertThat(new String(getContents(batch), "US-ASCII")).
          isEqualTo("Put(baz, boo)@102" +
              "Delete(box)@101" +
              "Put(foo, bar)@100");
    }
  }

  @Test
  public void testAppendOperation()
      throws UnsupportedEncodingException {
    try (final WriteBatch b1 = new WriteBatch();
         final WriteBatch b2 = new WriteBatch()) {
      WriteBatchTestInternalHelper.setSequence(b1, 200);
      WriteBatchTestInternalHelper.setSequence(b2, 300);
      WriteBatchTestInternalHelper.append(b1, b2);
      assertThat(getContents(b1).length).isEqualTo(0);
      assertThat(b1.count()).isEqualTo(0);
      b2.put("a".getBytes("US-ASCII"), "va".getBytes("US-ASCII"));
      WriteBatchTestInternalHelper.append(b1, b2);
      assertThat("Put(a, va)@200".equals(new String(getContents(b1),
          "US-ASCII")));
      assertThat(b1.count()).isEqualTo(1);
      b2.clear();
      b2.put("b".getBytes("US-ASCII"), "vb".getBytes("US-ASCII"));
      WriteBatchTestInternalHelper.append(b1, b2);
      assertThat(("Put(a, va)@200" +
          "Put(b, vb)@201")
          .equals(new String(getContents(b1), "US-ASCII")));
      assertThat(b1.count()).isEqualTo(2);
      b2.remove("foo".getBytes("US-ASCII"));
      WriteBatchTestInternalHelper.append(b1, b2);
      assertThat(("Put(a, va)@200" +
          "Put(b, vb)@202" +
          "Put(b, vb)@201" +
          "Delete(foo)@203")
          .equals(new String(getContents(b1), "US-ASCII")));
      assertThat(b1.count()).isEqualTo(4);
    }
  }

  @Test
  public void blobOperation()
      throws UnsupportedEncodingException {
    try (final WriteBatch batch = new WriteBatch()) {
      batch.put("k1".getBytes("US-ASCII"), "v1".getBytes("US-ASCII"));
      batch.put("k2".getBytes("US-ASCII"), "v2".getBytes("US-ASCII"));
      batch.put("k3".getBytes("US-ASCII"), "v3".getBytes("US-ASCII"));
      batch.putLogData("blob1".getBytes("US-ASCII"));
      batch.remove("k2".getBytes("US-ASCII"));
      batch.putLogData("blob2".getBytes("US-ASCII"));
      batch.merge("foo".getBytes("US-ASCII"), "bar".getBytes("US-ASCII"));
      assertThat(batch.count()).isEqualTo(5);
      assertThat(("Merge(foo, bar)@4" +
          "Put(k1, v1)@0" +
          "Delete(k2)@3" +
          "Put(k2, v2)@1" +
          "Put(k3, v3)@2")
          .equals(new String(getContents(batch), "US-ASCII")));
    }
  }

  @Test
  public void savePoints()
      throws UnsupportedEncodingException, RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {
      batch.put("k1".getBytes("US-ASCII"), "v1".getBytes("US-ASCII"));
      batch.put("k2".getBytes("US-ASCII"), "v2".getBytes("US-ASCII"));
      batch.put("k3".getBytes("US-ASCII"), "v3".getBytes("US-ASCII"));

      assertThat(getFromWriteBatch(batch, "k1")).isEqualTo("v1");
      assertThat(getFromWriteBatch(batch, "k2")).isEqualTo("v2");
      assertThat(getFromWriteBatch(batch, "k3")).isEqualTo("v3");


      batch.setSavePoint();

      batch.remove("k2".getBytes("US-ASCII"));
      batch.put("k3".getBytes("US-ASCII"), "v3-2".getBytes("US-ASCII"));

      assertThat(getFromWriteBatch(batch, "k2")).isNull();
      assertThat(getFromWriteBatch(batch, "k3")).isEqualTo("v3-2");


      batch.setSavePoint();

      batch.put("k3".getBytes("US-ASCII"), "v3-3".getBytes("US-ASCII"));
      batch.put("k4".getBytes("US-ASCII"), "v4".getBytes("US-ASCII"));

      assertThat(getFromWriteBatch(batch, "k3")).isEqualTo("v3-3");
      assertThat(getFromWriteBatch(batch, "k4")).isEqualTo("v4");


      batch.rollbackToSavePoint();

      assertThat(getFromWriteBatch(batch, "k2")).isNull();
      assertThat(getFromWriteBatch(batch, "k3")).isEqualTo("v3-2");
      assertThat(getFromWriteBatch(batch, "k4")).isNull();


      batch.rollbackToSavePoint();

      assertThat(getFromWriteBatch(batch, "k1")).isEqualTo("v1");
      assertThat(getFromWriteBatch(batch, "k2")).isEqualTo("v2");
      assertThat(getFromWriteBatch(batch, "k3")).isEqualTo("v3");
      assertThat(getFromWriteBatch(batch, "k4")).isNull();
    }
  }

  @Test(expected = RocksDBException.class)
  public void restorePoints_withoutSavePoints() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {
      batch.rollbackToSavePoint();
    }
  }

  @Test(expected = RocksDBException.class)
  public void restorePoints_withoutSavePoints_nested() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {

      batch.setSavePoint();
      batch.rollbackToSavePoint();

      // without previous corresponding setSavePoint
      batch.rollbackToSavePoint();
    }
  }

  static byte[] getContents(final WriteBatch wb) {
    return getContents(wb.nativeHandle_);
  }

  static String getFromWriteBatch(final WriteBatch wb, final String key)
      throws RocksDBException, UnsupportedEncodingException {
    final WriteBatchGetter getter =
        new WriteBatchGetter(key.getBytes("US-ASCII"));
    wb.iterate(getter);
    if(getter.getValue() != null) {
      return new String(getter.getValue(), "US-ASCII");
    } else {
      return null;
    }
  }

  private static native byte[] getContents(final long writeBatchHandle);

  private static class WriteBatchGetter extends WriteBatch.Handler {

    private final byte[] key;
    private byte[] value;

    public WriteBatchGetter(final byte[] key) {
      this.key = key;
    }

    public byte[] getValue() {
      return value;
    }

    @Override
    public void put(final byte[] key, final byte[] value) {
      if(Arrays.equals(this.key, key)) {
        this.value = value;
      }
    }

    @Override
    public void merge(final byte[] key, final byte[] value) {
      if(Arrays.equals(this.key, key)) {
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public void delete(final byte[] key) {
      if(Arrays.equals(this.key, key)) {
        this.value = null;
      }
    }

    @Override
    public void logData(final byte[] blob) {
    }
  }
}

/**
 * Package-private class which provides java api to access
 * c++ WriteBatchInternal.
 */
class WriteBatchTestInternalHelper {
  static void setSequence(final WriteBatch wb, final long sn) {
    setSequence(wb.nativeHandle_, sn);
  }

  static long sequence(final WriteBatch wb) {
    return sequence(wb.nativeHandle_);
  }

  static void append(final WriteBatch wb1, final WriteBatch wb2) {
    append(wb1.nativeHandle_, wb2.nativeHandle_);
  }

  private static native void setSequence(final long writeBatchHandle,
      final long sn);

  private static native long sequence(final long writeBatchHandle);

  private static native void append(final long writeBatchHandle1,
      final long writeBatchHandle2);
}
