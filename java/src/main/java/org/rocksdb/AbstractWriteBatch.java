// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

public abstract class AbstractWriteBatch extends RocksObject implements WriteBatchInterface {

  protected AbstractWriteBatch(final long nativeHandle) {
    super(nativeHandle);
  }

  @Override
  public int count() {
    assert (isOwningHandle());
    return count0();
  }

  @Override
  public void put(byte[] key, byte[] value) {
    assert (isOwningHandle());
    put(key, key.length, value, value.length);
  }

  @Override
  public void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) {
    assert (isOwningHandle());
    put(key, key.length, value, value.length, columnFamilyHandle.nativeHandle_);
  }

  @Override
  public void merge(byte[] key, byte[] value) {
    assert (isOwningHandle());
    merge(key, key.length, value, value.length);
  }

  @Override
  public void merge(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) {
    assert (isOwningHandle());
    merge(key, key.length, value, value.length, columnFamilyHandle.nativeHandle_);
  }

  @Override
  public void remove(byte[] key) {
    assert (isOwningHandle());
    remove(key, key.length);
  }

  @Override
  public void remove(ColumnFamilyHandle columnFamilyHandle, byte[] key) {
    assert (isOwningHandle());
    remove(key, key.length, columnFamilyHandle.nativeHandle_);
  }

  @Override
  public void putLogData(byte[] blob) {
    assert (isOwningHandle());
    putLogData(blob, blob.length);
  }

  @Override
  public void clear() {
    assert (isOwningHandle());
    clear0();
  }

  abstract int count0();

  abstract void put(byte[] key, int keyLen, byte[] value, int valueLen);

  abstract void put(byte[] key, int keyLen, byte[] value, int valueLen, long cfHandle);

  abstract void merge(byte[] key, int keyLen, byte[] value, int valueLen);

  abstract void merge(byte[] key, int keyLen, byte[] value, int valueLen, long cfHandle);

  abstract void remove(byte[] key, int keyLen);

  abstract void remove(byte[] key, int keyLen, long cfHandle);

  abstract void putLogData(byte[] blob, int blobLen);

  abstract void clear0();
}
