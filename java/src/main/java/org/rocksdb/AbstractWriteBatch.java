// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

public abstract class AbstractWriteBatch extends RocksObject
    implements WriteBatchInterface {

  protected AbstractWriteBatch(final long nativeHandle) {
    super(nativeHandle);
  }

  @Override
  public int count() {
    return count0(nativeHandle_);
  }

  @Override
  public void put(byte[] key, byte[] value) {
    put(nativeHandle_, key, key.length, value, value.length);
  }

  @Override
  public void put(ColumnFamilyHandle columnFamilyHandle, byte[] key,
      byte[] value) {
    put(nativeHandle_, key, key.length, value, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  @Override
  public void merge(byte[] key, byte[] value) {
    merge(nativeHandle_, key, key.length, value, value.length);
  }

  @Override
  public void merge(ColumnFamilyHandle columnFamilyHandle, byte[] key,
      byte[] value) {
    merge(nativeHandle_, key, key.length, value, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  @Override
  public void remove(byte[] key) {
    remove(nativeHandle_, key, key.length);
  }

  @Override
  public void remove(ColumnFamilyHandle columnFamilyHandle, byte[] key) {
    remove(nativeHandle_, key, key.length, columnFamilyHandle.nativeHandle_);
  }

  @Override
  public void deleteRange(byte[] beginKey, byte[] endKey) {
    deleteRange(nativeHandle_, beginKey, beginKey.length, endKey, endKey.length);
  }

  @Override
  public void deleteRange(ColumnFamilyHandle columnFamilyHandle, byte[] beginKey, byte[] endKey) {
    deleteRange(nativeHandle_, beginKey, beginKey.length, endKey, endKey.length,
        columnFamilyHandle.nativeHandle_);
  }

  @Override
  public void putLogData(byte[] blob) {
    putLogData(nativeHandle_, blob, blob.length);
  }

  @Override
  public void clear() {
    clear0(nativeHandle_);
  }

  @Override
  public void setSavePoint() {
    setSavePoint0(nativeHandle_);
  }

  @Override
  public void rollbackToSavePoint() throws RocksDBException {
    rollbackToSavePoint0(nativeHandle_);
  }

  abstract int count0(final long handle);

  abstract void put(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen);

  abstract void put(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen, final long cfHandle);

  abstract void merge(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen);

  abstract void merge(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen, final long cfHandle);

  abstract void remove(final long handle, final byte[] key,
      final int keyLen);

  abstract void remove(final long handle, final byte[] key,
      final int keyLen, final long cfHandle);

  abstract void deleteRange(final long handle, final byte[] beginKey, final int beginKeyLen,
      final byte[] endKey, final int endKeyLen);

  abstract void deleteRange(final long handle, final byte[] beginKey, final int beginKeyLen,
      final byte[] endKey, final int endKeyLen, final long cfHandle);

  abstract void putLogData(final long handle, final byte[] blob,
      final int blobLen);

  abstract void clear0(final long handle);

  abstract void setSavePoint0(final long handle);

  abstract void rollbackToSavePoint0(final long handle);
}
