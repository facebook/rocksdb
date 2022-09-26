// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.rocksdb.api.RocksNative;

import java.nio.ByteBuffer;

public abstract class AbstractWriteBatch extends RocksNative
    implements WriteBatchInterface {

  protected AbstractWriteBatch(final long nativeHandle) {
    super(nativeHandle);
  }

  @Override
  public int count() {
    return count0(getNative());
  }

  @Override
  public void put(byte[] key, byte[] value) throws RocksDBException {
    put(getNative(), key, key.length, value, value.length);
  }

  @Override
  public void put(ColumnFamilyHandle columnFamilyHandle, byte[] key,
      byte[] value) throws RocksDBException {
    put(getNative(), key, key.length, value, value.length,
        columnFamilyHandle.getNative());
  }

  @Override
  public void merge(byte[] key, byte[] value) throws RocksDBException {
    merge(getNative(), key, key.length, value, value.length);
  }

  @Override
  public void merge(ColumnFamilyHandle columnFamilyHandle, byte[] key,
      byte[] value) throws RocksDBException {
    merge(getNative(), key, key.length, value, value.length,
        columnFamilyHandle.getNative());
  }

  @Override
  public void put(final ByteBuffer key, final ByteBuffer value) throws RocksDBException {
    assert key.isDirect() && value.isDirect();
    putDirect(getNative(), key, key.position(), key.remaining(), value, value.position(),
        value.remaining(), 0);
    key.position(key.limit());
    value.position(value.limit());
  }

  @Override
  public void put(ColumnFamilyHandle columnFamilyHandle, final ByteBuffer key,
      final ByteBuffer value) throws RocksDBException {
    assert key.isDirect() && value.isDirect();
    putDirect(getNative(), key, key.position(), key.remaining(), value, value.position(),
        value.remaining(), columnFamilyHandle.getNative());
    key.position(key.limit());
    value.position(value.limit());
  }

  @Override
  public void delete(byte[] key) throws RocksDBException {
    delete(getNative(), key, key.length);
  }

  @Override
  public void delete(ColumnFamilyHandle columnFamilyHandle, byte[] key)
      throws RocksDBException {
    delete(getNative(), key, key.length, columnFamilyHandle.getNative());
  }

  @Override
  public void delete(final ByteBuffer key) throws RocksDBException {
    deleteDirect(getNative(), key, key.position(), key.remaining(), 0);
    key.position(key.limit());
  }

  @Override
  public void delete(ColumnFamilyHandle columnFamilyHandle, final ByteBuffer key)
      throws RocksDBException {
    deleteDirect(
        getNative(), key, key.position(), key.remaining(), columnFamilyHandle.getNative());
    key.position(key.limit());
  }

  @Override
  public void singleDelete(byte[] key) throws RocksDBException {
    singleDelete(getNative(), key, key.length);
  }

  @Override
  public void singleDelete(ColumnFamilyHandle columnFamilyHandle, byte[] key)
      throws RocksDBException {
    singleDelete(getNative(), key, key.length, columnFamilyHandle.getNative());
  }

  @Override
  public void deleteRange(byte[] beginKey, byte[] endKey)
      throws RocksDBException {
    deleteRange(getNative(), beginKey, beginKey.length, endKey, endKey.length);
  }

  @Override
  public void deleteRange(ColumnFamilyHandle columnFamilyHandle,
      byte[] beginKey, byte[] endKey) throws RocksDBException {
    deleteRange(getNative(), beginKey, beginKey.length, endKey, endKey.length,
        columnFamilyHandle.getNative());
  }

  @Override
  public void putLogData(byte[] blob) throws RocksDBException {
    putLogData(getNative(), blob, blob.length);
  }

  @Override
  public void clear() {
    clear0(getNative());
  }

  @Override
  public void setSavePoint() {
    setSavePoint0(getNative());
  }

  @Override
  public void rollbackToSavePoint() throws RocksDBException {
    rollbackToSavePoint0(getNative());
  }

  @Override
  public void popSavePoint() throws RocksDBException {
    popSavePoint(getNative());
  }

  @Override
  public void setMaxBytes(final long maxBytes) {
    setMaxBytes(getNative(), maxBytes);
  }

  @Override
  public WriteBatch getWriteBatch() {
    return getWriteBatch(getNative());
  }

  abstract int count0(final long handle);

  abstract void put(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen) throws RocksDBException;

  abstract void put(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen, final long cfHandle)
      throws RocksDBException;

  abstract void putDirect(final long handle, final ByteBuffer key, final int keyOffset,
      final int keyLength, final ByteBuffer value, final int valueOffset, final int valueLength,
      final long cfHandle) throws RocksDBException;

  abstract void merge(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen) throws RocksDBException;

  abstract void merge(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen, final long cfHandle)
      throws RocksDBException;

  abstract void delete(final long handle, final byte[] key,
      final int keyLen) throws RocksDBException;

  abstract void delete(final long handle, final byte[] key,
      final int keyLen, final long cfHandle) throws RocksDBException;

  abstract void singleDelete(final long handle, final byte[] key, final int keyLen)
      throws RocksDBException;

  abstract void singleDelete(final long handle, final byte[] key, final int keyLen,
      final long cfHandle) throws RocksDBException;

  abstract void deleteDirect(final long handle, final ByteBuffer key, final int keyOffset,
      final int keyLength, final long cfHandle) throws RocksDBException;

  abstract void deleteRange(final long handle, final byte[] beginKey, final int beginKeyLen,
      final byte[] endKey, final int endKeyLen) throws RocksDBException;

  abstract void deleteRange(final long handle, final byte[] beginKey, final int beginKeyLen,
      final byte[] endKey, final int endKeyLen, final long cfHandle) throws RocksDBException;

  abstract void putLogData(final long handle, final byte[] blob,
      final int blobLen) throws RocksDBException;

  abstract void clear0(final long handle);

  abstract void setSavePoint0(final long handle);

  abstract void rollbackToSavePoint0(final long handle);

  abstract void popSavePoint(final long handle) throws RocksDBException;

  abstract void setMaxBytes(final long handle, long maxBytes);

  abstract WriteBatch getWriteBatch(final long handle);
}
