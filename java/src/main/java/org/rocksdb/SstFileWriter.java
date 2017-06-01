// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

public class SstFileWriter extends RocksObject {
  static {
    RocksDB.loadLibrary();
  }

  public SstFileWriter(final EnvOptions envOptions, final Options options,
      final AbstractComparator<? extends AbstractSlice<?>> comparator) {
    super(newSstFileWriter(
        envOptions.nativeHandle_, options.nativeHandle_, comparator.getNativeHandle()));
  }

  public SstFileWriter(final EnvOptions envOptions, final Options options) {
    super(newSstFileWriter(
        envOptions.nativeHandle_, options.nativeHandle_));
  }

  public void open(final String filePath) throws RocksDBException {
    open(nativeHandle_, filePath);
  }

  @Deprecated
  public void add(final Slice key, final Slice value) throws RocksDBException {
    put(nativeHandle_, key.getNativeHandle(), value.getNativeHandle());
  }

  @Deprecated
  public void add(final DirectSlice key, final DirectSlice value) throws RocksDBException {
    put(nativeHandle_, key.getNativeHandle(), value.getNativeHandle());
  }

  public void put(final Slice key, final Slice value) throws RocksDBException {
    put(nativeHandle_, key.getNativeHandle(), value.getNativeHandle());
  }

  public void put(final DirectSlice key, final DirectSlice value) throws RocksDBException {
    put(nativeHandle_, key.getNativeHandle(), value.getNativeHandle());
  }

  public void merge(final Slice key, final Slice value) throws RocksDBException {
    merge(nativeHandle_, key.getNativeHandle(), value.getNativeHandle());
  }

  public void merge(final DirectSlice key, final DirectSlice value) throws RocksDBException {
    merge(nativeHandle_, key.getNativeHandle(), value.getNativeHandle());
  }

  public void delete(final Slice key) throws RocksDBException {
    delete(nativeHandle_, key.getNativeHandle());
  }

  public void delete(final DirectSlice key) throws RocksDBException {
    delete(nativeHandle_, key.getNativeHandle());
  }

  public void finish() throws RocksDBException {
    finish(nativeHandle_);
  }

  private native static long newSstFileWriter(
      final long envOptionsHandle, final long optionsHandle, final long userComparatorHandle);

  private native static long newSstFileWriter(final long envOptionsHandle,
      final long optionsHandle);

  private native void open(final long handle, final String filePath) throws RocksDBException;

  private native void put(final long handle, final long keyHandle, final long valueHandle)
      throws RocksDBException;

  private native void merge(final long handle, final long keyHandle, final long valueHandle)
      throws RocksDBException;

  private native void delete(final long handle, final long keyHandle) throws RocksDBException;

  private native void finish(final long handle) throws RocksDBException;

  @Override protected final native void disposeInternal(final long handle);
}
