// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public class SstFileReader extends RocksObject {
  static {
    RocksDB.loadLibrary();
  }

  public SstFileReader(final Options options) {
    super(newSstFileReader(options.nativeHandle_));
  }

  /**
   * Returns an iterator that will iterate on all keys in the default
   * column family including both keys in the DB and uncommitted keys in this
   * transaction.
   *
   * Setting {@link ReadOptions#setSnapshot(Snapshot)} will affect what is read
   * from the DB but will NOT change which keys are read from this transaction
   * (the keys in this transaction do not yet belong to any snapshot and will be
   * fetched regardless).
   *
   * Caller is responsible for deleting the returned Iterator.
   *
   * @param readOptions Read options.
   *
   * @return instance of iterator object.
   */
  public SstFileReaderIterator newIterator(final ReadOptions readOptions) {
    assert (isOwningHandle());
    long iter = newIterator(nativeHandle_, readOptions.nativeHandle_);
    return new SstFileReaderIterator(this, iter);
  }

  /**
   * Prepare SstFileReader to read a file.
   *
   * @param filePath the location of file
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void open(final String filePath) throws RocksDBException {
    open(nativeHandle_, filePath);
  }

  /**
   * Verify checksum
   *
   * @throws RocksDBException if the checksum is not valid
   */
  public void verifyChecksum() throws RocksDBException {
    verifyChecksum(nativeHandle_);
  }

  /**
   * Get the properties of the table.
   *
   * @return the properties
   *
   * @throws RocksDBException if an error occurs whilst getting the table
   *     properties
   */
  public TableProperties getTableProperties() throws RocksDBException {
    return getTableProperties(nativeHandle_);
  }

  @Override protected final native void disposeInternal(final long handle);
  private native long newIterator(final long handle, final long readOptionsHandle);

  private native void open(final long handle, final String filePath)
      throws RocksDBException;

  private native static long newSstFileReader(final long optionsHandle);
  private native void verifyChecksum(final long handle) throws RocksDBException;
  private native TableProperties getTableProperties(final long handle)
      throws RocksDBException;
}
