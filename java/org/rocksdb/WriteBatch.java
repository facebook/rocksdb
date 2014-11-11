// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * WriteBatch holds a collection of updates to apply atomically to a DB.
 *
 * The updates are applied in the order in which they are added
 * to the WriteBatch.  For example, the value of "key" will be "v3"
 * after the following batch is written:
 *
 *    batch.put("key", "v1");
 *    batch.remove("key");
 *    batch.put("key", "v2");
 *    batch.put("key", "v3");
 *
 * Multiple threads can invoke const methods on a WriteBatch without
 * external synchronization, but if any of the threads may call a
 * non-const method, all threads accessing the same WriteBatch must use
 * external synchronization.
 */
public class WriteBatch extends RocksObject {
  public WriteBatch() {
    super();
    newWriteBatch(0);
  }

  public WriteBatch(int reserved_bytes) {
    nativeHandle_ = 0;
    newWriteBatch(reserved_bytes);
  }

  /**
   * Returns the number of updates in the batch.
   */
  public native int count();

  /**
   * Store the mapping "key-&gt;value" in the database.
   */
  public void put(byte[] key, byte[] value) {
    put(key, key.length, value, value.length);
  }

  /**
   * Store the mapping "key-&gt;value" within given column
   * family.
   */
  public void put(ColumnFamilyHandle columnFamilyHandle,
      byte[] key, byte[] value) {
    put(key, key.length, value, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Merge "value" with the existing value of "key" in the database.
   * "key-&gt;merge(existing, value)"
   */
  public void merge(byte[] key, byte[] value) {
    merge(key, key.length, value, value.length);
  }

  /**
   * Merge "value" with the existing value of "key" in given column family.
   * "key-&gt;merge(existing, value)"
   */
  public void merge(ColumnFamilyHandle columnFamilyHandle,
      byte[] key, byte[] value) {
    merge(key, key.length, value, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * If the database contains a mapping for "key", erase it.  Else do nothing.
   */
  public void remove(byte[] key) {
    remove(key, key.length);
  }

  /**
   * If column family contains a mapping for "key", erase it.  Else do nothing.
   */
  public void remove(ColumnFamilyHandle columnFamilyHandle, byte[] key) {
    remove(key, key.length, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Append a blob of arbitrary size to the records in this batch. The blob will
   * be stored in the transaction log but not in any other file. In particular,
   * it will not be persisted to the SST files. When iterating over this
   * WriteBatch, WriteBatch::Handler::LogData will be called with the contents
   * of the blob as it is encountered. Blobs, puts, deletes, and merges will be
   * encountered in the same order in thich they were inserted. The blob will
   * NOT consume sequence number(s) and will NOT increase the count of the batch
   *
   * Example application: add timestamps to the transaction log for use in
   * replication.
   */
  public void putLogData(byte[] blob) {
    putLogData(blob, blob.length);
  }

  /**
   * Support for iterating over the contents of a batch.
   *
   * @param handler A handler that is called back for each
   *                update present in the batch
   *
   * @throws RocksDBException If we cannot iterate over the batch
   */
  public void iterate(Handler handler) throws RocksDBException {
    iterate(handler.nativeHandle_);
  }

  /**
   * Clear all updates buffered in this batch
   */
  public native void clear();

  /**
   * Delete the c++ side pointer.
   */
  @Override protected void disposeInternal() {
    assert(isInitialized());
    disposeInternal(nativeHandle_);
  }

  private native void newWriteBatch(int reserved_bytes);
  private native void put(byte[] key, int keyLen,
                          byte[] value, int valueLen);
  private native void put(byte[] key, int keyLen,
                          byte[] value, int valueLen,
                          long cfHandle);
  private native void merge(byte[] key, int keyLen,
                            byte[] value, int valueLen);
  private native void merge(byte[] key, int keyLen,
                            byte[] value, int valueLen,
                            long cfHandle);
  private native void remove(byte[] key, int keyLen);
  private native void remove(byte[] key, int keyLen,
                            long cfHandle);
  private native void putLogData(byte[] blob, int blobLen);
  private native void iterate(long handlerHandle) throws RocksDBException;
  private native void disposeInternal(long handle);

  /**
   * Handler callback for iterating over the contents of a batch.
   */
  public static abstract class Handler extends RocksObject {
    public Handler() {
      super();
      createNewHandler0();
    }

    public abstract void put(byte[] key, byte[] value);
    public abstract void merge(byte[] key, byte[] value);
    public abstract void delete(byte[] key);
    public abstract void logData(byte[] blob);

    /**
     * shouldContinue is called by the underlying iterator
     * WriteBatch::Iterate. If it returns false,
     * iteration is halted. Otherwise, it continues
     * iterating. The default implementation always
     * returns true.
     */
    public boolean shouldContinue() {
      return true;
    }

    /**
     * Deletes underlying C++ handler pointer.
     */
    @Override
    protected void disposeInternal() {
      assert(isInitialized());
      disposeInternal(nativeHandle_);
    }

    private native void createNewHandler0();
    private native void disposeInternal(long handle);
  }
}

/**
 * Package-private class which provides java api to access
 * c++ WriteBatchInternal.
 */
class WriteBatchInternal {
  static native void setSequence(WriteBatch batch, long sn);
  static native long sequence(WriteBatch batch);
  static native void append(WriteBatch b1, WriteBatch b2);
}
