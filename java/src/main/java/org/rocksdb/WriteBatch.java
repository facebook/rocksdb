// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

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
public class WriteBatch extends AbstractWriteBatch {
  /**
   * Constructs a WriteBatch instance.
   */
  public WriteBatch() {
    this(0);
  }

  /**
   * Constructs a WriteBatch instance with a given size.
   *
   * @param reserved_bytes reserved size for WriteBatch
   */
  public WriteBatch(final int reserved_bytes) {
    super(newWriteBatch(reserved_bytes));
  }

  /**
   * Support for iterating over the contents of a batch.
   *
   * @param handler A handler that is called back for each
   *                update present in the batch
   *
   * @throws RocksDBException If we cannot iterate over the batch
   */
  public void iterate(final Handler handler) throws RocksDBException {
    iterate(nativeHandle_, handler.nativeHandle_);
  }

  /**
   * <p>Private WriteBatch constructor which is used to construct
   * WriteBatch instances from C++ side. As the reference to this
   * object is also managed from C++ side the handle will be disowned.</p>
   *
   * @param nativeHandle address of native instance.
   */
  WriteBatch(final long nativeHandle) {
    this(nativeHandle, false);
  }

  /**
   * <p>Private WriteBatch constructor which is used to construct
   * WriteBatch instances. </p>
   *
   * @param nativeHandle address of native instance.
   * @param owningNativeHandle whether to own this reference from the C++ side or not
   */
  WriteBatch(final long nativeHandle, final boolean owningNativeHandle) {
    super(nativeHandle);
    if(!owningNativeHandle)
      disOwnNativeHandle();
  }

  @Override protected final native void disposeInternal(final long handle);
  @Override final native int count0(final long handle);
  @Override final native void put(final long handle, final byte[] key,
      final int keyLen, final byte[] value, final int valueLen);
  @Override final native void put(final long handle, final byte[] key,
      final int keyLen, final byte[] value, final int valueLen,
      final long cfHandle);
  @Override final native void merge(final long handle, final byte[] key,
      final int keyLen, final byte[] value, final int valueLen);
  @Override final native void merge(final long handle, final byte[] key,
      final int keyLen, final byte[] value, final int valueLen,
      final long cfHandle);
  @Override final native void remove(final long handle, final byte[] key,
      final int keyLen);
  @Override final native void remove(final long handle, final byte[] key,
      final int keyLen, final long cfHandle);
  @Override
  final native void deleteRange(final long handle, final byte[] beginKey, final int beginKeyLen,
      final byte[] endKey, final int endKeyLen);
  @Override
  final native void deleteRange(final long handle, final byte[] beginKey, final int beginKeyLen,
      final byte[] endKey, final int endKeyLen, final long cfHandle);
  @Override final native void putLogData(final long handle,
      final byte[] blob, final int blobLen);
  @Override final native void clear0(final long handle);
  @Override final native void setSavePoint0(final long handle);
  @Override final native void rollbackToSavePoint0(final long handle);

  private native static long newWriteBatch(final int reserved_bytes);
  private native void iterate(final long handle, final long handlerHandle)
      throws RocksDBException;


  /**
   * Handler callback for iterating over the contents of a batch.
   */
  public static abstract class Handler
      extends RocksCallbackObject {
    public Handler() {
      super(null);
    }

    @Override
    protected long initializeNative(final long... nativeParameterHandles) {
      return createNewHandler0();
    }

    public abstract void put(byte[] key, byte[] value);
    public abstract void merge(byte[] key, byte[] value);
    public abstract void delete(byte[] key);
    public abstract void deleteRange(byte[] beginKey, byte[] endKey);
    public abstract void logData(byte[] blob);

    /**
     * shouldContinue is called by the underlying iterator
     * WriteBatch::Iterate. If it returns false,
     * iteration is halted. Otherwise, it continues
     * iterating. The default implementation always
     * returns true.
     *
     * @return boolean value indicating if the
     *     iteration is halted.
     */
    public boolean shouldContinue() {
      return true;
    }

    private native long createNewHandler0();
  }
}
