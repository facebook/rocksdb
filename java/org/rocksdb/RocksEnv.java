// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * <p>A RocksEnv is an interface used by the rocksdb implementation to access
 * operating system functionality like the filesystem etc.</p>
 *
 * <p>All Env implementations are safe for concurrent access from
 * multiple threads without any external synchronization.</p>
 */
public class RocksEnv extends RocksObject {
  public static final int FLUSH_POOL = 0;
  public static final int COMPACTION_POOL = 1;

  static {
    default_env_ = new RocksEnv(getDefaultEnvInternal());
  }
  private static native long getDefaultEnvInternal();

  /**
   * <p>Returns the default environment suitable for the current operating
   * system.</p>
   *
   * <p>The result of {@code getDefault()} is a singleton whose ownership
   * belongs to rocksdb c++.  As a result, the returned RocksEnv will not
   * have the ownership of its c++ resource, and calling its dispose()
   * will be no-op.</p>
   *
   * @return the default {@link org.rocksdb.RocksEnv} instance.
   */
  public static RocksEnv getDefault() {
    return default_env_;
  }

  /**
   * <p>Sets the number of background worker threads of the flush pool
   * for this environment.</p>
   * <p>Default number: 1</p>
   *
   * @param num the number of threads
   *
   * @return current {@link org.rocksdb.RocksEnv} instance.
   */
  public RocksEnv setBackgroundThreads(int num) {
    return setBackgroundThreads(num, FLUSH_POOL);
  }

  /**
   * <p>Sets the number of background worker threads of the specified thread
   * pool for this environment.</p>
   *
   * @param num the number of threads
   * @param poolID the id to specified a thread pool.  Should be either
   *     FLUSH_POOL or COMPACTION_POOL.
   *
   * <p>Default number: 1</p>
   * @return current {@link org.rocksdb.RocksEnv} instance.
   */
  public RocksEnv setBackgroundThreads(int num, int poolID) {
    setBackgroundThreads(nativeHandle_, num, poolID);
    return this;
  }
  private native void setBackgroundThreads(
      long handle, int num, int priority);

  /**
   * <p>Returns the length of the queue associated with the specified
   * thread pool.</p>
   *
   * @param poolID the id to specified a thread pool.  Should be either
   *     FLUSH_POOL or COMPACTION_POOL.
   *
   * @return the thread pool queue length.
   */
  public int getThreadPoolQueueLen(int poolID) {
    return getThreadPoolQueueLen(nativeHandle_, poolID);
  }
  private native int getThreadPoolQueueLen(long handle, int poolID);

  /**
   * <p>Package-private constructor that uses the specified native handle
   * to construct a RocksEnv.</p>
   *
   * <p>Note that the ownership of the input handle
   * belongs to the caller, and the newly created RocksEnv will not take
   * the ownership of the input handle.  As a result, calling
   * {@code dispose()} of the created RocksEnv will be no-op.</p>
   */
  RocksEnv(long handle) {
    super();
    nativeHandle_ = handle;
    disOwnNativeHandle();
  }

  /**
   * The helper function of {@link #dispose()} which all subclasses of
   * {@link RocksObject} must implement to release their associated C++
   * resource.
   */
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }
  private native void disposeInternal(long handle);

  /**
   * <p>The static default RocksEnv. The ownership of its native handle
   * belongs to rocksdb c++ and is not able to be released on the Java
   * side.</p>
   */
  static RocksEnv default_env_;
}
