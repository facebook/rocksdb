// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * A RocksEnv is an interface used by the rocksdb implementation to access
 * operating system functionality like the filesystem etc.
 *
 * All Env implementations are safe for concurrent access from
 * multiple threads without any external synchronization.
 */
public class RocksEnv extends RocksObject {
  public static final int FLUSH_POOL = 0;
  public static final int COMPACTION_POOL = 1;

  static {
    default_env_ = new RocksEnv(getDefaultEnvInternal());
  }
  private static native long getDefaultEnvInternal();

  /**
   * Returns the default environment suitable for the current operating
   * system.
   *
   * The result of getDefault() is a singleton whose ownership belongs
   * to rocksdb c++.  As a result, the returned RocksEnv will not
   * have the ownership of its c++ resource, and calling its dispose()
   * will be no-op.
   */
  public static RocksEnv getDefault() {
    return default_env_;
  }

  /**
   * Sets the number of background worker threads of the flush pool
   * for this environment.
   * default number: 1
   */
  public RocksEnv setBackgroundThreads(int num) {
    return setBackgroundThreads(num, FLUSH_POOL);
  }

  /**
   * Sets the number of background worker threads of the specified thread
   * pool for this environment.
   *
   * @param num the number of threads
   * @param poolID the id to specified a thread pool.  Should be either
   *     FLUSH_POOL or COMPACTION_POOL.
   * Default number: 1
   */
  public RocksEnv setBackgroundThreads(int num, int poolID) {
    setBackgroundThreads(nativeHandle_, num, poolID);
    return this;
  }
  private native void setBackgroundThreads(
      long handle, int num, int priority);

  /**
   * Returns the length of the queue associated with the specified
   * thread pool.
   *
   * @param poolID the id to specified a thread pool.  Should be either
   *     FLUSH_POOL or COMPACTION_POOL.
   */
  public int getThreadPoolQueueLen(int poolID) {
    return getThreadPoolQueueLen(nativeHandle_, poolID);
  }
  private native int getThreadPoolQueueLen(long handle, int poolID);

  /**
   * Package-private constructor that uses the specified native handle
   * to construct a RocksEnv.  Note that the ownership of the input handle
   * belongs to the caller, and the newly created RocksEnv will not take
   * the ownership of the input handle.  As a result, calling dispose()
   * of the created RocksEnv will be no-op.
   */
  RocksEnv(long handle) {
    super();
    nativeHandle_ = handle;
    disOwnNativeHandle();
  }

  /**
   * The helper function of dispose() which all subclasses of RocksObject
   * must implement to release their associated C++ resource.
   */
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }
  private native void disposeInternal(long handle);

  /**
   * The static default RocksEnv.  The ownership of its native handle
   * belongs to rocksdb c++ and is not able to be released on the Java
   * side.
   */
  static RocksEnv default_env_;
}
