// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public class TransactionOptions extends RocksObject
    implements TransactionalOptions {

  public TransactionOptions() {
    super(newTransactionOptions());
  }

  @Override
  public boolean isSetSnapshot() {
    assert(isOwningHandle());
    return isSetSnapshot(nativeHandle_);
  }

  @Override
  public TransactionOptions setSetSnapshot(final boolean setSnapshot) {
    assert(isOwningHandle());
    setSetSnapshot(nativeHandle_, setSnapshot);
    return this;
  }

  /**
   * The wait timeout in milliseconds when a transaction attempts to lock a key.
   *
   * If 0, no waiting is done if a lock cannot instantly be acquired.
   * If negative, {@link TransactionDBOptions#getTransactionLockTimeout(long)}
   * will be used
   *
   * @return the lock tiemout in milliseconds
   */
  public long getLockTimeout() {
    assert(isOwningHandle());
    return getLockTimeout(nativeHandle_);
  }

  /**
   * If positive, specifies the wait timeout in milliseconds when
   * a transaction attempts to lock a key.
   *
   * If 0, no waiting is done if a lock cannot instantly be acquired.
   * If negative, {@link TransactionDBOptions#getTransactionLockTimeout(long)}
   * will be used
   *
   * Default: -1
   *
   * @param lockTimeout the lock tiemout in milliseconds
   *
   * @return this TransactionOptions instance
   */
  public TransactionOptions setLockTimeout(final long lockTimeout) {
    assert(isOwningHandle());
    setLockTimeout(nativeHandle_, lockTimeout);
    return this;
  }

  /**
   * Expiration duration in milliseconds.
   *
   * If non-negative, transactions that last longer than this many milliseconds
   * will fail to commit. If not set, a forgotten transaction that is never
   * committed, rolled back, or deleted will never relinquish any locks it
   * holds. This could prevent keys from being written by other writers.
   *
   * @return expiration the expiration duration in milliseconds
   */
  public long getExpiration() {
    assert(isOwningHandle());
    return getExpiration(nativeHandle_);
  }

  /**
   * Expiration duration in milliseconds.
   *
   * If non-negative, transactions that last longer than this many milliseconds
   * will fail to commit. If not set, a forgotten transaction that is never
   * committed, rolled back, or deleted will never relinquish any locks it
   * holds. This could prevent keys from being written by other writers.
   *
   * Default: -1
   *
   * @param expiration the expiration duration in milliseconds
   *
   * @return this TransactionOptions instance
   */
  public TransactionOptions setExpiration(final long expiration) {
    assert(isOwningHandle());
    setExpiration(nativeHandle_, expiration);
    return this;
  }

  private native static long newTransactionOptions();
  private native boolean isSetSnapshot(final long handle);
  private native void setSetSnapshot(final long handle,
      final boolean setSnapshot);
  private native long getLockTimeout(final long handle);
  private native void setLockTimeout(final long handle, final long lockTimeout);
  private native long getExpiration(final long handle);
  private native void setExpiration(final long handle, final long expiration);
  @Override protected final native void disposeInternal(final long handle);
}
