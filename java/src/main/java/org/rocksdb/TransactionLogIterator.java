// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.rocksdb;

/**
 * <p>Old name for {@link WalIterator}, retained for compatibility. This API
 * reads the write-ahead log and is unrelated to
 * {@link org.rocksdb.TransactionDB}; "transaction log" is an old synonym for
 * WAL. Prefer declaring {@link WalIterator} in new code.</p>
 *
 * <p>Note that this class is not annotated {@code @Deprecated} so that
 * existing callers do not start failing builds that treat warnings as
 * errors, matching how the equivalent C++ name is retired.</p>
 */
public class TransactionLogIterator extends WalIterator {
  /**
   * <p>TransactionLogIterator constructor.</p>
   *
   * @param nativeHandle address to native address.
   */
  TransactionLogIterator(final long nativeHandle) {
    super(nativeHandle);
  }
}
