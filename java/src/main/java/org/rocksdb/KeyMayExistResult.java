// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Result class for {@link RocksDB#keyMayExist(byte[], boolean)} and
 * overloaded methods.
 */
public class KeyMayExistResult {

  /**
   * Indicates that either the found value was not requested, or could
   * not be retrieved.
   */
  public static final KeyMayExistResult VALUE_NOT_SET =
      new KeyMayExistResult(null);

  /**
   * The value if it could be retrieved when found or null otherwise.
   */
  /* @Nullable */ public final byte[] value;

  /**
   * @param value the found value or null.
   */
  public KeyMayExistResult(/* @Nullable */ final byte[] value) {
    this.value = value;
  }
}
