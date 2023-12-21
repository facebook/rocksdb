// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Objects;

/**
 * Indicates whether a key exists or not, and its corresponding value's length.
 */
public class KeyMayExist {
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final KeyMayExist that = (KeyMayExist) o;
    return (valueLength == that.valueLength && exists == that.exists);
  }

  @Override
  public int hashCode() {
    return Objects.hash(exists, valueLength);
  }

  /**
   * Part of the return type from {@link RocksDB#keyMayExist(ColumnFamilyHandle,
   * java.nio.ByteBuffer, java.nio.ByteBuffer)}.
   */
  public enum KeyMayExistEnum {
    /**
     * Key does not exist.
     */
    kNotExist,

    /**
     * Key may exist without a value.
     */
    kExistsWithoutValue,

    /**
     * Key may exist with a value.
     */
    kExistsWithValue
  }

  /**
   * Constructs a KeyMayExist.
   *
   * @param exists indicates if the key exists.
   * @param valueLength the length of the value pointed to by the key (if it exists).
   */
  KeyMayExist(final KeyMayExistEnum exists, final int valueLength) {
    this.exists = exists;
    this.valueLength = valueLength;
  }

  /**
   * Indicates if the key exists.
   */
  public final KeyMayExistEnum exists;

  /**
   * The length of the value pointed to by the key (if it exists).
   */
  public final int valueLength;
}
