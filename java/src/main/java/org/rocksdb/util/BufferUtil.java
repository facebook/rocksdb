//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

/**
 * Utility functions for working with buffers.
 */
public class BufferUtil {

  /**
   * Check the bounds for an operation on a buffer.
   *
   * @param offset the offset
   * @param len the length
   * @param size the size
   *
   * @throws IndexOutOfBoundsException if the values are out of bounds
   */
  public static void CheckBounds(final int offset, final int len, final int size) {
    if ((offset | len | (offset + len) | (size - (offset + len))) < 0) {
      throw new IndexOutOfBoundsException(
          String.format("offset(%d), len(%d), size(%d)", offset, len, size));
    }
  }
}
