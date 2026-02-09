//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

public class BufferUtil {
  public static void CheckBounds(final int offset, final int len, final int size) {
    if ((offset | len | (offset + len) | (size - (offset + len))) < 0) {
      throw new IndexOutOfBoundsException(
          String.format("offset(%d), len(%d), size(%d)", offset, len, size));
    }
  }
}
