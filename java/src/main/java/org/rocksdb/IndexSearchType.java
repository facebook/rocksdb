//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * BlockSearchType used in conjunction with BlockBasedTable.
 */
public enum IndexSearchType {
  /**
   * Standard binary search
   */
  kBinary((byte) 0x0),

  /**
   * Interpolation search, which may be better suited for uniformly
   * distributed keys. Only applicable if the comparator is the
   * byte-wise comparator.
   */
  kInterpolation((byte) 0x1);

  private final byte value;

  IndexSearchType(final byte value) {
    this.value = value;
  }

  byte getValue() {
    return value;
  }
}
