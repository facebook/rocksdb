// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * IndexType used in conjunction with BlockBasedTable.
 */
public enum IndexType {
  /**
   * A space efficient index block that is optimized for
   * binary-search-based index.
   */
  kBinarySearch((byte) 0),
  /**
   * The hash index, if enabled, will do the hash lookup when
   * {@code Options.prefix_extractor} is provided.
   */
  kHashSearch((byte) 1),
  /**
   * A two-level index implementation. Both levels are binary search indexes.
   */
  kTwoLevelIndexSearch((byte) 2),
  /**
   * Like {@link #kBinarySearch}, but index also contains first key of each block.
   * This allows iterators to defer reading the block until it's actually
   * needed. May significantly reduce read amplification of short range scans.
   * Without it, iterator seek usually reads one block from each level-0 file
   * and from each level, which may be expensive.
   * Works best in combination with:
   *   - IndexShorteningMode::kNoShortening,
   *   - custom FlushBlockPolicy to cut blocks at some meaningful boundaries,
   *     e.g. when prefix changes.
   * Makes the index significantly bigger (2x or more), especially when keys
   * are long.
   */
  kBinarySearchWithFirstKey((byte) 3);

  /**
   * Returns the byte value of the enumerations value
   *
   * @return byte representation
   */
  public byte getValue() {
    return value_;
  }

  IndexType(byte value) {
    value_ = value;
  }

  private final byte value_;
}
