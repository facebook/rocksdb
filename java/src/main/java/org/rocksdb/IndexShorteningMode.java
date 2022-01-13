// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

/**
 * This enum allows trading off increased index size for improved iterator
 * seek performance in some situations, particularly when block cache is
 * disabled ({@link ReadOptions#fillCache()} == false and direct IO is
 * enabled ({@link DBOptions#useDirectReads()} == true).
 * The default mode is the best tradeoff for most use cases.
 * This option only affects newly written tables.
 *
 * The index contains a key separating each pair of consecutive blocks.
 * Let A be the highest key in one block, B the lowest key in the next block,
 * and I the index entry separating these two blocks:
 * [ ... A] I [B ...]
 * I is allowed to be anywhere in [A, B).
 * If an iterator is seeked to a key in (A, I], we'll unnecessarily read the
 * first block, then immediately fall through to the second block.
 * However, if I=A, this can't happen, and we'll read only the second block.
 * In kNoShortening mode, we use I=A. In other modes, we use the shortest
 * key in [A, B), which usually significantly reduces index size.
 *
 * There's a similar story for the last index entry, which is an upper bound
 * of the highest key in the file. If it's shortened and therefore
 * overestimated, iterator is likely to unnecessarily read the last data block
 * from each file on each seek.
 */
public enum IndexShorteningMode {
  /**
   * Use full keys.
   */
  kNoShortening((byte) 0),
  /**
   * Shorten index keys between blocks, but use full key for the last index
   * key, which is the upper bound of the whole file.
   */
  kShortenSeparators((byte) 1),
  /**
   * Shorten both keys between blocks and key after last block.
   */
  kShortenSeparatorsAndSuccessor((byte) 2);

  private final byte value;

  IndexShorteningMode(final byte value) {
    this.value = value;
  }

  /**
   * Returns the byte value of the enumerations value.
   *
   * @return byte representation
   */
  byte getValue() {
    return value;
  }
}
