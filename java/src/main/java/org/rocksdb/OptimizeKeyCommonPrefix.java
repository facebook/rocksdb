//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Criteria controlling the {@code format_version} 8+ "common user-key prefix"
 * block optimization used with a {@link BlockBasedTableConfig}. When enabled,
 * the common user-key prefix shared by all keys in a data or index block is
 * stored once at the start of the block instead of at every restart point,
 * shrinking prefix-heavy blocks (and their block-cache footprint) and, for
 * bytewise-ordered keys, speeding up Seek by comparing prefix-stripped
 * suffixes.
 *
 * <p>Only takes effect at {@code format_version >= 8} and requires delta
 * encoding ({@link BlockBasedTableConfig#useDeltaEncoding()}); otherwise it is
 * a no-op.</p>
 */
public enum OptimizeKeyCommonPrefix {
  /**
   * Never use the optimization.
   */
  kDisabled((byte) 0x0),

  /**
   * Use it only where it also enables the Seek speedup: {@code format_version
   * >= 8} with a built-in (reverse-)bytewise comparator. This is the default.
   */
  kIfFastSeek((byte) 0x1),

  /**
   * Use it for all comparators at {@code format_version >= 8}. (Reverse-)bytewise
   * comparators get the space savings plus the Seek speedup; other comparators
   * get the space savings only (the reader reconstructs full keys).
   */
  kEnabled((byte) 0x2);

  private final byte value;

  OptimizeKeyCommonPrefix(final byte value) {
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
