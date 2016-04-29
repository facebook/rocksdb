// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * RocksDB {@link ReadOptions} read tiers.
 */
public enum ReadTier {
  READ_ALL_TIER((byte)0),
  BLOCK_CACHE_TIER((byte)1),
  PERSISTED_TIER((byte)2);

  private final byte value;

  ReadTier(final byte value) {
    this.value = value;
  }

  /**
   * Returns the byte value of the enumerations value
   *
   * @return byte representation
   */
  public byte getValue() {
    return value;
  }

  /**
   * Get ReadTier by byte value.
   *
   * @param value byte representation of ReadTier.
   *
   * @return {@link org.rocksdb.ReadTier} instance or null.
   * @throws java.lang.IllegalArgumentException if an invalid
   *     value is provided.
   */
  public static ReadTier getReadTier(final byte value) {
    for (final ReadTier readTier : ReadTier.values()) {
      if (readTier.getValue() == value){
        return readTier;
      }
    }
    throw new IllegalArgumentException("Illegal value provided for ReadTier.");
  }
}
