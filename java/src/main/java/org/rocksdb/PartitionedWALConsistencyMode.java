// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Consistency mode for Partitioned WAL.
 */
public enum PartitionedWALConsistencyMode {
  /**
   * Strong consistency mode (default).
   * Writes are visible immediately after return.
   */
  STRONG((byte) 0),

  /**
   * Weak consistency mode.
   * Writes may be buffered and visible with some delay.
   * Provides better write throughput.
   */
  WEAK((byte) 1);

  private final byte value;

  PartitionedWALConsistencyMode(final byte value) {
    this.value = value;
  }

  /**
   * Returns the byte value of the enumerations value.
   *
   * @return byte representation
   */
  public byte getValue() {
    return value;
  }

  /**
   * Get PartitionedWALConsistencyMode by byte value.
   *
   * @param value byte representation of PartitionedWALConsistencyMode.
   *
   * @return {@link org.rocksdb.PartitionedWALConsistencyMode} instance.
   * @throws java.lang.IllegalArgumentException if an invalid
   *     value is provided.
   */
  public static PartitionedWALConsistencyMode getPartitionedWALConsistencyMode(final byte value) {
    for (final PartitionedWALConsistencyMode mode : PartitionedWALConsistencyMode.values()) {
      if (mode.getValue() == value) {
        return mode;
      }
    }
    throw new IllegalArgumentException(
        "Illegal value provided for PartitionedWALConsistencyMode: " + value);
  }
}
