// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/** Selects the mutable memtable flushed under write-buffer-manager pressure. */
public enum WriteBufferManagerFlushPolicy {
  /** Flush the oldest mutable memtable. */
  FLUSH_OLDEST((byte) 0),

  /** Flush the largest mutable memtable in the current database. */
  FLUSH_LARGEST((byte) 1),

  /** Flush the database with the most reclaimable memory among all sharing the manager. */
  FLUSH_LARGEST_ACROSS_DBS((byte) 2);

  private final byte value;

  WriteBufferManagerFlushPolicy(final byte value) {
    this.value = value;
  }

  /**
   * Returns the native value for this policy.
   *
   * @return native policy value
   */
  public byte getValue() {
    return value;
  }

  /**
   * Returns the policy represented by a native value.
   *
   * @param value native policy value
   * @return matching policy
   * @throws IllegalArgumentException if {@code value} is unknown
   */
  public static WriteBufferManagerFlushPolicy fromValue(final byte value) {
    for (final WriteBufferManagerFlushPolicy policy : values()) {
      if (policy.value == value) {
        return policy;
      }
    }
    throw new IllegalArgumentException("Unknown write buffer manager flush policy: " + value);
  }
}
