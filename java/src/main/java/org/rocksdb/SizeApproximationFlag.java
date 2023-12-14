// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.rocksdb;

import java.util.List;

/**
 * Flags for
 * {@link RocksDB#getApproximateSizes(ColumnFamilyHandle, List, SizeApproximationFlag...)}
 * that specify whether memtable stats should be included,
 * or file stats approximation or both.
 */
public enum SizeApproximationFlag {

  /**
   * None
   */
  NONE((byte)0x0),

  /**
   * Include Memtable(s).
   */
  INCLUDE_MEMTABLES((byte)0x1),

  /**
   * Include file(s).
   */
  INCLUDE_FILES((byte)0x2);

  private final byte value;

  SizeApproximationFlag(final byte value) {
    this.value = value;
  }

  /**
   * Get the internal byte representation.
   *
   * @return the internal representation.
   */
  byte getValue() {
    return value;
  }
}
