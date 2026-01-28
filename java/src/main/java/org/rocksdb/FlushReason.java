// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Reasons for a flush.
 */
public enum FlushReason {
  /**
   * Other.
   */
  OTHERS((byte) 0x00),

  /**
   * Get live files.
   */
  GET_LIVE_FILES((byte) 0x01),

  /**
   * Shutdown.
   */
  SHUTDOWN((byte) 0x02),

  /**
   * External file ingestion.
   */
  EXTERNAL_FILE_INGESTION((byte) 0x03),

  /**
   * Manual compaction.
   */
  MANUAL_COMPACTION((byte) 0x04),

  /**
   * Write buffer manager.
   */
  WRITE_BUFFER_MANAGER((byte) 0x05),

  /**
   * Write buffer full.
   */
  WRITE_BUFFER_FULL((byte) 0x06),

  /**
   * Test.
   */
  TEST((byte) 0x07),

  /**
   * Delete file(s).
   */
  DELETE_FILES((byte) 0x08),

  /**
   * Automatic compaction.
   */
  AUTO_COMPACTION((byte) 0x09),

  /**
   * Manual flush.
   */
  MANUAL_FLUSH((byte) 0x0a),

  /**
   * Error recovery.
   */
  ERROR_RECOVERY((byte) 0x0b),

  /**
   * Error recovery retry flush.
   */
  ERROR_RECOVERY_RETRY_FLUSH((byte) 0x0c),

  /**
   * Write Ahead Log full.
   */
  WAL_FULL((byte) 0x0d),

  /**
   * Catch up after error recovery.
   */
  CATCH_UP_AFTER_ERROR_RECOVERY((byte) 0x0e);

  private final byte value;

  FlushReason(final byte value) {
    this.value = value;
  }

  /**
   * Get the internal representation.
   *
   * @return the internal representation
   */
  byte getValue() {
    return value;
  }

  /**
   * Get the FlushReason from the internal representation value.
   *
   * @return the flush reason.
   *
   * @throws IllegalArgumentException if the value is unknown.
   */
  static FlushReason fromValue(final byte value) {
    for (final FlushReason flushReason : FlushReason.values()) {
      if (flushReason.value == value) {
        return flushReason;
      }
    }

    throw new IllegalArgumentException("Illegal value provided for FlushReason: " + value);
  }
}
