// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.rocksdb;

/**
 * RocksDB log levels.
 */
public enum InfoLogLevel {
  /**
   * Log 'debug' level events.
   */
  DEBUG_LEVEL((byte)0),

  /**
   * Log 'info' level events.
   */
  INFO_LEVEL((byte)1),

  /**
   * Log 'warn' level events.
   */
  WARN_LEVEL((byte)2),

  /**
   * Log 'error' level events.
   */
  ERROR_LEVEL((byte)3),

  /**
   * Log 'fatal' level events.
   */
  FATAL_LEVEL((byte)4),

  /**
   * Log 'header' level events.
   */
  HEADER_LEVEL((byte)5),

  /**
   * The number of log levels available.
   */
  NUM_INFO_LOG_LEVELS((byte)6);

  private final byte value_;

  InfoLogLevel(final byte value) {
    value_ = value;
  }

  /**
   * Returns the byte value of the enumerations value
   *
   * @return byte representation
   */
  public byte getValue() {
    return value_;
  }

  /**
   * Get InfoLogLevel by byte value.
   *
   * @param value byte representation of InfoLogLevel.
   *
   * @return {@link org.rocksdb.InfoLogLevel} instance.
   * @throws java.lang.IllegalArgumentException if an invalid
   *     value is provided.
   */
  public static InfoLogLevel getInfoLogLevel(final byte value) {
    for (final InfoLogLevel infoLogLevel : InfoLogLevel.values()) {
      if (infoLogLevel.getValue() == value) {
        return infoLogLevel;
      }
    }
    throw new IllegalArgumentException(
        "Illegal value provided for InfoLogLevel.");
  }
}
