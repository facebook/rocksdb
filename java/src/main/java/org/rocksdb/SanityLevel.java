//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.text.MessageFormat;

public enum SanityLevel {
  NONE((byte) 0x0),
  LOOSELY_COMPATIBLE((byte) 0x1),
  @SuppressWarnings("NumericCastThatLosesPrecision") EXACT_MATCH((byte) 0xFF);

  private final byte value;

  SanityLevel(final byte value) {
    this.value = value;
  }

  /**
   * Get the internal representation value.
   *
   * @return the internal representation value.
   */
  byte getValue() {
    return value;
  }

  /**
   * Get the SanityLevel from the internal representation value.
   *
   * @param value the internal representation value.
   *
   * @return the SanityLevel
   *
   * @throws IllegalArgumentException if the value does not match a
   *     SanityLevel
   */
  @SuppressWarnings("unused")
  static SanityLevel fromValue(final byte value) {
    for (final SanityLevel level : values()) {
      if (level.value == value) {
        return level;
      }
    }
    throw new IllegalArgumentException(
        MessageFormat.format("Unknown value for SanityLevel: {0}", value));
  }
}
