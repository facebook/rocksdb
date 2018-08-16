// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Parallel for compaction_filter.h's Decision enum.
 */
public enum CompactionDecision {

  // Enum values
  kKeep((byte) 0x0),
  kRemove((byte) 0x1),
  kChangeValue((byte) 0x2),
  kRemoveAndSkipUntil((byte) 0x3);

  // We use our own value field so we can ensure a tight binding to C++ enum values
  private final byte value_;

  /**
   * Private constructor
   *
   * @param value     Byte value we want to bind to this enum value
   */
  CompactionDecision(final byte value) {
    value_ = value;
  }

  /**
   * <p>Returns the byte value of the enumerations value.</p>
   *
   * @return byte representation
   */
  public byte getValue() {
    return value_;
  }

  /**
   * <p>Get the CompactionDecision enumeration value by
   * passing the byte identifier to this method.</p>
   *
   * @param byteIdentifier of CompactionDecision.
   *
   * @return CompactionDecision instance.
   *
   * @throws IllegalArgumentException If CompactionDecision cannot be found for the
   *   provided byteIdentifier
   */
  public static CompactionDecision getCompactionDecision(byte byteIdentifier) {
    for (final CompactionDecision decision : CompactionDecision.values()) {
      if (decision.getValue() == byteIdentifier) {
        return decision;
      }
    }
    throw new IllegalArgumentException("Illegal value provided for CompactionDecision.");
  }

}
