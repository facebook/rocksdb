// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Parallel for compaction_filter.h's ValueType enum.
 */
public enum CompactionValueType {

  kValue((byte)0x0),
  kMergeOperand((byte)0x1);

  // We use our own value field so we can ensure a tight binding to C++ enum values
  private final byte value_;

  /**
   * Private constructor
   *
   * @param value     Byte value we want to bind to this enum value
   */
  CompactionValueType(final byte value) {
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
   * <p>Get the Decision enumeration value by
   * passing the byte identifier to this method.</p>
   *
   * @param byteIdentifier of CompactionValueType.
   *
   * @return CompactionValueType instance.
   *
   * @throws IllegalArgumentException If CompactionValueType cannot be found for the
   *   provided byteIdentifier
   */
  public static CompactionValueType getCompactionValueType(byte byteIdentifier) {
    for (final CompactionValueType valueType : CompactionValueType.values()) {
      if (valueType.getValue() == byteIdentifier) {
        return valueType;
      }
    }
    throw new IllegalArgumentException("Illegal value provided for CompactionValueType.");
  }
}

