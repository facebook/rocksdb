// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;


import java.util.HashMap;
import java.util.Map;

/**
 * Value types of entry in SST file.
 */
public enum ValueType {
  kTypeDeletion((byte) 0x0),
  kTypeValue((byte) 0x1),
  kTypeMerge((byte) 0x2),
  kTypeSingleDeletion((byte) 0x7),
  kTypeRangeDeletion((byte) 0xF),
  kTypeColumnFamilyBlobIndex((byte) 0x10),
  kTypeBlobIndex((byte) 0x11),
  kTypeDeletionWithTimestamp((byte) 0x14),
  kTypeWideColumnEntity((byte) 0x16),
  kTypeValuePreferredSeqno((byte) 0x18);
  private final byte value;
  private static Map<Byte, ValueType> reverseLookupMap = new HashMap<>();

  ValueType(byte value) {
    this.value = value;
  }

  static {
    for (ValueType valueType : ValueType.values()) {
      reverseLookupMap.put(valueType.value, valueType);
    }
  }

  public static ValueType getValueType(final byte value) {
    if (reverseLookupMap.containsKey(value)) {
      return reverseLookupMap.get(value);
    }
    throw new IllegalArgumentException("Invalid ValueType byte " + value);
  }
}
