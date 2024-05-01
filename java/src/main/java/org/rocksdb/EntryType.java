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
public enum EntryType {
  kEntryPut((byte)0x0),
  kEntryDelete((byte)0x1),
  kEntrySingleDelete((byte)0x2),
  kEntryMerge((byte)0x3),
  kEntryRangeDeletion((byte)0x4),
  kEntryBlobIndex((byte)0x5),
  kEntryDeleteWithTimestamp((byte)0x6),
  kEntryWideColumnEntity((byte)0x7),
  kEntryOther((byte)0x8);
  private final byte value;
  private static Map<Byte, EntryType> reverseLookupMap = new HashMap<>();

  EntryType(byte value) {
    this.value = value;
  }

  static {
    for (EntryType entryType : EntryType.values()) {
      reverseLookupMap.put(entryType.value, entryType);
    }
  }

  public static EntryType getEntryType(final byte value) {
    if (reverseLookupMap.containsKey(value)) {
      return reverseLookupMap.get(value);
    }
    throw new IllegalArgumentException("Invalid ValueType byte " + value);
  }
}
