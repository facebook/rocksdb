// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>An iterator that yields a sequence of internal key/value pairs from a source.
 * Multiple implementations are provided by this library.
 * In particular, iterators are provided
 * to access the contents of a Table or a DB.</p>
 *
 * <p>Multiple threads can invoke const methods on an RocksIterator without
 * external synchronization, but if any of the threads may call a
 * non-const method, all threads accessing the same RocksIterator must use
 * external synchronization.</p>
 *
 * @see RocksObject
 */
 class RawSstFileReaderIterator extends SstFileReaderIterator {

  protected RawSstFileReaderIterator(SstFileReader reader, long nativeHandle) {
    super(reader, nativeHandle);
  }

  public enum ValueType {
    kTypeDeletion(0x0),
    kTypeValue(0x1),
    kTypeMerge(0x2),
    kTypeLogData(0x3),
    kTypeColumnFamilyDeletion(0x4),
    kTypeColumnFamilyValue(0x5),
    kTypeColumnFamilyMerge(0x6),
    kTypeSingleDeletion(0x7),
    kTypeColumnFamilySingleDeletion(0x8),
    kTypeBeginPrepareXID(0x9),
    kTypeEndPrepareXID(0xA),
    kTypeCommitXID(0xB),
    kTypeRollbackXID(0xC),
    kTypeNoop(0xD),
    kTypeColumnFamilyRangeDeletion(0xE),
    kTypeRangeDeletion(0xF),
    kTypeColumnFamilyBlobIndex(0x10),
    kTypeBlobIndex(0x11),
    kTypeBeginPersistedPrepareXID(0x12),
    kTypeBeginUnprepareXID(0x13),
    kTypeDeletionWithTimestamp(0x14),
    kTypeCommitXIDAndTimestamp(0x15),
    kTypeWideColumnEntity(0x16),
    kTypeColumnFamilyWideColumnEntity(0x17),
    kTypeValuePreferredSeqno(0x18),
    kTypeColumnFamilyValuePreferredSeqno(0x19),
    kMaxValue(0x7F);
    private int value;
    private static Map<Integer, ValueType> reverseLookupMap = new HashMap<>();

    ValueType(int value) {
      this.value = value;
    }

    static {
      for (ValueType valueType : ValueType.values()) {
        reverseLookupMap.put(valueType.value, valueType);
      }
    }

    public static ValueType getValueType(int value) {
      return reverseLookupMap.get(value);
    }
  }

  public long getSequenceNumber() {
    return sequenceNumber(nativeHandle_);
  }

  public ValueType getType() {
    return ValueType.getValueType(type(nativeHandle_));
  }

  private static native long sequenceNumber(final long handle);
  private static native int type(final long handle);
}