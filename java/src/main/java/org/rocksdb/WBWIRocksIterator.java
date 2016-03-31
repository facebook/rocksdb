// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

public class WBWIRocksIterator
    extends AbstractRocksIterator<WriteBatchWithIndex> {
  private final WriteEntry entry = new WriteEntry();

  protected WBWIRocksIterator(final WriteBatchWithIndex wbwi,
      final long nativeHandle) {
    super(wbwi, nativeHandle);
  }

  /**
   * Get the current entry
   *
   * The WriteEntry is only valid
   * until the iterator is repositioned.
   * If you want to keep the WriteEntry across iterator
   * movements, you must make a copy of its data!
   *
   * Note - This method is not thread-safe with respect to the WriteEntry
   * as it performs a non-atomic update across the fields of the WriteEntry
   *
   * @return The WriteEntry of the current entry
   */
  public WriteEntry entry() {
    assert(isOwningHandle());
    assert(entry != null);
    final long ptrs[] = entry1(nativeHandle_);

    entry.type = WriteType.fromId((byte)ptrs[0]);
    entry.key.setNativeHandle(ptrs[1], true);
    entry.value.setNativeHandle(ptrs[2], ptrs[2] != 0);

    return entry;
  }

  @Override protected final native void disposeInternal(final long handle);
  @Override final native boolean isValid0(long handle);
  @Override final native void seekToFirst0(long handle);
  @Override final native void seekToLast0(long handle);
  @Override final native void next0(long handle);
  @Override final native void prev0(long handle);
  @Override final native void seek0(long handle, byte[] target, int targetLen);
  @Override final native void status0(long handle) throws RocksDBException;

  private native long[] entry1(final long handle);

  /**
   * Enumeration of the Write operation
   * that created the record in the Write Batch
   */
  public enum WriteType {
    PUT((byte)0x1),
    MERGE((byte)0x2),
    DELETE((byte)0x4),
    LOG((byte)0x8);

    final byte id;
    WriteType(final byte id) {
      this.id = id;
    }

    public static WriteType fromId(final byte id) {
      for(final WriteType wt : WriteType.values()) {
        if(id == wt.id) {
          return wt;
        }
      }
      throw new IllegalArgumentException("No WriteType with id=" + id);
    }
  }

  /**
   * Represents an entry returned by
   * {@link org.rocksdb.WBWIRocksIterator#entry()}
   *
   * It is worth noting that a WriteEntry with
   * the type {@link org.rocksdb.WBWIRocksIterator.WriteType#DELETE}
   * or {@link org.rocksdb.WBWIRocksIterator.WriteType#LOG}
   * will not have a value.
   */
  public static class WriteEntry {
    WriteType type = null;
    final DirectSlice key;
    final DirectSlice value;

    /**
     * Intentionally private as this
     * should only be instantiated in
     * this manner by the outer WBWIRocksIterator
     * class; The class members are then modified
     * by calling {@link org.rocksdb.WBWIRocksIterator#entry()}
     */
    private WriteEntry() {
      key = new DirectSlice();
      value = new DirectSlice();
    }

    public WriteEntry(WriteType type, DirectSlice key, DirectSlice value) {
      this.type = type;
      this.key = key;
      this.value = value;
    }

    /**
     * Returns the type of the Write Entry
     *
     * @return the WriteType of the WriteEntry
     */
    public WriteType getType() {
      return type;
    }

    /**
     * Returns the key of the Write Entry
     *
     * @return The slice containing the key
     * of the WriteEntry
     */
    public DirectSlice getKey() {
      return key;
    }

    /**
     * Returns the value of the Write Entry
     *
     * @return The slice containing the value of
     * the WriteEntry or null if the WriteEntry has
     * no value
     */
    public DirectSlice getValue() {
      if(!value.isOwningHandle()) {
        return null; //TODO(AR) migrate to JDK8 java.util.Optional#empty()
      } else {
        return value;
      }
    }

    /**
     * Generates a hash code for the Write Entry. NOTE: The hash code is based
     * on the string representation of the key, so it may not work correctly
     * with exotic custom comparators.
     *
     * @return The hash code for the Write Entry
     */
    @Override
    public int hashCode() {
      return (key == null) ? 0 : key.hashCode();
    }

    @Override
    public boolean equals(Object other) {
      if(other == null) {
        return false;
      } else if (this == other) {
        return true;
      } else if(other instanceof WriteEntry) {
        final WriteEntry otherWriteEntry = (WriteEntry)other;
        return type.equals(otherWriteEntry.type)
            && key.equals(otherWriteEntry.key)
            && value.equals(otherWriteEntry.value);
      } else {
        return false;
      }
    }
  }
}
