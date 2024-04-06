// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * <p>An iterator that yields a sequence of key/value pairs from a source(including all tombstone entries).
 * Multiple implementations are provided by this library.
 * In particular, iterators are provided
 * to access the contents of a Table.</p>
 * <p>This also provides access to internal contracts the sequence number and
 * type of the entry in the table.</p>
 * <p>Multiple threads can invoke const methods on an RocksIterator without
 * external synchronization, but if any of the threads may call a
 * non-const method, all threads accessing the same RocksIterator must use
 * external synchronization.</p>
 *
 * @see RocksObject
 */
 public class RawSstFileReaderIterator extends SstFileReaderIterator {

  protected RawSstFileReaderIterator(final SstFileReader reader, final long nativeHandle) {
    super(reader, nativeHandle);
  }

  public long getSequenceNumber() {
    return sequenceNumber(nativeHandle_);
  }

  public ValueType getType() {
    return ValueType.getValueType(type(nativeHandle_));
  }

  private static native long sequenceNumber(final long handle);
  private static native byte type(final long handle);
}