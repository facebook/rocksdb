// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.ByteBuffer;

/**
 * <p>Defines the interface for an Iterator which provides
 * access to data one entry at a time. Multiple implementations
 * are provided by this library.  In particular, iterators are provided
 * to access the contents of a DB and Write Batch.</p>
 *
 * <p>Multiple threads can invoke const methods on an RocksIterator without
 * external synchronization, but if any of the threads may call a
 * non-const method, all threads accessing the same RocksIterator must use
 * external synchronization.</p>
 *
 * @see org.rocksdb.RocksObject
 */
public interface RocksIteratorInterface {

  /**
   * <p>An iterator is either positioned at an entry, or
   * not valid.  This method returns true if the iterator is valid.</p>
   *
   * @return true if iterator is valid.
   */
  boolean isValid();

  /**
   * <p>Position at the first entry in the source.  The iterator is Valid()
   * after this call if the source is not empty.</p>
   */
  void seekToFirst();

  /**
   * <p>Position at the last entry in the source.  The iterator is
   * valid after this call if the source is not empty.</p>
   */
  void seekToLast();

  /**
   * <p>Position at the first entry in the source whose key is at or
   * past target.</p>
   *
   * <p>The iterator is valid after this call if the source contains
   * a key that comes at or past target.</p>
   *
   * @param target byte array describing a key or a
   *               key prefix to seek for.
   */
  void seek(byte[] target);

  /**
   * <p>Position at the first entry in the source whose key is that or
   * before target.</p>
   *
   * <p>The iterator is valid after this call if the source contains
   * a key that comes at or before target.</p>
   *
   * @param target byte array describing a key or a
   *               key prefix to seek for.
   */
  void seekForPrev(byte[] target);

  /**
   * <p>Position at the first entry in the source whose key is that or
   * past target.</p>
   *
   * <p>The iterator is valid after this call if the source contains
   * a key that comes at or past target.</p>
   *
   * @param target byte array describing a key or a
   *               key prefix to seek for. Supports direct buffer only.
   */
  void seek(ByteBuffer target);

  /**
   * <p>Position at the last key that is less than or equal to the target key.</p>
   *
   * <p>The iterator is valid after this call if the source contains
   * a key that comes at or past target.</p>
   *
   * @param target byte array describing a key or a
   *               key prefix to seek for. Supports direct buffer only.
   */
  void seekForPrev(ByteBuffer target);

  /**
   * <p>Moves to the next entry in the source.  After this call, Valid() is
   * true if the iterator was not positioned at the last entry in the source.</p>
   *
   * <p>REQUIRES: {@link #isValid()}</p>
   */
  void next();

  /**
   * <p>Moves to the previous entry in the source.  After this call, Valid() is
   * true if the iterator was not positioned at the first entry in source.</p>
   *
   * <p>REQUIRES: {@link #isValid()}</p>
   */
  void prev();

  /**
   * <p>If an error has occurred, return it.  Else return an ok status.
   * If non-blocking IO is requested and this operation cannot be
   * satisfied without doing some IO, then this returns Status::Incomplete().</p>
   *
   * @throws RocksDBException thrown if error happens in underlying
   *                          native library.
   */
  void status() throws RocksDBException;

  /**
   * <p>If supported, the DB state that the iterator reads from is updated to
   * the latest state. The iterator will be invalidated after the call.
   * Regardless of whether the iterator was created/refreshed previously with
   * or without a snapshot, the iterator will be reading the latest DB state
   * after this call.</p>
   * <p>Note that you will need to call a Seek*() function to get the iterator
   * back into a valid state before calling a function that assumes the
   * state is already valid, like Next().</p>
   *
   * @throws RocksDBException thrown if the operation is not supported or an error happens in the
   *     underlying native library
   */
  void refresh() throws RocksDBException;

  /**
   * Similar to {@link #refresh()} but the iterator will be reading the latest DB state under the
   * given snapshot.
   *
   * @param snapshot to refresh from
   * @throws RocksDBException if an error occurs during the refresh
   */
  void refresh(Snapshot snapshot) throws RocksDBException;
}
