// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.ByteBuffer;
import org.rocksdb.RocksNative;

/**
 * Base class implementation for Rocks Iterators
 * in the Java API
 *
 * <p>Multiple threads can invoke const methods on an RocksIterator without
 * external synchronization, but if any of the threads may call a
 * non-const method, all threads accessing the same RocksIterator must use
 * external synchronization.</p>
 *
 * @param <P> The type of the Parent Object from which the Rocks Iterator was
 *          created. This is used by disposeInternal to avoid double-free
 *          issues with the underlying C++ object.
 * @see org.rocksdb.RocksObject
 */
public abstract class AbstractRocksIterator<P extends RocksNative>
    extends RocksNative implements RocksIteratorInterface {
  final P parent_;

  protected AbstractRocksIterator(final P parent,
      final long nativeHandle) {
    super(nativeHandle);
    // parent must point to a valid RocksDB instance.
    assert (parent != null);
    // RocksIterator must hold a reference to the related parent instance
    // to guarantee that while a GC cycle starts RocksIterator instances
    // are freed prior to parent instances.
    parent_ = parent;
  }

  @Override
  public boolean isValid() {
    return isValid0(getNative());
  }

  @Override
  public void seekToFirst() {
    seekToFirst0(getNative());
  }

  @Override
  public void seekToLast() {
    seekToLast0(getNative());
  }

  @Override
  public void seek(final byte[] target) {
    seek0(getNative(), target, target.length);
  }

  @Override
  public void seekForPrev(final byte[] target) {
    seekForPrev0(getNative(), target, target.length);
  }

  @Override
  public void seek(final ByteBuffer target) {
    if (target.isDirect()) {
      seekDirect0(getNative(), target, target.position(), target.remaining());
    } else {
      seekByteArray0(getNative(), target.array(), target.arrayOffset() + target.position(),
          target.remaining());
    }
    target.position(target.limit());
  }

  @Override
  public void seekForPrev(final ByteBuffer target) {
    if (target.isDirect()) {
      seekForPrevDirect0(getNative(), target, target.position(), target.remaining());
    } else {
      seekForPrevByteArray0(getNative(), target.array(), target.arrayOffset() + target.position(),
          target.remaining());
    }
    target.position(target.limit());
  }

 @Override
 public void next() throws RocksDBException {
   next0(getNative());
 }

 @Override
 public void prev() throws RocksDBException {
   prev0(getNative());
 }

  @Override
  public void refresh() throws RocksDBException {
    refresh0(getNative());
  }

  @Override
  public void status() throws RocksDBException {
    status0(getNative());
  }

  abstract boolean isValid0(long handle);
  abstract void seekToFirst0(long handle);
  abstract void seekToLast0(long handle);
  abstract void next0(long handle);
  abstract void prev0(long handle);
  abstract void refresh0(long handle) throws RocksDBException;
  abstract void seek0(long handle, byte[] target, int targetLen);
  abstract void seekForPrev0(long handle, byte[] target, int targetLen);
  abstract void seekDirect0(long handle, ByteBuffer target, int targetOffset, int targetLen);
  abstract void seekForPrevDirect0(long handle, ByteBuffer target, int targetOffset, int targetLen);
  abstract void seekByteArray0(long handle, byte[] target, int targetOffset, int targetLen);
  abstract void seekForPrevByteArray0(long handle, byte[] target, int targetOffset, int targetLen);

  abstract void status0(long handle) throws RocksDBException;
}
