//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import java.nio.ByteBuffer;

public final class HeapByteBufferAllocator implements ByteBufferAllocator {
  HeapByteBufferAllocator(){};

  @Override
  public ByteBuffer allocate(final int capacity) {
    return ByteBuffer.allocate(capacity);
  }
}
