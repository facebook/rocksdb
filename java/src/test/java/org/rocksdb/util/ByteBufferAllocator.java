//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import java.nio.ByteBuffer;

public interface ByteBufferAllocator {
  ByteBuffer allocate(int capacity);

  ByteBufferAllocator DIRECT = new DirectByteBufferAllocator();
  ByteBufferAllocator HEAP = new HeapByteBufferAllocator();
}
