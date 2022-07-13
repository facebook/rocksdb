package org.rocksdb.util;

import java.nio.ByteBuffer;

public final class HeapByteBufferAllocator implements ByteBufferAllocator {
  HeapByteBufferAllocator(){};

  @Override
  public ByteBuffer allocate(final int capacity) {
    return ByteBuffer.allocate(capacity);
  }
}
