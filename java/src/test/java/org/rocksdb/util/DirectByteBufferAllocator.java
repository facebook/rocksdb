package org.rocksdb.util;

import java.nio.ByteBuffer;

public final class DirectByteBufferAllocator implements ByteBufferAllocator {
  DirectByteBufferAllocator(){};

  @Override
  public ByteBuffer allocate(final int capacity) {
    return ByteBuffer.allocateDirect(capacity);
  }
}
