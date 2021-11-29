package org.rocksdb.util;

import java.nio.ByteBuffer;

public interface ByteBufferAllocator {
  ByteBuffer allocate(int capacity);

  ByteBufferAllocator DIRECT = new DirectByteBufferAllocator();
  ByteBufferAllocator INDIRECT = new IndirectByteBufferAllocator();

  final class DirectByteBufferAllocator implements ByteBufferAllocator {
    private DirectByteBufferAllocator(){};

    @Override
    public ByteBuffer allocate(final int capacity) {
      return ByteBuffer.allocateDirect(capacity);
    }
  }

  final class IndirectByteBufferAllocator implements ByteBufferAllocator {
    private IndirectByteBufferAllocator(){};

    @Override
    public ByteBuffer allocate(final int capacity) {
      return ByteBuffer.allocate(capacity);
    }
  }
}
