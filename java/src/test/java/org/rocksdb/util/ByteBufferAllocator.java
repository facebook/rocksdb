package org.rocksdb.util;

import java.nio.ByteBuffer;

public interface ByteBufferAllocator {
  ByteBuffer allocate(int capacity);

  static class Direct implements ByteBufferAllocator {
    @Override
    public ByteBuffer allocate(final int capacity) {
      return ByteBuffer.allocateDirect(capacity);
    }
  }

  static class Indirect implements ByteBufferAllocator {
    @Override
    public ByteBuffer allocate(final int capacity) {
      return ByteBuffer.allocate(capacity);
    }
  }
}
