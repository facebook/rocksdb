package org.rocksdb.util;

import java.nio.ByteBuffer;

public interface ByteBufferAllocator {
  ByteBuffer allocate(int capacity);

  ByteBufferAllocator DIRECT = new DirectByteBufferAllocator();
  ByteBufferAllocator HEAP = new HeapByteBufferAllocator();
}
