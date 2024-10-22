package org.rocksdb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class WriteBatchJavaNativeDirect extends WriteBatchJavaNative {
  public WriteBatchJavaNativeDirect(final int reserved_bytes) {
    super(reserved_bytes);
  }

  @Override
  protected ByteBuffer allocateBuffer(int reserved_bytes) {
    return ByteBuffer.allocateDirect(reserved_bytes).order(ByteOrder.nativeOrder());
  }

  @Override
  protected ByteBuffer expandBuffer(int expanded_bytes, ByteBuffer original) {
    ByteBuffer expanded = ByteBuffer.allocateDirect(expanded_bytes).order(ByteOrder.nativeOrder());
    original.flip();
    expanded.put(original);

    return expanded;
  }

  @Override
  public void flush() throws RocksDBException {
    flushWriteBatchJavaNativeDirect(nativeHandle_, buffer.position(), buffer);
    buffer.clear();
  }

}
