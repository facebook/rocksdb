package org.rocksdb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class WriteBatchJavaNativeArray extends WriteBatchJavaNative {
  public WriteBatchJavaNativeArray(final int reserved_bytes) {
    super(reserved_bytes);
  }

  @Override
  protected ByteBuffer allocateBuffer(int reserved_bytes) {
    return ByteBuffer.wrap(new byte[reserved_bytes]).order(ByteOrder.nativeOrder());
  }

  /**
   * Expand a buffer based on a byte[]
   *
   * @param expanded_bytes size of expanded buffer
   * @param original original buffer wrapping a byte[]
   *
   * @return the expanded buffer
   */
  @Override
  protected ByteBuffer expandBuffer(int expanded_bytes, final ByteBuffer original) {
    int currentPosition = original.position();
    byte[] expanded = new byte[expanded_bytes];
    System.arraycopy(original.array(), 0, expanded, 0, currentPosition);
    ByteBuffer buffer =
        ByteBuffer.wrap(expanded, 0, expanded.length).order(ByteOrder.nativeOrder());
    buffer.position(currentPosition);

    return buffer;
  }

  @Override
  public void flush() throws RocksDBException {
    flushWriteBatchJavaNativeArray(nativeHandle_, buffer.position(), buffer.array());
    buffer.clear();
  }
}
