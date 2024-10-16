package org.rocksdb;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class WriteBatchJavaNative extends RocksObject implements WriteBatchInterface {

  private final static int DEFAULT_BUFFER_SIZE = 32768;

  private ByteBuffer buffer;

  public WriteBatchJavaNative(final int reserved_bytes) {
    super(newWriteBatchJavaNative(reserved_bytes));
    buffer = ByteBuffer.wrap(new byte[reserved_bytes]).order(ByteOrder.nativeOrder());
  }

  public WriteBatchJavaNative() {
    this(0);
  }

  @Override
  public int count() {
    return 0;
  }

  private void alignBuffer() {
    buffer.position((buffer.position() + 3) & ~3);
  }

  private void wrapBytes() {}

  private void expandable(Runnable r) {
    int current = buffer.position();
    while (true) {
      try {
        r.run();
        return;
      } catch (BufferOverflowException boe) {
        // Build a bigger buffer, positioned before we tried to run r()
        // Then we can run r() again.
        byte[] expanded = new byte[Math.max(buffer.capacity() * 2, DEFAULT_BUFFER_SIZE)];
        System.arraycopy(buffer.array(), 0, expanded, 0, current);
        buffer = ByteBuffer.wrap(expanded, 0, expanded.length).order(ByteOrder.nativeOrder());
        buffer.position(current);
      }
    }
  }

  @Override
  public void put(byte[] key, byte[] value) throws RocksDBException {
    expandable(() -> {
      buffer.putInt(WriteBatchInternal.ValueType.kTypeValue.ordinal());
      buffer.putInt(key.length);
      buffer.putInt(value.length);
      buffer.put(key);
      alignBuffer();
      buffer.put(value);
      alignBuffer();
    });
  }

  public void flush() throws RocksDBException {
    flushWriteBatchJavaNative(nativeHandle_, buffer.position(), buffer.array());
    //TODO - RESET buffer

  }

  @Override
  public void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value)
      throws RocksDBException {
    expandable(() -> {
      buffer.putInt(WriteBatchInternal.ValueType.kTypeColumnFamilyValue.ordinal());
      buffer.putLong(columnFamilyHandle.getNativeHandle());
      buffer.putInt(key.length);
      buffer.putInt(value.length);
      buffer.put(key);
      alignBuffer();
      buffer.put(value);
      alignBuffer();
    });
  }

  @Override
  public void put(ByteBuffer key, ByteBuffer value) throws RocksDBException {}

  @Override
  public void put(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key, ByteBuffer value)
      throws RocksDBException {}

  @Override
  public void merge(byte[] key, byte[] value) throws RocksDBException {}

  @Override
  public void merge(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value)
      throws RocksDBException {}

  @Override
  public void delete(byte[] key) throws RocksDBException {}

  @Override
  public void delete(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {}

  @Override
  public void delete(ByteBuffer key) throws RocksDBException {}

  @Override
  public void delete(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key)
      throws RocksDBException {}

  @Override
  public void singleDelete(byte[] key) throws RocksDBException {}

  @Override
  public void singleDelete(ColumnFamilyHandle columnFamilyHandle, byte[] key)
      throws RocksDBException {}

  @Override
  public void deleteRange(byte[] beginKey, byte[] endKey) throws RocksDBException {}

  @Override
  public void deleteRange(ColumnFamilyHandle columnFamilyHandle, byte[] beginKey, byte[] endKey)
      throws RocksDBException {}

  @Override
  public void putLogData(byte[] blob) throws RocksDBException {}

  @Override
  public void clear() {}

  @Override
  public void setSavePoint() {}

  @Override
  public void rollbackToSavePoint() throws RocksDBException {}

  @Override
  public void popSavePoint() throws RocksDBException {}

  @Override
  public void setMaxBytes(long maxBytes) {}

  @Override
  public WriteBatch getWriteBatch() {
    return new WriteBatch(nativeHandle_);
  }

  @Override
  protected void disposeInternal(long handle) {
    disposeInternalWriteBatchJavaNative(handle);
  }

  private static native long newWriteBatchJavaNative(final int reserved_bytes);

  private static native void disposeInternalWriteBatchJavaNative(final long handle);

  private static native void flushWriteBatchJavaNative(
      final long handle, final long position, final byte[] buf);
}
