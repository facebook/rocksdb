package org.rocksdb;

import java.nio.ByteBuffer;

public abstract class WriteBatchJavaNative extends RocksObject implements WriteBatchInterface {
  private final static int DEFAULT_BUFFER_SIZE = 32768;

  protected ByteBuffer buffer;

  public WriteBatchJavaNative(final int reserved_bytes) {
    super(newWriteBatchJavaNative(reserved_bytes));
    buffer = allocateBuffer(reserved_bytes);
  }

  protected abstract ByteBuffer allocateBuffer(final int reserved_bytes);

  protected abstract ByteBuffer expandBuffer(final int expanded_bytes, final ByteBuffer original);

  public WriteBatchJavaNative() {
    this(0);
  }

  @Override
  public int count() {
    return 0;
  }

  private void alignBuffer() {
    buffer.position(align(buffer.position()));
  }

  private int align(final int num) {
    return (num + 3) & ~3;
  }

  private void expand(final int requiredBytes) {
    if (buffer.remaining() < requiredBytes) {
      int current = buffer.position();
      int request = buffer.capacity() * 2;
      while (request - current < requiredBytes) request = request + request;
      buffer = expandBuffer(Math.max(request, DEFAULT_BUFFER_SIZE), buffer);
      buffer.position(current);
    }
  }

  @Override
  public void put(byte[] key, byte[] value) throws RocksDBException {
    expand(
        Integer.BYTES // kTypeValue
        + Integer.BYTES // key
        + align(key.length)
        + Integer.BYTES // value
        + align(value.length));

    buffer.putInt(WriteBatchInternal.ValueType.kTypeValue.ordinal());
    buffer.putInt(key.length);
    buffer.putInt(value.length);
    buffer.put(key);
    alignBuffer();
    buffer.put(value);
    alignBuffer();
  }

  public abstract void flush() throws RocksDBException;

  @Override
  public void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value)
      throws RocksDBException {
    expand(Integer.BYTES // kTypeColumnFamilyValue
        + Long.BYTES // columnFamilyHandle
        + Integer.BYTES // key
        + align(key.length
        + Integer.BYTES // value
        + align(value.length)));

    buffer.putInt(WriteBatchInternal.ValueType.kTypeColumnFamilyValue.ordinal());
    buffer.putLong(columnFamilyHandle.getNativeHandle());
    buffer.putInt(key.length);
    buffer.putInt(value.length);
    buffer.put(key);
    alignBuffer();
    buffer.put(value);
    alignBuffer();
  }

  @Override
  public void put(ByteBuffer key, ByteBuffer value) throws RocksDBException {
    expand(
        Integer.BYTES // kTypeValue
            + Integer.BYTES // key
            + align(key.remaining())
            + Integer.BYTES // value
            + align(value.remaining()));

    buffer.putInt(WriteBatchInternal.ValueType.kTypeValue.ordinal());
    buffer.putInt(key.remaining());
    buffer.putInt(value.remaining());
    buffer.put(key);
    alignBuffer();
    buffer.put(value);
    alignBuffer();
  }

  @Override
  public void put(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key, ByteBuffer value)
      throws RocksDBException {
    throw new UnsupportedOperationException(getClass().getName() + "<CF ByteBuffer put()>");
  }

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


  protected static native void flushWriteBatchJavaNativeArray(
      final long handle, final long position, final byte[] buf);

  protected static native void flushWriteBatchJavaNativeDirect(
      final long handle, final long position, final ByteBuffer buf);

}
