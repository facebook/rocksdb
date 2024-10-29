package org.rocksdb;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class WriteBatchJavaNative implements WriteBatchInterface, Closeable {
  @Override
  public void close() {
    try {
      flush();
    } catch (RocksDBException e) {
      throw new RuntimeException("Could not flush/close WriteBatch");
    }
    if (nativeWrapper != null) {
      nativeWrapper.close();
      nativeWrapper = null;
    }
  }

  static class NativeWrapper extends RocksObject {
    protected NativeWrapper(long nativeHandle) {
      super(nativeHandle);
    }

    @Override
    protected void disposeInternal(long handle) {
      disposeInternalWriteBatchJavaNative(handle);
    }
  }

  private NativeWrapper nativeWrapper;

  private ByteBuffer buffer;
  private int entryCount;

  public static WriteBatchJavaNative allocate(int reserved_bytes) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(reserved_bytes + WriteBatchInternal.kHeaderEnd)
                                .order(ByteOrder.LITTLE_ENDIAN);
    return new WriteBatchJavaNative(byteBuffer);
  }

  public static WriteBatchJavaNative allocateDirect(int reserved_bytes) {
    ByteBuffer byteBuffer =
        ByteBuffer.allocateDirect(reserved_bytes + WriteBatchInternal.kHeaderEnd)
            .order(ByteOrder.LITTLE_ENDIAN);
    return new WriteBatchJavaNative(byteBuffer);
  }

  private WriteBatchJavaNative(final ByteBuffer buffer) {
    super();
    this.buffer = buffer;
    resetBuffer();
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

  @Override
  public void put(byte[] key, byte[] value) throws RocksDBException {
    flushIfFull(Integer.BYTES // kTypeValue
        + Integer.BYTES // key
        + align(key.length) + Integer.BYTES // value
        + align(value.length));

    entryCount++;
    buffer.putInt(WriteBatchInternal.ValueType.kTypeValue.ordinal());
    buffer.putInt(key.length);
    buffer.putInt(value.length);
    buffer.put(key);
    alignBuffer();
    buffer.put(value);
    alignBuffer();
  }

  /**
   * Flush the Java-side buffer to the C++ over JNI
   * Create the C++ write batch at this point if it does not exist.
   *
   * @throws RocksDBException
   */
  void flush() throws RocksDBException {
    buffer.putInt(WriteBatchInternal.kCountOffset, entryCount);

    long nativeHandle = 0L;
    if (nativeWrapper != null) {
      nativeHandle = nativeWrapper.nativeHandle_;
    }
    long result;

    if (buffer.isDirect()) {
      result = flushWriteBatchJavaNativeDirect(
          nativeHandle, buffer.capacity(), buffer.position(), buffer);
    } else {
      result = flushWriteBatchJavaNativeArray(
          nativeHandle, buffer.capacity(), buffer.position(), buffer.array());
    }
    if (nativeWrapper == null) {
      nativeWrapper = new NativeWrapper(result);
    }

    resetBuffer();
  }

  /**
   * Write the write batch to the RocksDB database.
   *
   * A C++ write batch may not yet exist.
   * If it does not, the C++ method will create it,
   * and anyway it will flush the Java-side buffer to.
   *
   * This is like `flush()` followed by a `write()`, just more efficient;
   * it only crosses the JNI boundary once. There is no separate `write()` method
   * for `WriteBatchJavaNative`.
   *
   * @throws RocksDBException
   */
  void write(final RocksDB db, final WriteOptions writeOptions) throws RocksDBException {
    final long sequence = buffer.getLong(WriteBatchInternal.kSequenceOffset);
    buffer.putInt(WriteBatchInternal.kCountOffset, entryCount);

    long nativeHandle = 0L;
    if (nativeWrapper != null) {
      nativeHandle = nativeWrapper.nativeHandle_;
    }
    long result;
    if (buffer.isDirect()) {
      result = writeWriteBatchJavaNativeDirect(db.getNativeHandle(), writeOptions.getNativeHandle(),
          nativeHandle, buffer.capacity(), buffer.position(), buffer);
    } else {
      result = writeWriteBatchJavaNativeArray(db.getNativeHandle(), writeOptions.getNativeHandle(),
          nativeHandle, buffer.capacity(), buffer.position(), buffer.array());
    }
    if (nativeWrapper == null) {
      nativeWrapper = new NativeWrapper(result);
    }

    resetBuffer();
  }

  private void resetBuffer() {
    buffer.clear();
    buffer.putLong(0L);
    buffer.putInt(0);

    entryCount = 0;
  }

  private void flushIfFull(int requiredSpace) throws RocksDBException {
    if (buffer.remaining() < requiredSpace) {
      if (entryCount > 0) {
        flush();
      }
    }
    if (buffer.remaining() < requiredSpace) {
      // empty buffer is not big enough, so extend
      throw new RocksDBException("Ni!");
    }
  }

  void setSequence(final long sequence) {
    buffer.putLong(WriteBatchInternal.kSequenceOffset, sequence);
  }

  void countEntry() {
    entryCount++;
  }

  @Override
  public void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value)
      throws RocksDBException {
    flushIfFull(Integer.BYTES // kTypeColumnFamilyValue
        + Long.BYTES // columnFamilyHandle
        + Integer.BYTES // key
        + align(key.length + Integer.BYTES // value
            + align(value.length)));

    entryCount++;
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
    flushIfFull(Integer.BYTES // kTypeValue
        + Integer.BYTES // key
        + align(key.remaining()) + Integer.BYTES // value
        + align(value.remaining()));

    entryCount++;
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
    flushIfFull(Integer.BYTES // kTypeColumnFamilyValue
        + Long.BYTES // columnFamilyHandle
        + Integer.BYTES // key
        + align(key.remaining() + Integer.BYTES // value
            + align(value.remaining())));

    entryCount++;
    buffer.putInt(WriteBatchInternal.ValueType.kTypeColumnFamilyValue.ordinal());
    buffer.putLong(columnFamilyHandle.getNativeHandle());
    buffer.putInt(key.remaining());
    buffer.putInt(value.remaining());
    buffer.put(key);
    alignBuffer();
    buffer.put(value);
    alignBuffer();
  }

  @Override
  public void merge(byte[] key, byte[] value) throws RocksDBException {
    throw new UnsupportedOperationException(
        getClass().getName() + "<WriteBatchJavaNative merge()>");
  }

  @Override
  public void merge(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value)
      throws RocksDBException {
    throw new UnsupportedOperationException(
        getClass().getName() + "<WriteBatchJavaNative CF merge()>");
  }

  @Override
  public void delete(byte[] key) throws RocksDBException {
    throw new UnsupportedOperationException(
        getClass().getName() + "<WriteBatchJavaNative delete()>");
  }

  @Override
  public void delete(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
    throw new UnsupportedOperationException(
        getClass().getName() + "<WriteBatchJavaNative CF delete()>");
  }

  @Override
  public void delete(ByteBuffer key) throws RocksDBException {
    throw new UnsupportedOperationException(
        getClass().getName() + "<WriteBatchJavaNative ByteBuffer delete()>");
  }

  @Override
  public void delete(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key)
      throws RocksDBException {
    throw new UnsupportedOperationException(
        getClass().getName() + "<WriteBatchJavaNative CF ByteBuffer delete()>");
  }

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
    if (nativeWrapper == null) {
      return null;
    } else {
      return new WriteBatch(nativeWrapper.nativeHandle_);
    }
  }

  public long getNativeHandle() {
    if (nativeWrapper == null) {
      return 0L;
    }
    return nativeWrapper.nativeHandle_;
  }

  private static native void disposeInternalWriteBatchJavaNative(final long handle);

  private static native long flushWriteBatchJavaNativeArray(
      final long handle, final long capacity, final long position, final byte[] buf);

  private static native long flushWriteBatchJavaNativeDirect(
      final long handle, final long capacity, final long position, final ByteBuffer buf);
  private static native long writeWriteBatchJavaNativeArray(final long dbHandle,
      final long woHandle, final long handle, final long capacity, final long position,
      final byte[] buf);

  private static native long writeWriteBatchJavaNativeDirect(final long dbHandle,
      final long woHandle, final long handle, final long capacity, final long position,
      final ByteBuffer buf);
}
