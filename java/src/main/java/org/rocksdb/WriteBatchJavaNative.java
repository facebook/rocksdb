package org.rocksdb;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import org.rocksdb.util.Varint32;

public class WriteBatchJavaNative implements WriteBatchInterface, Closeable {
  @SuppressWarnings("PMD.NullAssignment")
  @Override
  public void close() {
    try {
      flush();
    } catch (RocksDBException e) {
      throw new RuntimeException("Could not flush/close WriteBatch", e);
    }
    if (nativeWrapper != null) {
      nativeWrapper.close();
      nativeWrapper = null;
    }
  }

  static class CFIDCache {
    private final Map<Long, Integer> idMap = new HashMap<>(4);

    final int getCFID(final ColumnFamilyHandle columnFamilyHandle) {
      long handle = columnFamilyHandle.getNativeHandle();
      if (!idMap.containsKey(handle)) {
        idMap.put(handle, columnFamilyHandle.getID());
      }
      return idMap.get(handle);
    }
  }

  ThreadLocal<CFIDCache> cfidCacheThreadLocal = ThreadLocal.withInitial(CFIDCache::new);

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

  private final ByteBuffer buffer;
  final int entrySizeLimit;
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
    entrySizeLimit = buffer.capacity() / 2;
    resetBuffer();
  }

  @Override
  public int count() {
    return 0;
  }

  @Override
  public void put(byte[] key, byte[] value) throws RocksDBException {
    int requiredSpace = 1 // kTypeValue
        + Varint32.numBytes(key.length) + key.length + Varint32.numBytes(value.length)
        + value.length;
    if (bufferAvailable(requiredSpace)) {
      entryCount++;
      buffer.put((byte) WriteBatchInternal.ValueType.kTypeValue.ordinal());
      putEntry(key);
      putEntry(value);
    } else {
      setNativeHandle(putWriteBatchJavaNativeArray(
          getNativeHandle(), buffer.capacity(), key, key.length, value, value.length, 0L));
    }
  }

  /**
   * Flush the Java-side buffer to the C++ over JNI
   * Does nothing if the write batch is empty
   * Creates the C++ write batch at this point if it does not already exist,
   * and there is data in the batch to send.
   *
   * @throws RocksDBException
   */
  void flush() throws RocksDBException {
    if (entryCount > 0) {
      buffer.putInt(WriteBatchInternal.kCountOffset, entryCount);

      buffer.flip();
      if (buffer.isDirect()) {
        setNativeHandle(flushWriteBatchJavaNativeDirect(
            // assert position == 0
            getNativeHandle(), buffer.capacity(), buffer, buffer.position(), buffer.limit()));
      } else {
        setNativeHandle(flushWriteBatchJavaNativeArray(
            getNativeHandle(), buffer.capacity(), buffer.array(), buffer.limit()));
      }

      resetBuffer();
    }
  }

  /**
   * Write the write batch to the RocksDB database.
   * <p></p>
   * A C++ write batch may not yet exist.
   * If it does not, the C++ method will create it,
   * and anyway it will flush the Java-side buffer to.
   * <p></p>
   * This is like `flush()` followed by a `write()`, just more efficient;
   * it only crosses the JNI boundary once. There is no separate `write()` method
   * for `WriteBatchJavaNative`.
   *
   * @throws RocksDBException
   */
  void write(final RocksDB db, final WriteOptions writeOptions) throws RocksDBException {
    buffer.putInt(WriteBatchInternal.kCountOffset, entryCount);

    buffer.flip();
    if (buffer.isDirect()) {
      setNativeHandle(
          writeWriteBatchJavaNativeDirect(db.getNativeHandle(), writeOptions.getNativeHandle(),
              // assert position == 0 (we just flipped)
              getNativeHandle(), buffer.capacity(), buffer, buffer.position(), buffer.limit()));
    } else {
      setNativeHandle(
          writeWriteBatchJavaNativeArray(db.getNativeHandle(), writeOptions.getNativeHandle(),
              getNativeHandle(), buffer.capacity(), buffer.array(), buffer.limit()));
    }

    resetBuffer();
  }

  private void resetBuffer() {
    buffer.clear();
    buffer.putLong(0L);
    buffer.putInt(0);

    entryCount = 0;
  }

  private boolean bufferAvailable(int requiredSpace) throws RocksDBException {
    if (buffer.remaining() < requiredSpace) {
      if (entryCount > 0) {
        flush();
      }
    }
    if (requiredSpace > entrySizeLimit || buffer.remaining() < requiredSpace) {
      // tell the caller not to use the buffer, it isn't big enough
      // they should instead use the direct mode over JNI
      return false;
    }

    return true;
  }

  void setSequence(final long sequence) {
    buffer.putLong(WriteBatchInternal.kSequenceOffset, sequence);
  }

  private void putEntry(final byte[] array) {
    putVarint32(array.length);
    buffer.put(array);
  }

  @Override
  public void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value)
      throws RocksDBException {
    int columnFamilyID = cfidCacheThreadLocal.get().getCFID(columnFamilyHandle);
    int requiredSpace = 1 // kTypeColumnFamilyValue
        + Varint32.numBytes(columnFamilyID) + Varint32.numBytes(key.length) + key.length
        + Varint32.numBytes(value.length) + value.length;

    if (bufferAvailable(requiredSpace)) {
      entryCount++;
      buffer.put((byte) WriteBatchInternal.ValueType.kTypeColumnFamilyValue.ordinal());
      putVarint32(columnFamilyID);
      putEntry(key);
      putEntry(value);
    } else {
      setNativeHandle(putWriteBatchJavaNativeArray(getNativeHandle(), buffer.capacity(), key,
          key.length, value, value.length, columnFamilyHandle.getNativeHandle()));
    }
  }

  private void putVarint32(final int value) {
    Varint32.write(buffer, value);
  }

  private void putEntry(final ByteBuffer bb) {
    putVarint32(bb.remaining());
    buffer.put(bb);
  }

  @Override
  public void put(ByteBuffer key, ByteBuffer value) throws RocksDBException {
    int requiredSpace = 1 // kTypeValue
        + Varint32.numBytes(key.remaining()) + key.remaining()
        + Varint32.numBytes(value.remaining()) + value.remaining();

    if (bufferAvailable(requiredSpace)) {
      entryCount++;
      buffer.put((byte) WriteBatchInternal.ValueType.kTypeValue.ordinal());
      putEntry(key);
      putEntry(value);
    } else {
      setNativeHandle(putWriteBatchJavaNativeDirect(getNativeHandle(), buffer.capacity(), key,
          key.position(), key.remaining(), value, value.position(), value.remaining(), 0L));
      key.position(key.limit());
      value.position(value.limit());
    }
  }

  @Override
  public void put(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key, ByteBuffer value)
      throws RocksDBException {
    int columnFamilyID = cfidCacheThreadLocal.get().getCFID(columnFamilyHandle);
    int requiredSpace = 1 // kTypeColumnFamilyValue
        + Varint32.numBytes(columnFamilyID) + Varint32.numBytes(key.remaining()) + key.remaining()
        + Varint32.numBytes(value.remaining()) + value.remaining();

    if (bufferAvailable(requiredSpace)) {
      entryCount++;
      buffer.put((byte) WriteBatchInternal.ValueType.kTypeColumnFamilyValue.ordinal());
      putVarint32(columnFamilyID);
      putEntry(key);
      putEntry(value);
    } else {
      setNativeHandle(putWriteBatchJavaNativeDirect(getNativeHandle(), buffer.capacity(), key,
          key.position(), key.remaining(), value, value.position(), value.remaining(),
          columnFamilyHandle.getNativeHandle()));
      key.position(key.limit());
      value.position(value.limit());
    }
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

  private void setNativeHandle(final long newHandle) {
    if (nativeWrapper == null) {
      nativeWrapper = new NativeWrapper(newHandle);
    } else {
      assert nativeWrapper.nativeHandle_ == newHandle;
    }
  }

  private static native void disposeInternalWriteBatchJavaNative(final long handle);

  private static native long flushWriteBatchJavaNativeArray(
      final long handle, final long capacity, final byte[] buf, final int bufLen);

  private static native long flushWriteBatchJavaNativeDirect(final long handle, final long capacity,
      final ByteBuffer buf, final int bufPosition, final int bufLimit);
  private static native long writeWriteBatchJavaNativeArray(final long dbHandle,
      final long woHandle, final long handle, final long capacity, final byte[] buf,
      final int bufLen);

  private static native long writeWriteBatchJavaNativeDirect(final long dbHandle,
      final long woHandle, final long handle, final long capacity, final ByteBuffer buf,
      final int bufPos, final int bufLimit);

  private static native long putWriteBatchJavaNativeArray(final long handle, final long capacity,
      final byte[] key, final int keyLen, final byte[] value, final int valueLen,
      final long cfHandle);

  private static native long putWriteBatchJavaNativeDirect(final long handle, final long capacity,
      final ByteBuffer key, final int keyPosition, final int keyRemaining, final ByteBuffer value,
      final int valuePosition, final int valueRemaining, final long cfHandle);
}
