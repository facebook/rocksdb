package org.rocksdb;

import java.lang.foreign.*;
import java.util.Optional;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class FFIDB implements AutoCloseable {
  static {
    RocksDB.loadLibrary();
  }

  private final RocksDB rocksDB;

  private final MemorySession memorySession;
  private final SegmentAllocator segmentAllocator;

  public FFIDB(final RocksDB rocksDB) throws RocksDBException {
    this.rocksDB = rocksDB;

    memorySession = MemorySession.openConfined();
    segmentAllocator = SegmentAllocator.newNativeArena(memorySession);
  }

  public RocksDB getRocksDB() {
    return this.rocksDB;
  }

  @Override
  public void close() {
    if (memorySession != null)
      memorySession.close();
  }

  public static void copy(final MemorySegment addr, final byte[] bytes) {
    final var heapSegment = MemorySegment.ofArray(bytes);
    addr.copyFrom(heapSegment);
    addr.set(JAVA_BYTE, bytes.length, (byte)0);
  }

  public record GetBytes(Status.Code code, byte[] value, long size) {

    /**
     * Convert the pinnable slice based result into a byte[]-based result
     * <p/>
     * Reset the pinnable slice returned, as it does not need to be held any more;
     * the caller should not use it after this point as it is not valid.
     *
     * @param slice to copy from
     * @param value to copy to
     * @return an object containing status (and the byte[] if the status is ok)
     * @throws RocksDBException
     */
    static GetBytes fromPinnable(final GetPinnableSlice slice, final byte[] value) throws RocksDBException {
      if (slice.code == Status.Code.Ok) {
        final var pinnableSlice = slice.pinnableSlice().get();
        final var size = pinnableSlice.data().byteSize();
        pinnableSlice.data().asByteBuffer().get(0, value, 0, (int) Math.min(size, value.length));
        pinnableSlice.reset();
        return new GetBytes(slice.code, value, size);
      } else {
        return new GetBytes(slice.code, value, 0);
      }
    }
  }
  public GetBytes get(final ColumnFamilyHandle columnFamilyHandle, final byte[] key, final byte[] value) throws RocksDBException {
    return GetBytes.fromPinnable(getPinnableSlice(columnFamilyHandle, key), value);
  }

  public GetBytes get(final ColumnFamilyHandle columnFamilyHandle, final byte[] key) throws RocksDBException {
    final var pinnable = getPinnableSlice(columnFamilyHandle, key);
    byte[] value = null;
    if (pinnable.code == Status.Code.Ok) {
      var pinnableSlice = pinnable.pinnableSlice().get();
      value = new byte[(int) pinnableSlice.data().byteSize()];
    }
    return GetBytes.fromPinnable(pinnable, value);
  }

  public GetBytes get(final byte[] key) throws RocksDBException {
    return get(rocksDB.getDefaultColumnFamily(), key);
  }

  public record GetPinnableSlice(Status.Code code, Optional<FFIPinnableSlice> pinnableSlice) {}

  public GetPinnableSlice getPinnableSlice(final byte[] key) throws RocksDBException {
    return getPinnableSlice(rocksDB.getDefaultColumnFamily(), key);
  }

  /**
   * Get the value of the supplied key in the indicated column family from RocksDB
   *
   * @param columnFamilyHandle column family containing the value to read
   * @param key of the value to read
   * @return an object wrapping status and (if the status is ok) a pinnable slice referring to the value of the key
   * @throws RocksDBException
   */
  public GetPinnableSlice getPinnableSlice(
      final ColumnFamilyHandle columnFamilyHandle, final byte[] key) throws RocksDBException {
    final MemorySegment keySegment = segmentAllocator.allocate(key.length + 1);
    copy(keySegment, key);
    final MemorySegment inputSegment = segmentAllocator.allocate(FFILayout.InputSlice.Layout);
    FFILayout.InputSlice.Data.set(inputSegment, keySegment.address());
    FFILayout.InputSlice.Size.set(
        inputSegment, keySegment.byteSize() - 1); // ignore null-terminator, of C-style string

    final MemorySegment outputSegment = segmentAllocator.allocate(FFILayout.OutputSlice.Layout);

    final Object result;
    try {
      result = FFIMethod.Get.invoke(MemoryAddress.ofLong(rocksDB.nativeHandle_),
          MemoryAddress.ofLong(columnFamilyHandle.nativeHandle_), inputSegment.address(),
          outputSegment.address());
    } catch (final Throwable methodException) {
      throw new RocksDBException("Internal error invoking FFI (Java to C++) function call: "
          + methodException.getMessage());
    }
    if (!(result instanceof Integer)) {
      throw new RocksDBException("rocksdb_ffi_get.invokeExact returned: " + result);
    }
    final Status.Code code = Status.Code.values()[(Integer) result];
    switch (code) {
      case NotFound -> { return new GetPinnableSlice(code, Optional.empty()); }
      case Ok -> {
        final MemoryAddress data = (MemoryAddress) FFILayout.OutputSlice.Data.get(outputSegment);
        final Long size = (Long) FFILayout.OutputSlice.Size.get(outputSegment);

        //TODO (AP) Review whether this is the correct session to use
        //The "never closed" global() session may well be correct,
        //because the underlying pinnable slice should get explicitly cleared by us, not by the session
        final MemorySegment valueSegment = MemorySegment.ofAddress(data, size, MemorySession.global());
        return new GetPinnableSlice(code, Optional.of(new FFIPinnableSlice(valueSegment, outputSegment)));
      }
      default -> throw new RocksDBException(new Status(code, Status.SubCode.None, "[Rocks FFI - no detailed reason provided]"));
    }
  }
}
