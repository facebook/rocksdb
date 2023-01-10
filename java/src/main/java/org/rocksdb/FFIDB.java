package org.rocksdb;

import java.lang.foreign.*;
import java.lang.invoke.*;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

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

  public FFIPinnableSlice get(final String key) throws RocksDBException {
    return get(rocksDB.getDefaultColumnFamily(), key);
  }

  public FFIPinnableSlice get(final ColumnFamilyHandle columnFamilyHandle, final String key)
      throws RocksDBException {
    try {
      final MemorySegment keySegment = segmentAllocator.allocateUtf8String(key);
      final MemorySegment inputSegment = segmentAllocator.allocate(FFILayout.InputSlice.Layout);
      FFILayout.InputSlice.Data.set(inputSegment, keySegment.address());
      FFILayout.InputSlice.Size.set(
          inputSegment, keySegment.byteSize() - 1); // ignore null-terminator, of C-style string

      final MemorySegment outputSegment = segmentAllocator.allocate(FFILayout.OutputSlice.Layout);

      final Object result = FFIMethod.Get.invoke(MemoryAddress.ofLong(rocksDB.nativeHandle_),
          MemoryAddress.ofLong(columnFamilyHandle.nativeHandle_), inputSegment.address(),
          outputSegment.address());
      if (!(result instanceof Integer)) {
        throw new RocksDBException("rocksdb_ffi_get.invokeExact returned: " + result);
      }
      final Status.Code code = Status.Code.values()[(Integer) result];
      switch (code) {
        case NotFound -> { return new FFIPinnableSlice(code); }
        case Ok -> {
          final MemoryAddress data = (MemoryAddress) FFILayout.OutputSlice.Data.get(outputSegment);
          final Long size = (Long) FFILayout.OutputSlice.Size.get(outputSegment);

          //TODO (AP) Review whether this is the correct session to use
          //The "never closed" global() session may well be correct,
          //because the underlying pinnable slice should get explicitly cleared by us, not by the session
          final MemorySegment valueSegment = MemorySegment.ofAddress(data, size, MemorySession.global());
          return new FFIPinnableSlice(code, valueSegment, outputSegment);
        }
        default -> throw new RocksDBException(new Status(code, Status.SubCode.None, "[Rocks FFI - no detailed reason provided]"));
      }
    } catch (final Throwable methodException) {
      throw new RocksDBException("Internal error invoking FFI (Java to C++) function call: " +
          methodException.getMessage());
    }
  }
}
