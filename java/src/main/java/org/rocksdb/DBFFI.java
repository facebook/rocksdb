package org.rocksdb;

import java.lang.foreign.*;
import java.lang.invoke.*;
import java.util.Optional;

public class DBFFI implements AutoCloseable {

  static {
    RocksDB.loadLibrary();
  }

  private final RocksDB rocksDB;
  private final GroupLayout inputSliceLayout;
  private final GroupLayout outputSliceLayout;
  private final MemorySegment symbolSegment_rocksdb_ffi_get;
  private final FunctionDescriptor functionDescriptor_rocksdb_ffi_get;
  private final MethodHandle methodHandle_rocksdb_ffi_get;
  private final MemorySession memorySession;
  private final SegmentAllocator segmentAllocator;

  public DBFFI(final RocksDB rocksDB) throws RocksDBException {
    this.rocksDB = rocksDB;

    //final String name = "Java_org_rocksdb_RocksDB_getDirect";
    final String name = "rocksdb_ffi_get";

    final SymbolLookup symbolLookup = SymbolLookup.loaderLookup();
    final Optional<MemorySegment> symbolSegment = symbolLookup.lookup(name);

    if (symbolSegment.isEmpty()) {
      throw new RocksDBException("FFI linker lookup failed for name: " + name);
    }
    symbolSegment_rocksdb_ffi_get = symbolSegment.get();

    inputSliceLayout = MemoryLayout.structLayout(
        ValueLayout.ADDRESS.withName("data"),
        ValueLayout.JAVA_LONG.withName("size"),
        ValueLayout.JAVA_LONG.withName("ignore_this_padding") // without this, the struct seems to be broken
    ).withName("input_slice");

    outputSliceLayout = MemoryLayout.structLayout(
        ValueLayout.ADDRESS.withName("data"),
        ValueLayout.JAVA_LONG.withName("size"),
        ValueLayout.ADDRESS.withName("pinnable_slice")
    ).withName("output_slice");

    functionDescriptor_rocksdb_ffi_get =
        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, inputSliceLayout, outputSliceLayout);

    final Linker linker = Linker.nativeLinker();
    methodHandle_rocksdb_ffi_get = linker.downcallHandle(
        symbolSegment_rocksdb_ffi_get,
        functionDescriptor_rocksdb_ffi_get);

    memorySession = MemorySession.openConfined();
    segmentAllocator = SegmentAllocator.newNativeArena(memorySession);
  }

  public RocksDB getRocksDB() {
    return this.rocksDB;
  }

  @Override
  public void close() {
    if (memorySession != null) memorySession.close();
  }

  public Status.Code get(final String key) throws RocksDBException {
    return get(rocksDB.getDefaultColumnFamily(), key);
  }

  public Status.Code get(final ColumnFamilyHandle columnFamilyHandle, final String key) throws RocksDBException {

    try {

      // TODO (AP) cache the handles as class members
      final MemorySegment keySegment = segmentAllocator.allocateUtf8String(key);
      final MemorySegment inputSegment = segmentAllocator.allocate(inputSliceLayout);
      final VarHandle keyData
          = inputSliceLayout.varHandle(MemoryLayout.PathElement.groupElement("data"));
      keyData.set(inputSegment, keySegment.address());
      final VarHandle keySize
          = inputSliceLayout.varHandle(MemoryLayout.PathElement.groupElement("size"));
      keySize.set(inputSegment, keySegment.byteSize() - 1); // ignore null-terminator, of C-style string

      final MemorySegment outputSegment = segmentAllocator.allocate(outputSliceLayout);

      final Object result = methodHandle_rocksdb_ffi_get.invoke(
          MemoryAddress.ofLong(rocksDB.nativeHandle_),
          MemoryAddress.ofLong(columnFamilyHandle.nativeHandle_),
          inputSegment,
          outputSegment);
      if (!(result instanceof Integer)) {
        throw new RocksDBException("rocksdb_ffi_get.invokeExact returned: " + result);
      }
      final Status.Code code = Status.Code.values()[(Integer)result];
      switch (code) {
        case NotFound -> { return code; }
        case Ok -> {
          final VarHandle valueData
              = outputSliceLayout.varHandle(MemoryLayout.PathElement.groupElement("data"));
          final MemoryAddress data = (MemoryAddress) valueData.get(outputSegment);
          final VarHandle valueSize
              = outputSliceLayout.varHandle(MemoryLayout.PathElement.groupElement("size"));
          final Long size = (Long) valueSize.get(outputSegment);

          //Review whether this is the correct session to use
          //The "never closed" global() session may well be correct,
          //because the underlying pinnable slice should get explicitly cleared by us, not by the session
          final MemorySegment valueSegment = MemorySegment.ofAddress(data, size, MemorySession.global());

          return code;
        }
        default -> { throw new RocksDBException(new Status(code, Status.SubCode.None, "[Rocks FFI - no detailed reason provided]")); }
      }
    } catch (final Throwable methodException) {
      throw new RocksDBException("Internal error invoking FFI (Java to C++) function call: " +
          methodException.getMessage());
    }
  }
}
