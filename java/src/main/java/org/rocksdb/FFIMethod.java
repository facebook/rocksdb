package org.rocksdb;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.util.Optional;

public class FFIMethod {
  public static MethodHandle Lookup(final String name, final FunctionDescriptor functionDescriptor)
      throws RocksDBException {
    final Linker linker = Linker.nativeLinker();
    final SymbolLookup symbolLookup = SymbolLookup.loaderLookup();
    final Optional<MemorySegment> symbolSegment = symbolLookup.lookup(name);

    if (symbolSegment.isPresent()) {
      return linker.downcallHandle(symbolSegment.get(), functionDescriptor);
    }
    throw new RocksDBException(
        "FFI linker lookup failed for name: " + name + ", function: " + functionDescriptor);
  }

  static {
    try {
      Get = Lookup("rocksdb_ffi_get",
          FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
              ValueLayout.ADDRESS, ValueLayout.ADDRESS));
      ResetOutput = Lookup("rocksdb_ffi_reset_output",
          FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
    } catch (final RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public static MethodHandle Get;
  public static MethodHandle ResetOutput;
}
