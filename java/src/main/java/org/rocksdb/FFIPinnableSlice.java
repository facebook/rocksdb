package org.rocksdb;

import java.lang.foreign.MemorySegment;
import java.util.Optional;

public record FFIPinnableSlice(MemorySegment data, MemorySegment outputSlice) {

  public void reset() throws RocksDBException {
    try {
      FFIMethod.ResetOutput.invoke(outputSlice.address());
    } catch (final Throwable methodException) {
      throw new RocksDBException("Internal error invoking FFI (Java to C++) function call: "
          + methodException.getMessage());
    }
  }
}
