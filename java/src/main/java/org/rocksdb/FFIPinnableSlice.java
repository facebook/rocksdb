package org.rocksdb;

import java.lang.foreign.MemorySegment;
import java.util.Optional;

public class FFIPinnableSlice {
  public final Status.Code code;
  public final Optional<MemorySegment> data;
  private final Optional<MemorySegment> outputSlice;
  FFIPinnableSlice(
      final Status.Code code, final MemorySegment data, final MemorySegment outputSlice) {
    this.code = code;
    this.data = Optional.of(data);
    this.outputSlice = Optional.of(outputSlice);
  }

  FFIPinnableSlice(final Status.Code code) {
    this.code = code;
    this.data = Optional.empty();
    this.outputSlice = Optional.empty();
  }

  final Status.Code reset() throws RocksDBException {
    if (outputSlice.isEmpty()) {
      return Status.Code.Ok;
    }

    try {
      final Object result = FFIMethod.ResetOutput.invoke(outputSlice.get().address());
      return Status.Code.values()[(Integer) result];
    } catch (final Throwable methodException) {
      throw new RocksDBException("Internal error invoking FFI (Java to C++) function call: "
          + methodException.getMessage());
    }
  }
}
