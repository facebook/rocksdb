package org.rocksdb;

import java.lang.foreign.GroupLayout;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

public class FFILayout {
  public static class InputSlice {
    static final GroupLayout Layout =
        MemoryLayout
            .structLayout(
                ValueLayout.ADDRESS.withName("data"), ValueLayout.JAVA_LONG.withName("size"),
                ValueLayout.JAVA_LONG.withName(
                    "ignore_this_padding") // without this, the struct seems to be broken
                )
            .withName("input_slice");

    static final VarHandle Data = Layout.varHandle(MemoryLayout.PathElement.groupElement("data"));

    static final VarHandle Size = Layout.varHandle(MemoryLayout.PathElement.groupElement("size"));
  };

  public static class OutputSlice {
    static final GroupLayout Layout = MemoryLayout
                                          .structLayout(ValueLayout.ADDRESS.withName("data"),
                                              ValueLayout.JAVA_LONG.withName("size"),
                                              ValueLayout.ADDRESS.withName("pinnable_slice"))
                                          .withName("output_slice");

    static final VarHandle Data = Layout.varHandle(MemoryLayout.PathElement.groupElement("data"));

    static final VarHandle Size = Layout.varHandle(MemoryLayout.PathElement.groupElement("size"));

    static final VarHandle PinnableSlice =
        Layout.varHandle(MemoryLayout.PathElement.groupElement("pinnable_slice"));
  };
}
