// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.lang.foreign.GroupLayout;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

/**
 * A FFI {@link MemoryLayout} corresponding to each of the structure(s) passed to/from RocksDB C++
 * by the Java side API
 * <p>
 * Some layouts/structures are (or will be) used in more than one API call
 * <p>
 * Each layout is associated with a {@link VarHandle} for each of the elements of the layout.
 * The layout and its {@link VarHandle}s are bundled together as static objects of a static class.
 */
public class FFILayout {
  public static class InputSlice {
    public static final GroupLayout Layout =
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

  public static class PinnableSlice {
    public static final GroupLayout Layout = MemoryLayout
                                                 .structLayout(ValueLayout.ADDRESS.withName("data"),
                                                     ValueLayout.JAVA_LONG.withName("size"),
                                                     ValueLayout.ADDRESS.withName("pinnable_slice"),
                                                     ValueLayout.JAVA_BOOLEAN.withName("is_pinned"))
                                                 .withName("pinnable_slice");

    static final VarHandle Data = Layout.varHandle(MemoryLayout.PathElement.groupElement("data"));

    static final VarHandle Size = Layout.varHandle(MemoryLayout.PathElement.groupElement("size"));

    static final VarHandle PinnableSlice =
        Layout.varHandle(MemoryLayout.PathElement.groupElement("pinnable_slice"));

    static final VarHandle IsPinned =
        Layout.varHandle(MemoryLayout.PathElement.groupElement("is_pinned"));
  };

  public static class GetParamsSegment {
    public static final GroupLayout Layout =
        MemoryLayout.structLayout(InputSlice.Layout.withName("input_struct"),
            PinnableSlice.Layout.withName("pinnable_struct"));

    static final long InputStructOffset =
        Layout.byteOffset(MemoryLayout.PathElement.groupElement("input_struct"));
    static final long PinnableStructOffset =
        Layout.byteOffset(MemoryLayout.PathElement.groupElement("pinnable_struct"));
  }

  public static class OutputSlice {
    public static final GroupLayout Layout =
        MemoryLayout
            .structLayout(ValueLayout.ADDRESS.withName("data"),
                ValueLayout.JAVA_LONG.withName("capacity"), ValueLayout.JAVA_LONG.withName("size"))
            .withName("output_slice");

    static final VarHandle Data = Layout.varHandle(MemoryLayout.PathElement.groupElement("data"));

    static final VarHandle Capacity =
        Layout.varHandle(MemoryLayout.PathElement.groupElement("capacity"));

    static final VarHandle Size = Layout.varHandle(MemoryLayout.PathElement.groupElement("size"));
  }
}
