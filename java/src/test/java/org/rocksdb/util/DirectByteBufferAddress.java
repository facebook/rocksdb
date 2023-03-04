//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

public final class DirectByteBufferAddress {
  private DirectByteBufferAddress(){};

  public static long getAddress(ByteBuffer buf) {
    try {
      return ADDRESS_FIELD.getLong(buf);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static final Field ADDRESS_FIELD;

  static {
    Field addressField = null;

    try {
      addressField = Buffer.class.getDeclaredField("address");
      addressField.setAccessible(true);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    } finally {
      ADDRESS_FIELD = addressField;
    }
  }
}
