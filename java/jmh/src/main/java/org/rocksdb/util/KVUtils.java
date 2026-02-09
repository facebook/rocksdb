/**
 * Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
 *  This source code is licensed under both the GPLv2 (found in the
 *  COPYING file in the root directory) and Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory).
 */
package org.rocksdb.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class KVUtils {

  /**
   * Get a byte array from a string.
   *
   * Assumes UTF-8 encoding
   *
   * @param string the string
   *
   * @return the bytes.
   */
  public static byte[] ba(final String string) {
    return string.getBytes(UTF_8);
  }

  /**
   * Get a string from a byte array.
   *
   * Assumes UTF-8 encoding
   *
   * @param bytes the bytes
   *
   * @return the string.
   */
  public static String str(final byte[] bytes) {
    return new String(bytes, UTF_8);
  }

  /**
   * Get a list of keys where the keys are named key1..key1+N
   * in the range of {@code from} to {@code to} i.e. keyFrom..keyTo.
   *
   * @param from the first key
   * @param to the last key
   *
   * @return the array of keys
   */
  public static List<byte[]> keys(final int from, final int to) {
    final List<byte[]> keys = new ArrayList<>(to - from);
    for (int i = from; i < to; i++) {
      keys.add(ba("key" + i));
    }
    return keys;
  }

  public static List<ByteBuffer> keys(
      final List<ByteBuffer> keyBuffers, final int from, final int to) {
    final List<ByteBuffer> keys = new ArrayList<>(to - from);
    for (int i = from; i < to; i++) {
      final ByteBuffer key = keyBuffers.get(i);
      key.clear();
      key.put(ba("key" + i));
      key.flip();
      keys.add(key);
    }
    return keys;
  }
}
