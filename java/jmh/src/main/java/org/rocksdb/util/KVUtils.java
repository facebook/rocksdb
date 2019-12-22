/**
 * Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
 *  This source code is licensed under both the GPLv2 (found in the
 *  COPYING file in the root directory) and Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory).
 */
package org.rocksdb.util;

import static java.nio.charset.StandardCharsets.UTF_8;

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
}
