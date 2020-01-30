package org.rocksdb.util;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ByteUtil {

  /**
   * Convert a String to a UTF-8 byte array.
   *
   * @param str the string
   *
   * @return the byte array.
   */
  public static byte[] bytes(final String str) {
    return str.getBytes(UTF_8);
  }

  /**
   * Compares the first {@code count} bytes of two areas of memory.  Returns
   * zero if they are the same, a value less than zero if {@code x} is
   * lexically less than {@code y}, or a value greater than zero if {@code x}
   * is lexically greater than {@code y}.  Note that lexical order is determined
   * as if comparing unsigned char arrays.
   *
   * Similar to <a href="https://github.com/gcc-mirror/gcc/blob/master/libiberty/memcmp.c">memcmp.c</a>.
   *
   * @param x the first value to compare with
   * @param y the second value to compare against
   * @param count the number of bytes to compare
   *
   * @return the result of the comparison
   */
  public static int memcmp(final ByteBuffer x, final ByteBuffer y,
                            final int count) {
    for (int idx = 0; idx < count; idx++) {
      final int aa = x.get(idx) & 0xff;
      final int bb = y.get(idx) & 0xff;
      if (aa != bb) {
        return aa - bb;
      }
    }
    return 0;
  }
}
