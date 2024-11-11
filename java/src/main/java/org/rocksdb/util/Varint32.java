/**
 * Copyright 2024 Andrew Steinborn
 * (https://steinborn.me/posts/performance/how-fast-can-you-write-a-varint/)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the “Software”), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.rocksdb.util;

import java.nio.ByteBuffer;

public class Varint32 {
  public static void writeNaive(ByteBuffer buf, int value) {
    while (true) {
      if ((value & ~0x7FL) == 0) {
        buf.put((byte) value);
        return;
      } else {
        buf.put((byte) ((value & 0x7F) | 0x80));
        value >>>= 7;
      }
    }
  }

  /**
   * This is not the fastest possible varint32 encoding,
   * but it is fast enough for our purposes.
   * And the alleged fastest version appears to break
   * when we translate it. That is probable me (AP)
   * but I am not going to waste any more time.
   *
   * @param buf
   * @param value
   */
  public static void write(ByteBuffer buf, final int value) {
    if ((value & (0xFFFFFFFF << 7)) == 0) {
      buf.put((byte) value);
    } else {
      buf.put((byte) (value & 0x7F | 0x80));
      if ((value & (0xFFFFFFFF << 14)) == 0) {
        buf.put((byte) (value >>> 7));
      } else {
        buf.put((byte) ((value >>> 7) & 0x7F | 0x80));
        if ((value & (0xFFFFFFFF << 21)) == 0) {
          buf.put((byte) (value >>> 14));
        } else {
          buf.put((byte) ((value >>> 14) & 0x7F | 0x80));
          if ((value & (0xFFFFFFFF << 28)) == 0) {
            buf.put((byte) (value >>> 21));
          } else {
            buf.put((byte) ((value >>> 21) & 0x7F | 0x80));
            buf.put((byte) (value >>> 28));
          }
        }
      }
    }
  }

  public static int numBytes(final int value) {
    return (31 - Integer.numberOfLeadingZeros(value)) / 7;
  }
}