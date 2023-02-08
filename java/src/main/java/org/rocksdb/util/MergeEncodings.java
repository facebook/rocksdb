// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Java implementations of merge encodings for merge operators including {@link
 * org.rocksdb.Int64AddOperator}
 */
public class MergeEncodings {
  /**
   * Zigzag decode an unsigned long value to a signed long value
   * Use the parameter to index into the list [0,-1,1,-2,2,-3,3,...]
   * unsigned integers are in 1:1 mapping to the signed integers
   *
   * @param zigzag to decode
   * @return the value at {@code zigzag}'s index in the list [0,-1,1,-2,2,-3,3,...]
   */
  public static long fromZigzag(final long zigzag) {
    final long half = zigzag / 2;
    return (half + half == zigzag) ? half : -half - 1;
  }

  /**
   * Zigzag encode a signed long value to an unsigned long value
   * Each signed long integer is represented by its position in the list [0,-1,1,-2,2,-3,3,...]
   * unsigned integers are in 1:1 mapping to the signed integers
   *
   * @param value to encode
   * @return {@code value}'s position in the list [0,-1,1,-2,2,-3,3,...]
   */
  public static long toZigzag(final long value) {
    final long MAX_ZIGZAG = Long.MAX_VALUE / 2;
    final long MIN_ZIGZAG = Long.MIN_VALUE / 2;
    if (value < MIN_ZIGZAG || value > MAX_ZIGZAG) {
      throw new IllegalArgumentException(
          "Zigzag can only be applied to Long.MIN_VALUE/2..Long.MAX_VALUE/2, parameter is "
          + value);
    }
    final long twice = value + value;
    return (value >= 0) ? twice : -twice - 1;
  }

  public static int encodingSizeVarint(long value) {
    int bytes = 0;
    while (value >= 128) {
      value = value >>> 7;
      bytes++;
    }
    return bytes + 1;
  }

  public static byte[] encodeVarint(long value) {
    if (value < 0) {
      throw new IllegalArgumentException(
          "Varint encoding cannot be applied to negative values, value is " + value);
    }
    final byte[] bytes = new byte[encodingSizeVarint(value)];
    int i = 0;
    while (value >= 128) {
      bytes[i++] = (byte) (128 | (value & 127));
      value = value >>> 7;
    }
    bytes[i] = (byte) value;

    return bytes;
  }

  public static long decodeVarint(final byte[] bytes) {
    int i = 0;
    int shift = 0;
    long acc = 0;
    while (bytes[i] < 0) { // continuation bit reads as -ve 2s complement
      acc |= (long) (bytes[i++] & 127) << shift;
      shift += 7;
    }
    acc |= (long) (bytes[i++]) << shift;

    return acc;
  }

  /**
   * Encode a signed value
   * @param value to encode
   * @return the signed value's encoding as a variable-length run of bytes
   */
  public static byte[] encodeSigned(final long value) {
    return encodeVarint(toZigzag(value));
  }

  /**
   * Decode a run of bytes into a signed values
   * @param bytes to decode
   * @return the signed value represented by the encoding
   */
  public static long decodeSigned(final byte[] bytes) {
    return fromZigzag(decodeVarint(bytes));
  }

  /**
   * Fixed-length encode a long into a byte array
   * @param l the long value to encode
   * @return the byte array into which the value is encoded
   */
  public static byte[] longToByteArray(final long l) {
    final ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    buf.putLong(l);
    return buf.array();
  }

  /**
   * Decode a fixed-length byte array into a long value
   * @param a the byte-array to decode
   * @return the long value encoded in the input array
   */
  public static long longFromByteArray(final byte[] a) {
    final ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    buf.put(a);
    buf.position(Math.max(buf.position(), Long.BYTES)); // guard against BufferOverflowException
    buf.flip();
    return buf.getLong();
  }
}
