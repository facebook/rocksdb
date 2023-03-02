// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.rocksdb.util.MergeEncodings.*;
import static org.rocksdb.util.MergeEncodings.encodeVarint;

import org.junit.Test;

import java.util.Arrays;

public class MergeEncodingsTest {
  @Test
  public void testZigzagEncoding() {
    assertThat(toZigzag(0)).isEqualTo(0);
    assertThat(toZigzag(-1)).isEqualTo(1);
    assertThat(toZigzag(1)).isEqualTo(2);
    assertThat(toZigzag(-2)).isEqualTo(3);
    assertThat(toZigzag(2)).isEqualTo(4);
    assertThat(toZigzag(-3)).isEqualTo(5);
    assertThat(toZigzag(3)).isEqualTo(6);
    assertThat(toZigzag(-4)).isEqualTo(7);
    assertThat(toZigzag(100)).isEqualTo(200);
    assertThat(fromZigzag(202)).isEqualTo(101);
  }

  @Test
  public void testVarintEncoding() {
    byte[] bytes = encodeVarint(127);
    assertThat(bytes[0]).isEqualTo((byte) 127);
    assertThat(bytes.length).isEqualTo(1);
    bytes = encodeVarint(128);
    assertThat(bytes[0]).isEqualTo((byte) 128);
    assertThat(bytes.length).isEqualTo(1);
    bytes = encodeVarint(255);
    assertThat(bytes[0]).isEqualTo((byte) 255);
    assertThat(bytes.length).isEqualTo(1);

    bytes = new byte[2];
    bytes[0] = 127;
    bytes[1] = 1;
    assertThat(decodeVarint(bytes)).isEqualTo(383);
    bytes[0] = -128;
    assertThat(decodeVarint(bytes)).isEqualTo(384);
    bytes[0] = -127;
    assertThat(decodeVarint(bytes)).isEqualTo(385);

    assertThat(decodeVarint(encodeVarint(127))).isEqualTo(127);
    assertThat(decodeVarint(encodeVarint(128))).isEqualTo(128);
    assertThat(decodeVarint(encodeVarint(129))).isEqualTo(129);
    assertThat(decodeVarint(encodeVarint(65536))).isEqualTo(65536);
    assertThat(decodeVarint(encodeVarint(65537))).isEqualTo(65537);
    assertThat(decodeVarint(encodeVarint(0))).isEqualTo(0);
    assertThat(decodeVarint(encodeVarint(1))).isEqualTo(1);
    assertThat(decodeVarint(encodeVarint(Long.MAX_VALUE))).isEqualTo(Long.MAX_VALUE);

    assertThatThrownBy(() -> decodeVarint(encodeVarint(-65536)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Varint encoding cannot be applied to negative values");
  }

  /**
   * Explicit check that we can read back fixed encodings correctly.
   */
  @Test
  public void testVarintEncodingFromFixed() {

    final byte[] bytes = new byte[Long.BYTES];
    Arrays.fill(bytes, (byte) 0);
    bytes[0] = 127;
    bytes[1] = 1;
    assertThat(decodeVarint(bytes)).isEqualTo(383);
    bytes[0] = -128;
    assertThat(decodeVarint(bytes)).isEqualTo(384);
    bytes[0] = -127;
    assertThat(decodeVarint(bytes)).isEqualTo(385);

  }

  @Test
  public void testSignedEncoding() {
    assertThat(decodeSigned(encodeSigned(127))).isEqualTo(127);
    assertThat(decodeSigned(encodeSigned(-1))).isEqualTo(-1);
    assertThat(decodeSigned(encodeSigned(0))).isEqualTo(0);
    assertThat(decodeSigned(encodeSigned(Integer.MAX_VALUE))).isEqualTo(Integer.MAX_VALUE);
    assertThat(decodeSigned(encodeSigned(Integer.MIN_VALUE))).isEqualTo(Integer.MIN_VALUE);
    assertThatThrownBy(() -> decodeSigned(encodeSigned(Long.MAX_VALUE)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Zigzag");
    assertThatThrownBy(() -> decodeSigned(encodeSigned(Long.MIN_VALUE)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Zigzag");
  }
}
