// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.rocksdb.util.MergeEncodings.*;

import org.junit.Test;

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
  public void testVarintDecoding() {
    byte[] bytes = new byte[1];
    bytes[0] = 127;
    assertThat(decodeVarintSigned(bytes)).isEqualTo(127);
    bytes[0] = -127;
    assertThat(decodeVarintSigned(bytes)).isEqualTo(-127);

    bytes = new byte[2];
    bytes[1] = -1;
    assertThat(decodeVarintSigned(bytes)).isEqualTo(-256);
    bytes[0] = 3;
    assertThat(decodeVarintSigned(bytes)).isEqualTo(-253);

    bytes = new byte[3];
    bytes[2] = -1;
    assertThat(decodeVarintSigned(bytes)).isEqualTo(-65536);
    bytes[1] = -128;
    assertThat(decodeVarintSigned(bytes)).isEqualTo(-32768);
    bytes[2] = 0;
    assertThat(decodeVarintSigned(bytes)).isEqualTo(32768);
    bytes[1] = 0;
    bytes[2] = 1;
    assertThat(decodeVarintSigned(bytes)).isEqualTo(65536);
  }

  @Test
  public void testVarintEncoding() {
    byte[] bytes;

    bytes = encodeVarintSigned(127);
    assertThat(bytes.length).isEqualTo(1);
    assertThat(bytes[0]).isEqualTo((byte) 127);

    bytes = encodeVarintSigned(128);
    assertThat(bytes.length).isEqualTo(2);
    assertThat(bytes[0]).isEqualTo((byte) -128);
    assertThat(bytes[1]).isEqualTo((byte) 0);

    bytes = encodeVarintSigned(32768);
    assertThat(bytes.length).isEqualTo(3);
    assertThat(bytes[0]).isEqualTo((byte) 0);
    assertThat(bytes[1]).isEqualTo((byte) -128);
    assertThat(bytes[2]).isEqualTo((byte) 0);
  }
  @Test
  public void testSignedEncoding() {
    final long[] values = new long[] {127, 128, 129, -1, 0, 1, -32769, 32769, -32768, 32768, -32767,
        32767, -65537, 65537, -65536, 65536, -65535, 65535, Integer.MAX_VALUE, Integer.MIN_VALUE,
        Long.MAX_VALUE, Long.MIN_VALUE};
    for (long value : values) {
      assertThat(decodeSigned(encodeSigned(value))).isEqualTo(value);
    }
    for (long value : values) {
      assertThat(decodeVarintSigned(encodeVarintSigned(value))).isEqualTo(value);
    }
  }
}
