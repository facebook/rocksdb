// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

public enum CompressionType {
  NO_COMPRESSION((byte) 0),
  SNAPPY_COMPRESSION((byte) 1),
  ZLIB_COMPRESSION((byte) 2),
  BZLIB2_COMPRESSION((byte) 3),
  LZ4_COMPRESSION((byte) 4),
  LZ4HC_COMPRESSION((byte) 5);
  
  private final byte value_;

  private CompressionType(byte value) {
    value_ = value;
  }

  public byte getValue() {
    return value_;
  }
}
