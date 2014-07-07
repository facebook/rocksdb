// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

public enum CompressionType {
  NO_COMPRESSION(0),
  SNAPPY_COMPRESSION(1),
  ZLIB_COMPRESSION(2),
  BZLIB2_COMPRESSION(3),
  LZ4_COMPRESSION(4),
  LZ4HC_COMPRESSION(5);
  
  private final int value_;

  private CompressionType(int value) {
    value_ = value;
  }

  public int getValue() {
    return value_;
  }
}