// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

public enum CompactionStyle {
  LEVEL((byte) 0),
  UNIVERSAL((byte) 1),
  FIFO((byte) 2);
  
  private final byte value_;

  private CompactionStyle(byte value) {
    value_ = value;
  }

  public byte getValue() {
    return value_;
  }
}
