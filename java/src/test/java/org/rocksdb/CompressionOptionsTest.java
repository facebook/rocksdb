// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CompressionOptionsTest {

  static {
    RocksDB.loadLibrary();
  }

  @Test
  public void windowBits() {
    final int windowBits = 7;
    try(final CompressionOptions opt = new CompressionOptions()) {
      opt.setWindowBits(windowBits);
      assertThat(opt.windowBits()).isEqualTo(windowBits);
    }
  }

  @Test
  public void level() {
    final int level = 6;
    try(final CompressionOptions opt = new CompressionOptions()) {
      opt.setLevel(level);
      assertThat(opt.level()).isEqualTo(level);
    }
  }

  @Test
  public void strategy() {
    final int strategy = 2;
    try(final CompressionOptions opt = new CompressionOptions()) {
      opt.setStrategy(strategy);
      assertThat(opt.strategy()).isEqualTo(strategy);
    }
  }

  @Test
  public void maxDictBytes() {
    final int maxDictBytes = 999;
    try(final CompressionOptions opt = new CompressionOptions()) {
      opt.setMaxDictBytes(maxDictBytes);
      assertThat(opt.maxDictBytes()).isEqualTo(maxDictBytes);
    }
  }
}
