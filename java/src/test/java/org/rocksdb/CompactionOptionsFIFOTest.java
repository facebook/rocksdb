// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionOptionsFIFOTest {

  static {
    RocksDB.loadLibrary();
  }

  @Test
  public void maxTableFilesSize() {
    final long size = 500 * 1024 * 1026;
    try(final CompactionOptionsFIFO opt = new CompactionOptionsFIFO()) {
      opt.setMaxTableFilesSize(size);
      assertThat(opt.maxTableFilesSize()).isEqualTo(size);
    }
  }
}
