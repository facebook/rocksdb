// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionOptionsTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Test
  public void compression() {
    try (final CompactionOptions compactionOptions = new CompactionOptions()) {
      assertThat(compactionOptions.compression())
          .isEqualTo(CompressionType.DISABLE_COMPRESSION_OPTION);
      compactionOptions.setCompression(CompressionType.SNAPPY_COMPRESSION);
      assertThat(compactionOptions.compression()).isEqualTo(CompressionType.SNAPPY_COMPRESSION);
    }
  }

  @Test
  public void outputFileSizeLimit() {
    final long mb250 = 1024 * 1024 * 250;
    try (final CompactionOptions compactionOptions = new CompactionOptions()) {
      assertThat(compactionOptions.outputFileSizeLimit())
          .isEqualTo(-1);
      compactionOptions.setOutputFileSizeLimit(mb250);
      assertThat(compactionOptions.outputFileSizeLimit())
          .isEqualTo(mb250);
    }
  }

  @Test
  public void maxSubcompactions() {
    try (final CompactionOptions compactionOptions = new CompactionOptions()) {
      assertThat(compactionOptions.maxSubcompactions())
          .isEqualTo(0);
      compactionOptions.setMaxSubcompactions(9);
      assertThat(compactionOptions.maxSubcompactions())
          .isEqualTo(9);
    }
  }
}
