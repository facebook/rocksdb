// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class SizeApproximationOptionsTest {
  @Test
  public void includeMemtables() {
    try (final SizeApproximationOptions approxOptions = new SizeApproximationOptions()) {
      assertThat(approxOptions.includeMemtables()).isFalse();
      approxOptions.setIncludeMemtables(true);
      assertThat(approxOptions.includeMemtables()).isTrue();
    }
  }

  @Test
  public void includeFiles() {
    try (final SizeApproximationOptions approxOptions = new SizeApproximationOptions()) {
      assertThat(approxOptions.includeFiles()).isTrue();
      approxOptions.setIncludeFiles(false);
      assertThat(approxOptions.includeFiles()).isFalse();
    }
  }

  @Test
  public void fileSizeErrorMargin() {
    try (final SizeApproximationOptions approxOptions = new SizeApproximationOptions()) {
      assertThat(approxOptions.filesSizeErrorMargin()).isEqualTo(-1.0);
      approxOptions.setFilesSizeErrorMargin(25.5);
      assertThat(approxOptions.filesSizeErrorMargin()).isEqualTo(25.5);
    }
  }
}
