// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ComparatorOptionsTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Test
  public void reusedSynchronisationType() {
    try(final ComparatorOptions copt = new ComparatorOptions()) {

      copt.setReusedSynchronisationType(ReusedSynchronisationType.MUTEX);
      assertThat(copt.reusedSynchronisationType())
          .isEqualTo(ReusedSynchronisationType.MUTEX);

      copt.setReusedSynchronisationType(ReusedSynchronisationType.ADAPTIVE_MUTEX);
      assertThat(copt.reusedSynchronisationType())
          .isEqualTo(ReusedSynchronisationType.ADAPTIVE_MUTEX);

      copt.setReusedSynchronisationType(ReusedSynchronisationType.THREAD_LOCAL);
      assertThat(copt.reusedSynchronisationType())
          .isEqualTo(ReusedSynchronisationType.THREAD_LOCAL);
    }
  }

  @Test
  public void useDirectBuffer() {
    try(final ComparatorOptions copt = new ComparatorOptions()) {
      copt.setUseDirectBuffer(true);
      assertThat(copt.useDirectBuffer()).isTrue();

      copt.setUseDirectBuffer(false);
      assertThat(copt.useDirectBuffer()).isFalse();
    }
  }

  @Test
  public void maxReusedBufferSize() {
    try(final ComparatorOptions copt = new ComparatorOptions()) {
      copt.setMaxReusedBufferSize(12345);
      assertThat(copt.maxReusedBufferSize()).isEqualTo(12345);

      copt.setMaxReusedBufferSize(-1);
      assertThat(copt.maxReusedBufferSize()).isEqualTo(-1);
    }
  }
}
