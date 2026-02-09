// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class CompressionTypesTest {
  @Test
  public void getCompressionType() {
    for (final CompressionType compressionType : CompressionType.values()) {
      final String libraryName = compressionType.getLibraryName();
      if (compressionType == CompressionType.DISABLE_COMPRESSION_OPTION) {
        assertThat(CompressionType.getCompressionType(libraryName))
            .isEqualTo(CompressionType.NO_COMPRESSION);
      } else {
        assertThat(CompressionType.getCompressionType(libraryName)).isEqualTo(compressionType);
      }
    }
  }
}