// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;
import org.junit.ClassRule;
import org.junit.Test;

public class ImportColumnFamilyOptionsTest {
  static {
    RocksDB.loadLibrary();
  }

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  public static final Random rand = PlatformRandomHelper.getPlatformSpecificRandomFactory();

  @Test
  public void createImportColumnFamilyOptionsWithoutParameters() {
    try (final ImportColumnFamilyOptions options = new ImportColumnFamilyOptions()) {
      assertThat(options).isNotNull();
    }
  }

  @Test
  public void createImportColumnFamilyOptionsWithParameters() {
    final boolean moveFiles = rand.nextBoolean();
    try (final ImportColumnFamilyOptions options = new ImportColumnFamilyOptions(moveFiles)) {
      assertThat(options).isNotNull();
      assertThat(options.moveFiles()).isEqualTo(moveFiles);
    }
  }
}
