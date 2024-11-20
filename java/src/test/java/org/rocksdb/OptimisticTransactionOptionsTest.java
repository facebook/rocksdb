// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.util.BytewiseComparator;

public class OptimisticTransactionOptionsTest {

  private static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @BeforeClass
  public static void beforeAll() {
    RocksDB.loadLibrary();
  }

  @Test
  public void setSnapshot() {
    try (final OptimisticTransactionOptions opt = new OptimisticTransactionOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setSetSnapshot(boolValue);
      assertThat(opt.isSetSnapshot()).isEqualTo(boolValue);
    }
  }

  @Test
  public void comparator() {
    try (final OptimisticTransactionOptions opt = new OptimisticTransactionOptions();
         final ComparatorOptions copt = new ComparatorOptions()
             .setUseDirectBuffer(true);
         final AbstractComparator comparator = new BytewiseComparator(copt)) {
      opt.setComparator(comparator);
    }
  }
}
