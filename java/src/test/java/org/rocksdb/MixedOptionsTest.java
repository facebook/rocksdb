// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MixedOptionsTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Test
  public void mixedOptionsTest(){
    // Set a table factory and check the names
    try(final Filter bloomFilter = new BloomFilter();
        final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
            .setTableFormatConfig(
                new BlockBasedTableConfig().setFilter(bloomFilter))
    ) {
      assertThat(cfOptions.tableFactoryName()).isEqualTo(
          "BlockBasedTable");
      cfOptions.setTableFormatConfig(new PlainTableConfig());
      assertThat(cfOptions.tableFactoryName()).isEqualTo("PlainTable");
      // Initialize a dbOptions object from cf options and
      // db options
      try (final DBOptions dbOptions = new DBOptions();
           final Options options = new Options(dbOptions, cfOptions)) {
        assertThat(options.tableFactoryName()).isEqualTo("PlainTable");
        // Free instances
      }
    }

    // Test Optimize for statements
    try(final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()) {
    cfOptions.optimizeUniversalStyleCompaction();
    cfOptions.optimizeLevelStyleCompaction();
    cfOptions.optimizeForPointLookup(1024);
    try(final Options options = new Options()) {
        options.optimizeLevelStyleCompaction();
        options.optimizeLevelStyleCompaction(400);
        options.optimizeUniversalStyleCompaction();
        options.optimizeUniversalStyleCompaction(400);
        options.optimizeForPointLookup(1024);
        options.prepareForBulkLoad();
      }
    }
  }
}
