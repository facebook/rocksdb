// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Test;

public class FilterTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Test
  public void filter() {
    // new Bloom filter
    final BlockBasedTableConfig blockConfig = new BlockBasedTableConfig();
    try(final Options options = new Options()) {

      try(final Filter bloomFilter = new BloomFilter()) {
        blockConfig.setFilterPolicy(bloomFilter);
        options.setTableFormatConfig(blockConfig);
      }

      try(final Filter bloomFilter = new BloomFilter(10)) {
        blockConfig.setFilterPolicy(bloomFilter);
        options.setTableFormatConfig(blockConfig);
      }

      try(final Filter bloomFilter = new BloomFilter(10, false)) {
        blockConfig.setFilterPolicy(bloomFilter);
        options.setTableFormatConfig(blockConfig);
        assertThat(bloomFilter.isInstanceOf("bloomfilter")).isTrue();
        assertThat(bloomFilter.isInstanceOf("ribbonfilter")).isFalse();
      }
    }
  }

  @Test
  public void createFromString() throws RocksDBException {
    final BlockBasedTableConfig blockConfig = new BlockBasedTableConfig();
    try (final Options options = new Options()) {
      try (final Filter filter = Filter.createFromString("ribbonfilter:20")) {
        assertThat(filter.getId()).startsWith("ribbonfilter");
        assertThat(filter.isInstanceOf("ribbonfilter")).isTrue();
        assertThat(filter.isInstanceOf("bloomfilter")).isFalse();
        blockConfig.setFilterPolicy(filter);
        options.setTableFormatConfig(blockConfig);
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void createUnknownFromString() throws RocksDBException {
    try (final Filter filter = Filter.createFromString("unknown")) {
    }
  }
}
