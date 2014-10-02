// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.rocksdb.*;

public class FilterTest {
  static {
    RocksDB.loadLibrary();
  }
  public static void main(String[] args) {
    Options options = new Options();
    // test table config without filter
    BlockBasedTableConfig blockConfig = new BlockBasedTableConfig();
    options.setTableFormatConfig(blockConfig);
    options.dispose();
    // new Bloom filter
    options = new Options();
    blockConfig = new BlockBasedTableConfig();
    blockConfig.setFilter(new BloomFilter());
    options.setTableFormatConfig(blockConfig);
    System.out.println("Filter test passed");
  }
}
