// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.rocksdb.*;

public class MixedOptionsTest {
  static {
    RocksDB.loadLibrary();
  }
  public static void main(String[] args) {
    // Set a table factory and check the names
    ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
    cfOptions.setTableFormatConfig(new BlockBasedTableConfig().
        setFilter(new BloomFilter()));
    assert(cfOptions.tableFactoryName().equals(
        "BlockBasedTable"));
    cfOptions.setTableFormatConfig(new PlainTableConfig());
    assert(cfOptions.tableFactoryName().equals("PlainTable"));
    // Initialize a dbOptions object from cf options and
    // db options
    DBOptions dbOptions = new DBOptions();
    Options options = new Options(dbOptions, cfOptions);
    assert(options.tableFactoryName().equals("PlainTable"));
    // Free instances
    options.dispose();
    options = null;
    cfOptions.dispose();
    cfOptions = null;
    dbOptions.dispose();
    dbOptions = null;
    System.gc();
    System.runFinalization();
    // Test Optimize for statements
    cfOptions = new ColumnFamilyOptions();
    cfOptions.optimizeUniversalStyleCompaction();
    cfOptions.optimizeLevelStyleCompaction();
    cfOptions.optimizeForPointLookup(1024);
    options = new Options();
    options.optimizeLevelStyleCompaction();
    options.optimizeLevelStyleCompaction(400);
    options.optimizeUniversalStyleCompaction();
    options.optimizeUniversalStyleCompaction(400);
    options.optimizeForPointLookup(1024);
    options.prepareForBulkLoad();
    System.out.println("Mixed options test passed");
  }
}
