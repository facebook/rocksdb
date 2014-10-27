// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.rocksdb.*;

public class MemTableTest {
  static {
    RocksDB.loadLibrary();
  }
  public static void main(String[] args) {
    Options options = new Options();
    // Test HashSkipListMemTableConfig
    HashSkipListMemTableConfig memTableConfig =
        new HashSkipListMemTableConfig();
    assert(memTableConfig.bucketCount() == 1000000);
    memTableConfig.setBucketCount(2000000);
    assert(memTableConfig.bucketCount() == 2000000);
    assert(memTableConfig.height() == 4);
    memTableConfig.setHeight(5);
    assert(memTableConfig.height() == 5);
    assert(memTableConfig.branchingFactor() == 4);
    memTableConfig.setBranchingFactor(6);
    assert(memTableConfig.branchingFactor() == 6);
    try {
      options.setMemTableConfig(memTableConfig);
    } catch (RocksDBException e) {
      assert(false);
    }
    memTableConfig = null;
    options.dispose();
    System.gc();
    System.runFinalization();
    // Test SkipList
    options = new Options();
    SkipListMemTableConfig skipMemTableConfig =
        new SkipListMemTableConfig();
    assert(skipMemTableConfig.lookahead() == 0);
    skipMemTableConfig.setLookahead(20);
    assert(skipMemTableConfig.lookahead() == 20);
    try {
      options.setMemTableConfig(skipMemTableConfig);
    } catch (RocksDBException e) {
      assert(false);
    }
    skipMemTableConfig = null;
    options.dispose();
    System.gc();
    System.runFinalization();
    // Test HashLinkedListMemTableConfig
    options = new Options();
    HashLinkedListMemTableConfig hashLinkedListMemTableConfig =
        new HashLinkedListMemTableConfig();
    assert(hashLinkedListMemTableConfig.bucketCount() == 50000);
    hashLinkedListMemTableConfig.setBucketCount(100000);
    assert(hashLinkedListMemTableConfig.bucketCount() == 100000);
    assert(hashLinkedListMemTableConfig.hugePageTlbSize() == 0);
    hashLinkedListMemTableConfig.setHugePageTlbSize(1);
    assert(hashLinkedListMemTableConfig.hugePageTlbSize() == 1);
    assert(hashLinkedListMemTableConfig.
       bucketEntriesLoggingThreshold() == 4096);
    hashLinkedListMemTableConfig.
        setBucketEntriesLoggingThreshold(200);
    assert(hashLinkedListMemTableConfig.
       bucketEntriesLoggingThreshold() == 200);
    assert(hashLinkedListMemTableConfig.
        ifLogBucketDistWhenFlush() == true);
    hashLinkedListMemTableConfig.
        setIfLogBucketDistWhenFlush(false);
    assert(hashLinkedListMemTableConfig.
        ifLogBucketDistWhenFlush() == false);
    assert(hashLinkedListMemTableConfig.
        thresholdUseSkiplist() == 256);
    hashLinkedListMemTableConfig.setThresholdUseSkiplist(29);
    assert(hashLinkedListMemTableConfig.
        thresholdUseSkiplist() == 29);
    try {
      options.setMemTableConfig(hashLinkedListMemTableConfig);
    } catch (RocksDBException e) {
      assert(false);
    }
    hashLinkedListMemTableConfig = null;
    options.dispose();
    System.gc();
    System.runFinalization();
    // test VectorMemTableConfig
    options = new Options();
    VectorMemTableConfig vectorMemTableConfig =
        new VectorMemTableConfig();
    assert(vectorMemTableConfig.reservedSize() == 0);
    vectorMemTableConfig.setReservedSize(123);
    assert(vectorMemTableConfig.reservedSize() == 123);
    try {
      options.setMemTableConfig(vectorMemTableConfig);
    } catch (RocksDBException e) {
      assert(false);
    }
    vectorMemTableConfig = null;
    options.dispose();
    System.gc();
    System.runFinalization();
    System.out.println("Mem-table test passed");
  }
}
