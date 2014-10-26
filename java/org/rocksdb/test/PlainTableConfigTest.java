// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.rocksdb.EncodingType;
import org.rocksdb.PlainTableConfig;

public class PlainTableConfigTest {

  public static void main(String[] args) {
    PlainTableConfig plainTableConfig = new PlainTableConfig();
    assert(plainTableConfig.keySize() == 0);
    plainTableConfig.setKeySize(5);
    assert(plainTableConfig.keySize() == 5);
    assert(plainTableConfig.bloomBitsPerKey() == 10);
    plainTableConfig.setBloomBitsPerKey(11);
    assert(plainTableConfig.bloomBitsPerKey() == 11);
    assert(plainTableConfig.hashTableRatio() == 0.75);
    plainTableConfig.setHashTableRatio(0.95);
    assert(plainTableConfig.hashTableRatio() == 0.95);
    assert(plainTableConfig.indexSparseness() == 16);
    plainTableConfig.setIndexSparseness(18);
    assert(plainTableConfig.indexSparseness() == 18);
    assert(plainTableConfig.hugePageTlbSize() == 0);
    plainTableConfig.setHugePageTlbSize(1);
    assert(plainTableConfig.hugePageTlbSize() == 1);
    assert(plainTableConfig.encodingType().equals(
        EncodingType.kPlain));
    plainTableConfig.setEncodingType(EncodingType.kPrefix);
    assert(plainTableConfig.encodingType().equals(
        EncodingType.kPrefix));
    assert(!plainTableConfig.fullScanMode());
    plainTableConfig.setFullScanMode(true);
    assert(plainTableConfig.fullScanMode());
    assert(!plainTableConfig.storeIndexInFile());
    plainTableConfig.setStoreIndexInFile(true);
    assert(plainTableConfig.storeIndexInFile());
    System.out.println("PlainTableConfig test passed");
  }
}
