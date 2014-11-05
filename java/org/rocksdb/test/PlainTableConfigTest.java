// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.junit.ClassRule;
import org.junit.Test;
import org.rocksdb.EncodingType;
import org.rocksdb.PlainTableConfig;

import static org.assertj.core.api.Assertions.assertThat;

public class PlainTableConfigTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Test
  public void plainTableConfig() {
    PlainTableConfig plainTableConfig = new PlainTableConfig();
    plainTableConfig.setKeySize(5);
    assertThat(plainTableConfig.keySize()).
        isEqualTo(5);
    plainTableConfig.setBloomBitsPerKey(11);
    assertThat(plainTableConfig.bloomBitsPerKey()).
        isEqualTo(11);
    plainTableConfig.setHashTableRatio(0.95);
    assertThat(plainTableConfig.hashTableRatio()).
        isEqualTo(0.95);
    plainTableConfig.setIndexSparseness(18);
    assertThat(plainTableConfig.indexSparseness()).
        isEqualTo(18);
    plainTableConfig.setHugePageTlbSize(1);
    assertThat(plainTableConfig.hugePageTlbSize()).
        isEqualTo(1);
    plainTableConfig.setEncodingType(EncodingType.kPrefix);
    assertThat(plainTableConfig.encodingType()).isEqualTo(
        EncodingType.kPrefix);
    plainTableConfig.setFullScanMode(true);
    assertThat(plainTableConfig.fullScanMode()).isTrue();
    plainTableConfig.setStoreIndexInFile(true);
    assertThat(plainTableConfig.storeIndexInFile()).
        isTrue();
  }
}
