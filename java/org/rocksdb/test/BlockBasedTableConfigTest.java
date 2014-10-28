// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ChecksumType;
import org.rocksdb.IndexType;

public class BlockBasedTableConfigTest {

  public static void main(String[] args) {
    BlockBasedTableConfig blockBasedTableConfig =
        new BlockBasedTableConfig();
    blockBasedTableConfig.setNoBlockCache(true);
    assert(blockBasedTableConfig.noBlockCache());
    blockBasedTableConfig.setBlockCacheSize(8*1024);
    assert(blockBasedTableConfig.blockCacheSize() == (8*1024));
    blockBasedTableConfig.setBlockSizeDeviation(12);
    assert(blockBasedTableConfig.blockSizeDeviation() == 12);
    blockBasedTableConfig.setBlockRestartInterval(15);
    assert(blockBasedTableConfig.blockRestartInterval() == 15);
    blockBasedTableConfig.setWholeKeyFiltering(false);
    assert(!blockBasedTableConfig.wholeKeyFiltering());
    blockBasedTableConfig.setCacheIndexAndFilterBlocks(true);
    assert(blockBasedTableConfig.cacheIndexAndFilterBlocks());
    blockBasedTableConfig.setHashIndexAllowCollision(false);
    assert(!blockBasedTableConfig.hashIndexAllowCollision());
    blockBasedTableConfig.setBlockCacheCompressedSize(40);
    assert(blockBasedTableConfig.blockCacheCompressedSize() == 40);
    blockBasedTableConfig.setChecksumType(ChecksumType.kNoChecksum);
    blockBasedTableConfig.setChecksumType(ChecksumType.kxxHash);
    assert(blockBasedTableConfig.checksumType().equals(
        ChecksumType.kxxHash));
    blockBasedTableConfig.setIndexType(IndexType.kHashSearch);
    assert(blockBasedTableConfig.indexType().equals(
        IndexType.kHashSearch));
    blockBasedTableConfig.setBlockCacheCompressedNumShardBits(4);
    assert(blockBasedTableConfig.blockCacheCompressedNumShardBits()
        == 4);
    blockBasedTableConfig.setCacheNumShardBits(5);
    assert(blockBasedTableConfig.cacheNumShardBits() == 5);
    System.out.println("BlockBasedTableConfig test passed");
  }
}
