// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
package org.rocksdb;

/**
 * The config for plain table sst format.
 *
 * PlainTable is a RocksDB's SST file format optimized for low query latency
 * on pure-memory or really low-latency media.  It also support prefix
 * hash feature.
 */
public class PlainTableConfig extends TableFormatConfig {
  public static final int VARIABLE_LENGTH = 0;
  public static final int DEFAULT_BLOOM_BITS_PER_KEY = 10;
  public static final double DEFAULT_HASH_TABLE_RATIO = 0.75;
  public static final int DEFAULT_INDEX_SPARSENESS = 16;

  public PlainTableConfig() {
    keySize_ = VARIABLE_LENGTH;
    bloomBitsPerKey_ = DEFAULT_BLOOM_BITS_PER_KEY;
    hashTableRatio_ = DEFAULT_HASH_TABLE_RATIO;
    indexSparseness_ = DEFAULT_INDEX_SPARSENESS;
  }

  /**
   * Set the length of the user key. If it is set to be VARIABLE_LENGTH,
   * then it indicates the user keys are variable-lengthed.  Otherwise,
   * all the keys need to have the same length in byte.
   * DEFAULT: VARIABLE_LENGTH
   *
   * @param keySize the length of the user key.
   * @return the reference to the current config.
   */
  public PlainTableConfig setKeySize(int keySize) {
    keySize_ = keySize;
    return this;
  }

  /**
   * @return the specified size of the user key.  If VARIABLE_LENGTH,
   *     then it indicates variable-length key.
   */
  public int keySize() {
    return keySize_;
  }

  /**
   * Set the number of bits per key used by the internal bloom filter
   * in the plain table sst format.
   *
   * @param bitsPerKey the number of bits per key for bloom filer.
   * @return the reference to the current config.
   */
  public PlainTableConfig setBloomBitsPerKey(int bitsPerKey) {
    bloomBitsPerKey_ = bitsPerKey;
    return this;
  }

  /**
   * @return the number of bits per key used for the bloom filter.
   */
  public int bloomBitsPerKey() {
    return bloomBitsPerKey_;
  }

  /**
   * hashTableRatio is the desired utilization of the hash table used
   * for prefix hashing.  The ideal ratio would be the number of
   * prefixes / the number of hash buckets.  If this value is set to
   * zero, then hash table will not be used.
   *
   * @param ratio the hash table ratio.
   * @return the reference to the current config.
   */
  public PlainTableConfig setHashTableRatio(double ratio) {
    hashTableRatio_ = ratio;
    return this;
  }

  /**
   * @return the hash table ratio.
   */
  public double hashTableRatio() {
    return hashTableRatio_;
  }

  /**
   * Index sparseness determines the index interval for keys inside the
   * same prefix.  This number is equal to the maximum number of linear
   * search required after hash and binary search.  If it's set to 0,
   * then each key will be indexed.
   *
   * @param sparseness the index sparseness.
   * @return the reference to the current config.
   */
  public PlainTableConfig setIndexSparseness(int sparseness) {
    indexSparseness_ = sparseness;
    return this;
  }

  /**
   * @return the index sparseness.
   */
  public int indexSparseness() {
    return indexSparseness_;
  }

  @Override protected long newTableFactoryHandle() {
    return newTableFactoryHandle(keySize_, bloomBitsPerKey_,
        hashTableRatio_, indexSparseness_);
  }

  private native long newTableFactoryHandle(
      int keySize, int bloomBitsPerKey,
      double hashTableRatio, int indexSparseness);

  private int keySize_;
  private int bloomBitsPerKey_;
  private double hashTableRatio_;
  private int indexSparseness_;
}
