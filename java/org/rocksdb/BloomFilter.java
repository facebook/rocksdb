// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * This class creates a new filter policy that uses a bloom filter
 * with approximately the specified number of bits per key.
 * A good value for bitsPerKey is 10, which yields a filter
 * with ~ 1% false positive rate.
 *
 * Default value of bits per key is 10.
 */
public class BloomFilter extends Filter {
  private static final int DEFAULT_BITS_PER_KEY = 10;
  private final int bitsPerKey_;

  public BloomFilter() {
    this(DEFAULT_BITS_PER_KEY);
  }

  public BloomFilter(int bitsPerKey) {
    super();
    bitsPerKey_ = bitsPerKey;

    createNewFilter();
  }

  @Override
  protected void createNewFilter() {
    createNewFilter0(bitsPerKey_);
  }

  private native void createNewFilter0(int bitsKeyKey);
}
