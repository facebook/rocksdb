// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

public class BloomFilter extends Filter {
  private final int bitsPerKey_;
  
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