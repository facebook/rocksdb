//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once
#include <cassert>

namespace rocksdb {

class BitSet {
 public:
  /**
   * Create a bit set of numBits, with the bits set to either true or false.
   */
  explicit BitSet(size_t numBits, bool initial=false)
    : numBits_(numBits),
      data_(numWords(), initial ? ~0UL : 0UL) {
  }

  /**
   * Set bit b to 1.
   */
  void set(size_t b) {
    assert(b >= 0 && b < numBits_);
    data_[word(b)] |= wordOffsetMask(b);
  }

  /**
   * Set bit b to 0;
   */
  void reset(size_t b) {
    assert(b >= 0 && b < numBits_);
    data_[word(b)] &= ~wordOffsetMask(b);
  }

  /**
   * Get a bit.
   */
  bool test(int b) const {
    return data_[word(b)] & wordOffsetMask(b);
  }

 /**
   * Return the size of the BitSet, in bits.
   */
  size_t size() const {
    return numBits_;
  }

 private:

 inline size_t numWords() const {
    if (numBits_ == 0) return 0;
    return 1 + (numBits_-1) / (8*sizeof(unsigned long));
  }
  inline static size_t word(int b) {
    return b / (8*sizeof(unsigned long));
  }
  inline static int wordOffset(int b) {
    return b % (8*sizeof(unsigned long));
  }
  inline static unsigned long wordOffsetMask(int b) {
    return 1UL << wordOffset(b);
  }

  size_t numBits_;
  std::vector<unsigned long> data_;
};

}  // namespace facebook
