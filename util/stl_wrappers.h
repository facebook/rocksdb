//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "util/murmurhash.h"
#include "util/coding.h"

#include "rocksdb/memtablerep.h"
#include "rocksdb/slice.h"

namespace rocksdb {
namespace stl_wrappers {
  class Base {
   protected:
    const MemTableRep::KeyComparator& compare_;
    explicit Base(const MemTableRep::KeyComparator& compare)
      : compare_(compare) { }
  };

  struct Compare : private Base {
    explicit Compare(const MemTableRep::KeyComparator& compare)
      : Base(compare) { }
    inline bool operator()(const char* a, const char* b) const {
      return compare_(a, b) < 0;
    }
  };

  struct Hash {
    inline size_t operator()(const char* buf) const {
      Slice internal_key = GetLengthPrefixedSlice(buf);
      Slice value =
        GetLengthPrefixedSlice(internal_key.data() + internal_key.size());
      unsigned int hval = MurmurHash(internal_key.data(), internal_key.size(),
        0);
      hval = MurmurHash(value.data(), value.size(), hval);
      return hval;
    }
  };

  struct KeyEqual : private Base {
    explicit KeyEqual(const MemTableRep::KeyComparator& compare)
      : Base(compare) { }
    inline bool operator()(const char* a, const char* b) const {
      return this->compare_(a, b) == 0;
    }
  };
}
}
