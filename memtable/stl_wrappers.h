//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
#pragma once

#include <map>
#include <string>

#include "rocksdb/comparator.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "util/murmurhash.h"

namespace rocksdb {
namespace stl_wrappers {

class Base {
 protected:
  const MemTableRep::KeyComparator& compare_;
  explicit Base(const MemTableRep::KeyComparator& compare)
      : compare_(compare) {}
};

struct Compare : private Base {
  explicit Compare(const MemTableRep::KeyComparator& compare) : Base(compare) {}
  inline bool operator()(const char* a, const char* b) const {
    return compare_(a, b) < 0;
  }
};

}
}
