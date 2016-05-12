//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <map>
#include <string>

#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "util/murmurhash.h"

namespace rocksdb {
namespace stl_wrappers {

struct LessOfComparator {
  explicit LessOfComparator(const Comparator* c = BytewiseComparator())
      : cmp(c) {}

  bool operator()(const std::string& a, const std::string& b) const {
    return cmp->Compare(Slice(a), Slice(b)) < 0;
  }

  const Comparator* cmp;
};

typedef std::map<std::string, std::string, LessOfComparator> KVMap;
}
}
