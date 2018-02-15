//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <memory>
#include <stdexcept>
#include <stdint.h>
#include <stdlib.h>

#include "db/dbformat.h"
#include "util/coding.h"

namespace rocksdb {

class KeyComparator {
public:
  const InternalKeyComparator comparator;
  explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }

  // Compare a and b. Return a negative value if a is less than b, 0 if they
  // are equal, and a positive value if a is greater than b
  int operator()(const char* prefix_len_key1,
                 const char* prefix_len_key2) const;
  int operator()(const char* prefix_len_key,
                 const Slice& key) const;
  // TODO(rzarzynski): introduce a variant for the typical use case:
  // comparing ONE key with MANY others keys. We could squeeze a lot
  // of work spent on transforming the same data here and in Internal
  // KeyComparator.
};


inline int KeyComparator::operator()(const char* prefix_len_key1,
                                     const char* prefix_len_key2) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice k1 = GetLengthPrefixedSlice(prefix_len_key1);
  Slice k2 = GetLengthPrefixedSlice(prefix_len_key2);
  return comparator.CompareKeySeq(k1, k2);
}

inline int KeyComparator::operator()(const char* prefix_len_key,
                                     const Slice& key)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(prefix_len_key);
  return comparator.CompareKeySeq(a, key);
}

}  // namespace rocksdb
