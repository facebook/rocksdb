//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cassert>
#include <cstdint>

#include "port/port.h"
#include "rocksdb/slice.h"

namespace rocksdb {
// An entry is a variable length heap-allocated structure.
// Entries are referenced by cache and/or by any external entity.
// The cache keeps all its entries in table. Some elements
// are also stored on LRU list.
//
// LRUHandle can be in these states:
// 1. Referenced externally AND in hash table.
//  In that case the entry is *not* in the LRU. (refs > 1 && in_cache == true)
// 2. Not referenced externally and in hash table. In that case the entry is
// in the LRU and can be freed. (refs == 1 && in_cache == true)
// 3. Referenced externally and not in hash table. In that case the entry is
// in not on LRU and not in table. (refs >= 1 && in_cache == false)
//
// All newly created LRUHandles are in state 1. If you call LRUCache::Release
// on entry in state 1, it will go into state 2. To move from state 1 to
// state 3, either call LRUCache::Erase or LRUCache::Insert with the same key.
// To move from state 2 to state 1, use LRUCache::Lookup.
// Before destruction, make sure that no handles are in state 1. This means
// that any successful LRUCache::Lookup/LRUCache::Insert have a matching
// RUCache::Release (to move into state 2) or LRUCache::Erase (for state 3)

struct LRUHandle {
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;
  uint32_t refs;     // a number of refs to this entry
                     // cache itself is counted as 1
  bool in_cache;     // true, if this entry is referenced by the hash table
  uint32_t hash;     // Hash of key(); used for fast sharding and comparisons
  char key_data[1];  // Beginning of key

  Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) {
      return *(reinterpret_cast<Slice*>(value));
    } else {
      return Slice(key_data, key_length);
    }
  }

  void Free() {
    assert((refs == 1 && in_cache) || (refs == 0 && !in_cache));
    (*deleter)(key(), value);
    delete[] reinterpret_cast<char*>(this);
  }
};

}  // end namespace rocksdb
