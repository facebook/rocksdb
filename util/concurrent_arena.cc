//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/concurrent_arena.h"
#include <thread>
#include "port/port.h"
#include "util/random.h"

namespace rocksdb {

#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
__thread size_t ConcurrentArena::tls_cpuid = 0;
#endif

ConcurrentArena::ConcurrentArena(size_t block_size, AllocTracker* tracker,
                                 size_t huge_page_size)
    : shard_block_size_(block_size / 8),
      shards_(),
      arena_(block_size, tracker, huge_page_size) {
  Fixup();
}

ConcurrentArena::Shard* ConcurrentArena::Repick() {
  auto shard_and_index = shards_.AccessElementAndIndex();
#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
  // even if we are cpu 0, use a non-zero tls_cpuid so we can tell we
  // have repicked
  tls_cpuid = shard_and_index.second | shards_.Size();
#endif
  return shard_and_index.first;
}

}  // namespace rocksdb
