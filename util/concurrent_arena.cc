//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/concurrent_arena.h"
#include <algorithm>
#include <thread>
#include "port/port.h"
#include "util/random.h"

namespace rocksdb {

#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
__thread size_t ConcurrentArena::tls_cpuid = 0;
#endif

ConcurrentArena::ConcurrentArena(size_t block_size, AllocTracker* tracker,
                                 size_t huge_page_size)
    : shards_(),
      shard_block_size_(
          (tracker != nullptr)
              ? std::max(static_cast<size_t>(4 * 1024),
                         block_size / shards_.Size())
              : block_size / 8),  // If being tracked, allocate smaller blocks
                                  // to avoid too sensitive flush triggering
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
