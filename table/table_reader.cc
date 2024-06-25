//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/table_reader.h"

namespace ROCKSDB_NAMESPACE {

#ifndef NDEBUG
namespace {
// Thread local flags to connect different instances and calls to
// EnforceReadOpts within a thread.
thread_local bool tl_enforce_block_cache_tier = false;
}  // namespace

EnforceReadOpts::EnforceReadOpts(const ReadOptions& read_opts) {
  saved_enforce_block_cache_tier = tl_enforce_block_cache_tier;
  if (read_opts.read_tier == ReadTier::kBlockCacheTier) {
    tl_enforce_block_cache_tier = true;
  } else {
    // We should not be entertaining a full read in a context only allowing
    // memory-only reads.
    assert(tl_enforce_block_cache_tier == false);
  }
}

EnforceReadOpts::~EnforceReadOpts() {
  tl_enforce_block_cache_tier = saved_enforce_block_cache_tier;
}

void EnforceReadOpts::UsedIO() { assert(!tl_enforce_block_cache_tier); }
#endif

}  // namespace ROCKSDB_NAMESPACE
