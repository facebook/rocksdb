//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This class implements a custom SecondaryCache that randomly injects an
// error status into Inserts/Lookups based on a specified probability.

#include "utilities/fault_injection_secondary_cache.h"

namespace ROCKSDB_NAMESPACE {

FaultInjectionSecondaryCache::ErrorContext*
FaultInjectionSecondaryCache::GetErrorContext() {
  ErrorContext* ctx = static_cast<ErrorContext*>(thread_local_error_->Get());
  if (!ctx) {
    ctx = new ErrorContext(seed_);
    thread_local_error_->Reset(ctx);
  }

  return ctx;
}

Status FaultInjectionSecondaryCache::Insert(
    const Slice& key, void* value, const Cache::CacheItemHelper* helper) {
  ErrorContext* ctx = GetErrorContext();
  if (ctx->rand.OneIn(prob_)) {
    return Status::IOError();
  }

  return base_->Insert(key, value, helper);
}

std::unique_ptr<SecondaryCacheResultHandle>
FaultInjectionSecondaryCache::Lookup(const Slice& key,
                                     const Cache::CreateCallback& create_cb,
                                     bool wait, bool erase_handle,
                                     bool& is_in_sec_cache) {
  std::unique_ptr<SecondaryCacheResultHandle> hdl;
  ErrorContext* ctx = GetErrorContext();
  if (ctx->rand.OneIn(prob_)) {
    return nullptr;
  } else {
    return base_->Lookup(key, create_cb, wait, erase_handle, is_in_sec_cache);
  }
}

void FaultInjectionSecondaryCache::Erase(const Slice& key) {
  base_->Erase(key);
}

void FaultInjectionSecondaryCache::WaitAll(
    std::vector<SecondaryCacheResultHandle*> handles) {
  ErrorContext* ctx = GetErrorContext();
  std::vector<SecondaryCacheResultHandle*> base_handles;
  for (SecondaryCacheResultHandle* hdl : handles) {
    if (ctx->rand.OneIn(prob_)) {
      continue;
    }
    base_handles.push_back(hdl);
  }

  base_->WaitAll(base_handles);
}

}  // namespace ROCKSDB_NAMESPACE
