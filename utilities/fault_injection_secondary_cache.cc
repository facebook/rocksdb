//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This class implements a custom SecondaryCache that randomly injects an
// error status into Inserts/Lookups based on a specified probability.

#include "utilities/fault_injection_secondary_cache.h"

namespace ROCKSDB_NAMESPACE {

void FaultInjectionSecondaryCache::ResultHandle::UpdateHandleValue(
    FaultInjectionSecondaryCache::ResultHandle* handle) {
  ErrorContext* ctx = handle->cache_->GetErrorContext();
  if (!ctx->rand.OneIn(handle->cache_->prob_)) {
    handle->value_ = handle->base_->Value();
    handle->size_ = handle->base_->Size();
  }
  handle->base_.reset();
}

bool FaultInjectionSecondaryCache::ResultHandle::IsReady() {
  bool ready = true;
  if (base_) {
    ready = base_->IsReady();
    if (ready) {
      UpdateHandleValue(this);
    }
  }
  return ready;
}

void FaultInjectionSecondaryCache::ResultHandle::Wait() {
  base_->Wait();
  UpdateHandleValue(this);
}

void* FaultInjectionSecondaryCache::ResultHandle::Value() { return value_; }

size_t FaultInjectionSecondaryCache::ResultHandle::Size() { return size_; }

void FaultInjectionSecondaryCache::ResultHandle::WaitAll(
    FaultInjectionSecondaryCache* cache,
    std::vector<SecondaryCacheResultHandle*> handles) {
  std::vector<SecondaryCacheResultHandle*> base_handles;
  for (SecondaryCacheResultHandle* hdl : handles) {
    FaultInjectionSecondaryCache::ResultHandle* handle =
        static_cast<FaultInjectionSecondaryCache::ResultHandle*>(hdl);
    if (!handle->base_) {
      continue;
    }
    base_handles.emplace_back(handle->base_.get());
  }

  cache->base_->WaitAll(base_handles);
  for (SecondaryCacheResultHandle* hdl : handles) {
    FaultInjectionSecondaryCache::ResultHandle* handle =
        static_cast<FaultInjectionSecondaryCache::ResultHandle*>(hdl);
    if (handle->base_) {
      UpdateHandleValue(handle);
    }
  }
}

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
                                     bool wait) {
  std::unique_ptr<SecondaryCacheResultHandle> hdl =
      base_->Lookup(key, create_cb, wait);
  ErrorContext* ctx = GetErrorContext();
  if (wait && ctx->rand.OneIn(prob_)) {
    hdl.reset();
  }
  return std::unique_ptr<FaultInjectionSecondaryCache::ResultHandle>(
      new FaultInjectionSecondaryCache::ResultHandle(this, std::move(hdl)));
}

void FaultInjectionSecondaryCache::Erase(const Slice& key) {
  base_->Erase(key);
}

void FaultInjectionSecondaryCache::WaitAll(
    std::vector<SecondaryCacheResultHandle*> handles) {
  FaultInjectionSecondaryCache::ResultHandle::WaitAll(this, handles);
}

}  // namespace ROCKSDB_NAMESPACE
