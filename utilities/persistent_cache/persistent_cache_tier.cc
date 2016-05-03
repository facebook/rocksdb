//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#ifndef ROCKSDB_LITE

#include "utilities/persistent_cache/persistent_cache_tier.h"

#include <string>

namespace rocksdb {

//
// PersistentCacheTier implementation
//
Status PersistentCacheTier::Open() {
  if (next_tier_) {
    return next_tier_->Open();
  }
  return Status::OK();
}

Status PersistentCacheTier::Close() {
  if (next_tier_) {
    return next_tier_->Close();
  }
  return Status::OK();
}

bool PersistentCacheTier::Reserve(const size_t size) {
  // default implementation is a pass through
  return true;
}

bool PersistentCacheTier::Erase(const Slice& key) {
  // default implementation is a pass through since not all cache tiers might
  // support erase
  return true;
}

std::string PersistentCacheTier::PrintStats() {
  if (next_tier_) {
    return next_tier_->PrintStats();
  }
  return std::string();
}

std::vector<PersistentCacheTier::TierStats> PersistentCacheTier::Stats() {
  if (next_tier_) {
    return next_tier_->Stats();
  }
  return std::vector<TierStats>{};
}

//
// PersistentTieredCache implementation
//
PersistentTieredCache::~PersistentTieredCache() { assert(tiers_.empty()); }

Status PersistentTieredCache::Open() {
  assert(!tiers_.empty());
  return tiers_.front()->Open();
}

Status PersistentTieredCache::Close() {
  assert(!tiers_.empty());
  Status status = tiers_.front()->Close();
  if (status.ok()) {
    tiers_.clear();
  }
  return status;
}

bool PersistentTieredCache::Erase(const Slice& key) {
  assert(!tiers_.empty());
  return tiers_.front()->Erase(key);
}

std::vector<PersistentCacheTier::TierStats> PersistentTieredCache::Stats() {
  assert(!tiers_.empty());
  return tiers_.front()->Stats();
}

std::string PersistentTieredCache::PrintStats() {
  assert(!tiers_.empty());
  return tiers_.front()->PrintStats();
}

Status PersistentTieredCache::Insert(const Slice& page_key, const char* data,
                                     const size_t size) {
  assert(!tiers_.empty());
  return tiers_.front()->Insert(page_key, data, size);
}

Status PersistentTieredCache::Lookup(const Slice& page_key,
                                     std::unique_ptr<char[]>* data,
                                     size_t* size) {
  assert(!tiers_.empty());
  return tiers_.front()->Lookup(page_key, data, size);
}

void PersistentTieredCache::AddTier(const Tier& tier) {
  if (!tiers_.empty()) {
    tiers_.back()->set_next_tier(tier);
  }
  tiers_.push_back(tier);
}

bool PersistentTieredCache::IsCompressed() {
  assert(tiers_.size());
  return tiers_.front()->IsCompressed();
}

}  // namespace rocksdb

#endif
