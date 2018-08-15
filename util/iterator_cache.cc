//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/iterator_cache.h"

#include "db/range_del_aggregator.h"


namespace rocksdb {
  

IteratorCache::IteratorCache(const CreateIterCallback& create_iterator)
    : create_iterator_(create_iterator),
      pinned_iters_mgr_(nullptr),
      range_del_agg_(nullptr) {}

IteratorCache::~IteratorCache() {
  for (auto pair : iterator_cache_) {
    pair.second.iter->~InternalIterator();
  }
  if (range_del_agg_ != nullptr) {
    range_del_agg_->~RangeDelAggregator();
  }
}

void IteratorCache::NewRangeDelAgg(
    const InternalKeyComparator& icmp,
    const std::vector<SequenceNumber>& snapshots) {
  assert(iterator_cache_.empty());
  char* buffer = arena_.AllocateAligned(sizeof(RangeDelAggregator));
  range_del_agg_ = new(buffer) RangeDelAggregator(icmp, snapshots);
}

InternalIterator* IteratorCache::GetIterator(
    const FileMetaData* f, TableReader** reader_ptr) {
  auto find = iterator_cache_.find(f->fd.GetNumber());
  if (find != iterator_cache_.end()) {
    if (reader_ptr != nullptr) {
      *reader_ptr = find->second.reader;
    }
    return find->second.iter;
  }
  CacheItem item;
  item.iter = create_iterator_(f, uint64_t(-1), &arena_, range_del_agg_,
                                &item.reader);
  assert(item.iter != nullptr);
  item.iter->SetPinnedItersMgr(pinned_iters_mgr_);
  iterator_cache_.emplace(f->fd.GetNumber(), item);
  if (reader_ptr != nullptr) {
    *reader_ptr = find->second.reader;
  }
  return item.iter;
}

InternalIterator* IteratorCache::GetIterator(
    uint64_t sst_id, TableReader** reader_ptr) {
  auto find = iterator_cache_.find(sst_id);
  if (find != iterator_cache_.end()) {
    if (reader_ptr != nullptr) {
      *reader_ptr = find->second.reader;
    }
    return find->second.iter;
  }
  CacheItem item;
  item.iter = create_iterator_(nullptr, sst_id, &arena_, range_del_agg_,
                                &item.reader);
  assert(item.iter != nullptr);
  item.iter->SetPinnedItersMgr(pinned_iters_mgr_);
  iterator_cache_.emplace(sst_id, item);
  if (reader_ptr != nullptr) {
    *reader_ptr = find->second.reader;
  }
  return item.iter;
}

void IteratorCache::SetPinnedItersMgr(
    PinnedIteratorsManager* pinned_iters_mgr) {
  pinned_iters_mgr_ = pinned_iters_mgr;
  for (auto pair : iterator_cache_) {
    pair.second.iter->SetPinnedItersMgr(pinned_iters_mgr);
  }
}


}
