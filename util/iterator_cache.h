//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <cstdint>
#include <functional>
#include <unordered_map>

#include "table/internal_iterator.h"
#include "util/arena.h"

namespace rocksdb {

struct FileMetaData;
class PinnedIteratorsManager;
class RangeDelAggregator;
class TableReader;

class IteratorCache {
 public:
  using CreateIterCallback =
      std::function<InternalIterator*(const FileMetaData*, uint64_t, Arena*,
                                      RangeDelAggregator*, TableReader**)>;

  IteratorCache(const CreateIterCallback& create_iterator);
  ~IteratorCache();

  void NewRangeDelAgg(const InternalKeyComparator& icmp,
                      const std::vector<SequenceNumber>& snapshots);

  InternalIterator* GetIterator(const FileMetaData* f,
                                TableReader** reader_ptr = nullptr);

  InternalIterator* GetIterator(uint64_t sst_id,
                                TableReader** reader_ptr = nullptr);

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr);

  RangeDelAggregator* GetRangeDelAggregator() const {
    return range_del_agg_;
  }

 private:
  CreateIterCallback create_iterator_;
  Arena arena_;
  PinnedIteratorsManager* pinned_iters_mgr_;
  RangeDelAggregator* range_del_agg_;

  struct CacheItem {
    InternalIterator* iter;
    TableReader* reader;
  };
  std::unordered_map<uint64_t, CacheItem> iterator_cache_;
};

}