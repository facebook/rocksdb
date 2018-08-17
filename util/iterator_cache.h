//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <cstdint>
#include <unordered_map>

#include "table/internal_iterator.h"
#include "util/arena.h"

namespace rocksdb {

struct FileMetaData;
class PinnedIteratorsManager;
class RangeDelAggregator;
class TableReader;

typedef std::unordered_map<uint64_t, const FileMetaData*> DependFileMap;

class IteratorCache {
 public:
  using CreateIterCallback =
      InternalIterator*(*)(void* arg, const FileMetaData*,
                           const DependFileMap&, Arena*, RangeDelAggregator*,
                           TableReader**);

  IteratorCache(const DependFileMap& depend_files, void* create_iter_arg,
                const CreateIterCallback& create_iter);
  ~IteratorCache();

  void NewRangeDelAgg(const InternalKeyComparator& icmp,
                      const std::vector<SequenceNumber>& snapshots);

  InternalIterator* GetIterator(const FileMetaData* f,
                                TableReader** reader_ptr = nullptr);

  InternalIterator* GetIterator(uint64_t sst_id,
                                TableReader** reader_ptr = nullptr);

  const FileMetaData* GetFileMetaData(uint64_t sst_id);

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr);

  Arena* GetArena() { return &arena_; }

  RangeDelAggregator* GetRangeDelAggregator() const {
    return range_del_agg_;
  }

 private:
  const std::unordered_map<uint64_t, const FileMetaData*>& depend_files_;
  void* create_iter_arg_;
  CreateIterCallback create_iter_;
  Arena arena_;
  PinnedIteratorsManager* pinned_iters_mgr_;
  RangeDelAggregator* range_del_agg_;

  struct CacheItem {
    InternalIterator* iter;
    TableReader* reader;
    const FileMetaData* meta;
  };
  std::unordered_map<uint64_t, CacheItem> iterator_map_;
};

}