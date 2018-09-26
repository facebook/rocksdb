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
      InternalIterator* (*)(void* arg, const FileMetaData*,
                            const DependFileMap&, Arena*, TableReader**);

  IteratorCache(const DependFileMap& depend_files, void* create_iter_arg,
                const CreateIterCallback& create_iter);
  ~IteratorCache();

  InternalIterator* GetIterator(const FileMetaData* f,
                                TableReader** reader_ptr = nullptr);

  InternalIterator* GetIterator(uint64_t file_number,
                                TableReader** reader_ptr = nullptr);

  const FileMetaData* GetFileMetaData(uint64_t file_number);

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr);

  Arena* GetArena() { return &arena_; }

 private:
  const std::unordered_map<uint64_t, const FileMetaData*>& depend_files_;
  void* callback_arg_;
  CreateIterCallback create_iter_;
  Arena arena_;
  PinnedIteratorsManager* pinned_iters_mgr_;

  struct CacheItem {
    InternalIterator* iter;
    TableReader* reader;
    const FileMetaData* meta;
  };
  std::unordered_map<uint64_t, CacheItem> iterator_map_;
};

}  // namespace rocksdb