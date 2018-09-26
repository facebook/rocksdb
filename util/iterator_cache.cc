//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/iterator_cache.h"

#include "db/range_del_aggregator.h"

namespace rocksdb {

IteratorCache::IteratorCache(const DependFileMap& depend_files,
                             void* callback_arg,
                             const CreateIterCallback& create_iter)
    : depend_files_(depend_files),
      callback_arg_(callback_arg),
      create_iter_(create_iter),
      pinned_iters_mgr_(nullptr) {}

IteratorCache::~IteratorCache() {
  for (auto pair : iterator_map_) {
    pair.second.iter->~InternalIterator();
  }
}

InternalIterator* IteratorCache::GetIterator(const FileMetaData* f,
                                             TableReader** reader_ptr) {
  auto find = iterator_map_.find(f->fd.GetNumber());
  if (find != iterator_map_.end()) {
    if (reader_ptr != nullptr) {
      *reader_ptr = find->second.reader;
    }
    return find->second.iter;
  }
  CacheItem item;
  item.iter =
      create_iter_(callback_arg_, f, depend_files_, &arena_, &item.reader);
  item.meta = f;
  assert(item.iter != nullptr);
  item.iter->SetPinnedItersMgr(pinned_iters_mgr_);
  iterator_map_.emplace(f->fd.GetNumber(), item);
  if (reader_ptr != nullptr) {
    *reader_ptr = item.reader;
  }
  return item.iter;
}

InternalIterator* IteratorCache::GetIterator(uint64_t file_number,
                                             TableReader** reader_ptr) {
  auto find = iterator_map_.find(file_number);
  if (find != iterator_map_.end()) {
    if (reader_ptr != nullptr) {
      *reader_ptr = find->second.reader;
    }
    return find->second.iter;
  }
  CacheItem item;
  auto find_f = depend_files_.find(file_number);
  if (find_f == depend_files_.end()) {
    auto s = Status::Corruption("Composite sst depend files missing");
    item.iter = NewErrorInternalIterator<Slice>(s, &arena_);
    item.reader = nullptr;
    item.meta = nullptr;
  } else {
    auto f = find_f->second;
    item.iter =
        create_iter_(callback_arg_, f, depend_files_, &arena_, &item.reader);
    item.meta = f;
    assert(item.iter != nullptr);
  }
  item.iter->SetPinnedItersMgr(pinned_iters_mgr_);
  iterator_map_.emplace(file_number, item);
  if (reader_ptr != nullptr) {
    *reader_ptr = item.reader;
  }
  return item.iter;
}

const FileMetaData* IteratorCache::GetFileMetaData(uint64_t file_number) {
  auto find = iterator_map_.find(file_number);
  if (find != iterator_map_.end()) {
    return find->second.meta;
  }
  auto find_depend = depend_files_.find(file_number);
  if (find_depend != depend_files_.end()) {
    return find_depend->second;
  }
  return nullptr;
}

void IteratorCache::SetPinnedItersMgr(
    PinnedIteratorsManager* pinned_iters_mgr) {
  pinned_iters_mgr_ = pinned_iters_mgr;
  for (auto pair : iterator_map_) {
    pair.second.iter->SetPinnedItersMgr(pinned_iters_mgr);
  }
}

}  // namespace rocksdb
