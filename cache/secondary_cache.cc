//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/secondary_cache.h"

#include "cache/cache_entry_roles.h"

namespace ROCKSDB_NAMESPACE {

namespace {

size_t SliceSize(void* obj) { return static_cast<Slice*>(obj)->size(); }

Status SliceSaveTo(void* from_obj, size_t from_offset, size_t length,
                   void* out) {
  const Slice& slice = *static_cast<Slice*>(from_obj);
  std::memcpy(out, slice.data() + from_offset, length);
  return Status::OK();
}

}  // namespace

Status SecondaryCache::InsertSaved(const Slice& key, const Slice& saved) {
  static Cache::CacheItemHelper helper{
      &SliceSize, &SliceSaveTo, GetNoopDeleterForRole<CacheEntryRole::kMisc>()};
  // NOTE: depends on Insert() being synchronous, not keeping pointer `&saved`
  return Insert(key, const_cast<Slice*>(&saved), &helper);
}

}  // namespace ROCKSDB_NAMESPACE
