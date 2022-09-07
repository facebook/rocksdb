//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_contents.h"

#include <cassert>

#include "cache/cache_entry_roles.h"
#include "cache/cache_helpers.h"
#include "port/malloc.h"

namespace ROCKSDB_NAMESPACE {

std::unique_ptr<BlobContents> BlobContents::Create(
    CacheAllocationPtr&& allocation, size_t size) {
  return std::unique_ptr<BlobContents>(
      new BlobContents(std::move(allocation), size));
}

size_t BlobContents::ApproximateMemoryUsage() const {
  size_t usage = 0;

  if (allocation_) {
    MemoryAllocator* const allocator = allocation_.get_deleter().allocator;

    if (allocator) {
      usage += allocator->UsableSize(allocation_.get(), data_.size());
    } else {
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      usage += malloc_usable_size(allocation_.get());
#else
      usage += data_.size();
#endif
    }
  }

#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  usage += malloc_usable_size(const_cast<BlobContents*>(this));
#else
  usage += sizeof(*this);
#endif

  return usage;
}

size_t BlobContents::SizeCallback(void* obj) {
  assert(obj);

  return static_cast<const BlobContents*>(obj)->size();
}

Status BlobContents::SaveToCallback(void* from_obj, size_t from_offset,
                                    size_t length, void* out) {
  assert(from_obj);

  const BlobContents* buf = static_cast<const BlobContents*>(from_obj);
  assert(buf->size() >= from_offset + length);

  memcpy(out, buf->data().data() + from_offset, length);

  return Status::OK();
}

Cache::CacheItemHelper* BlobContents::GetCacheItemHelper() {
  static Cache::CacheItemHelper cache_helper(
      &SizeCallback, &SaveToCallback,
      GetCacheEntryDeleterForRole<BlobContents, CacheEntryRole::kBlobValue>());

  return &cache_helper;
}

Status BlobContents::CreateCallback(CacheAllocationPtr&& allocation,
                                    const void* buf, size_t size,
                                    void** out_obj, size_t* charge) {
  assert(allocation);

  memcpy(allocation.get(), buf, size);

  std::unique_ptr<BlobContents> obj = Create(std::move(allocation), size);
  BlobContents* const contents = obj.release();

  *out_obj = contents;
  *charge = contents->ApproximateMemoryUsage();

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
