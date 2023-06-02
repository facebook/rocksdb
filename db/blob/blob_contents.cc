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

}  // namespace ROCKSDB_NAMESPACE
