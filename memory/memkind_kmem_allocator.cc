//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  Copyright (c) 2019 Intel Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef MEMKIND

#include "memkind_kmem_allocator.h"

namespace rocksdb {

void* MemkindKmemAllocator::Allocate(size_t size) {
  void* p = memkind_malloc(MEMKIND_DAX_KMEM, size);
  if (p == NULL) {
    throw std::bad_alloc();
  }
  return p;
}

void MemkindKmemAllocator::Deallocate(void* p) {
  memkind_free(MEMKIND_DAX_KMEM, p);
}

#ifdef ROCKSDB_MALLOC_USABLE_SIZE
size_t MemkindKmemAllocator::UsableSize(void* p,
                                        size_t /*allocation_size*/) const {
  return memkind_malloc_usable_size(MEMKIND_DAX_KMEM, p);
}
#endif  // ROCKSDB_MALLOC_USABLE_SIZE

}  // namespace rocksdb
#endif  // MEMKIND
