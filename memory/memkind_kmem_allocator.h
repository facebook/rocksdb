//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  Copyright (c) 2019 Intel Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifdef MEMKIND

#include <memkind.h>
#include "rocksdb/memory_allocator.h"

namespace rocksdb {

class MemkindKmemAllocator : public MemoryAllocator {
 public:
  const char* Name() const override { return "MemkindKmemAllocator"; };
  void* Allocate(size_t size) override;
  void Deallocate(void* p) override;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  size_t UsableSize(void* p, size_t /*allocation_size*/) const override;
#endif
};

}  // namespace rocksdb
#endif  // MEMKIND
