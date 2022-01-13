//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  Copyright (c) 2019 Intel Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/memory_allocator.h"
#include "utilities/memory_allocators.h"

namespace ROCKSDB_NAMESPACE {

class MemkindKmemAllocator : public BaseMemoryAllocator {
 public:
  static const char* kClassName() { return "MemkindKmemAllocator"; }
  const char* Name() const override { return kClassName(); }
  static bool IsSupported() {
    std::string unused;
    return IsSupported(&unused);
  }

  static bool IsSupported(std::string* msg) {
#ifdef MEMKIND
    (void)msg;
    return true;
#else
    *msg = "Not compiled with MemKind";
    return false;
#endif
  }
  Status PrepareOptions(const ConfigOptions& options) override;

#ifdef MEMKIND
  void* Allocate(size_t size) override;
  void Deallocate(void* p) override;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  size_t UsableSize(void* p, size_t /*allocation_size*/) const override;
#endif
#endif  // MEMKIND
};

}  // namespace ROCKSDB_NAMESPACE
