//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  Copyright (c) 2019 Intel Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/memory_allocator.h"

#include "memory/jemalloc_nodump_allocator.h"
#include "memory/memkind_kmem_allocator.h"
#include "options/customizable_helper.h"

namespace ROCKSDB_NAMESPACE {
static bool LoadAllocator(const std::string& id,
                          std::shared_ptr<MemoryAllocator>* result) {
  bool success = true;
  if (id == "JemallocNodumpAllocator") {
    JemallocAllocatorOptions options;
    Status status = NewJemallocNodumpAllocator(options, result);
    success = status.ok();
#ifdef MEMKIND
  } else if (id == "MemkindKmemAllocator") {
    result->reset(new MemkindkMemAllocator());
#endif  // MEMKIND
  } else {
    success = false;
  }
  return success;
}

Status MemoryAllocator::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    std::shared_ptr<MemoryAllocator>* result) {
  // Note that this can become much simpler if the ObjectRegistry supports
  // static configuration objects
  // (https://github.com/facebook/rocksdb/issues/6644). The DefaultAllocator
  // should not really be here, but should be part of the static configuration
  // objects
  static std::shared_ptr<MemoryAllocator> DefaultAllocator;
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status = Customizable::GetOptionsMap(value, &id, &opt_map);
  if (!status.ok()) {
    return status;
  } else if (DefaultAllocator != nullptr && id == DefaultAllocator->Name()) {
    *result = DefaultAllocator;
  } else {
    status = LoadSharedObject<MemoryAllocator>(config_options, value,
                                               LoadAllocator, result);
    if (status.ok() && DefaultAllocator.get() == nullptr) {
      DefaultAllocator = *result;
    }
  }
  return status;
}
}  // end namespace ROCKSDB_NAMESPACE
