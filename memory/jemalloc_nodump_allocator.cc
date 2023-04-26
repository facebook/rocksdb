//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "memory/jemalloc_nodump_allocator.h"

#include <string>
#include <thread>

#include "port/port.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

Status NewJemallocNodumpAllocator(
    JemallocAllocatorOptions& options,
    std::shared_ptr<MemoryAllocator>* memory_allocator) {
  return NewJemallocNodumpAllocator(options, 1 /* num_arenas */,
                                    memory_allocator);
}

Status NewJemallocNodumpAllocator(
    JemallocAllocatorOptions& options, size_t num_arenas,
    std::shared_ptr<MemoryAllocator>* memory_allocator) {
  if (memory_allocator == nullptr) {
    return Status::InvalidArgument("memory_allocator must be non-null.");
  }
#ifndef ROCKSDB_JEMALLOC
  (void)options;
  (void)num_arenas;
  return Status::NotSupported("Not compiled with JEMALLOC");
#else
  std::unique_ptr<MemoryAllocator> allocator;
  switch (num_arenas) {
    case 1:
      allocator.reset(
          new JemallocNodumpAllocator<0 /* kLog2NumArenas */>(options));
      break;
    case 2:
      allocator.reset(
          new JemallocNodumpAllocator<1 /* kLog2NumArenas */>(options));
      break;
    case 4:
      allocator.reset(
          new JemallocNodumpAllocator<2 /* kLog2NumArenas */>(options));
      break;
    case 8:
      allocator.reset(
          new JemallocNodumpAllocator<3 /* kLog2NumArenas */>(options));
      break;
    case 16:
      allocator.reset(
          new JemallocNodumpAllocator<4 /* kLog2NumArenas */>(options));
      break;
    case 32:
      allocator.reset(
          new JemallocNodumpAllocator<5 /* kLog2NumArenas */>(options));
      break;
    case 64:
      allocator.reset(
          new JemallocNodumpAllocator<6 /* kLog2NumArenas */>(options));
      break;
    default:
      return Status::InvalidArgument(
          "log2_num_arenas must be between 0 and 8, inclusive");
  }
  Status s = allocator->PrepareOptions(ConfigOptions());
  if (s.ok()) {
    memory_allocator->reset(allocator.release());
  }
  return s;
#endif
}

}  // namespace ROCKSDB_NAMESPACE
