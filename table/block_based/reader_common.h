//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include "rocksdb/cache.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {
// Release the cached entry and decrement its ref count.
extern void ForceReleaseCachedEntry(void* arg, void* h);

inline MemoryAllocator* GetMemoryAllocator(
    const BlockBasedTableOptions& table_options) {
  return table_options.block_cache.get()
             ? table_options.block_cache->memory_allocator()
             : nullptr;
}

inline MemoryAllocator* GetMemoryAllocatorForCompressedBlock(
    const BlockBasedTableOptions& table_options) {
  return table_options.block_cache_compressed.get()
             ? table_options.block_cache_compressed->memory_allocator()
             : nullptr;
}

// Assumes block has a trailer as in format.h. file_name and offset provided
// for generating a diagnostic message in returned status.
extern Status VerifyBlockChecksum(ChecksumType type, const char* data,
                                  size_t block_size,
                                  const std::string& file_name,
                                  uint64_t offset);
}  // namespace ROCKSDB_NAMESPACE
