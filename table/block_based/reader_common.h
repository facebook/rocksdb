//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include "rocksdb/advanced_cache.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {
class Footer;

// Release the cached entry and decrement its ref count.
extern void ForceReleaseCachedEntry(void* arg, void* h);

inline MemoryAllocator* GetMemoryAllocator(
    const BlockBasedTableOptions& table_options) {
  return table_options.block_cache.get()
             ? table_options.block_cache->memory_allocator()
             : nullptr;
}

// Assumes block has a trailer past `data + block_size` as in format.h.
// `file_name` provided for generating diagnostic message in returned status.
// `offset` might be required for proper verification (also used for message).
//
// Returns Status::OK() on checksum match, or Status::Corruption() on checksum
// mismatch.
extern Status VerifyBlockChecksum(const Footer& footer, const char* data,
                                  size_t block_size,
                                  const std::string& file_name,
                                  uint64_t offset);
}  // namespace ROCKSDB_NAMESPACE
