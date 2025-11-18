//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class BlockHandle;
struct ReadOptions;
template <typename T>
class CachableEntry;
class Block;
class BlockBasedTable;

class IOJob {
 public:
  std::vector<BlockHandle> block_handles;

  // Table reader for accessing block cache
  BlockBasedTable* table = nullptr;

  // Read options for the operation
  ReadOptions* read_options = nullptr;
};

class JobHandle {
 public:
  // Storage for pinned blocks (one per block handle)
  // Must be resized to match block_handles.size()
  std::vector<CachableEntry<Block>> pinned_blocks;
};

/*
 * IODispatcher is a component that dispatches IO operations to a ThreadPool
 * for asynchronous execution. It internally manages a ThreadPool with the
 * specified number of background threads.
 * */
class IODispatcher {
 public:
  virtual ~IODispatcher() {}

  virtual std::shared_ptr<JobHandle> SubmitJob(std::shared_ptr<IOJob> job) = 0;

  virtual unsigned int GetQueueLen() const = 0;
};

IODispatcher* NewIODispatcher(int num_threads);

}  // namespace ROCKSDB_NAMESPACE
