//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <memory>
#include <unordered_map>
#include <vector>

#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class BlockHandle;
struct ReadOptions;
struct AsyncIOState;

template <typename T>
class CachableEntry;
class Block;
class BlockBasedTable;

struct JobOptions {
  uint64_t io_coalesce_threshold = 16 * 1024;
  ReadOptions read_options;
};

class IOJob {
 public:
  std::vector<BlockHandle> block_handles;

  // Table reader for accessing block cache and index
  BlockBasedTable* table = nullptr;

  // Job execution options
  JobOptions job_options;
};

/*
 * ReadSet represents a set of blocks that may be in cache, being read
 * asynchronously, or need to be read synchronously. The Read() method
 * transparently handles all three cases.
 */
class ReadSet {
 public:
  ReadSet() = default;
  ~ReadSet();

  ReadSet(const ReadSet&) = delete;
  ReadSet& operator=(const ReadSet&) = delete;
  ReadSet(ReadSet&&) noexcept = delete;
  ReadSet& operator=(ReadSet&&) noexcept = delete;

  // Read a block by index
  // - If the block is in cache, returns it immediately
  // - If the block is being read asynchronously, polls for completion and
  // returns it
  // - If the block needs to be read, performs a synchronous read and returns it
  //
  // block_index: Index into the original IOJob's block_handles vector
  // out: Output parameter for the pinned block entry
  //
  // Returns: Status::OK() on success, error status otherwise
  Status ReadIndex(size_t block_index, CachableEntry<Block>* out);
  Status ReadOffset(size_t offset, CachableEntry<Block>* out);

  // Statistics accessors
  uint64_t GetNumSyncReads() const { return num_sync_reads_; }
  uint64_t GetNumAsyncReads() const { return num_async_reads_; }
  uint64_t GetNumCacheHits() const { return num_cache_hits_; }

 private:
  friend class IODispatcherImpl;

  // Job data
  std::shared_ptr<IOJob> job_;

  // Storage for pinned blocks (one per block handle in the job)
  std::vector<CachableEntry<Block>> pinned_blocks_;

  // Map from block index to async IO state for blocks being read asynchronously
  std::unordered_map<size_t, std::shared_ptr<AsyncIOState>> async_io_map_;

  // Statistics counters
  std::atomic<uint64_t> num_sync_reads_ = 0;
  std::atomic<uint64_t> num_async_reads_ = 0;
  std::atomic<uint64_t> num_cache_hits_ = 0;

  // Poll and process a specific async IO request
  Status PollAndProcessAsyncIO(size_t block_index);

  // Perform synchronous read for a specific block
  Status SyncRead(size_t block_index);
};

/*
 * IODispatcher handles IO operations synchronously or asynchronously based
 * on JobOptions. When async is true, it uses ReadAsync; when false, it uses
 * standard synchronous reads.
 * */
class IODispatcher {
 protected:
  IODispatcher() = default;

 public:
  virtual ~IODispatcher() {}

  IODispatcher(const IODispatcher&) = delete;
  IODispatcher& operator=(const IODispatcher&) = delete;
  IODispatcher(IODispatcher&&) = delete;
  IODispatcher& operator=(IODispatcher&&) = delete;

  // Submit a job for IO processing
  // job: The IO job to submit
  // read_set: Output parameter that will be populated with the ReadSet on
  // success Returns: Status::OK() on success, error status otherwise
  virtual Status SubmitJob(const std::shared_ptr<IOJob>& job,
                           std::shared_ptr<ReadSet>* read_set) = 0;
};

IODispatcher* NewIODispatcher();

}  // namespace ROCKSDB_NAMESPACE
