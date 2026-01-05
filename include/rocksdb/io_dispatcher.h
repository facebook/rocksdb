//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <memory>
#include <unordered_map>
#include <vector>

#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
/*
 * IODispatcher is a class that allows users to submit groups of IO jobs to be
 * dispatched asynchronously (or synchronously), upon submission the
 * IODispatcher will return a ReadSet which act as an ownership object of those
 * IOs. Users read from their readset when they require the data, and either
 * poll for completion of the block, or read synchronously if the block is not
 * in cache at that point.
 *
 * ReadSets have RAII semantics, meaning on destruction they will cancel any on
 * going IO, and release the underlying pinned blocks.
 *
 * IODispatcher main goal is to act as control plane for all readers using the
 * dispatcher, allowing for future ratelimiting and smarter dispatching policies
 * in the future.
 *
* Example:
 // Submitting an IO job and reading blocks:
 //
 // std::shared_ptr<IOJob> job = std::make_shared<IOJob>();
 // job->table = table_reader;  // Provided BlockBasedTable*
 // job->job_options.io_coalesce_threshold = 32 * 1024;
 // job->job_options.read_options = read_options;  // Provided ReadOptions
 //
 // // Populate the job with block handles (e.g., from an index/iterator)
 // job->block_handles.push_back(handle1);
 // job->block_handles.push_back(handle2);
 // job->block_handles.push_back(handle3);
 //
 // std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
 // std::shared_ptr<ReadSet> read_set;
 // Status s = dispatcher->SubmitJob(job, &read_set);
 // if (!s.ok()) {
 //   // Handle submit error
 // }
 //
 // // Read by index
 // for (size_t i = 1; i < job->block_handles.size(); ++i) {
 //   CachableEntry<Block> block_entry;
 //   Status rs = read_set->ReadIndex(i, &block_entry);
 //   if (!rs.ok()) {
 //     // Handle read error
 //     continue;
 //   }
 //   // Use block_entry (block contents are pinned here)
 // }
 //
 // // Or read by byte offset
 // {
 //   size_t offset = static_cast<size_t>(job->block_handles.front().offset());
 //   CachableEntry<Block> block_entry;
 //   Status rs = read_set->ReadOffset(offset, &block_entry);
 //   if (rs.ok()) {
 //     // Use block_entry
 //   }
 // }
 //
 // // Stats
 // uint64_t cache_hits = read_set->GetNumCacheHits();
 // uint64_t async_reads = read_set->GetNumAsyncReads();
 // uint64_t sync_reads = read_set->GetNumSyncReads();

 */

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
  // Read a block by offset
  // - If the block is in cache, returns it immediately
  // - If the block is being read asynchronously, polls for completion and
  // returns it
  // - If the block needs to be read, performs a synchronous read and returns it

  // block_offset: Byte Offset into the SST file of the block.

  // out: Output parameter for the pinned block entry
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

  // Sorted index for binary search in ReadOffset.
  // sorted_block_indices_[i] is the original index of the i-th smallest block
  // by offset. Built once during SubmitJob for O(log n) ReadOffset lookups.
  std::vector<size_t> sorted_block_indices_;

  // Map from block index to async IO state for blocks being read
  // asynchronously. Multiple block indices may map to the same async state when
  // blocks are coalesced into a single IO request.
  std::unordered_map<size_t, std::shared_ptr<AsyncIOState>> async_io_map_;

  // Statistics counters
  std::atomic<uint64_t> num_sync_reads_ = 0;
  std::atomic<uint64_t> num_async_reads_ = 0;
  std::atomic<uint64_t> num_cache_hits_ = 0;

  // Poll and process a specific async IO request
  Status PollAndProcessAsyncIO(
      const std::shared_ptr<AsyncIOState>& async_state);

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
