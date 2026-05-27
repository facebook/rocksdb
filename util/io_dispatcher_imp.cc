//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/io_dispatcher_imp.h"

#include <deque>
#include <memory>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "file/random_access_file_reader.h"
#include "monitoring/statistics_impl.h"
#include "port/port.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_dispatcher.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/cachable_entry.h"
#include "table/block_based/reader_common.h"
#include "table/format.h"
#include "test_util/sync_point.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

struct PendingPrefetchRequest;

namespace {

constexpr size_t kMinFairnessReservationBytes = 8 * 1024;

}  // namespace

// IODispatcherImplData is the base that provides ReleaseMemory interface
// for ReadSets to call back when releasing blocks. Defined here so it's
// visible to ReadSet methods.
struct IODispatcherImplData {
  virtual ~IODispatcherImplData() = default;
  virtual void ReleaseMemory(size_t bytes, void* fairness_group_token) = 0;
};

// Helper function to create and pin a block from a buffer
// Used by both ReadSet::PollAndProcessAsyncIO and IODispatcherImpl::Impl
static Status CreateAndPinBlockFromBuffer(
    const std::shared_ptr<IOJob>& job, const BlockHandle& block,
    uint64_t buffer_start_offset, const Slice& buffer_data,
    CachableEntry<Block>& pinned_block_entry) {
  auto* rep = job->table->get_rep();

  // Get decompressor
  UnownedPtr<Decompressor> decompressor = rep->decompressor.get();
  CachableEntry<DecompressorDict> cached_dict;

  if (rep->uncompression_dict_reader) {
    Status s = rep->uncompression_dict_reader->GetOrReadUncompressionDictionary(
        nullptr, job->job_options.read_options, nullptr, nullptr, &cached_dict);
    if (!s.ok()) {
      return s;
    }
    if (cached_dict.GetValue()) {
      decompressor = cached_dict.GetValue()->decompressor_.get();
    }
  }

  // Create block from buffer data
  const auto block_size_with_trailer =
      BlockBasedTable::BlockSizeWithTrailer(block);
  const auto block_offset_in_buffer = block.offset() - buffer_start_offset;

  CacheAllocationPtr data = AllocateBlock(
      block_size_with_trailer, GetMemoryAllocator(rep->table_options));
  memcpy(data.get(), buffer_data.data() + block_offset_in_buffer,
         block_size_with_trailer);
  BlockContents tmp_contents(std::move(data), block.size());

#ifndef NDEBUG
  tmp_contents.has_trailer = rep->footer.GetBlockTrailerSize() > 0;
#endif

  return job->table->CreateAndPinBlockInCache<Block_kData>(
      job->job_options.read_options, block, decompressor, &tmp_contents,
      &pinned_block_entry.As<Block_kData>());
}

// State for async IO operations (implementation detail)
struct AsyncIOState {
  AsyncIOState() : offset(static_cast<uint64_t>(-1)) {}
  ~AsyncIOState() { read_req.status.PermitUncheckedError(); }

  AsyncIOState(const AsyncIOState&) = delete;
  AsyncIOState& operator=(const AsyncIOState&) = delete;
  AsyncIOState(AsyncIOState&&) = default;
  AsyncIOState& operator=(AsyncIOState&&) = default;

  std::unique_ptr<char[]> buf;
  AlignedBuf aligned_buf;
  void* io_handle = nullptr;
  IOHandleDeleter del_fn;
  uint64_t offset;
  std::vector<size_t> block_indices;
  std::vector<BlockHandle> blocks;
  FSReadRequest read_req;
};

// ReadSet destructor - clean up IO handles
// Must call AbortIO before deleting handles to avoid use-after-free when
// io_uring completions arrive for deleted handles.
ReadSet::~ReadSet() {
  // Release memory for any blocks that actually acquired prefetch budget.
  if (auto dispatcher_data = dispatcher_data_.lock()) {
    for (size_t i = 0; i < acquired_bytes_.size(); ++i) {
      if (acquired_bytes_[i] > 0) {
        dispatcher_data->ReleaseMemory(acquired_bytes_[i],
                                       fairness_group_token_);
        acquired_bytes_[i] = 0;
      }
    }
  }

  if (async_io_map_.empty()) {
    return;
  }

  // Collect unique pending IO handles (multiple block indices may share the
  // same async_state due to coalescing)
  std::vector<void*> pending_handles;
  std::unordered_set<void*> seen_handles;
  for (auto& pair : async_io_map_) {
    auto& async_state = pair.second;
    if (async_state->io_handle != nullptr &&
        seen_handles.find(async_state->io_handle) == seen_handles.end()) {
      pending_handles.push_back(async_state->io_handle);
      seen_handles.insert(async_state->io_handle);
    }
  }

  // Abort all pending IO operations before deleting handles
  if (!pending_handles.empty() && fs_) {
    // AbortIO cancels pending requests and waits for completions
    IOStatus s = fs_->AbortIO(pending_handles);
    (void)s;  // Ignore errors in destructor
  }

  // Now safe to delete the handles
  for (auto& pair : async_io_map_) {
    auto& async_state = pair.second;
    if (async_state->io_handle != nullptr && async_state->del_fn != nullptr) {
      async_state->del_fn(async_state->io_handle);
      async_state->io_handle = nullptr;
    }
  }
}

// Main Read() method - transparently handles cache, async IO, and sync reads
Status ReadSet::ReadIndex(size_t block_index, CachableEntry<Block>* out) {
  // Bounds check
  if (block_index >= pinned_blocks_.size()) {
    return Status::InvalidArgument("Block index out of range");
  }

  // Case 1: Block is already available (from cache or sync read during
  // SubmitJob)
  if (pinned_blocks_[block_index].GetValue()) {
    *out = std::move(pinned_blocks_[block_index]);
    // Release memory accounting for prefetched blocks. After moving the value
    // out, ReleaseBlock() and the destructor check pinned_blocks_.GetValue()
    // which will be null, so they won't release memory again.
    if (block_index < acquired_bytes_.size() &&
        acquired_bytes_[block_index] > 0) {
      if (auto dispatcher_data = dispatcher_data_.lock()) {
        dispatcher_data->ReleaseMemory(acquired_bytes_[block_index],
                                       fairness_group_token_);
      }
      acquired_bytes_[block_index] = 0;
    }
    // Note: Statistics for this block were already counted during SubmitJob
    // (either as cache hit or sync read)
    return Status::OK();
  }

  // Case 2: Block has async IO in progress - poll and process
  if (job_->job_options.read_options.async_io) {
    auto it = async_io_map_.find(block_index);
    if (it != async_io_map_.end()) {
      // Get the number of blocks in this coalesced async request BEFORE polling
      // (since PollAndProcessAsyncIO will remove entries from the map)
      size_t num_blocks_in_request = it->second->block_indices.size();

      if (Status s = PollAndProcessAsyncIO(it->second); !s.ok()) {
        return s;
      }
      // Count all blocks that were read in this async request
      num_async_reads_.fetch_add(num_blocks_in_request,
                                 std::memory_order_relaxed);

      // After polling, the block should be in pinned_blocks_
      if (pinned_blocks_[block_index].GetValue()) {
        *out = std::move(pinned_blocks_[block_index]);
        // Release memory accounting (same as case 1 above)
        if (block_index < acquired_bytes_.size() &&
            acquired_bytes_[block_index] > 0) {
          if (auto dispatcher_data = dispatcher_data_.lock()) {
            dispatcher_data->ReleaseMemory(acquired_bytes_[block_index],
                                           fairness_group_token_);
          }
          acquired_bytes_[block_index] = 0;
        }
        return Status::OK();
      }

      return Status::IOError("Failed to process async IO result");
    }
  }

  // Case 3: Block needs synchronous read (pending or never-dispatched blocks).
  // No ReleaseMemory() needed here because blocks reaching this path never had
  // TryAcquireMemory() called -- they were either pending prefetch or skipped
  // during SubmitJob. block_sizes_[block_index] may be > 0 (set during
  // SubmitJob for all uncached blocks) but that does not imply memory was
  // acquired.
  RemoveFromPending(block_index);

  Status s = SyncRead(block_index);
  if (s.ok()) {
    *out = std::move(pinned_blocks_[block_index]);
    num_sync_reads_.fetch_add(1, std::memory_order_relaxed);
  }
  return s;
}

Status ReadSet::ReadOffset(size_t offset, CachableEntry<Block>* out) {
  if (sorted_block_indices_.empty()) {
    return Status::InvalidArgument("ReadSet not initialized");
  }

  // Use binary search on the sorted index to find the block containing offset.
  // sorted_block_indices_ contains original indices sorted by block offset.
  const auto& block_handles = job_->block_handles;

  // Binary search for the first block whose offset is > offset, then back up
  auto it = std::upper_bound(sorted_block_indices_.begin(),
                             sorted_block_indices_.end(), offset,
                             [&block_handles](size_t off, size_t idx) {
                               return off < block_handles[idx].offset();
                             });

  // If it == begin(), offset is before all blocks
  if (it == sorted_block_indices_.begin()) {
    return Status::InvalidArgument("Offset not found in any block");
  }

  // Back up to the candidate block (largest offset <= our offset)
  --it;
  size_t candidate_idx = *it;
  const auto& handle = block_handles[candidate_idx];

  // Check if offset falls within this block
  if (offset >= handle.offset() && offset < (handle.offset() + handle.size())) {
    return ReadIndex(candidate_idx, out);
  }

  return Status::InvalidArgument("Offset not found in any block");
}

void ReadSet::ReleaseBlock(size_t block_index) {
  if (block_index >= pinned_blocks_.size()) {
    return;
  }

  // Remove from pending if applicable
  RemoveFromPending(block_index);

  // Release memory BEFORE unpinning
  if (pinned_blocks_[block_index].GetValue() &&
      block_index < acquired_bytes_.size() &&
      acquired_bytes_[block_index] > 0) {
    if (auto dispatcher_data = dispatcher_data_.lock()) {
      dispatcher_data->ReleaseMemory(acquired_bytes_[block_index],
                                     fairness_group_token_);
    }
    acquired_bytes_[block_index] = 0;  // Prevent double-release
  }

  // Unpin the block from cache
  pinned_blocks_[block_index].Reset();
  // Clean up any pending async IO for this block
  async_io_map_.erase(block_index);
}

bool ReadSet::IsBlockAvailable(size_t block_index) const {
  if (block_index >= pinned_blocks_.size()) {
    return false;
  }
  // Block is available if it hasn't been released (still has a value or
  // has pending async IO)
  return pinned_blocks_[block_index].GetValue() != nullptr ||
         async_io_map_.find(block_index) != async_io_map_.end();
}

// Poll and process async IO for a specific block
Status ReadSet::PollAndProcessAsyncIO(
    const std::shared_ptr<AsyncIOState>& async_state) {
  auto* rep = job_->table->get_rep();

  // Poll for IO completion using FileSystem Poll API
  std::vector<void*> io_handles = {async_state->io_handle};
  IOStatus io_s = rep->ioptions.env->GetFileSystem()->Poll(io_handles, 1);
  if (!io_s.ok()) {
    return io_s;
  }

  // Check for read errors
  if (!async_state->read_req.status.ok()) {
    return async_state->read_req.status;
  }

  // Use the result slice from the callback which has been correctly set
  // with any necessary alignment adjustments for direct IO
  const Slice& buffer_data = async_state->read_req.result;

  // Process all blocks in this async request
  for (size_t i = 0; i < async_state->block_indices.size(); ++i) {
    const size_t idx = async_state->block_indices[i];
    const auto& block_handle = async_state->blocks[i];

    Status s =
        CreateAndPinBlockFromBuffer(job_, block_handle, async_state->offset,
                                    buffer_data, pinned_blocks_[idx]);
    if (!s.ok()) {
      return s;
    }
  }

  // Clean up IO handle
  if (async_state->io_handle != nullptr && async_state->del_fn != nullptr) {
    async_state->del_fn(async_state->io_handle);
    async_state->io_handle = nullptr;
  }

  // Remove from map - all blocks in this request have been processed
  // Store indices in a temporary vector to avoid iterator invalidation
  std::vector<size_t> indices_to_remove = async_state->block_indices;
  for (const auto idx : indices_to_remove) {
    async_io_map_.erase(idx);
  }

  return Status::OK();
}

// Perform synchronous read for a specific block
// This performs a direct synchronous read from disk when the block is not in
// cache
Status ReadSet::SyncRead(size_t block_index) {
  const auto& block_handle = job_->block_handles[block_index];
  auto* rep = job_->table->get_rep();

  // Get dictionary-aware decompressor if available
  UnownedPtr<Decompressor> decompressor = rep->decompressor.get();
  CachableEntry<DecompressorDict> cached_dict;
  if (rep->uncompression_dict_reader) {
    Status s = rep->uncompression_dict_reader->GetOrReadUncompressionDictionary(
        nullptr, job_->job_options.read_options, nullptr, nullptr,
        &cached_dict);
    if (!s.ok()) {
      return s;
    }
    if (cached_dict.GetValue()) {
      decompressor = cached_dict.GetValue()->decompressor_.get();
    }
  }

  return job_->table->RetrieveBlock<Block_kData>(
      /*prefetch_buffer=*/nullptr, job_->job_options.read_options, block_handle,
      decompressor, &pinned_blocks_[block_index].As<Block_kData>(),
      /*get_context=*/nullptr, /*lookup_context=*/nullptr,
      /*for_compaction=*/false, /*use_cache=*/true,
      /*async_read=*/false, /*use_block_cache_for_lookup=*/true);
}

// A pre-coalesced group of blocks for prefetching
struct CoalescedPrefetchGroup {
  std::vector<size_t> block_indices;  // Blocks in this group (sorted by offset)
  size_t total_bytes = 0;             // Total bytes for this IO
};

// State for a pending memory request waiting to be granted
// Groups are pre-coalesced at queue time for efficient dispatch
struct PendingPrefetchRequest {
  std::weak_ptr<ReadSet> read_set;
  std::shared_ptr<IOJob> job;

  // Pre-coalesced groups ready for dispatch (ordered by first block index)
  std::deque<CoalescedPrefetchGroup> coalesced_groups;

  // Individual block indices still pending (for RemoveFromPending lookup)
  std::unordered_set<size_t> block_indices_to_prefetch;

  // Protected by groups_mutex_ after publication into pending_requests under
  // pending_mutex_.
  size_t pending_bytes_ = 0;
  mutable port::Mutex groups_mutex_;  // Protects groups and set modifications
};

// Remove a block from pending prefetch (called when block is read or released)
void ReadSet::RemoveFromPending(size_t block_index) {
  if (!pending_prefetch_flags_ || block_index >= pending_prefetch_flags_size_) {
    return;
  }

  // Atomic exchange - returns true only if it was previously true
  if (!pending_prefetch_flags_[block_index].exchange(
          false, std::memory_order_relaxed)) {
    return;  // Already removed or never pending
  }

  if (pending_request_) {
    MutexLock lock(&pending_request_->groups_mutex_);
    pending_request_->block_indices_to_prefetch.erase(block_index);
    pending_request_->pending_bytes_ -= block_sizes_[block_index];
  }
}

// IODispatcherImpl::Impl inherits from IODispatcherImplData
struct IODispatcherImpl::Impl : public IODispatcherImplData,
                                public std::enable_shared_from_this<Impl> {
  // Fairness groups partition prefetch reservation by where data lives in the
  // LSM:
  // - L1+ files share one group per level.
  // - L0 files each get their own group (overlapping L0 files are treated
  //   independently).
  // - Unknown level/file metadata falls back to the default group.
  //
  // Example: for an LSM with L0 files {F101, F102}, L1 files {F201...}, and L2
  // files {F301...}, the dispatcher tracks four groups:
  //   1) L0 file F101
  //   2) L0 file F102
  //   3) Level 1
  //   4) Level 2
  // This lets one hot level/file use its own reserved bytes without starving
  // prefetch from other levels/files when the global budget is under pressure.
  //
  // Scheduling is intentionally two-stage:
  // - SubmitJob() first tries to dispatch that job's coalesced groups in order.
  // - Groups that cannot get memory become pending in a FIFO queue owned by
  //   their fairness group.
  // - TryDispatchPendingPrefetches() then walks the pending fairness groups
  //   round-robin, giving each group at most one coalesced prefetch group per
  //   pass before re-enqueueing it at the back if it still has work.
  //
  // In the example above, once prefetch starts queueing, F101, F102, L1, and
  // L2 each get turns instead of one hot level/file keeping the whole pending
  // queue in front of the others.
  enum class FairnessScope {
    kDefault,
    kLevel,
    kL0File,
  };

  struct FairnessGroupKey {
    FairnessScope scope = FairnessScope::kDefault;
    int level = -1;
    uint64_t file_number = 0;

    bool operator==(const FairnessGroupKey& other) const {
      return scope == other.scope && level == other.level &&
             file_number == other.file_number;
    }
  };

  struct FairnessGroupKeyHash {
    size_t operator()(const FairnessGroupKey& key) const {
      size_t hash = std::hash<int>()(static_cast<int>(key.scope));
      hash ^=
          std::hash<int>()(key.level) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
      hash ^= std::hash<uint64_t>()(key.file_number) + 0x9e3779b9 +
              (hash << 6) + (hash >> 2);
      return hash;
    }
  };

  struct FairnessGroupState {
    explicit FairnessGroupState(uint64_t initial_debug_group_id = 0)
        : debug_group_id(initial_debug_group_id) {}

    std::atomic<size_t> reserved_bytes{0};
    std::atomic<size_t> memory_used_{0};
    std::deque<std::shared_ptr<PendingPrefetchRequest>> pending_requests;
    bool in_dispatch_queue = false;
    uint64_t debug_group_id = 0;
  };

  explicit Impl(const IODispatcherOptions& options);
  ~Impl() override;

  // Non-copyable and non-movable
  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;
  Impl(Impl&&) = delete;
  Impl& operator=(Impl&&) = delete;

  Status SubmitJob(const std::shared_ptr<IOJob>& job,
                   std::shared_ptr<ReadSet>* read_set);

  FairnessGroupState* GetOrCreateFairnessGroup(
      const std::shared_ptr<IOJob>& job);
  size_t GetReservedBytesForJob(const std::shared_ptr<IOJob>& job) const;

  // Memory management methods.
  bool TryAcquireMemory(size_t bytes, FairnessGroupState* fairness_group,
                        bool* used_reserved_bytes = nullptr);
  bool TryAcquireMemoryLocked(size_t bytes, FairnessGroupState* fairness_group,
                              bool* used_reserved_bytes = nullptr);
  void ReleaseMemory(size_t bytes, void* fairness_group_token) override;

  // Memory limiting state
  size_t max_prefetch_memory_bytes_ = 0;
  std::atomic<size_t> memory_used_{0};  // Atomic for lock-free accounting
  std::atomic<bool> has_pending_requests_{false};  // Fast-path check
  port::Mutex accounting_mutex_;
  port::Mutex pending_mutex_;
  std::unique_ptr<FairnessGroupState> default_fairness_group_;
  std::unordered_map<FairnessGroupKey, std::unique_ptr<FairnessGroupState>,
                     FairnessGroupKeyHash>
      fairness_groups_;
  std::deque<FairnessGroupState*> pending_fairness_groups_;
  Statistics* statistics_ = nullptr;
  uint64_t next_fairness_group_id_ = 1;

 private:
  void EnqueuePendingGroupLocked(FairnessGroupState* fairness_group);
  std::shared_ptr<PendingPrefetchRequest> GetFrontPendingRequestLocked(
      FairnessGroupState* fairness_group);

  void PrepareIORequests(
      const std::shared_ptr<IOJob>& job,
      const std::vector<size_t>& block_indices_to_read,
      const std::vector<BlockHandle>& block_handles,
      std::vector<FSReadRequest>* read_reqs,
      std::vector<std::vector<size_t>>* coalesced_block_indices);

  // Surface actual async IO errors to caller, but allow fallback for
  // unsupported cases. Returns block indices that need sync fallback.
  std::vector<size_t> ExecuteAsyncIO(
      const std::shared_ptr<IOJob>& job,
      const std::shared_ptr<ReadSet>& read_set,
      std::vector<FSReadRequest>& read_reqs,
      const std::vector<std::vector<size_t>>& coalesced_block_indices,
      Status* out_status);

  Status ExecuteSyncIO(
      const std::shared_ptr<IOJob>& job,
      const std::shared_ptr<ReadSet>& read_set,
      std::vector<FSReadRequest>& read_reqs,
      const std::vector<std::vector<size_t>>& coalesced_block_indices);

  // Try to dispatch pending prefetch requests when memory becomes available
  void TryDispatchPendingPrefetches();

  // Dispatch prefetch for a specific ReadSet (called when memory is available)
  void DispatchPrefetch(const std::shared_ptr<ReadSet>& read_set,
                        const std::shared_ptr<IOJob>& job,
                        const std::vector<size_t>& block_indices);

  // Pre-coalesce blocks into groups, respecting max_group_bytes size limit.
  // Returns groups ordered by first block index (earlier blocks first).
  std::vector<CoalescedPrefetchGroup> PreCoalesceBlocks(
      const std::shared_ptr<IOJob>& job, const std::shared_ptr<ReadSet>& rs,
      const std::vector<size_t>& block_indices, size_t max_group_bytes);
};

IODispatcherImpl::Impl::Impl(const IODispatcherOptions& options)
    : max_prefetch_memory_bytes_(options.max_prefetch_memory_bytes),
      default_fairness_group_(std::make_unique<FairnessGroupState>(0)),
      statistics_(options.statistics) {}

IODispatcherImpl::Impl::~Impl() {}

size_t IODispatcherImpl::Impl::GetReservedBytesForJob(
    const std::shared_ptr<IOJob>& job) const {
  auto* rep = job->table->get_rep();
  // Match the normal iterator's readahead policy as closely as possible:
  // reserve the steady-state max readahead when configured, otherwise fall
  // back to the initial readahead hint, then to a small floor so each fairness
  // group still gets some protected headroom.
  size_t reserved_bytes = rep->table_options.max_auto_readahead_size;
  if (reserved_bytes == 0) {
    reserved_bytes = rep->table_options.initial_auto_readahead_size;
  }
  if (reserved_bytes == 0) {
    reserved_bytes = kMinFairnessReservationBytes;
  }
  return reserved_bytes;
}

IODispatcherImpl::Impl::FairnessGroupState*
IODispatcherImpl::Impl::GetOrCreateFairnessGroup(
    const std::shared_ptr<IOJob>& job) {
  auto* rep = job->table ? job->table->get_rep() : nullptr;
  const int level = rep ? rep->level : -1;
  const uint64_t file_number = rep ? rep->sst_number_for_tracing() : UINT64_MAX;
  size_t reserved_bytes = 0;

  FairnessGroupState* fairness_group = default_fairness_group_.get();
  FairnessGroupKey key;
  if (rep != nullptr) {
    if (level == 0 && file_number != UINT64_MAX) {
      key.scope = FairnessScope::kL0File;
      key.level = level;
      key.file_number = file_number;
    } else if (level > 0) {
      key.scope = FairnessScope::kLevel;
      key.level = level;
    }
  }

  if (key.scope != FairnessScope::kDefault) {
    reserved_bytes = GetReservedBytesForJob(job);
    MutexLock lock(&accounting_mutex_);
    auto it = fairness_groups_.find(key);
    if (it == fairness_groups_.end()) {
      auto new_group =
          std::make_unique<FairnessGroupState>(next_fairness_group_id_++);
      new_group->reserved_bytes.store(reserved_bytes,
                                      std::memory_order_relaxed);
      fairness_group = new_group.get();
      fairness_groups_.emplace(key, std::move(new_group));
    } else {
      fairness_group = it->second.get();
      const size_t existing_reserved =
          fairness_group->reserved_bytes.load(std::memory_order_relaxed);
      fairness_group->reserved_bytes.store(
          std::max(existing_reserved, reserved_bytes),
          std::memory_order_relaxed);
    }
  }

#ifndef NDEBUG
  auto sync_point_info =
      std::make_tuple(level, file_number, fairness_group->debug_group_id);
  TEST_SYNC_POINT_CALLBACK("IODispatcherImpl::GetFairnessGroup",
                           &sync_point_info);
#endif

  return fairness_group;
}

bool IODispatcherImpl::Impl::TryAcquireMemory(
    size_t bytes, IODispatcherImpl::Impl::FairnessGroupState* fairness_group,
    bool* used_reserved_bytes) {
  if (max_prefetch_memory_bytes_ == 0) {
    if (used_reserved_bytes != nullptr) {
      *used_reserved_bytes = false;
    }
    return true;  // No limit configured
  }

  MutexLock lock(&accounting_mutex_);
  return TryAcquireMemoryLocked(bytes, fairness_group, used_reserved_bytes);
}

bool IODispatcherImpl::Impl::TryAcquireMemoryLocked(
    size_t bytes, IODispatcherImpl::Impl::FairnessGroupState* fairness_group,
    bool* used_reserved_bytes) {
  if (max_prefetch_memory_bytes_ == 0) {
    if (used_reserved_bytes != nullptr) {
      *used_reserved_bytes = false;
    }
    return true;
  }

  const size_t current = memory_used_.load(std::memory_order_relaxed);
  const size_t group_current =
      fairness_group != nullptr
          ? fairness_group->memory_used_.load(std::memory_order_relaxed)
          : 0;
  const size_t group_reserved =
      fairness_group != nullptr
          ? fairness_group->reserved_bytes.load(std::memory_order_relaxed)
          : 0;
  const bool use_reserved = fairness_group != nullptr &&
                            current + bytes > max_prefetch_memory_bytes_ &&
                            group_current + bytes <= group_reserved;

  if (!use_reserved && current + bytes > max_prefetch_memory_bytes_) {
    RecordTick(statistics_, PREFETCH_MEMORY_REQUESTS_BLOCKED);
    if (used_reserved_bytes != nullptr) {
      *used_reserved_bytes = false;
    }
    return false;
  }

  memory_used_.fetch_add(bytes, std::memory_order_relaxed);
  if (fairness_group != nullptr) {
    fairness_group->memory_used_.fetch_add(bytes, std::memory_order_relaxed);
  }
  RecordTick(statistics_, PREFETCH_MEMORY_BYTES_GRANTED, bytes);

  if (used_reserved_bytes != nullptr) {
    *used_reserved_bytes = use_reserved;
  }

  if (fairness_group != nullptr) {
#ifndef NDEBUG
    auto sync_point_info =
        std::make_tuple(fairness_group->debug_group_id, bytes, use_reserved);
    TEST_SYNC_POINT_CALLBACK("IODispatcherImpl::TryAcquireMemory:Granted",
                             &sync_point_info);
#endif
  }

  return true;
}

void IODispatcherImpl::Impl::ReleaseMemory(size_t bytes,
                                           void* fairness_group_token) {
  if (max_prefetch_memory_bytes_ == 0 || bytes == 0) {
    return;  // No limit configured
  }

  auto* fairness_group =
      static_cast<IODispatcherImpl::Impl::FairnessGroupState*>(
          fairness_group_token);

  auto release_bytes = [bytes](std::atomic<size_t>* counter) {
    size_t current = counter->load(std::memory_order_relaxed);
    while (true) {
      const size_t next = current >= bytes ? current - bytes : 0;
      if (counter->compare_exchange_weak(current, next,
                                         std::memory_order_relaxed)) {
        assert(current >= bytes);
        return;
      }
    }
  };

  // Keep release lock-free, but saturate on unexpected underflow in release
  // builds so accounting cannot wrap to SIZE_MAX.
  release_bytes(&memory_used_);
  if (fairness_group != nullptr) {
    release_bytes(&fairness_group->memory_used_);
  }

  RecordTick(statistics_, PREFETCH_MEMORY_BYTES_RELEASED, bytes);

  // Fast-path: skip dispatch attempt if no pending requests
  // This avoids mutex contention in the common single-threaded iterator case
  if (!has_pending_requests_.load(std::memory_order_relaxed)) {
    return;
  }

  // Try to dispatch pending prefetches now that memory is available
  TryDispatchPendingPrefetches();
}

void IODispatcherImpl::Impl::EnqueuePendingGroupLocked(
    IODispatcherImpl::Impl::FairnessGroupState* fairness_group) {
  assert(fairness_group != nullptr);
  if (fairness_group->in_dispatch_queue ||
      fairness_group->pending_requests.empty()) {
    return;
  }
  pending_fairness_groups_.push_back(fairness_group);
  fairness_group->in_dispatch_queue = true;
  has_pending_requests_.store(true, std::memory_order_relaxed);
}

std::shared_ptr<PendingPrefetchRequest>
IODispatcherImpl::Impl::GetFrontPendingRequestLocked(
    IODispatcherImpl::Impl::FairnessGroupState* fairness_group) {
  assert(fairness_group != nullptr);
  while (!fairness_group->pending_requests.empty()) {
    const auto& pending = fairness_group->pending_requests.front();
    if (pending != nullptr && !pending->read_set.expired()) {
      return pending;
    }
    fairness_group->pending_requests.pop_front();
  }
  return nullptr;
}

void IODispatcherImpl::Impl::TryDispatchPendingPrefetches() {
  while (true) {
    size_t groups_to_process = 0;
    {
      MutexLock lock(&pending_mutex_);
      groups_to_process = pending_fairness_groups_.size();
      if (groups_to_process == 0) {
        has_pending_requests_.store(false, std::memory_order_relaxed);
        return;
      }
    }

    bool dispatched_in_round = false;
    while (groups_to_process-- > 0) {
      IODispatcherImpl::Impl::FairnessGroupState* fairness_group = nullptr;
      std::shared_ptr<PendingPrefetchRequest> pending;
      {
        MutexLock lock(&pending_mutex_);
        if (pending_fairness_groups_.empty()) {
          has_pending_requests_.store(false, std::memory_order_relaxed);
          return;
        }
        fairness_group = pending_fairness_groups_.front();
        pending_fairness_groups_.pop_front();
        fairness_group->in_dispatch_queue = false;
        pending = GetFrontPendingRequestLocked(fairness_group);
        if (pending == nullptr) {
          continue;
        }
      }

      auto read_set = pending->read_set.lock();
      if (!read_set) {
        {
          MutexLock lock(&pending_mutex_);
          if (!fairness_group->pending_requests.empty() &&
              fairness_group->pending_requests.front() == pending) {
            fairness_group->pending_requests.pop_front();
          }
          if (GetFrontPendingRequestLocked(fairness_group) != nullptr) {
            EnqueuePendingGroupLocked(fairness_group);
          } else if (pending_fairness_groups_.empty()) {
            has_pending_requests_.store(false, std::memory_order_relaxed);
          }
        }
        continue;
      }

      std::vector<size_t> blocks_to_dispatch;
      bool request_complete = false;
      {
        MutexLock lock(&pending->groups_mutex_);

        while (!pending->coalesced_groups.empty()) {
          auto& coalesced_group = pending->coalesced_groups.front();

          std::vector<size_t> remaining_blocks;
          size_t remaining_bytes = 0;
          for (size_t idx : coalesced_group.block_indices) {
            if (pending->block_indices_to_prefetch.count(idx) > 0) {
              remaining_blocks.push_back(idx);
              remaining_bytes += read_set->block_sizes_[idx];
            }
          }

          if (remaining_blocks.empty()) {
            pending->coalesced_groups.pop_front();
            continue;
          }

          coalesced_group.block_indices = remaining_blocks;
          coalesced_group.total_bytes = remaining_bytes;

          if (TryAcquireMemory(remaining_bytes, fairness_group)) {
            blocks_to_dispatch = std::move(coalesced_group.block_indices);
            for (size_t idx : blocks_to_dispatch) {
              pending->block_indices_to_prefetch.erase(idx);
            }
            pending->pending_bytes_ -= remaining_bytes;
            pending->coalesced_groups.pop_front();
          }
          break;
        }

        request_complete = pending->coalesced_groups.empty() ||
                           pending->block_indices_to_prefetch.empty() ||
                           pending->pending_bytes_ == 0;
      }

      {
        MutexLock lock(&pending_mutex_);
        if (request_complete && !fairness_group->pending_requests.empty() &&
            fairness_group->pending_requests.front() == pending) {
          fairness_group->pending_requests.pop_front();
          read_set->pending_request_.reset();
        }

        if (GetFrontPendingRequestLocked(fairness_group) != nullptr) {
          EnqueuePendingGroupLocked(fairness_group);
        } else if (pending_fairness_groups_.empty()) {
          has_pending_requests_.store(false, std::memory_order_relaxed);
        }
      }

      if (read_set->pending_prefetch_flags_) {
        for (size_t idx : blocks_to_dispatch) {
          if (idx < read_set->pending_prefetch_flags_size_) {
            read_set->pending_prefetch_flags_[idx].store(
                false, std::memory_order_relaxed);
          }
        }
      }

      if (!blocks_to_dispatch.empty()) {
        DispatchPrefetch(read_set, pending->job, blocks_to_dispatch);
        dispatched_in_round = true;
      }
    }

    if (!dispatched_in_round) {
      return;
    }
  }
}

void IODispatcherImpl::Impl::DispatchPrefetch(
    const std::shared_ptr<ReadSet>& read_set, const std::shared_ptr<IOJob>& job,
    const std::vector<size_t>& block_indices) {
  for (size_t idx : block_indices) {
    read_set->acquired_bytes_[idx] = read_set->block_sizes_[idx];
  }

  // Sync point for testing partial prefetch - passes number of blocks being
  // dispatched
  TEST_SYNC_POINT_CALLBACK("IODispatcherImpl::DispatchPrefetch:BlockCount",
                           const_cast<std::vector<size_t>*>(&block_indices));

  auto release_undelivered_memory = [&]() {
    for (size_t idx : block_indices) {
      if (idx >= read_set->acquired_bytes_.size() ||
          read_set->acquired_bytes_[idx] == 0) {
        continue;
      }

      const bool delivered =
          read_set->pinned_blocks_[idx].GetValue() != nullptr ||
          read_set->async_io_map_.find(idx) != read_set->async_io_map_.end();
      if (!delivered) {
        ReleaseMemory(read_set->acquired_bytes_[idx],
                      read_set->fairness_group_token_);
        read_set->acquired_bytes_[idx] = 0;
      }
    }
  };

  // Prepare and execute IO for the given blocks
  std::vector<FSReadRequest> read_reqs;
  std::vector<std::vector<size_t>> coalesced_block_indices;
  PrepareIORequests(job, block_indices, job->block_handles, &read_reqs,
                    &coalesced_block_indices);

  if (job->job_options.read_options.async_io) {
    Status async_status;
    std::vector<size_t> fallback_indices = ExecuteAsyncIO(
        job, read_set, read_reqs, coalesced_block_indices, &async_status);

    // For blocks where async is not supported, do sync IO
    if (!fallback_indices.empty()) {
      std::vector<FSReadRequest> sync_read_reqs;
      std::vector<std::vector<size_t>> sync_coalesced_indices;
      PrepareIORequests(job, fallback_indices, job->block_handles,
                        &sync_read_reqs, &sync_coalesced_indices);
      // Prefetch errors are ignored - user will get the error when reading
      Status s =
          ExecuteSyncIO(job, read_set, sync_read_reqs, sync_coalesced_indices);
      s.PermitUncheckedError();
      read_set->num_sync_reads_.fetch_add(fallback_indices.size(),
                                          std::memory_order_relaxed);
    }
    // Async errors are also ignored - user will get the error when reading
    async_status.PermitUncheckedError();
    release_undelivered_memory();
  } else {
    // Prefetch errors are ignored - user will get the error when reading
    Status s = ExecuteSyncIO(job, read_set, read_reqs, coalesced_block_indices);
    s.PermitUncheckedError();
    read_set->num_sync_reads_.fetch_add(block_indices.size(),
                                        std::memory_order_relaxed);
    release_undelivered_memory();
  }
}

Status IODispatcherImpl::Impl::SubmitJob(const std::shared_ptr<IOJob>& job,
                                         std::shared_ptr<ReadSet>* read_set) {
  if (!read_set) {
    return Status::InvalidArgument("read_set output parameter is null");
  }

  auto rs = std::make_shared<ReadSet>();

  // Initialize ReadSet
  rs->job_ = job;
  rs->fs_ = job->table->get_rep()->ioptions.env->GetFileSystem();
  rs->pinned_blocks_.resize(job->block_handles.size());
  rs->block_sizes_.resize(job->block_handles.size(), 0);
  rs->acquired_bytes_.resize(job->block_handles.size(), 0);
  auto* fairness_group = GetOrCreateFairnessGroup(job);
  rs->fairness_group_token_ = fairness_group;

  // Build sorted index for O(log n) ReadOffset lookups via binary search.
  // sorted_block_indices_[i] = original index of i-th smallest block by offset.
  rs->sorted_block_indices_.resize(job->block_handles.size());
  for (size_t i = 0; i < job->block_handles.size(); ++i) {
    rs->sorted_block_indices_[i] = i;
  }
  std::sort(rs->sorted_block_indices_.begin(), rs->sorted_block_indices_.end(),
            [&job](size_t a, size_t b) {
              return job->block_handles[a].offset() <
                     job->block_handles[b].offset();
            });

  // Step 1: Check cache and pin cached blocks
  std::vector<size_t> block_indices_to_read;

  for (size_t i = 0; i < job->block_handles.size(); ++i) {
    const auto& data_block_handle = job->block_handles[i];

    // Lookup and pin block in cache
    Status s = job->table->LookupAndPinBlocksInCache<Block_kData>(
        job->job_options.read_options, data_block_handle,
        &(rs->pinned_blocks_)[i].As<Block_kData>());

    if (!s.ok()) {
      continue;
    }

    if (!(rs->pinned_blocks_)[i].GetValue()) {
      // Block not in cache - needs to be read from disk
      block_indices_to_read.emplace_back(i);
    }
  }

  // Step 2: Prepare IO requests for blocks not in cache
  if (block_indices_to_read.empty()) {
    // All blocks found in cache - count them as cache hits
    rs->num_cache_hits_.store(job->block_handles.size(),
                              std::memory_order_relaxed);
    *read_set = std::move(rs);
    return Status::OK();
  }

  // Count cache hits (blocks that were found in cache during lookup above)
  rs->num_cache_hits_.store(
      job->block_handles.size() - block_indices_to_read.size(),
      std::memory_order_relaxed);

  // Calculate block sizes for uncached blocks
  for (const auto& idx : block_indices_to_read) {
    size_t block_size =
        BlockBasedTable::BlockSizeWithTrailer(job->block_handles[idx]);
    rs->block_sizes_[idx] = block_size;
  }

  // Store dispatcher reference for release callbacks
  rs->dispatcher_data_ = shared_from_this();

  // Pre-coalesce blocks into groups, respecting memory budget per group
  // This ensures we dispatch meaningful IO sizes, not tiny single-block IOs
  // Both memory-limited and non-memory-limited paths use the same coalescing
  const size_t max_coalesced_group_bytes =
      max_prefetch_memory_bytes_ == 0
          ? 0
          : std::max(
                max_prefetch_memory_bytes_,
                fairness_group->reserved_bytes.load(std::memory_order_relaxed));
  auto coalesced_groups = PreCoalesceBlocks(job, rs, block_indices_to_read,
                                            max_coalesced_group_bytes);

  std::vector<size_t> blocks_to_dispatch;
  std::deque<CoalescedPrefetchGroup> groups_to_queue;

  // Try to acquire memory for entire coalesced groups
  for (auto& group : coalesced_groups) {
    if (TryAcquireMemory(group.total_bytes, fairness_group)) {
      // Add all blocks from this group to dispatch
      for (size_t idx : group.block_indices) {
        blocks_to_dispatch.push_back(idx);
      }
    } else {
      // Queue this group for later
      groups_to_queue.push_back(std::move(group));
    }
  }

  // Dispatch acquired blocks immediately
  if (!blocks_to_dispatch.empty()) {
    DispatchPrefetch(rs, job, blocks_to_dispatch);
  }

  // Queue remaining groups for later (only applies when memory limiting)
  if (!groups_to_queue.empty()) {
    auto pending = std::make_shared<PendingPrefetchRequest>();
    pending->read_set = rs;
    pending->job = job;
    size_t pending_bytes = 0;
    for (const auto& group : groups_to_queue) {
      for (size_t idx : group.block_indices) {
        pending->block_indices_to_prefetch.insert(idx);
      }
      pending_bytes += group.total_bytes;
    }
    pending->coalesced_groups = std::move(groups_to_queue);
    pending->pending_bytes_ = pending_bytes;

    // Set up pending flags for queued blocks only
    size_t num_blocks = job->block_handles.size();
    rs->pending_prefetch_flags_ =
        std::make_unique<std::atomic<bool>[]>(num_blocks);
    rs->pending_prefetch_flags_size_ = num_blocks;
    for (size_t idx : pending->block_indices_to_prefetch) {
      rs->pending_prefetch_flags_[idx].store(true, std::memory_order_relaxed);
    }
    rs->pending_request_ = pending;

    {
      MutexLock lock(&pending_mutex_);
      fairness_group->pending_requests.push_back(std::move(pending));
      EnqueuePendingGroupLocked(fairness_group);
    }
  }

  *read_set = std::move(rs);
  return Status::OK();
}

void IODispatcherImpl::Impl::PrepareIORequests(
    const std::shared_ptr<IOJob>& job,
    const std::vector<size_t>& block_indices_to_read,
    const std::vector<BlockHandle>& block_handles,
    std::vector<FSReadRequest>* read_reqs,
    std::vector<std::vector<size_t>>* coalesced_block_indices) {
  // This is necessary because block handles may not be in sorted order
  std::vector<size_t> sorted_block_indices = block_indices_to_read;
  std::sort(sorted_block_indices.begin(), sorted_block_indices.end(),
            [&block_handles](size_t a, size_t b) {
              return block_handles[a].offset() < block_handles[b].offset();
            });

  assert(coalesced_block_indices->empty());
  coalesced_block_indices->resize(1);

  for (const auto& block_idx : sorted_block_indices) {
    if (!coalesced_block_indices->back().empty()) {
      // Check if we can coalesce with previous block
      const auto& last_block_handle =
          block_handles[coalesced_block_indices->back().back()];
      uint64_t last_block_end =
          last_block_handle.offset() +
          BlockBasedTable::BlockSizeWithTrailer(last_block_handle);
      uint64_t current_start = block_handles[block_idx].offset();

      if (current_start >
          last_block_end + job->job_options.io_coalesce_threshold) {
        // Gap too large - start new IO request
        coalesced_block_indices->emplace_back();
      }
    }
    coalesced_block_indices->back().emplace_back(block_idx);
  }

  // Create FSReadRequest for each coalesced group
  assert(read_reqs->empty());
  read_reqs->reserve(coalesced_block_indices->size());

  for (const auto& block_indices : *coalesced_block_indices) {
    assert(!block_indices.empty());

    // Find the min and max offsets in this coalesced group
    // Since blocks are now sorted, first has min offset and last has max
    const auto& first_block_handle = block_handles[block_indices[0]];
    const auto& last_block_handle = block_handles[block_indices.back()];

    const auto start_offset = first_block_handle.offset();
    const auto end_offset =
        last_block_handle.offset() +
        BlockBasedTable::BlockSizeWithTrailer(last_block_handle);

    assert(end_offset > start_offset);

    read_reqs->emplace_back();
    read_reqs->back().offset = start_offset;
    read_reqs->back().len = end_offset - start_offset;
    read_reqs->back().scratch = nullptr;
  }
}

std::vector<CoalescedPrefetchGroup> IODispatcherImpl::Impl::PreCoalesceBlocks(
    const std::shared_ptr<IOJob>& job, const std::shared_ptr<ReadSet>& rs,
    const std::vector<size_t>& block_indices, size_t max_group_bytes) {
  std::vector<CoalescedPrefetchGroup> groups;

  if (block_indices.empty()) {
    return groups;
  }

  const auto& block_handles = job->block_handles;
  const uint64_t coalesce_threshold = job->job_options.io_coalesce_threshold;

  // Sort block indices by offset for coalescing
  std::vector<size_t> sorted_indices = block_indices;
  std::sort(sorted_indices.begin(), sorted_indices.end(),
            [&block_handles](size_t a, size_t b) {
              return block_handles[a].offset() < block_handles[b].offset();
            });

  // Build coalesced groups respecting max_group_bytes
  groups.emplace_back();

  for (size_t idx : sorted_indices) {
    size_t block_size = rs->block_sizes_[idx];

    // Skip blocks that are individually larger than the memory budget
    // These will be read synchronously when needed (via ReadIndex fallback)
    if (max_group_bytes > 0 && block_size > max_group_bytes) {
      continue;
    }

    // Check if we need to start a new group
    bool start_new_group = false;

    if (!groups.back().block_indices.empty()) {
      // Check gap with previous block
      size_t last_idx = groups.back().block_indices.back();
      const auto& last_handle = block_handles[last_idx];
      uint64_t last_end = last_handle.offset() +
                          BlockBasedTable::BlockSizeWithTrailer(last_handle);
      uint64_t current_start = block_handles[idx].offset();

      if (current_start > last_end + coalesce_threshold) {
        start_new_group = true;  // Gap too large
      } else if (max_group_bytes > 0 &&
                 groups.back().total_bytes + block_size > max_group_bytes) {
        start_new_group = true;  // Would exceed size limit
      }
    }

    if (start_new_group) {
      groups.emplace_back();
    }

    groups.back().block_indices.push_back(idx);
    groups.back().total_bytes += block_size;
  }

  return groups;
}

std::vector<size_t> IODispatcherImpl::Impl::ExecuteAsyncIO(
    const std::shared_ptr<IOJob>& job, const std::shared_ptr<ReadSet>& read_set,
    std::vector<FSReadRequest>& read_reqs,
    const std::vector<std::vector<size_t>>& coalesced_block_indices,
    Status* out_status) {
  std::vector<size_t> fallback_block_indices;
  *out_status = Status::OK();

  // Get file and IO options
  auto* rep = job->table->get_rep();
  IOOptions io_opts;
  Status s =
      rep->file->PrepareIOOptions(job->job_options.read_options, io_opts);
  if (!s.ok()) {
    *out_status = s;
    return fallback_block_indices;
  }

  const bool direct_io = rep->file->use_direct_io();

  // Submit async read requests and store them in the ReadSet
  for (size_t i = 0; i < read_reqs.size(); ++i) {
    auto async_state = std::make_shared<AsyncIOState>();

    async_state->offset = read_reqs[i].offset;
    async_state->block_indices = coalesced_block_indices[i];
    async_state->read_req = std::move(read_reqs[i]);

    for (const auto idx : coalesced_block_indices[i]) {
      async_state->blocks.emplace_back(job->block_handles[idx]);
    }

    if (direct_io) {
      async_state->read_req.scratch = nullptr;
    } else {
      async_state->buf.reset(new char[async_state->read_req.len]);
      async_state->read_req.scratch = async_state->buf.get();
    }

    // Callback for async read completion
    // Store the result slice and status back into async_state so we can access
    // them after Poll() completes.
    auto cb = [](const FSReadRequest& req, void* cb_arg) {
      auto* state = static_cast<AsyncIOState*>(cb_arg);
      state->read_req.result = req.result;
      state->read_req.status = req.status;
    };

    s = rep->file->ReadAsync(async_state->read_req, io_opts, cb,
                             async_state.get(), &async_state->io_handle,
                             &async_state->del_fn,
                             direct_io ? &async_state->aligned_buf : nullptr);

    if (s.IsNotSupported()) {
      // Async IO may be compiled in but unavailable at runtime. Fall back to
      // the synchronous coalesced path for these blocks.
      for (const auto idx : coalesced_block_indices[i]) {
        fallback_block_indices.push_back(idx);
      }
      continue;
    }

    if (!s.ok()) {
      // Actual error - surface to caller
      *out_status = s;
      return fallback_block_indices;
    }

    if (async_state->io_handle == nullptr) {
      // Async IO not supported - add to fallback list for sync IO
      for (const auto idx : coalesced_block_indices[i]) {
        fallback_block_indices.push_back(idx);
      }
      continue;
    }

    // Add async state to map for all blocks in this request
    for (const auto idx : async_state->block_indices) {
      read_set->async_io_map_[idx] = async_state;
    }
  }

  return fallback_block_indices;
}

Status IODispatcherImpl::Impl::ExecuteSyncIO(
    const std::shared_ptr<IOJob>& job, const std::shared_ptr<ReadSet>& read_set,
    std::vector<FSReadRequest>& read_reqs,
    const std::vector<std::vector<size_t>>& coalesced_block_indices) {
  // Get file and IO options
  auto* rep = job->table->get_rep();
  IOOptions io_opts;
  if (Status s =
          rep->file->PrepareIOOptions(job->job_options.read_options, io_opts);
      !s.ok()) {
    return s;
  }

  const bool direct_io = rep->file->use_direct_io();

  // Setup scratch buffers for MultiRead
  std::unique_ptr<char[]> buf;

  if (direct_io) {
    for (auto& read_req : read_reqs) {
      read_req.scratch = nullptr;
    }
  } else {
    // Allocate a single contiguous buffer for all requests
    size_t total_len = 0;
    for (const auto& req : read_reqs) {
      total_len += req.len;
    }
    buf.reset(new char[total_len]);
    size_t offset = 0;
    for (auto& read_req : read_reqs) {
      read_req.scratch = buf.get() + offset;
      offset += read_req.len;
    }
  }

  // Execute MultiRead
  AlignedBuf aligned_buf;
  if (Status s =
          rep->file->MultiRead(io_opts, read_reqs.data(), read_reqs.size(),
                               direct_io ? &aligned_buf : nullptr);
      !s.ok()) {
    return s;
  }

  for (const auto& rq : read_reqs) {
    if (!rq.status.ok()) {
      return rq.status;
    }
  }

  // Process all blocks from the MultiRead results
  for (size_t i = 0; i < coalesced_block_indices.size(); ++i) {
    const auto& read_req = read_reqs[i];
    for (const auto& block_idx : coalesced_block_indices[i]) {
      const auto& block_handle = job->block_handles[block_idx];

      Status create_status = CreateAndPinBlockFromBuffer(
          job, block_handle, read_req.offset, read_req.result,
          read_set->pinned_blocks_[block_idx]);
      if (!create_status.ok()) {
        return create_status;
      }
    }
  }

  return Status::OK();
}

IODispatcherImpl::IODispatcherImpl()
    : impl_(std::make_shared<Impl>(IODispatcherOptions())) {}

IODispatcherImpl::IODispatcherImpl(const IODispatcherOptions& options)
    : impl_(std::make_shared<Impl>(options)) {}

IODispatcherImpl::~IODispatcherImpl() = default;

Status IODispatcherImpl::SubmitJob(const std::shared_ptr<IOJob>& job,
                                   std::shared_ptr<ReadSet>* read_set) {
  return impl_->SubmitJob(job, read_set);
}

IODispatcher* NewIODispatcher() { return new IODispatcherImpl(); }

IODispatcher* NewIODispatcher(const IODispatcherOptions& options) {
  return new IODispatcherImpl(options);
}

}  // namespace ROCKSDB_NAMESPACE
