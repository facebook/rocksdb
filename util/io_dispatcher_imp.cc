//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/io_dispatcher_imp.h"

#include <memory>
#include <unordered_map>
#include <vector>

#include "file/random_access_file_reader.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_dispatcher.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/cachable_entry.h"
#include "table/block_based/reader_common.h"
#include "table/format.h"

namespace ROCKSDB_NAMESPACE {

// State for async IO operations (implementation detail)
struct AsyncIOState {
  ~AsyncIOState() { read_req.status.PermitUncheckedError(); }
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
ReadSet::~ReadSet() {
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

  // Case 1: Block is already in cache (from initial cache lookup)
  if (pinned_blocks_[block_index].GetValue()) {
    *out = std::move(pinned_blocks_[block_index]);
    // We only bump this if we are in async_io mode, as this means we have not
    // yet polled, and its in the cache, meaning we didn't put this in the
    // cache.
    if (job_->job_options.read_options.async_io) {
      num_async_reads_++;
    }
    return Status::OK();
  }

  // Case 2: Block has async IO in progress - poll and process
  if (job_->job_options.read_options.async_io) {
    auto it = async_io_map_.find(block_index);
    if (it != async_io_map_.end()) {
      Status s = PollAndProcessAsyncIO(block_index);
      if (!s.ok()) {
        return s;
      }
      num_async_reads_++;

      // After polling, the block should be in pinned_blocks_
      if (pinned_blocks_[block_index].GetValue()) {
        *out = std::move(pinned_blocks_[block_index]);
        return Status::OK();
      }

      return Status::IOError("Failed to process async IO result");
    }
  }

  // Case 3: Block needs synchronous read
  Status s = SyncRead(block_index);
  if (s.ok()) {
    *out = std::move(pinned_blocks_[block_index]);
    num_sync_reads_++;
  }
  return s;
}

Status ReadSet::ReadOffset(size_t offset, CachableEntry<Block>* out) {
  for (size_t i = 0; i < job_->block_handles.size(); i++) {
    const auto& handle = job_->block_handles[i];
    if (offset >= handle.offset() &&
        (offset < (handle.offset() + handle.size()))) {
      return ReadOffset(i, out);
    }
  }
  return Status::InvalidArgument();
}

// Poll and process async IO for a specific block
Status ReadSet::PollAndProcessAsyncIO(size_t block_index) {
  auto it = async_io_map_.find(block_index);
  if (it == async_io_map_.end()) {
    return Status::InvalidArgument("No async IO in progress for this block");
  }

  // IMPORTANT: Hold a copy of the shared_ptr, not a reference,
  // because we'll be erasing from async_io_map_ later which could
  // invalidate the reference
  auto async_state = it->second;
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

  // Determine which buffer to use
  const Slice buffer_data =
      rep->file->use_direct_io()
          ? Slice(static_cast<const char*>(async_state->aligned_buf.get()),
                  async_state->read_req.len)
          : Slice(async_state->buf.get(), async_state->read_req.len);

  // Process all blocks in this async request
  for (size_t i = 0; i < async_state->block_indices.size(); ++i) {
    const size_t idx = async_state->block_indices[i];
    const auto& block_handle = async_state->blocks[i];

    // Get decompressor
    UnownedPtr<Decompressor> decompressor = rep->decompressor.get();
    CachableEntry<DecompressorDict> cached_dict;

    if (rep->uncompression_dict_reader) {
      Status s =
          rep->uncompression_dict_reader->GetOrReadUncompressionDictionary(
              nullptr, job_->job_options.read_options, nullptr, nullptr,
              &cached_dict);
      if (!s.ok()) {
        return s;
      }
      if (cached_dict.GetValue()) {
        decompressor = cached_dict.GetValue()->decompressor_.get();
      }
    }

    // Create block from buffer
    const auto block_size_with_trailer =
        BlockBasedTable::BlockSizeWithTrailer(block_handle);
    const auto block_offset_in_buffer =
        block_handle.offset() - async_state->offset;

    CacheAllocationPtr data = AllocateBlock(
        block_size_with_trailer, GetMemoryAllocator(rep->table_options));
    memcpy(data.get(), buffer_data.data() + block_offset_in_buffer,
           block_size_with_trailer);
    BlockContents tmp_contents(std::move(data), block_handle.size());

#ifndef NDEBUG
    tmp_contents.has_trailer = rep->footer.GetBlockTrailerSize() > 0;
#endif

    Status s = job_->table->CreateAndPinBlockInCache<Block_kData>(
        job_->job_options.read_options, block_handle, decompressor,
        &tmp_contents, &pinned_blocks_[idx].As<Block_kData>());

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

  // Try to lookup and pin the block again (maybe it was added to cache by
  // another thread)
  Status s = job_->table->LookupAndPinBlocksInCache<Block_kData>(
      job_->job_options.read_options, block_handle,
      &pinned_blocks_[block_index].As<Block_kData>());

  if (s.ok() && pinned_blocks_[block_index].GetValue()) {
    return Status::OK();
  }

  // Block not in cache - perform synchronous read
  IOOptions io_opts;
  s = rep->file->PrepareIOOptions(job_->job_options.read_options, io_opts);
  if (!s.ok()) {
    return s;
  }

  const bool direct_io = rep->file->use_direct_io();
  const auto block_size_with_trailer =
      BlockBasedTable::BlockSizeWithTrailer(block_handle);

  // Setup read request
  FSReadRequest read_req;
  read_req.offset = block_handle.offset();
  read_req.len = block_size_with_trailer;

  std::unique_ptr<char[]> buf;
  AlignedBuf aligned_buf;

  if (direct_io) {
    read_req.scratch = nullptr;
  } else {
    buf.reset(new char[block_size_with_trailer]);
    read_req.scratch = buf.get();
  }

  // Perform synchronous read
  s = rep->file->Read(io_opts, read_req.offset, read_req.len, &read_req.result,
                      read_req.scratch, direct_io ? &aligned_buf : nullptr);
  if (!s.ok()) {
    return s;
  }

  if (!read_req.status.ok()) {
    return read_req.status;
  }

  // Determine which buffer to use
  const Slice buffer_data =
      direct_io ? Slice(static_cast<const char*>(aligned_buf.get()),
                        read_req.result.size())
                : Slice(buf.get(), read_req.result.size());

  // Get decompressor
  UnownedPtr<Decompressor> decompressor = rep->decompressor.get();
  CachableEntry<DecompressorDict> cached_dict;

  if (rep->uncompression_dict_reader) {
    s = rep->uncompression_dict_reader->GetOrReadUncompressionDictionary(
        nullptr, job_->job_options.read_options, nullptr, nullptr,
        &cached_dict);
    if (!s.ok()) {
      return s;
    }
    if (cached_dict.GetValue()) {
      decompressor = cached_dict.GetValue()->decompressor_.get();
    }
  }

  // Create block from buffer data
  CacheAllocationPtr data = AllocateBlock(
      block_size_with_trailer, GetMemoryAllocator(rep->table_options));
  memcpy(data.get(), buffer_data.data(), block_size_with_trailer);
  BlockContents tmp_contents(std::move(data), block_handle.size());

#ifndef NDEBUG
  tmp_contents.has_trailer = rep->footer.GetBlockTrailerSize() > 0;
#endif

  s = job_->table->CreateAndPinBlockInCache<Block_kData>(
      job_->job_options.read_options, block_handle, decompressor, &tmp_contents,
      &pinned_blocks_[block_index].As<Block_kData>());

  return s;
}

struct IODispatcherImpl::Impl {
  Impl();
  ~Impl();

  Status SubmitJob(std::shared_ptr<IOJob> job,
                   std::shared_ptr<ReadSet>* read_set);

 private:
  void PrepareIORequests(
      std::shared_ptr<IOJob> job,
      const std::vector<size_t>& block_indices_to_read,
      const std::vector<BlockHandle>& block_handles,
      std::vector<FSReadRequest>* read_reqs,
      std::vector<std::vector<size_t>>* coalesced_block_indices);

  void ExecuteAsyncIO(
      std::shared_ptr<IOJob> job, std::shared_ptr<ReadSet> read_set,
      std::vector<FSReadRequest>& read_reqs,
      const std::vector<std::vector<size_t>>& coalesced_block_indices);

  Status ExecuteSyncIO(
      std::shared_ptr<IOJob> job, std::shared_ptr<ReadSet> read_set,
      std::vector<FSReadRequest>& read_reqs,
      const std::vector<std::vector<size_t>>& coalesced_block_indices);

  Status CreateAndPinBlockFromBuffer(std::shared_ptr<IOJob> job,
                                     const BlockHandle& block,
                                     uint64_t buffer_start_offset,
                                     const Slice& buffer_data,
                                     CachableEntry<Block>& pinned_block_entry);
};

IODispatcherImpl::Impl::Impl() {}

IODispatcherImpl::Impl::~Impl() {}

Status IODispatcherImpl::Impl::SubmitJob(std::shared_ptr<IOJob> job,
                                         std::shared_ptr<ReadSet>* read_set) {
  if (!read_set) {
    return Status::InvalidArgument("read_set output parameter is null");
  }

  auto rs = std::make_shared<ReadSet>();

  // Initialize ReadSet
  rs->job_ = job;
  rs->pinned_blocks_.resize(job->block_handles.size());

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
    // All blocks found in cache
    *read_set = std::move(rs);
    return Status::OK();
  }

  // Prepare read requests - coalesce adjacent blocks
  std::vector<FSReadRequest> read_reqs;
  std::vector<std::vector<size_t>> coalesced_block_indices;
  PrepareIORequests(job, block_indices_to_read, job->block_handles, &read_reqs,
                    &coalesced_block_indices);

  // Step 3: Execute IO requests based on JobOptions
  if (job->job_options.read_options.async_io) {
    ExecuteAsyncIO(job, rs, read_reqs, coalesced_block_indices);
  } else {
    Status s = ExecuteSyncIO(job, rs, read_reqs, coalesced_block_indices);
    if (!s.ok()) {
      return s;
    }
    // We bump this for sync reads
    rs->num_sync_reads_ += block_indices_to_read.size();
  }

  *read_set = std::move(rs);
  return Status::OK();
}

void IODispatcherImpl::Impl::PrepareIORequests(
    std::shared_ptr<IOJob> job,
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

void IODispatcherImpl::Impl::ExecuteAsyncIO(
    std::shared_ptr<IOJob> job, std::shared_ptr<ReadSet> read_set,
    std::vector<FSReadRequest>& read_reqs,
    const std::vector<std::vector<size_t>>& coalesced_block_indices) {
  // Get file and IO options
  auto* rep = job->table->get_rep();
  IOOptions io_opts;
  Status s =
      rep->file->PrepareIOOptions(job->job_options.read_options, io_opts);
  if (!s.ok()) {
    return;
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
    // TODO: Probably need to make this more useful.
    auto cb = [](const FSReadRequest& /*req*/, void* /*cb_arg*/) {
      // Placeholder callback - currently does nothing
    };

    s = rep->file->ReadAsync(async_state->read_req, io_opts, cb,
                             async_state.get(), &async_state->io_handle,
                             &async_state->del_fn,
                             direct_io ? &async_state->aligned_buf : nullptr);

    if (!s.ok()) {
      continue;
    }
    assert(async_state->io_handle);

    // Mark the status as permitted unchecked since we'll check it later
    // in PollAndProcessAsyncIO

    // Add async state to map for all blocks in this request
    for (const auto idx : async_state->block_indices) {
      read_set->async_io_map_[idx] = async_state;
    }
  }
}

Status IODispatcherImpl::Impl::ExecuteSyncIO(
    std::shared_ptr<IOJob> job, std::shared_ptr<ReadSet> read_set,
    std::vector<FSReadRequest>& read_reqs,
    const std::vector<std::vector<size_t>>& coalesced_block_indices) {
  // Get file and IO options
  auto* rep = job->table->get_rep();
  IOOptions io_opts;
  Status s =
      rep->file->PrepareIOOptions(job->job_options.read_options, io_opts);
  if (!s.ok()) {
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
  s = rep->file->MultiRead(io_opts, read_reqs.data(), read_reqs.size(),
                           direct_io ? &aligned_buf : nullptr);
  if (!s.ok()) {
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

      s = CreateAndPinBlockFromBuffer(job, block_handle, read_req.offset,
                                      read_req.result,
                                      read_set->pinned_blocks_[block_idx]);

      if (!s.ok()) {
        return s;
      }
    }
  }

  return Status::OK();
}

Status IODispatcherImpl::Impl::CreateAndPinBlockFromBuffer(
    std::shared_ptr<IOJob> job, const BlockHandle& block,
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

IODispatcherImpl::IODispatcherImpl() : impl_(new Impl()) {}

IODispatcherImpl::~IODispatcherImpl() = default;

Status IODispatcherImpl::SubmitJob(std::shared_ptr<IOJob> job,
                                   std::shared_ptr<ReadSet>* read_set) {
  return impl_->SubmitJob(job, read_set);
}

IODispatcher* NewIODispatcher() { return new IODispatcherImpl(); }

}  // namespace ROCKSDB_NAMESPACE
