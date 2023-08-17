//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/async_file_reader.h"
#include "util/coro_utils.h"

#if defined(WITHOUT_COROUTINES) || \
    (defined(USE_COROUTINES) && defined(WITH_COROUTINES))

namespace ROCKSDB_NAMESPACE {

// This function reads multiple data blocks from disk using Env::MultiRead()
// and optionally inserts them into the block cache. It uses the scratch
// buffer provided by the caller, which is contiguous. If scratch is a nullptr
// it allocates a separate buffer for each block. Typically, if the blocks
// need to be uncompressed and there is no compressed block cache, callers
// can allocate a temporary scratch buffer in order to minimize memory
// allocations.
// If options.fill_cache is true, it inserts the blocks into cache. If its
// false and scratch is non-null and the blocks are uncompressed, it copies
// the buffers to heap. In any case, the CachableEntry<Block> returned will
// own the data bytes.
// If compression is enabled and also there is no compressed block cache,
// the adjacent blocks are read out in one IO (combined read)
// batch - A MultiGetRange with only those keys with unique data blocks not
//         found in cache
// handles - A vector of block handles. Some of them me be NULL handles
// scratch - An optional contiguous buffer to read compressed blocks into
DEFINE_SYNC_AND_ASYNC(void, BlockBasedTable::RetrieveMultipleBlocks)
(const ReadOptions& options, const MultiGetRange* batch,
 const autovector<BlockHandle, MultiGetContext::MAX_BATCH_SIZE>* handles,
 autovector<Status, MultiGetContext::MAX_BATCH_SIZE>* statuses,
 autovector<CachableEntry<Block>, MultiGetContext::MAX_BATCH_SIZE>* results,
 char* scratch, const UncompressionDict& uncompression_dict) const {
  RandomAccessFileReader* file = rep_->file.get();
  const Footer& footer = rep_->footer;
  const ImmutableOptions& ioptions = rep_->ioptions;
  size_t read_amp_bytes_per_bit = rep_->table_options.read_amp_bytes_per_bit;
  MemoryAllocator* memory_allocator = GetMemoryAllocator(rep_->table_options);

  if (ioptions.allow_mmap_reads) {
    size_t idx_in_batch = 0;
    for (auto mget_iter = batch->begin(); mget_iter != batch->end();
         ++mget_iter, ++idx_in_batch) {
      BlockCacheLookupContext lookup_data_block_context(
          TableReaderCaller::kUserMultiGet);
      const BlockHandle& handle = (*handles)[idx_in_batch];
      if (handle.IsNull()) {
        continue;
      }

      (*statuses)[idx_in_batch] =
          RetrieveBlock(nullptr, options, handle, uncompression_dict,
                        &(*results)[idx_in_batch].As<Block_kData>(),
                        mget_iter->get_context, &lookup_data_block_context,
                        /* for_compaction */ false, /* use_cache */ true,
                        /* wait_for_cache */ true, /* async_read */ false);
    }
    CO_RETURN;
  }

  // In direct IO mode, blocks share the direct io buffer.
  // Otherwise, blocks share the scratch buffer.
  const bool use_shared_buffer = file->use_direct_io() || scratch != nullptr;

  autovector<FSReadRequest, MultiGetContext::MAX_BATCH_SIZE> read_reqs;
  size_t buf_offset = 0;
  size_t idx_in_batch = 0;

  uint64_t prev_offset = 0;
  size_t prev_len = 0;
  autovector<size_t, MultiGetContext::MAX_BATCH_SIZE> req_idx_for_block;
  autovector<size_t, MultiGetContext::MAX_BATCH_SIZE> req_offset_for_block;
  for (auto mget_iter = batch->begin(); mget_iter != batch->end();
       ++mget_iter, ++idx_in_batch) {
    const BlockHandle& handle = (*handles)[idx_in_batch];
    if (handle.IsNull()) {
      continue;
    }

    size_t prev_end = static_cast<size_t>(prev_offset) + prev_len;

    // If current block is adjacent to the previous one, at the same time,
    // compression is enabled and there is no compressed cache, we combine
    // the two block read as one.
    // We don't combine block reads here in direct IO mode, because when doing
    // direct IO read, the block requests will be realigned and merged when
    // necessary.
    if (use_shared_buffer && !file->use_direct_io() &&
        prev_end == handle.offset()) {
      req_offset_for_block.emplace_back(prev_len);
      prev_len += BlockSizeWithTrailer(handle);
    } else {
      // No compression or current block and previous one is not adjacent:
      // Step 1, create a new request for previous blocks
      if (prev_len != 0) {
        FSReadRequest req;
        req.offset = prev_offset;
        req.len = prev_len;
        if (file->use_direct_io()) {
          req.scratch = nullptr;
        } else if (use_shared_buffer) {
          req.scratch = scratch + buf_offset;
          buf_offset += req.len;
        } else {
          req.scratch = new char[req.len];
        }
        read_reqs.emplace_back(req);
      }

      // Step 2, remeber the previous block info
      prev_offset = handle.offset();
      prev_len = BlockSizeWithTrailer(handle);
      req_offset_for_block.emplace_back(0);
    }
    req_idx_for_block.emplace_back(read_reqs.size());

    PERF_COUNTER_ADD(block_read_count, 1);
    PERF_COUNTER_ADD(block_read_byte, BlockSizeWithTrailer(handle));
  }
  // Handle the last block and process the pending last request
  if (prev_len != 0) {
    FSReadRequest req;
    req.offset = prev_offset;
    req.len = prev_len;
    if (file->use_direct_io()) {
      req.scratch = nullptr;
    } else if (use_shared_buffer) {
      req.scratch = scratch + buf_offset;
    } else {
      req.scratch = new char[req.len];
    }
    read_reqs.emplace_back(req);
  }

  AlignedBuf direct_io_buf;
  {
    IOOptions opts;
    IOStatus s = file->PrepareIOOptions(options, opts);
    if (s.ok()) {
#if defined(WITH_COROUTINES)
      if (file->use_direct_io()) {
#endif  // WITH_COROUTINES
        s = file->MultiRead(opts, &read_reqs[0], read_reqs.size(),
                            &direct_io_buf, options.rate_limiter_priority);
#if defined(WITH_COROUTINES)
      } else {
        co_await batch->context()->reader().MultiReadAsync(
            file, opts, &read_reqs[0], read_reqs.size(), &direct_io_buf);
      }
#endif  // WITH_COROUTINES
    }
    if (!s.ok()) {
      // Discard all the results in this batch if there is any time out
      // or overall MultiRead error
      for (FSReadRequest& req : read_reqs) {
        req.status = s;
      }
    }
  }

  idx_in_batch = 0;
  size_t valid_batch_idx = 0;
  for (auto mget_iter = batch->begin(); mget_iter != batch->end();
       ++mget_iter, ++idx_in_batch) {
    const BlockHandle& handle = (*handles)[idx_in_batch];

    if (handle.IsNull()) {
      continue;
    }

    assert(valid_batch_idx < req_idx_for_block.size());
    assert(valid_batch_idx < req_offset_for_block.size());
    assert(req_idx_for_block[valid_batch_idx] < read_reqs.size());
    size_t& req_idx = req_idx_for_block[valid_batch_idx];
    size_t& req_offset = req_offset_for_block[valid_batch_idx];
    valid_batch_idx++;
    FSReadRequest& req = read_reqs[req_idx];
    Status s = req.status;
    if (s.ok()) {
      if ((req.result.size() != req.len) ||
          (req_offset + BlockSizeWithTrailer(handle) > req.result.size())) {
        s = Status::Corruption("truncated block read from " +
                               rep_->file->file_name() + " offset " +
                               std::to_string(handle.offset()) + ", expected " +
                               std::to_string(req.len) + " bytes, got " +
                               std::to_string(req.result.size()));
      }
    }

    BlockContents serialized_block;
    if (s.ok()) {
      if (!use_shared_buffer) {
        // We allocated a buffer for this block. Give ownership of it to
        // BlockContents so it can free the memory
        assert(req.result.data() == req.scratch);
        assert(req.result.size() == BlockSizeWithTrailer(handle));
        assert(req_offset == 0);
        serialized_block =
            BlockContents(std::unique_ptr<char[]>(req.scratch), handle.size());
      } else {
        // We used the scratch buffer or direct io buffer
        // which are shared by the blocks.
        // serialized_block does not have the ownership.
        serialized_block =
            BlockContents(Slice(req.result.data() + req_offset, handle.size()));
      }
#ifndef NDEBUG
      serialized_block.has_trailer = true;
#endif

      if (options.verify_checksums) {
        PERF_TIMER_GUARD(block_checksum_time);
        const char* data = req.result.data();
        // Since the scratch might be shared, the offset of the data block in
        // the buffer might not be 0. req.result.data() only point to the
        // begin address of each read request, we need to add the offset
        // in each read request. Checksum is stored in the block trailer,
        // beyond the payload size.
        s = VerifyBlockChecksum(footer.checksum_type(), data + req_offset,
                                handle.size(), rep_->file->file_name(),
                                handle.offset());
        TEST_SYNC_POINT_CALLBACK("RetrieveMultipleBlocks:VerifyChecksum", &s);
      }
    } else if (!use_shared_buffer) {
      // Free the allocated scratch buffer.
      delete[] req.scratch;
    }

    if (s.ok()) {
      // When the blocks share the same underlying buffer (scratch or direct io
      // buffer), we may need to manually copy the block into heap if the
      // serialized block has to be inserted into a cache. That falls into the
      // following cases -
      // 1. serialized block is not compressed, it needs to be inserted into
      //    the uncompressed block cache if there is one
      // 2. If the serialized block is compressed, it needs to be inserted
      //    into the compressed block cache if there is one
      //
      // In all other cases, the serialized block is either uncompressed into a
      // heap buffer or there is no cache at all.
      CompressionType compression_type =
          GetBlockCompressionType(serialized_block);
      if (use_shared_buffer && (compression_type == kNoCompression ||
                                (compression_type != kNoCompression &&
                                 rep_->table_options.block_cache_compressed))) {
        Slice serialized =
            Slice(req.result.data() + req_offset, BlockSizeWithTrailer(handle));
        serialized_block = BlockContents(
            CopyBufferToHeap(GetMemoryAllocator(rep_->table_options),
                             serialized),
            handle.size());
#ifndef NDEBUG
        serialized_block.has_trailer = true;
#endif
      }
    }

    if (s.ok()) {
      if (options.fill_cache) {
        BlockCacheLookupContext lookup_data_block_context(
            TableReaderCaller::kUserMultiGet);
        CachableEntry<Block>* block_entry = &(*results)[idx_in_batch];
        // MaybeReadBlockAndLoadToCache will insert into the block caches if
        // necessary. Since we're passing the serialized block contents, it
        // will avoid looking up the block cache
        s = MaybeReadBlockAndLoadToCache(
            nullptr, options, handle, uncompression_dict, /*wait=*/true,
            /*for_compaction=*/false, &block_entry->As<Block_kData>(),
            mget_iter->get_context, &lookup_data_block_context,
            &serialized_block, /*async_read=*/false);

        // block_entry value could be null if no block cache is present, i.e
        // BlockBasedTableOptions::no_block_cache is true and no compressed
        // block cache is configured. In that case, fall
        // through and set up the block explicitly
        if (block_entry->GetValue() != nullptr) {
          s.PermitUncheckedError();
          continue;
        }
      }

      CompressionType compression_type =
          GetBlockCompressionType(serialized_block);
      BlockContents contents;
      if (compression_type != kNoCompression) {
        UncompressionContext context(compression_type);
        UncompressionInfo info(context, uncompression_dict, compression_type);
        s = UncompressSerializedBlock(
            info, req.result.data() + req_offset, handle.size(), &contents,
            footer.format_version(), rep_->ioptions, memory_allocator);
      } else {
        // There are two cases here:
        // 1) caller uses the shared buffer (scratch or direct io buffer);
        // 2) we use the requst buffer.
        // If scratch buffer or direct io buffer is used, we ensure that
        // all serialized blocks are copyed to the heap as single blocks. If
        // scratch buffer is not used, we also have no combined read, so the
        // serialized block can be used directly.
        contents = std::move(serialized_block);
      }
      if (s.ok()) {
        (*results)[idx_in_batch].SetOwnedValue(std::make_unique<Block>(
            std::move(contents), read_amp_bytes_per_bit, ioptions.stats));
      }
    }
    (*statuses)[idx_in_batch] = s;
  }
}

using MultiGetRange = MultiGetContext::Range;
DEFINE_SYNC_AND_ASYNC(void, BlockBasedTable::MultiGet)
(const ReadOptions& read_options, const MultiGetRange* mget_range,
 const SliceTransform* prefix_extractor, bool skip_filters) {
  if (mget_range->empty()) {
    // Caller should ensure non-empty (performance bug)
    assert(false);
    CO_RETURN;  // Nothing to do
  }

  FilterBlockReader* const filter =
      !skip_filters ? rep_->filter.get() : nullptr;
  MultiGetRange sst_file_range(*mget_range, mget_range->begin(),
                               mget_range->end());

  // First check the full filter
  // If full filter not useful, Then go into each block
  const bool no_io = read_options.read_tier == kBlockCacheTier;
  uint64_t tracing_mget_id = BlockCacheTraceHelper::kReservedGetId;
  if (sst_file_range.begin()->get_context) {
    tracing_mget_id = sst_file_range.begin()->get_context->get_tracing_get_id();
  }
  BlockCacheLookupContext lookup_context{
      TableReaderCaller::kUserMultiGet, tracing_mget_id,
      /*_get_from_user_specified_snapshot=*/read_options.snapshot != nullptr};
  FullFilterKeysMayMatch(filter, &sst_file_range, no_io, prefix_extractor,
                         &lookup_context, read_options.rate_limiter_priority);

  if (!sst_file_range.empty()) {
    IndexBlockIter iiter_on_stack;
    // if prefix_extractor found in block differs from options, disable
    // BlockPrefixIndex. Only do this check when index_type is kHashSearch.
    bool need_upper_bound_check = false;
    if (rep_->index_type == BlockBasedTableOptions::kHashSearch) {
      need_upper_bound_check = PrefixExtractorChanged(prefix_extractor);
    }
    auto iiter =
        NewIndexIterator(read_options, need_upper_bound_check, &iiter_on_stack,
                         sst_file_range.begin()->get_context, &lookup_context);
    std::unique_ptr<InternalIteratorBase<IndexValue>> iiter_unique_ptr;
    if (iiter != &iiter_on_stack) {
      iiter_unique_ptr.reset(iiter);
    }

    uint64_t prev_offset = std::numeric_limits<uint64_t>::max();
    autovector<BlockHandle, MultiGetContext::MAX_BATCH_SIZE> block_handles;
    autovector<CachableEntry<Block>, MultiGetContext::MAX_BATCH_SIZE> results;
    autovector<Status, MultiGetContext::MAX_BATCH_SIZE> statuses;
    MultiGetContext::Mask reused_mask = 0;
    char stack_buf[kMultiGetReadStackBufSize];
    std::unique_ptr<char[]> block_buf;
    {
      MultiGetRange data_block_range(sst_file_range, sst_file_range.begin(),
                                     sst_file_range.end());
      std::vector<Cache::Handle*> cache_handles;
      bool wait_for_cache_results = false;

      CachableEntry<UncompressionDict> uncompression_dict;
      Status uncompression_dict_status;
      uncompression_dict_status.PermitUncheckedError();
      bool uncompression_dict_inited = false;
      size_t total_len = 0;
      ReadOptions ro = read_options;
      ro.read_tier = kBlockCacheTier;

      for (auto miter = data_block_range.begin();
           miter != data_block_range.end(); ++miter) {
        const Slice& key = miter->ikey;
        iiter->Seek(miter->ikey);

        IndexValue v;
        if (iiter->Valid()) {
          v = iiter->value();
        }
        if (!iiter->Valid() ||
            (!v.first_internal_key.empty() && !skip_filters &&
             UserComparatorWrapper(rep_->internal_comparator.user_comparator())
                     .CompareWithoutTimestamp(
                         ExtractUserKey(key),
                         ExtractUserKey(v.first_internal_key)) < 0)) {
          // The requested key falls between highest key in previous block and
          // lowest key in current block.
          if (!iiter->status().IsNotFound()) {
            *(miter->s) = iiter->status();
          }
          data_block_range.SkipKey(miter);
          sst_file_range.SkipKey(miter);
          continue;
        }

        if (!uncompression_dict_inited && rep_->uncompression_dict_reader) {
          uncompression_dict_status =
              rep_->uncompression_dict_reader->GetOrReadUncompressionDictionary(
                  nullptr /* prefetch_buffer */, no_io,
                  read_options.verify_checksums,
                  sst_file_range.begin()->get_context, &lookup_context,
                  &uncompression_dict);
          uncompression_dict_inited = true;
        }

        if (!uncompression_dict_status.ok()) {
          assert(!uncompression_dict_status.IsNotFound());
          *(miter->s) = uncompression_dict_status;
          data_block_range.SkipKey(miter);
          sst_file_range.SkipKey(miter);
          continue;
        }

        statuses.emplace_back();
        results.emplace_back();
        if (v.handle.offset() == prev_offset) {
          // This key can reuse the previous block (later on).
          // Mark previous as "reused"
          reused_mask |= MultiGetContext::Mask{1} << (block_handles.size() - 1);
          // Use null handle to indicate this one reuses same block as
          // previous.
          block_handles.emplace_back(BlockHandle::NullBlockHandle());
          continue;
        }
        // Lookup the cache for the given data block referenced by an index
        // iterator value (i.e BlockHandle). If it exists in the cache,
        // initialize block to the contents of the data block.
        prev_offset = v.handle.offset();
        BlockHandle handle = v.handle;
        BlockCacheLookupContext lookup_data_block_context(
            TableReaderCaller::kUserMultiGet);
        const UncompressionDict& dict = uncompression_dict.GetValue()
                                            ? *uncompression_dict.GetValue()
                                            : UncompressionDict::GetEmptyDict();
        Status s = RetrieveBlock(
            nullptr, ro, handle, dict, &(results.back()).As<Block_kData>(),
            miter->get_context, &lookup_data_block_context,
            /* for_compaction */ false, /* use_cache */ true,
            /* wait_for_cache */ false, /* async_read */ false);
        if (s.IsIncomplete()) {
          s = Status::OK();
        }
        if (s.ok() && !results.back().IsEmpty()) {
          // Since we have a valid handle, check the value. If its nullptr,
          // it means the cache is waiting for the final result and we're
          // supposed to call WaitAll() to wait for the result.
          if (results.back().GetValue() != nullptr) {
            // Found it in the cache. Add NULL handle to indicate there is
            // nothing to read from disk.
            if (results.back().GetCacheHandle()) {
              results.back().UpdateCachedValue();
            }
            block_handles.emplace_back(BlockHandle::NullBlockHandle());
          } else {
            // We have to wait for the cache lookup to finish in the
            // background, and then we may have to read the block from disk
            // anyway
            assert(results.back().GetCacheHandle());
            wait_for_cache_results = true;
            block_handles.emplace_back(handle);
            cache_handles.emplace_back(results.back().GetCacheHandle());
          }
        } else {
          block_handles.emplace_back(handle);
          total_len += BlockSizeWithTrailer(handle);
        }
      }

      if (wait_for_cache_results) {
        Cache* block_cache = rep_->table_options.block_cache.get();
        block_cache->WaitAll(cache_handles);
        for (size_t i = 0; i < block_handles.size(); ++i) {
          // If this block was a success or failure or not needed because
          // the corresponding key is in the same block as a prior key, skip
          if (block_handles[i] == BlockHandle::NullBlockHandle() ||
              results[i].IsEmpty()) {
            continue;
          }
          results[i].UpdateCachedValue();
          void* val = results[i].GetValue();
          Cache::Handle* handle = results[i].GetCacheHandle();
          // GetContext for any key will do, as the stats will be aggregated
          // anyway
          GetContext* get_context = sst_file_range.begin()->get_context;
          if (!val) {
            // The async cache lookup failed - could be due to an error
            // or a false positive. We need to read the data block from
            // the SST file
            results[i].Reset();
            total_len += BlockSizeWithTrailer(block_handles[i]);
            UpdateCacheMissMetrics(BlockType::kData, get_context);
          } else {
            block_handles[i] = BlockHandle::NullBlockHandle();
            UpdateCacheHitMetrics(BlockType::kData, get_context,
                                  block_cache->GetUsage(handle));
          }
        }
      }

      if (total_len) {
        char* scratch = nullptr;
        const UncompressionDict& dict = uncompression_dict.GetValue()
                                            ? *uncompression_dict.GetValue()
                                            : UncompressionDict::GetEmptyDict();
        assert(uncompression_dict_inited || !rep_->uncompression_dict_reader);
        assert(uncompression_dict_status.ok());
        // If using direct IO, then scratch is not used, so keep it nullptr.
        // If the blocks need to be uncompressed and we don't need the
        // compressed blocks, then we can use a contiguous block of
        // memory to read in all the blocks as it will be temporary
        // storage
        // 1. If blocks are compressed and compressed block cache is there,
        //    alloc heap bufs
        // 2. If blocks are uncompressed, alloc heap bufs
        // 3. If blocks are compressed and no compressed block cache, use
        //    stack buf
        if (!rep_->file->use_direct_io() &&
            rep_->table_options.block_cache_compressed == nullptr &&
            rep_->blocks_maybe_compressed) {
          if (total_len <= kMultiGetReadStackBufSize) {
            scratch = stack_buf;
          } else {
            scratch = new char[total_len];
            block_buf.reset(scratch);
          }
        }
        CO_AWAIT(RetrieveMultipleBlocks)
        (read_options, &data_block_range, &block_handles, &statuses, &results,
         scratch, dict);
        if (sst_file_range.begin()->get_context) {
          ++(sst_file_range.begin()
                 ->get_context->get_context_stats_.num_sst_read);
        }
      }
    }

    DataBlockIter first_biter;
    DataBlockIter next_biter;
    size_t idx_in_batch = 0;
    SharedCleanablePtr shared_cleanable;
    for (auto miter = sst_file_range.begin(); miter != sst_file_range.end();
         ++miter) {
      Status s;
      GetContext* get_context = miter->get_context;
      const Slice& key = miter->ikey;
      bool matched = false;  // if such user key matched a key in SST
      bool done = false;
      bool first_block = true;
      do {
        DataBlockIter* biter = nullptr;
        bool reusing_prev_block;
        bool later_reused;
        uint64_t referenced_data_size = 0;
        bool does_referenced_key_exist = false;
        BlockCacheLookupContext lookup_data_block_context(
            TableReaderCaller::kUserMultiGet, tracing_mget_id,
            /*_get_from_user_specified_snapshot=*/read_options.snapshot !=
                nullptr);
        if (first_block) {
          if (!block_handles[idx_in_batch].IsNull() ||
              !results[idx_in_batch].IsEmpty()) {
            first_biter.Invalidate(Status::OK());
            NewDataBlockIterator<DataBlockIter>(
                read_options, results[idx_in_batch], &first_biter,
                statuses[idx_in_batch]);
            reusing_prev_block = false;
          } else {
            // If handler is null and result is empty, then the status is never
            // set, which should be the initial value: ok().
            assert(statuses[idx_in_batch].ok());
            reusing_prev_block = true;
          }
          biter = &first_biter;
          later_reused =
              (reused_mask & (MultiGetContext::Mask{1} << idx_in_batch)) != 0;
          idx_in_batch++;
        } else {
          IndexValue v = iiter->value();
          if (!v.first_internal_key.empty() && !skip_filters &&
              UserComparatorWrapper(rep_->internal_comparator.user_comparator())
                      .CompareWithoutTimestamp(
                          ExtractUserKey(key),
                          ExtractUserKey(v.first_internal_key)) < 0) {
            // The requested key falls between highest key in previous block and
            // lowest key in current block.
            break;
          }

          next_biter.Invalidate(Status::OK());
          Status tmp_s;
          NewDataBlockIterator<DataBlockIter>(
              read_options, iiter->value().handle, &next_biter,
              BlockType::kData, get_context, &lookup_data_block_context,
              /* prefetch_buffer= */ nullptr, /* for_compaction = */ false,
              /*async_read = */ false, tmp_s);
          biter = &next_biter;
          reusing_prev_block = false;
          later_reused = false;
        }

        if (read_options.read_tier == kBlockCacheTier &&
            biter->status().IsIncomplete()) {
          // couldn't get block from block_cache
          // Update Saver.state to Found because we are only looking for
          // whether we can guarantee the key is not there when "no_io" is set
          get_context->MarkKeyMayExist();
          break;
        }
        if (!biter->status().ok()) {
          s = biter->status();
          break;
        }

        // Reusing blocks complicates pinning/Cleanable, because the cache
        // entry referenced by biter can only be released once all returned
        // pinned values are released. This code previously did an extra
        // block_cache Ref for each reuse, but that unnecessarily increases
        // block cache contention. Instead we can use a variant of shared_ptr
        // to release in block cache only once.
        //
        // Although the biter loop below might SaveValue multiple times for
        // merges, just one value_pinner suffices, as MultiGet will merge
        // the operands before returning to the API user.
        Cleanable* value_pinner;
        if (biter->IsValuePinned()) {
          if (reusing_prev_block) {
            // Note that we don't yet know if the MultiGet results will need
            // to pin this block, so we might wrap a block for sharing and
            // still end up with 1 (or 0) pinning ref. Not ideal but OK.
            //
            // Here we avoid adding redundant cleanups if we didn't end up
            // delegating the cleanup from last time around.
            if (!biter->HasCleanups()) {
              assert(shared_cleanable.get());
              if (later_reused) {
                shared_cleanable.RegisterCopyWith(biter);
              } else {
                shared_cleanable.MoveAsCleanupTo(biter);
              }
            }
          } else if (later_reused) {
            assert(biter->HasCleanups());
            // Make the existing cleanups on `biter` sharable:
            shared_cleanable.Allocate();
            // Move existing `biter` cleanup(s) to `shared_cleanable`
            biter->DelegateCleanupsTo(&*shared_cleanable);
            // Reference `shared_cleanable` as new cleanup for `biter`
            shared_cleanable.RegisterCopyWith(biter);
          }
          assert(biter->HasCleanups());
          value_pinner = biter;
        } else {
          value_pinner = nullptr;
        }

        bool may_exist = biter->SeekForGet(key);
        if (!may_exist) {
          // HashSeek cannot find the key this block and the the iter is not
          // the end of the block, i.e. cannot be in the following blocks
          // either. In this case, the seek_key cannot be found, so we break
          // from the top level for-loop.
          break;
        }

        // Call the *saver function on each entry/block until it returns false
        for (; biter->Valid(); biter->Next()) {
          ParsedInternalKey parsed_key;
          Status pik_status = ParseInternalKey(
              biter->key(), &parsed_key, false /* log_err_key */);  // TODO
          if (!pik_status.ok()) {
            s = pik_status;
          }
          if (!get_context->SaveValue(parsed_key, biter->value(), &matched,
                                      value_pinner)) {
            if (get_context->State() == GetContext::GetState::kFound) {
              does_referenced_key_exist = true;
              referenced_data_size =
                  biter->key().size() + biter->value().size();
            }
            done = true;
            break;
          }
          s = biter->status();
        }
        // Write the block cache access.
        // XXX: There appear to be 'break' statements above that bypass this
        // writing of the block cache trace record
        if (block_cache_tracer_ && block_cache_tracer_->is_tracing_enabled() &&
            !reusing_prev_block) {
          // Avoid making copy of block_key, cf_name, and referenced_key when
          // constructing the access record.
          Slice referenced_key;
          if (does_referenced_key_exist) {
            referenced_key = biter->key();
          } else {
            referenced_key = key;
          }
          BlockCacheTraceRecord access_record(
              rep_->ioptions.clock->NowMicros(),
              /*_block_key=*/"", lookup_data_block_context.block_type,
              lookup_data_block_context.block_size, rep_->cf_id_for_tracing(),
              /*_cf_name=*/"", rep_->level_for_tracing(),
              rep_->sst_number_for_tracing(), lookup_data_block_context.caller,
              lookup_data_block_context.is_cache_hit,
              lookup_data_block_context.no_insert,
              lookup_data_block_context.get_id,
              lookup_data_block_context.get_from_user_specified_snapshot,
              /*_referenced_key=*/"", referenced_data_size,
              lookup_data_block_context.num_keys_in_block,
              does_referenced_key_exist);
          // TODO: Should handle status here?
          block_cache_tracer_
              ->WriteBlockAccess(access_record,
                                 lookup_data_block_context.block_key,
                                 rep_->cf_name_for_tracing(), referenced_key)
              .PermitUncheckedError();
        }
        s = biter->status();
        if (done) {
          // Avoid the extra Next which is expensive in two-level indexes
          break;
        }
        if (first_block) {
          iiter->Seek(key);
          if (!iiter->Valid()) {
            break;
          }
        }
        first_block = false;
        iiter->Next();
      } while (iiter->Valid());

      if (matched && filter != nullptr) {
        RecordTick(rep_->ioptions.stats, BLOOM_FILTER_FULL_TRUE_POSITIVE);
        PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_full_true_positive, 1,
                                  rep_->level);
      }
      if (s.ok() && !iiter->status().IsNotFound()) {
        s = iiter->status();
      }
      *(miter->s) = s;
    }
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
    // Not sure why we need to do it. Should investigate more.
    for (auto& st : statuses) {
      st.PermitUncheckedError();
    }
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED
  }
}
}  // namespace ROCKSDB_NAMESPACE
#endif
