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
 Status* statuses, CachableEntry<Block_kData>* results, char* scratch,
 const UncompressionDict& uncompression_dict, bool use_fs_scratch) const {
  RandomAccessFileReader* file = rep_->file.get();
  const Footer& footer = rep_->footer;
  const ImmutableOptions& ioptions = rep_->ioptions;
  size_t read_amp_bytes_per_bit = rep_->table_options.read_amp_bytes_per_bit;
  MemoryAllocator* memory_allocator = GetMemoryAllocator(rep_->table_options);

  if (ioptions.allow_mmap_reads) {
    size_t idx_in_batch = 0;
    for (auto mget_iter = batch->begin(); mget_iter != batch->end();
         ++mget_iter, ++idx_in_batch) {
      const BlockHandle& handle = (*handles)[idx_in_batch];
      if (handle.IsNull()) {
        continue;
      }

      // XXX: use_cache=true means double cache query?
      statuses[idx_in_batch] = RetrieveBlock(
          nullptr, options, handle, uncompression_dict,
          &results[idx_in_batch].As<Block_kData>(), mget_iter->get_context,
          /* lookup_context */ nullptr,
          /* for_compaction */ false, /* use_cache */ true,
          /* async_read */ false, /* use_block_cache_for_lookup */ true);
    }
    assert(idx_in_batch == handles->size());
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
    if ((use_shared_buffer || use_fs_scratch) && !file->use_direct_io() &&
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
        if (file->use_direct_io() || use_fs_scratch) {
          req.scratch = nullptr;
        } else if (use_shared_buffer) {
          req.scratch = scratch + buf_offset;
          buf_offset += req.len;
        } else {
          req.scratch = new char[req.len];
        }
        read_reqs.emplace_back(std::move(req));
      }

      // Step 2, remember the previous block info
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
    if (file->use_direct_io() || use_fs_scratch) {
      req.scratch = nullptr;
    } else if (use_shared_buffer) {
      req.scratch = scratch + buf_offset;
    } else {
      req.scratch = new char[req.len];
    }
    read_reqs.emplace_back(std::move(req));
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
                            &direct_io_buf);
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
      if (!use_fs_scratch && !use_shared_buffer) {
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
        // In case of use_fs_scratch, underlying file system provided buffer is
        // used. serialized_block does not have the ownership.
        serialized_block =
            BlockContents(Slice(req.result.data() + req_offset, handle.size()));
      }
#ifndef NDEBUG
      serialized_block.has_trailer = true;
#endif

      if (options.verify_checksums) {
        PERF_TIMER_GUARD(block_checksum_time);
        const char* data = serialized_block.data.data();
        // Since the scratch might be shared, the offset of the data block in
        // the buffer might not be 0. req.result.data() only point to the
        // begin address of each read request, we need to add the offset
        // in each read request. Checksum is stored in the block trailer,
        // beyond the payload size.
        s = VerifyBlockChecksum(footer, data, handle.size(),
                                rep_->file->file_name(), handle.offset());
        RecordTick(ioptions.stats, BLOCK_CHECKSUM_COMPUTE_COUNT);
        if (!s.ok()) {
          RecordTick(ioptions.stats, BLOCK_CHECKSUM_MISMATCH_COUNT);
        }
        TEST_SYNC_POINT_CALLBACK("RetrieveMultipleBlocks:VerifyChecksum", &s);
        if (!s.ok() &&
            CheckFSFeatureSupport(ioptions.fs.get(),
                                  FSSupportedOps::kVerifyAndReconstructRead)) {
          assert(s.IsCorruption());
          assert(!ioptions.allow_mmap_reads);
          RecordTick(ioptions.stats, FILE_READ_CORRUPTION_RETRY_COUNT);

          // Repeat the read for this particular block using the regular
          // synchronous Read API. We can use the same chunk of memory
          // pointed to by data, since the size is identical and we know
          // its not a memory mapped file
          Slice result;
          IOOptions opts;
          IOStatus io_s = file->PrepareIOOptions(options, opts);
          opts.verify_and_reconstruct_read = true;
          io_s = file->Read(opts, handle.offset(), BlockSizeWithTrailer(handle),
                            &result, const_cast<char*>(data), nullptr);
          if (io_s.ok()) {
            assert(result.data() == data);
            assert(result.size() == BlockSizeWithTrailer(handle));
            s = VerifyBlockChecksum(footer, data, handle.size(),
                                    rep_->file->file_name(), handle.offset());
            if (s.ok()) {
              RecordTick(ioptions.stats,
                         FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT);
            }
          } else {
            s = io_s;
          }
        }
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
      if ((use_fs_scratch || use_shared_buffer) &&
          compression_type == kNoCompression) {
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
        CachableEntry<Block_kData>* block_entry = &results[idx_in_batch];
        // MaybeReadBlockAndLoadToCache will insert into the block caches if
        // necessary. Since we're passing the serialized block contents, it
        // will avoid looking up the block cache
        s = MaybeReadBlockAndLoadToCache(
            nullptr, options, handle, uncompression_dict,
            /*for_compaction=*/false, block_entry, mget_iter->get_context,
            /*lookup_context=*/nullptr, &serialized_block,
            /*async_read=*/false, /*use_block_cache_for_lookup=*/true);

        if (!s.ok()) {
          statuses[idx_in_batch] = s;
          continue;
        }
        // block_entry value could be null if no block cache is present, i.e
        // BlockBasedTableOptions::no_block_cache is true and no compressed
        // block cache is configured. In that case, fall
        // through and set up the block explicitly
        if (block_entry->GetValue() != nullptr) {
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
        results[idx_in_batch].SetOwnedValue(std::make_unique<Block_kData>(
            std::move(contents), read_amp_bytes_per_bit, ioptions.stats));
      }
    }
    statuses[idx_in_batch] = s;
  }

  if (use_fs_scratch) {
    // Free the allocated scratch buffer by fs here as read requests might have
    // been combined into one.
    for (FSReadRequest& req : read_reqs) {
      if (req.fs_scratch != nullptr) {
        req.fs_scratch.reset();
        req.fs_scratch = nullptr;
      }
    }
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
  uint64_t tracing_mget_id = BlockCacheTraceHelper::kReservedGetId;
  if (sst_file_range.begin()->get_context) {
    tracing_mget_id = sst_file_range.begin()->get_context->get_tracing_get_id();
  }
  // TODO: need more than one lookup_context here to track individual filter
  // and index partition hits and misses.
  BlockCacheLookupContext metadata_lookup_context{
      TableReaderCaller::kUserMultiGet, tracing_mget_id,
      /*_get_from_user_specified_snapshot=*/read_options.snapshot != nullptr};
  FullFilterKeysMayMatch(filter, &sst_file_range, prefix_extractor,
                         &metadata_lookup_context, read_options);

  if (!sst_file_range.empty()) {
    IndexBlockIter iiter_on_stack;
    // if prefix_extractor found in block differs from options, disable
    // BlockPrefixIndex. Only do this check when index_type is kHashSearch.
    bool need_upper_bound_check = false;
    if (rep_->index_type == BlockBasedTableOptions::kHashSearch) {
      need_upper_bound_check = PrefixExtractorChanged(prefix_extractor);
    }
    auto iiter = NewIndexIterator(
        read_options, need_upper_bound_check, &iiter_on_stack,
        sst_file_range.begin()->get_context, &metadata_lookup_context);
    std::unique_ptr<InternalIteratorBase<IndexValue>> iiter_unique_ptr;
    if (iiter != &iiter_on_stack) {
      iiter_unique_ptr.reset(iiter);
    }

    uint64_t prev_offset = std::numeric_limits<uint64_t>::max();
    autovector<BlockHandle, MultiGetContext::MAX_BATCH_SIZE> block_handles;
    std::array<CachableEntry<Block_kData>, MultiGetContext::MAX_BATCH_SIZE>
        results;
    std::array<Status, MultiGetContext::MAX_BATCH_SIZE> statuses;
    // Empty data_lookup_contexts means "unused," when block cache tracing is
    // disabled. (Limited options as element type is not default contructible.)
    std::vector<BlockCacheLookupContext> data_lookup_contexts;
    MultiGetContext::Mask reused_mask = 0;
    char stack_buf[kMultiGetReadStackBufSize];
    std::unique_ptr<char[]> block_buf;
    if (block_cache_tracer_ && block_cache_tracer_->is_tracing_enabled()) {
      // Awkward because BlockCacheLookupContext is not CopyAssignable
      data_lookup_contexts.reserve(MultiGetContext::MAX_BATCH_SIZE);
      for (size_t i = 0; i < MultiGetContext::MAX_BATCH_SIZE; ++i) {
        data_lookup_contexts.push_back(metadata_lookup_context);
      }
    }
    {
      MultiGetRange data_block_range(sst_file_range, sst_file_range.begin(),
                                     sst_file_range.end());
      CachableEntry<UncompressionDict> uncompression_dict;
      Status uncompression_dict_status;
      uncompression_dict_status.PermitUncheckedError();
      bool uncompression_dict_inited = false;
      size_t total_len = 0;

      // GetContext for any key will do, as the stats will be aggregated
      // anyway
      GetContext* get_context = sst_file_range.begin()->get_context;

      {
        using BCI = BlockCacheInterface<Block_kData>;
        BCI block_cache{rep_->table_options.block_cache.get()};
        std::array<BCI::TypedAsyncLookupHandle, MultiGetContext::MAX_BATCH_SIZE>
            async_handles;
        BlockCreateContext create_ctx = rep_->create_context;
        std::array<CacheKey, MultiGetContext::MAX_BATCH_SIZE> cache_keys;
        size_t cache_lookup_count = 0;

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
               UserComparatorWrapper(
                   rep_->internal_comparator.user_comparator())
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
                rep_->uncompression_dict_reader
                    ->GetOrReadUncompressionDictionary(
                        nullptr /* prefetch_buffer */, read_options,
                        get_context, &metadata_lookup_context,
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
          create_ctx.dict = uncompression_dict.GetValue()
                                ? uncompression_dict.GetValue()
                                : &UncompressionDict::GetEmptyDict();

          if (v.handle.offset() == prev_offset) {
            // This key can reuse the previous block (later on).
            // Mark previous as "reused"
            reused_mask |= MultiGetContext::Mask{1}
                           << (block_handles.size() - 1);
            // Use null handle to indicate this one reuses same block as
            // previous.
            block_handles.emplace_back(BlockHandle::NullBlockHandle());
            continue;
          }
          prev_offset = v.handle.offset();
          block_handles.emplace_back(v.handle);

          if (block_cache) {
            // Lookup the cache for the given data block referenced by an index
            // iterator value (i.e BlockHandle). If it exists in the cache,
            // initialize block to the contents of the data block.

            // An async version of MaybeReadBlockAndLoadToCache /
            // GetDataBlockFromCache
            BCI::TypedAsyncLookupHandle& async_handle =
                async_handles[cache_lookup_count];
            cache_keys[cache_lookup_count] =
                GetCacheKey(rep_->base_cache_key, v.handle);
            async_handle.key = cache_keys[cache_lookup_count].AsSlice();
            // NB: StartAsyncLookupFull populates async_handle.helper
            async_handle.create_context = &create_ctx;
            async_handle.priority = GetCachePriority<Block_kData>();
            async_handle.stats = rep_->ioptions.statistics.get();

            block_cache.StartAsyncLookupFull(
                async_handle, rep_->ioptions.lowest_used_cache_tier);
            ++cache_lookup_count;
            // TODO: stats?
          }
        }

        if (block_cache) {
          block_cache.get()->WaitAll(&async_handles[0], cache_lookup_count);
        }
        size_t lookup_idx = 0;
        for (size_t i = 0; i < block_handles.size(); ++i) {
          // If this block was a success or failure or not needed because
          // the corresponding key is in the same block as a prior key, skip
          if (block_handles[i] == BlockHandle::NullBlockHandle()) {
            continue;
          }
          if (!block_cache) {
            total_len += BlockSizeWithTrailer(block_handles[i]);
          } else {
            BCI::TypedHandle* h = async_handles[lookup_idx].Result();
            if (h) {
              // Cache hit
              results[i].SetCachedValue(block_cache.Value(h), block_cache.get(),
                                        h);
              // Don't need to fetch
              block_handles[i] = BlockHandle::NullBlockHandle();
              UpdateCacheHitMetrics(BlockType::kData, get_context,
                                    block_cache.get()->GetUsage(h));
            } else {
              // Cache miss
              total_len += BlockSizeWithTrailer(block_handles[i]);
              UpdateCacheMissMetrics(BlockType::kData, get_context);
            }
            if (!data_lookup_contexts.empty()) {
              // Populate cache key before it's discarded
              data_lookup_contexts[i].block_key =
                  async_handles[lookup_idx].key.ToString();
            }
            ++lookup_idx;
          }
        }
        assert(lookup_idx == cache_lookup_count);
      }

      if (total_len) {
        char* scratch = nullptr;
        bool use_fs_scratch = false;
        const UncompressionDict& dict = uncompression_dict.GetValue()
                                            ? *uncompression_dict.GetValue()
                                            : UncompressionDict::GetEmptyDict();
        assert(uncompression_dict_inited || !rep_->uncompression_dict_reader);
        assert(uncompression_dict_status.ok());

        if (!rep_->file->use_direct_io()) {
          if (CheckFSFeatureSupport(rep_->ioptions.fs.get(),
                                    FSSupportedOps::kFSBuffer)) {
            use_fs_scratch = true;
          }
        }

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
        if (!use_fs_scratch && !rep_->file->use_direct_io() &&
            rep_->blocks_maybe_compressed) {
          if (total_len <= kMultiGetReadStackBufSize) {
            scratch = stack_buf;
          } else {
            scratch = new char[total_len];
            block_buf.reset(scratch);
          }
        }
        CO_AWAIT(RetrieveMultipleBlocks)
        (read_options, &data_block_range, &block_handles, &statuses[0],
         &results[0], scratch, dict, use_fs_scratch);
        if (get_context) {
          ++(get_context->get_context_stats_.num_sst_read);
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
        uint64_t referenced_data_size = 0;
        Block_kData* parsed_block_value = nullptr;
        bool reusing_prev_block;
        bool later_reused;
        bool does_referenced_key_exist = false;
        bool handle_present = false;
        BlockCacheLookupContext* lookup_data_block_context =
            data_lookup_contexts.empty() ? nullptr
                                         : &data_lookup_contexts[idx_in_batch];
        if (first_block) {
          handle_present = !block_handles[idx_in_batch].IsNull();
          parsed_block_value = results[idx_in_batch].GetValue();
          if (handle_present || parsed_block_value) {
            first_biter.Invalidate(Status::OK());
            NewDataBlockIterator<DataBlockIter>(
                read_options, results[idx_in_batch].As<Block>(), &first_biter,
                statuses[idx_in_batch]);
            reusing_prev_block = false;
          } else {
            // If handle is null and result is empty, then the status is never
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
              BlockType::kData, get_context, lookup_data_block_context,
              /* prefetch_buffer= */ nullptr, /* for_compaction = */ false,
              /*async_read = */ false, tmp_s,
              /* use_block_cache_for_lookup = */ true);
          biter = &next_biter;
          reusing_prev_block = false;
          later_reused = false;
        }

        if (read_options.read_tier == kBlockCacheTier &&
            biter->status().IsIncomplete()) {
          // couldn't get block from block_cache
          // Update Saver.state to Found because we are only looking for
          // whether we can guarantee the key is not there with kBlockCacheTier
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
        for (; biter->status().ok() && biter->Valid(); biter->Next()) {
          ParsedInternalKey parsed_key;
          Status pik_status = ParseInternalKey(
              biter->key(), &parsed_key, false /* log_err_key */);  // TODO
          if (!pik_status.ok()) {
            s = pik_status;
            break;
          }
          Status read_status;
          bool ret = get_context->SaveValue(
              parsed_key, biter->value(), &matched, &read_status,
              value_pinner ? value_pinner : nullptr);
          if (!read_status.ok()) {
            s = read_status;
            break;
          }
          if (!ret) {
            if (get_context->State() == GetContext::GetState::kFound) {
              does_referenced_key_exist = true;
              referenced_data_size =
                  biter->key().size() + biter->value().size();
            }
            done = true;
            break;
          }
        }
        // Write the block cache access.
        // XXX: There appear to be 'break' statements above that bypass this
        // writing of the block cache trace record
        if (lookup_data_block_context && !reusing_prev_block && first_block) {
          Slice referenced_key;
          if (does_referenced_key_exist) {
            referenced_key = biter->key();
          } else {
            referenced_key = key;
          }

          // block_key is self-assigned here (previously assigned from
          // cache_keys / async_handles, now out of scope)
          SaveLookupContextOrTraceRecord(lookup_data_block_context->block_key,
                                         /*is_cache_hit=*/!handle_present,
                                         read_options, parsed_block_value,
                                         lookup_data_block_context);
          FinishTraceRecord(
              *lookup_data_block_context, lookup_data_block_context->block_key,
              referenced_key, does_referenced_key_exist, referenced_data_size);
        }
        if (s.ok()) {
          s = biter->status();
        }
        if (done || !s.ok()) {
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
        if (rep_->whole_key_filtering) {
          RecordTick(rep_->ioptions.stats, BLOOM_FILTER_FULL_TRUE_POSITIVE);
        } else {
          RecordTick(rep_->ioptions.stats, BLOOM_FILTER_PREFIX_TRUE_POSITIVE);
        }
        // Includes prefix stats
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
