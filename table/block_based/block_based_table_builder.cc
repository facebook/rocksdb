//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/block_based/block_based_table_builder.h"

#include <assert.h>
#include <stdio.h>

#include <atomic>
#include <list>
#include <map>
#include <memory>
#include <numeric>
#include <string>
#include <unordered_map>
#include <utility>

#include "db/dbformat.h"
#include "index_builder.h"
#include "memory/memory_allocator.h"
#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/table.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_filter_block.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_builder.h"
#include "table/block_based/filter_block.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/block_based/full_filter_block.h"
#include "table/block_based/partitioned_filter_block.h"
#include "table/format.h"
#include "table/table_builder.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/work_queue.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {

extern const std::string kHashIndexPrefixesBlock;
extern const std::string kHashIndexPrefixesMetadataBlock;


// Without anonymous namespace here, we fail the warning -Wmissing-prototypes
namespace {

// Create a filter block builder based on its type.
FilterBlockBuilder* CreateFilterBlockBuilder(
    const ImmutableCFOptions& /*opt*/, const MutableCFOptions& mopt,
    const FilterBuildingContext& context,
    const bool use_delta_encoding_for_index_values,
    PartitionedIndexBuilder* const p_index_builder) {
  const BlockBasedTableOptions& table_opt = context.table_options;
  assert(table_opt.filter_policy);  // precondition

  FilterBitsBuilder* filter_bits_builder =
      BloomFilterPolicy::GetBuilderFromContext(context);
  if (filter_bits_builder == nullptr) {
    return new BlockBasedFilterBlockBuilder(mopt.prefix_extractor.get(),
                                            table_opt);
  } else {
    if (table_opt.partition_filters) {
      assert(p_index_builder != nullptr);
      // Since after partition cut request from filter builder it takes time
      // until index builder actully cuts the partition, until the end of a
      // data block potentially with many keys, we take the lower bound as
      // partition size.
      assert(table_opt.block_size_deviation <= 100);
      auto partition_size =
          static_cast<uint32_t>(((table_opt.metadata_block_size *
                                  (100 - table_opt.block_size_deviation)) +
                                 99) /
                                100);
      partition_size = std::max(partition_size, static_cast<uint32_t>(1));
      return new PartitionedFilterBlockBuilder(
          mopt.prefix_extractor.get(), table_opt.whole_key_filtering,
          filter_bits_builder, table_opt.index_block_restart_interval,
          use_delta_encoding_for_index_values, p_index_builder, partition_size);
    } else {
      return new FullFilterBlockBuilder(mopt.prefix_extractor.get(),
                                        table_opt.whole_key_filtering,
                                        filter_bits_builder);
    }
  }
}

bool GoodCompressionRatio(size_t compressed_size, size_t raw_size) {
  // Check to see if compressed less than 12.5%
  return compressed_size < raw_size - (raw_size / 8u);
}

}  // namespace

// format_version is the block format as defined in include/rocksdb/table.h
Slice CompressBlock(const Slice& raw, const CompressionInfo& info,
                    CompressionType* type, uint32_t format_version,
                    bool do_sample, std::string* compressed_output,
                    std::string* sampled_output_fast,
                    std::string* sampled_output_slow) {
  assert(type);
  assert(compressed_output);
  assert(compressed_output->empty());

  // If requested, we sample one in every N block with a
  // fast and slow compression algorithm and report the stats.
  // The users can use these stats to decide if it is worthwhile
  // enabling compression and they also get a hint about which
  // compression algorithm wil be beneficial.
  if (do_sample && info.SampleForCompression() &&
      Random::GetTLSInstance()->OneIn(
          static_cast<int>(info.SampleForCompression()))) {
    // Sampling with a fast compression algorithm
    if (sampled_output_fast && (LZ4_Supported() || Snappy_Supported())) {
      CompressionType c =
          LZ4_Supported() ? kLZ4Compression : kSnappyCompression;
      CompressionContext context(c);
      CompressionOptions options;
      CompressionInfo info_tmp(options, context,
                               CompressionDict::GetEmptyDict(), c,
                               info.SampleForCompression());

      CompressData(raw, info_tmp, GetCompressFormatForVersion(format_version),
                   sampled_output_fast);
    }

    // Sampling with a slow but high-compression algorithm
    if (sampled_output_slow && (ZSTD_Supported() || Zlib_Supported())) {
      CompressionType c = ZSTD_Supported() ? kZSTD : kZlibCompression;
      CompressionContext context(c);
      CompressionOptions options;
      CompressionInfo info_tmp(options, context,
                               CompressionDict::GetEmptyDict(), c,
                               info.SampleForCompression());

      CompressData(raw, info_tmp, GetCompressFormatForVersion(format_version),
                   sampled_output_slow);
    }
  }

  if (info.type() == kNoCompression) {
    *type = kNoCompression;
    return raw;
  }

  // Actually compress the data; if the compression method is not supported,
  // or the compression fails etc., just fall back to uncompressed
  if (!CompressData(raw, info, GetCompressFormatForVersion(format_version),
                    compressed_output)) {
    *type = kNoCompression;
    return raw;
  }

  // Check the compression ratio; if it's not good enough, just fall back to
  // uncompressed
  if (!GoodCompressionRatio(compressed_output->size(), raw.size())) {
    *type = kNoCompression;
    return raw;
  }

  *type = info.type();
  return *compressed_output;
}

// kBlockBasedTableMagicNumber was picked by running
//    echo rocksdb.table.block_based | sha1sum
// and taking the leading 64 bits.
// Please note that kBlockBasedTableMagicNumber may also be accessed by other
// .cc files
// for that reason we declare it extern in the header but to get the space
// allocated
// it must be not extern in one place.
const uint64_t kBlockBasedTableMagicNumber = 0x88e241b785f4cff7ull;
// We also support reading and writing legacy block based table format (for
// backwards compatibility)
const uint64_t kLegacyBlockBasedTableMagicNumber = 0xdb4775248b80fb57ull;

// A collector that collects properties of interest to block-based table.
// For now this class looks heavy-weight since we only write one additional
// property.
// But in the foreseeable future, we will add more and more properties that are
// specific to block-based table.
class BlockBasedTableBuilder::BlockBasedTablePropertiesCollector
    : public IntTblPropCollector {
 public:
  explicit BlockBasedTablePropertiesCollector(
      BlockBasedTableOptions::IndexType index_type, bool whole_key_filtering,
      bool prefix_filtering)
      : index_type_(index_type),
        whole_key_filtering_(whole_key_filtering),
        prefix_filtering_(prefix_filtering) {}

  Status InternalAdd(const Slice& /*key*/, const Slice& /*value*/,
                     uint64_t /*file_size*/) override {
    // Intentionally left blank. Have no interest in collecting stats for
    // individual key/value pairs.
    return Status::OK();
  }

  virtual void BlockAdd(uint64_t /* block_raw_bytes */,
                        uint64_t /* block_compressed_bytes_fast */,
                        uint64_t /* block_compressed_bytes_slow */) override {
    // Intentionally left blank. No interest in collecting stats for
    // blocks.
    return;
  }

  Status Finish(UserCollectedProperties* properties) override {
    std::string val;
    PutFixed32(&val, static_cast<uint32_t>(index_type_));
    properties->insert({BlockBasedTablePropertyNames::kIndexType, val});
    properties->insert({BlockBasedTablePropertyNames::kWholeKeyFiltering,
                        whole_key_filtering_ ? kPropTrue : kPropFalse});
    properties->insert({BlockBasedTablePropertyNames::kPrefixFiltering,
                        prefix_filtering_ ? kPropTrue : kPropFalse});
    return Status::OK();
  }

  // The name of the properties collector can be used for debugging purpose.
  const char* Name() const override {
    return "BlockBasedTablePropertiesCollector";
  }

  UserCollectedProperties GetReadableProperties() const override {
    // Intentionally left blank.
    return UserCollectedProperties();
  }

 private:
  BlockBasedTableOptions::IndexType index_type_;
  bool whole_key_filtering_;
  bool prefix_filtering_;
};

struct BlockBasedTableBuilder::Rep {
  const ImmutableOptions ioptions;
  const MutableCFOptions moptions;
  const BlockBasedTableOptions table_options;
  const InternalKeyComparator& internal_comparator;
  WritableFileWriter* file;
  std::atomic<uint64_t> offset;
  size_t alignment;
  BlockBuilder data_block;
  // Buffers uncompressed data blocks to replay later. Needed when
  // compression dictionary is enabled so we can finalize the dictionary before
  // compressing any data blocks.
  std::vector<std::string> data_block_buffers;
  BlockBuilder range_del_block;

  InternalKeySliceTransform internal_prefix_transform;
  std::unique_ptr<IndexBuilder> index_builder;
  PartitionedIndexBuilder* p_index_builder_ = nullptr;

  std::string last_key;
  const Slice* first_key_in_next_block = nullptr;
  CompressionType compression_type;
  uint64_t sample_for_compression;
  std::atomic<uint64_t> compressible_input_data_bytes;
  std::atomic<uint64_t> uncompressible_input_data_bytes;
  std::atomic<uint64_t> sampled_input_data_bytes;
  std::atomic<uint64_t> sampled_output_slow_data_bytes;
  std::atomic<uint64_t> sampled_output_fast_data_bytes;
  CompressionOptions compression_opts;
  std::unique_ptr<CompressionDict> compression_dict;
  std::vector<std::unique_ptr<CompressionContext>> compression_ctxs;
  std::vector<std::unique_ptr<UncompressionContext>> verify_ctxs;
  std::unique_ptr<UncompressionDict> verify_dict;

  size_t data_begin_offset = 0;

  TableProperties props;

  // States of the builder.
  //
  // - `kBuffered`: This is the initial state where zero or more data blocks are
  //   accumulated uncompressed in-memory. From this state, call
  //   `EnterUnbuffered()` to finalize the compression dictionary if enabled,
  //   compress/write out any buffered blocks, and proceed to the `kUnbuffered`
  //   state.
  //
  // - `kUnbuffered`: This is the state when compression dictionary is finalized
  //   either because it wasn't enabled in the first place or it's been created
  //   from sampling previously buffered data. In this state, blocks are simply
  //   compressed/written out as they fill up. From this state, call `Finish()`
  //   to complete the file (write meta-blocks, etc.), or `Abandon()` to delete
  //   the partially created file.
  //
  // - `kClosed`: This indicates either `Finish()` or `Abandon()` has been
  //   called, so the table builder is no longer usable. We must be in this
  //   state by the time the destructor runs.
  enum class State {
    kBuffered,
    kUnbuffered,
    kClosed,
  };
  State state;
  // `kBuffered` state is allowed only as long as the buffering of uncompressed
  // data blocks (see `data_block_buffers`) does not exceed `buffer_limit`.
  uint64_t buffer_limit;

  const bool use_delta_encoding_for_index_values;
  std::unique_ptr<FilterBlockBuilder> filter_builder;
  char cache_key_prefix[BlockBasedTable::kMaxCacheKeyPrefixSize];
  size_t cache_key_prefix_size;
  char compressed_cache_key_prefix[BlockBasedTable::kMaxCacheKeyPrefixSize];
  size_t compressed_cache_key_prefix_size;

  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
  std::unique_ptr<FlushBlockPolicy> flush_block_policy;
  uint32_t column_family_id;
  std::string column_family_name;
  uint64_t creation_time = 0;
  uint64_t oldest_key_time = 0;
  uint64_t file_creation_time = 0;

  // DB IDs
  const std::string db_id;
  const std::string db_session_id;
  std::string db_host_id;

  std::vector<std::unique_ptr<IntTblPropCollector>> table_properties_collectors;

  std::unique_ptr<ParallelCompressionRep> pc_rep;

  uint64_t get_offset() { return offset.load(std::memory_order_relaxed); }
  void set_offset(uint64_t o) { offset.store(o, std::memory_order_relaxed); }

  bool IsParallelCompressionEnabled() const {
    return compression_opts.parallel_threads > 1;
  }

  Status GetStatus() {
    // We need to make modifications of status visible when status_ok is set
    // to false, and this is ensured by status_mutex, so no special memory
    // order for status_ok is required.
    if (status_ok.load(std::memory_order_relaxed)) {
      return Status::OK();
    } else {
      return CopyStatus();
    }
  }

  Status CopyStatus() {
    std::lock_guard<std::mutex> lock(status_mutex);
    return status;
  }

  IOStatus GetIOStatus() {
    // We need to make modifications of io_status visible when status_ok is set
    // to false, and this is ensured by io_status_mutex, so no special memory
    // order for io_status_ok is required.
    if (io_status_ok.load(std::memory_order_relaxed)) {
      return IOStatus::OK();
    } else {
      return CopyIOStatus();
    }
  }

  IOStatus CopyIOStatus() {
    std::lock_guard<std::mutex> lock(io_status_mutex);
    return io_status;
  }

  // Never erase an existing status that is not OK.
  void SetStatus(Status s) {
    if (!s.ok() && status_ok.load(std::memory_order_relaxed)) {
      // Locking is an overkill for non compression_opts.parallel_threads
      // case but since it's unlikely that s is not OK, we take this cost
      // to be simplicity.
      std::lock_guard<std::mutex> lock(status_mutex);
      status = s;
      status_ok.store(false, std::memory_order_relaxed);
    }
  }

  // Never erase an existing I/O status that is not OK.
  void SetIOStatus(IOStatus ios) {
    if (!ios.ok() && io_status_ok.load(std::memory_order_relaxed)) {
      // Locking is an overkill for non compression_opts.parallel_threads
      // case but since it's unlikely that s is not OK, we take this cost
      // to be simplicity.
      std::lock_guard<std::mutex> lock(io_status_mutex);
      io_status = ios;
      io_status_ok.store(false, std::memory_order_relaxed);
    }
  }

  Rep(const BlockBasedTableOptions& table_opt, const TableBuilderOptions& tbo,
      WritableFileWriter* f)
      : ioptions(tbo.ioptions),
        moptions(tbo.moptions),
        table_options(table_opt),
        internal_comparator(tbo.internal_comparator),
        file(f),
        offset(0),
        alignment(table_options.block_align
                      ? std::min(table_options.block_size, kDefaultPageSize)
                      : 0),
        data_block(table_options.block_restart_interval,
                   table_options.use_delta_encoding,
                   false /* use_value_delta_encoding */,
                   tbo.internal_comparator.user_comparator()
                           ->CanKeysWithDifferentByteContentsBeEqual()
                       ? BlockBasedTableOptions::kDataBlockBinarySearch
                       : table_options.data_block_index_type,
                   table_options.data_block_hash_table_util_ratio),
        range_del_block(1 /* block_restart_interval */),
        internal_prefix_transform(tbo.moptions.prefix_extractor.get()),
        compression_type(tbo.compression_type),
        sample_for_compression(tbo.moptions.sample_for_compression),
        compressible_input_data_bytes(0),
        uncompressible_input_data_bytes(0),
        sampled_input_data_bytes(0),
        sampled_output_slow_data_bytes(0),
        sampled_output_fast_data_bytes(0),
        compression_opts(tbo.compression_opts),
        compression_dict(),
        compression_ctxs(tbo.compression_opts.parallel_threads),
        verify_ctxs(tbo.compression_opts.parallel_threads),
        verify_dict(),
        state((tbo.compression_opts.max_dict_bytes > 0) ? State::kBuffered
                                                        : State::kUnbuffered),
        use_delta_encoding_for_index_values(table_opt.format_version >= 4 &&
                                            !table_opt.block_align),
        cache_key_prefix_size(0),
        compressed_cache_key_prefix_size(0),
        flush_block_policy(
            table_options.flush_block_policy_factory->NewFlushBlockPolicy(
                table_options, data_block)),
        column_family_id(tbo.column_family_id),
        column_family_name(tbo.column_family_name),
        creation_time(tbo.creation_time),
        oldest_key_time(tbo.oldest_key_time),
        file_creation_time(tbo.file_creation_time),
        db_id(tbo.db_id),
        db_session_id(tbo.db_session_id),
        db_host_id(ioptions.db_host_id),
        status_ok(true),
        io_status_ok(true) {
    if (tbo.target_file_size == 0) {
      buffer_limit = compression_opts.max_dict_buffer_bytes;
    } else if (compression_opts.max_dict_buffer_bytes == 0) {
      buffer_limit = tbo.target_file_size;
    } else {
      buffer_limit = std::min(tbo.target_file_size,
                              compression_opts.max_dict_buffer_bytes);
    }
    for (uint32_t i = 0; i < compression_opts.parallel_threads; i++) {
      compression_ctxs[i].reset(new CompressionContext(compression_type));
    }
    if (table_options.index_type ==
        BlockBasedTableOptions::kTwoLevelIndexSearch) {
      p_index_builder_ = PartitionedIndexBuilder::CreateIndexBuilder(
          &internal_comparator, use_delta_encoding_for_index_values,
          table_options);
      index_builder.reset(p_index_builder_);
    } else {
      index_builder.reset(IndexBuilder::CreateIndexBuilder(
          table_options.index_type, &internal_comparator,
          &this->internal_prefix_transform, use_delta_encoding_for_index_values,
          table_options));
    }
    if (ioptions.optimize_filters_for_hits && tbo.is_bottommost) {
      // Apply optimize_filters_for_hits setting here when applicable by
      // skipping filter generation
      filter_builder.reset();
    } else if (tbo.skip_filters) {
      // For SstFileWriter skip_filters
      filter_builder.reset();
    } else if (!table_options.filter_policy) {
      // Null filter_policy -> no filter
      filter_builder.reset();
    } else {
      FilterBuildingContext filter_context(table_options);

      filter_context.info_log = ioptions.logger;
      filter_context.column_family_name = tbo.column_family_name;
      filter_context.reason = tbo.reason;

      // Only populate other fields if known to be in LSM rather than
      // generating external SST file
      if (tbo.reason != TableFileCreationReason::kMisc) {
        filter_context.compaction_style = ioptions.compaction_style;
        filter_context.num_levels = ioptions.num_levels;
        filter_context.level_at_creation = tbo.level_at_creation;
        filter_context.is_bottommost = tbo.is_bottommost;
        assert(filter_context.level_at_creation < filter_context.num_levels);
      }

      filter_builder.reset(CreateFilterBlockBuilder(
          ioptions, moptions, filter_context,
          use_delta_encoding_for_index_values, p_index_builder_));
    }

    const auto& factory_range = tbo.int_tbl_prop_collector_factories;
    for (auto it = factory_range.first; it != factory_range.second; ++it) {
      assert(*it);

      table_properties_collectors.emplace_back(
          (*it)->CreateIntTblPropCollector(column_family_id));
    }
    table_properties_collectors.emplace_back(
        new BlockBasedTablePropertiesCollector(
            table_options.index_type, table_options.whole_key_filtering,
            moptions.prefix_extractor != nullptr));
    if (table_options.verify_compression) {
      for (uint32_t i = 0; i < compression_opts.parallel_threads; i++) {
        verify_ctxs[i].reset(new UncompressionContext(compression_type));
      }
    }

    if (!ReifyDbHostIdProperty(ioptions.env, &db_host_id).ok()) {
      ROCKS_LOG_INFO(ioptions.logger, "db_host_id property will not be set");
    }
  }

  Rep(const Rep&) = delete;
  Rep& operator=(const Rep&) = delete;

 private:
  // Synchronize status & io_status accesses across threads from main thread,
  // compression thread and write thread in parallel compression.
  std::mutex status_mutex;
  std::atomic<bool> status_ok;
  Status status;
  std::mutex io_status_mutex;
  std::atomic<bool> io_status_ok;
  IOStatus io_status;
};

struct BlockBasedTableBuilder::ParallelCompressionRep {
  // Keys is a wrapper of vector of strings avoiding
  // releasing string memories during vector clear()
  // in order to save memory allocation overhead
  class Keys {
   public:
    Keys() : keys_(kKeysInitSize), size_(0) {}
    void PushBack(const Slice& key) {
      if (size_ == keys_.size()) {
        keys_.emplace_back(key.data(), key.size());
      } else {
        keys_[size_].assign(key.data(), key.size());
      }
      size_++;
    }
    void SwapAssign(std::vector<std::string>& keys) {
      size_ = keys.size();
      std::swap(keys_, keys);
    }
    void Clear() { size_ = 0; }
    size_t Size() { return size_; }
    std::string& Back() { return keys_[size_ - 1]; }
    std::string& operator[](size_t idx) {
      assert(idx < size_);
      return keys_[idx];
    }

   private:
    const size_t kKeysInitSize = 32;
    std::vector<std::string> keys_;
    size_t size_;
  };
  std::unique_ptr<Keys> curr_block_keys;

  class BlockRepSlot;

  // BlockRep instances are fetched from and recycled to
  // block_rep_pool during parallel compression.
  struct BlockRep {
    Slice contents;
    Slice compressed_contents;
    std::unique_ptr<std::string> data;
    std::unique_ptr<std::string> compressed_data;
    CompressionType compression_type;
    std::unique_ptr<std::string> first_key_in_next_block;
    std::unique_ptr<Keys> keys;
    std::unique_ptr<BlockRepSlot> slot;
    Status status;
  };
  // Use a vector of BlockRep as a buffer for a determined number
  // of BlockRep structures. All data referenced by pointers in
  // BlockRep will be freed when this vector is destructed.
  typedef std::vector<BlockRep> BlockRepBuffer;
  BlockRepBuffer block_rep_buf;
  // Use a thread-safe queue for concurrent access from block
  // building thread and writer thread.
  typedef WorkQueue<BlockRep*> BlockRepPool;
  BlockRepPool block_rep_pool;

  // Use BlockRepSlot to keep block order in write thread.
  // slot_ will pass references to BlockRep
  class BlockRepSlot {
   public:
    BlockRepSlot() : slot_(1) {}
    template <typename T>
    void Fill(T&& rep) {
      slot_.push(std::forward<T>(rep));
    };
    void Take(BlockRep*& rep) { slot_.pop(rep); }

   private:
    // slot_ will pass references to BlockRep in block_rep_buf,
    // and those references are always valid before the destruction of
    // block_rep_buf.
    WorkQueue<BlockRep*> slot_;
  };

  // Compression queue will pass references to BlockRep in block_rep_buf,
  // and those references are always valid before the destruction of
  // block_rep_buf.
  typedef WorkQueue<BlockRep*> CompressQueue;
  CompressQueue compress_queue;
  std::vector<port::Thread> compress_thread_pool;

  // Write queue will pass references to BlockRep::slot in block_rep_buf,
  // and those references are always valid before the corresponding
  // BlockRep::slot is destructed, which is before the destruction of
  // block_rep_buf.
  typedef WorkQueue<BlockRepSlot*> WriteQueue;
  WriteQueue write_queue;
  std::unique_ptr<port::Thread> write_thread;

  // Estimate output file size when parallel compression is enabled. This is
  // necessary because compression & flush are no longer synchronized,
  // and BlockBasedTableBuilder::FileSize() is no longer accurate.
  // memory_order_relaxed suffices because accurate statistics is not required.
  class FileSizeEstimator {
   public:
    explicit FileSizeEstimator()
        : raw_bytes_compressed(0),
          raw_bytes_curr_block(0),
          raw_bytes_curr_block_set(false),
          raw_bytes_inflight(0),
          blocks_inflight(0),
          curr_compression_ratio(0),
          estimated_file_size(0) {}

    // Estimate file size when a block is about to be emitted to
    // compression thread
    void EmitBlock(uint64_t raw_block_size, uint64_t curr_file_size) {
      uint64_t new_raw_bytes_inflight =
          raw_bytes_inflight.fetch_add(raw_block_size,
                                       std::memory_order_relaxed) +
          raw_block_size;

      uint64_t new_blocks_inflight =
          blocks_inflight.fetch_add(1, std::memory_order_relaxed) + 1;

      estimated_file_size.store(
          curr_file_size +
              static_cast<uint64_t>(
                  static_cast<double>(new_raw_bytes_inflight) *
                  curr_compression_ratio.load(std::memory_order_relaxed)) +
              new_blocks_inflight * kBlockTrailerSize,
          std::memory_order_relaxed);
    }

    // Estimate file size when a block is already reaped from
    // compression thread
    void ReapBlock(uint64_t compressed_block_size, uint64_t curr_file_size) {
      assert(raw_bytes_curr_block_set);

      uint64_t new_raw_bytes_compressed =
          raw_bytes_compressed + raw_bytes_curr_block;
      assert(new_raw_bytes_compressed > 0);

      curr_compression_ratio.store(
          (curr_compression_ratio.load(std::memory_order_relaxed) *
               raw_bytes_compressed +
           compressed_block_size) /
              static_cast<double>(new_raw_bytes_compressed),
          std::memory_order_relaxed);
      raw_bytes_compressed = new_raw_bytes_compressed;

      uint64_t new_raw_bytes_inflight =
          raw_bytes_inflight.fetch_sub(raw_bytes_curr_block,
                                       std::memory_order_relaxed) -
          raw_bytes_curr_block;

      uint64_t new_blocks_inflight =
          blocks_inflight.fetch_sub(1, std::memory_order_relaxed) - 1;

      estimated_file_size.store(
          curr_file_size +
              static_cast<uint64_t>(
                  static_cast<double>(new_raw_bytes_inflight) *
                  curr_compression_ratio.load(std::memory_order_relaxed)) +
              new_blocks_inflight * kBlockTrailerSize,
          std::memory_order_relaxed);

      raw_bytes_curr_block_set = false;
    }

    void SetEstimatedFileSize(uint64_t size) {
      estimated_file_size.store(size, std::memory_order_relaxed);
    }

    uint64_t GetEstimatedFileSize() {
      return estimated_file_size.load(std::memory_order_relaxed);
    }

    void SetCurrBlockRawSize(uint64_t size) {
      raw_bytes_curr_block = size;
      raw_bytes_curr_block_set = true;
    }

   private:
    // Raw bytes compressed so far.
    uint64_t raw_bytes_compressed;
    // Size of current block being appended.
    uint64_t raw_bytes_curr_block;
    // Whether raw_bytes_curr_block has been set for next
    // ReapBlock call.
    bool raw_bytes_curr_block_set;
    // Raw bytes under compression and not appended yet.
    std::atomic<uint64_t> raw_bytes_inflight;
    // Number of blocks under compression and not appended yet.
    std::atomic<uint64_t> blocks_inflight;
    // Current compression ratio, maintained by BGWorkWriteRawBlock.
    std::atomic<double> curr_compression_ratio;
    // Estimated SST file size.
    std::atomic<uint64_t> estimated_file_size;
  };
  FileSizeEstimator file_size_estimator;

  // Facilities used for waiting first block completion. Need to Wait for
  // the completion of first block compression and flush to get a non-zero
  // compression ratio.
  std::atomic<bool> first_block_processed;
  std::condition_variable first_block_cond;
  std::mutex first_block_mutex;

  explicit ParallelCompressionRep(uint32_t parallel_threads)
      : curr_block_keys(new Keys()),
        block_rep_buf(parallel_threads),
        block_rep_pool(parallel_threads),
        compress_queue(parallel_threads),
        write_queue(parallel_threads),
        first_block_processed(false) {
    for (uint32_t i = 0; i < parallel_threads; i++) {
      block_rep_buf[i].contents = Slice();
      block_rep_buf[i].compressed_contents = Slice();
      block_rep_buf[i].data.reset(new std::string());
      block_rep_buf[i].compressed_data.reset(new std::string());
      block_rep_buf[i].compression_type = CompressionType();
      block_rep_buf[i].first_key_in_next_block.reset(new std::string());
      block_rep_buf[i].keys.reset(new Keys());
      block_rep_buf[i].slot.reset(new BlockRepSlot());
      block_rep_buf[i].status = Status::OK();
      block_rep_pool.push(&block_rep_buf[i]);
    }
  }

  ~ParallelCompressionRep() { block_rep_pool.finish(); }

  // Make a block prepared to be emitted to compression thread
  // Used in non-buffered mode
  BlockRep* PrepareBlock(CompressionType compression_type,
                         const Slice* first_key_in_next_block,
                         BlockBuilder* data_block) {
    BlockRep* block_rep =
        PrepareBlockInternal(compression_type, first_key_in_next_block);
    assert(block_rep != nullptr);
    data_block->SwapAndReset(*(block_rep->data));
    block_rep->contents = *(block_rep->data);
    std::swap(block_rep->keys, curr_block_keys);
    curr_block_keys->Clear();
    return block_rep;
  }

  // Used in EnterUnbuffered
  BlockRep* PrepareBlock(CompressionType compression_type,
                         const Slice* first_key_in_next_block,
                         std::string* data_block,
                         std::vector<std::string>* keys) {
    BlockRep* block_rep =
        PrepareBlockInternal(compression_type, first_key_in_next_block);
    assert(block_rep != nullptr);
    std::swap(*(block_rep->data), *data_block);
    block_rep->contents = *(block_rep->data);
    block_rep->keys->SwapAssign(*keys);
    return block_rep;
  }

  // Emit a block to compression thread
  void EmitBlock(BlockRep* block_rep) {
    assert(block_rep != nullptr);
    assert(block_rep->status.ok());
    if (!write_queue.push(block_rep->slot.get())) {
      return;
    }
    if (!compress_queue.push(block_rep)) {
      return;
    }

    if (!first_block_processed.load(std::memory_order_relaxed)) {
      std::unique_lock<std::mutex> lock(first_block_mutex);
      first_block_cond.wait(lock, [this] {
        return first_block_processed.load(std::memory_order_relaxed);
      });
    }
  }

  // Reap a block from compression thread
  void ReapBlock(BlockRep* block_rep) {
    assert(block_rep != nullptr);
    block_rep->compressed_data->clear();
    block_rep_pool.push(block_rep);

    if (!first_block_processed.load(std::memory_order_relaxed)) {
      std::lock_guard<std::mutex> lock(first_block_mutex);
      first_block_processed.store(true, std::memory_order_relaxed);
      first_block_cond.notify_one();
    }
  }

 private:
  BlockRep* PrepareBlockInternal(CompressionType compression_type,
                                 const Slice* first_key_in_next_block) {
    BlockRep* block_rep = nullptr;
    block_rep_pool.pop(block_rep);
    assert(block_rep != nullptr);

    assert(block_rep->data);

    block_rep->compression_type = compression_type;

    if (first_key_in_next_block == nullptr) {
      block_rep->first_key_in_next_block.reset(nullptr);
    } else {
      block_rep->first_key_in_next_block->assign(
          first_key_in_next_block->data(), first_key_in_next_block->size());
    }

    return block_rep;
  }
};

BlockBasedTableBuilder::BlockBasedTableBuilder(
    const BlockBasedTableOptions& table_options, const TableBuilderOptions& tbo,
    WritableFileWriter* file) {
  BlockBasedTableOptions sanitized_table_options(table_options);
  if (sanitized_table_options.format_version == 0 &&
      sanitized_table_options.checksum != kCRC32c) {
    ROCKS_LOG_WARN(
        tbo.ioptions.logger,
        "Silently converting format_version to 1 because checksum is "
        "non-default");
    // silently convert format_version to 1 to keep consistent with current
    // behavior
    sanitized_table_options.format_version = 1;
  }

  rep_ = new Rep(sanitized_table_options, tbo, file);

  if (rep_->filter_builder != nullptr) {
    rep_->filter_builder->StartBlock(0);
  }

  SetupCacheKeyPrefix(tbo);

  if (rep_->IsParallelCompressionEnabled()) {
    StartParallelCompression();
  }
}

BlockBasedTableBuilder::~BlockBasedTableBuilder() {
  // Catch errors where caller forgot to call Finish()
  assert(rep_->state == Rep::State::kClosed);
  delete rep_;
}

void BlockBasedTableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(rep_->state != Rep::State::kClosed);
  if (!ok()) return;
  ValueType value_type = ExtractValueType(key);
  if (IsValueType(value_type)) {
#ifndef NDEBUG
    if (r->props.num_entries > r->props.num_range_deletions) {
      assert(r->internal_comparator.Compare(key, Slice(r->last_key)) > 0);
    }
#endif  // !NDEBUG

    auto should_flush = r->flush_block_policy->Update(key, value);
    if (should_flush) {
      assert(!r->data_block.empty());
      r->first_key_in_next_block = &key;
      Flush();

      if (r->state == Rep::State::kBuffered && r->buffer_limit != 0 &&
          r->data_begin_offset > r->buffer_limit) {
        EnterUnbuffered();
      }

      // Add item to index block.
      // We do not emit the index entry for a block until we have seen the
      // first key for the next data block.  This allows us to use shorter
      // keys in the index block.  For example, consider a block boundary
      // between the keys "the quick brown fox" and "the who".  We can use
      // "the r" as the key for the index block entry since it is >= all
      // entries in the first block and < all entries in subsequent
      // blocks.
      if (ok() && r->state == Rep::State::kUnbuffered) {
        if (r->IsParallelCompressionEnabled()) {
          r->pc_rep->curr_block_keys->Clear();
        } else {
          r->index_builder->AddIndexEntry(&r->last_key, &key,
                                          r->pending_handle);
        }
      }
    }

    // Note: PartitionedFilterBlockBuilder requires key being added to filter
    // builder after being added to index builder.
    if (r->state == Rep::State::kUnbuffered) {
      if (r->IsParallelCompressionEnabled()) {
        r->pc_rep->curr_block_keys->PushBack(key);
      } else {
        if (r->filter_builder != nullptr) {
          size_t ts_sz =
              r->internal_comparator.user_comparator()->timestamp_size();
          r->filter_builder->Add(ExtractUserKeyAndStripTimestamp(key, ts_sz));
        }
      }
    }

    r->last_key.assign(key.data(), key.size());
    r->data_block.Add(key, value);
    if (r->state == Rep::State::kBuffered) {
      // Buffered keys will be replayed from data_block_buffers during
      // `Finish()` once compression dictionary has been finalized.
    } else {
      if (!r->IsParallelCompressionEnabled()) {
        r->index_builder->OnKeyAdded(key);
      }
    }
    // TODO offset passed in is not accurate for parallel compression case
    NotifyCollectTableCollectorsOnAdd(key, value, r->get_offset(),
                                      r->table_properties_collectors,
                                      r->ioptions.logger);

  } else if (value_type == kTypeRangeDeletion) {
    r->range_del_block.Add(key, value);
    // TODO offset passed in is not accurate for parallel compression case
    NotifyCollectTableCollectorsOnAdd(key, value, r->get_offset(),
                                      r->table_properties_collectors,
                                      r->ioptions.logger);
  } else {
    assert(false);
  }

  r->props.num_entries++;
  r->props.raw_key_size += key.size();
  r->props.raw_value_size += value.size();
  if (value_type == kTypeDeletion || value_type == kTypeSingleDeletion) {
    r->props.num_deletions++;
  } else if (value_type == kTypeRangeDeletion) {
    r->props.num_deletions++;
    r->props.num_range_deletions++;
  } else if (value_type == kTypeMerge) {
    r->props.num_merge_operands++;
  }
}

void BlockBasedTableBuilder::Flush() {
  Rep* r = rep_;
  assert(rep_->state != Rep::State::kClosed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  if (r->IsParallelCompressionEnabled() &&
      r->state == Rep::State::kUnbuffered) {
    r->data_block.Finish();
    ParallelCompressionRep::BlockRep* block_rep = r->pc_rep->PrepareBlock(
        r->compression_type, r->first_key_in_next_block, &(r->data_block));
    assert(block_rep != nullptr);
    r->pc_rep->file_size_estimator.EmitBlock(block_rep->data->size(),
                                             r->get_offset());
    r->pc_rep->EmitBlock(block_rep);
  } else {
    WriteBlock(&r->data_block, &r->pending_handle, true /* is_data_block */);
  }
}

void BlockBasedTableBuilder::WriteBlock(BlockBuilder* block,
                                        BlockHandle* handle,
                                        bool is_data_block) {
  block->Finish();
  std::string raw_block_contents;
  block->SwapAndReset(raw_block_contents);
  if (rep_->state == Rep::State::kBuffered) {
    assert(is_data_block);
    rep_->data_block_buffers.emplace_back(std::move(raw_block_contents));
    rep_->data_begin_offset += rep_->data_block_buffers.back().size();
    return;
  }
  WriteBlock(raw_block_contents, handle, is_data_block);
}

void BlockBasedTableBuilder::WriteBlock(const Slice& raw_block_contents,
                                        BlockHandle* handle,
                                        bool is_data_block) {
  Rep* r = rep_;
  assert(r->state == Rep::State::kUnbuffered);
  Slice block_contents;
  CompressionType type;
  Status compress_status;
  CompressAndVerifyBlock(raw_block_contents, is_data_block,
                         *(r->compression_ctxs[0]), r->verify_ctxs[0].get(),
                         &(r->compressed_output), &(block_contents), &type,
                         &compress_status);
  r->SetStatus(compress_status);
  if (!ok()) {
    return;
  }

  WriteRawBlock(block_contents, type, handle, is_data_block,
                &raw_block_contents);
  r->compressed_output.clear();
  if (is_data_block) {
    if (r->filter_builder != nullptr) {
      r->filter_builder->StartBlock(r->get_offset());
    }
    r->props.data_size = r->get_offset();
    ++r->props.num_data_blocks;
  }
}

void BlockBasedTableBuilder::BGWorkCompression(
    const CompressionContext& compression_ctx,
    UncompressionContext* verify_ctx) {
  ParallelCompressionRep::BlockRep* block_rep = nullptr;
  while (rep_->pc_rep->compress_queue.pop(block_rep)) {
    assert(block_rep != nullptr);
    CompressAndVerifyBlock(block_rep->contents, true, /* is_data_block*/
                           compression_ctx, verify_ctx,
                           block_rep->compressed_data.get(),
                           &block_rep->compressed_contents,
                           &(block_rep->compression_type), &block_rep->status);
    block_rep->slot->Fill(block_rep);
  }
}

void BlockBasedTableBuilder::CompressAndVerifyBlock(
    const Slice& raw_block_contents, bool is_data_block,
    const CompressionContext& compression_ctx, UncompressionContext* verify_ctx,
    std::string* compressed_output, Slice* block_contents,
    CompressionType* type, Status* out_status) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  Rep* r = rep_;
  bool is_status_ok = ok();
  if (!r->IsParallelCompressionEnabled()) {
    assert(is_status_ok);
  }

  *type = r->compression_type;
  uint64_t sample_for_compression = r->sample_for_compression;
  bool abort_compression = false;

  StopWatchNano timer(
      r->ioptions.clock,
      ShouldReportDetailedTime(r->ioptions.env, r->ioptions.stats));

  if (is_status_ok && raw_block_contents.size() < kCompressionSizeLimit) {
    if (is_data_block) {
      r->compressible_input_data_bytes.fetch_add(raw_block_contents.size(),
                                                 std::memory_order_relaxed);
    }
    const CompressionDict* compression_dict;
    if (!is_data_block || r->compression_dict == nullptr) {
      compression_dict = &CompressionDict::GetEmptyDict();
    } else {
      compression_dict = r->compression_dict.get();
    }
    assert(compression_dict != nullptr);
    CompressionInfo compression_info(r->compression_opts, compression_ctx,
                                     *compression_dict, *type,
                                     sample_for_compression);

    std::string sampled_output_fast;
    std::string sampled_output_slow;
    *block_contents = CompressBlock(
        raw_block_contents, compression_info, type,
        r->table_options.format_version, is_data_block /* do_sample */,
        compressed_output, &sampled_output_fast, &sampled_output_slow);

    if (sampled_output_slow.size() > 0 || sampled_output_fast.size() > 0) {
      // Currently compression sampling is only enabled for data block.
      assert(is_data_block);
      r->sampled_input_data_bytes.fetch_add(raw_block_contents.size(),
                                            std::memory_order_relaxed);
      r->sampled_output_slow_data_bytes.fetch_add(sampled_output_slow.size(),
                                                  std::memory_order_relaxed);
      r->sampled_output_fast_data_bytes.fetch_add(sampled_output_fast.size(),
                                                  std::memory_order_relaxed);
    }
    // notify collectors on block add
    NotifyCollectTableCollectorsOnBlockAdd(
        r->table_properties_collectors, raw_block_contents.size(),
        sampled_output_fast.size(), sampled_output_slow.size());

    // Some of the compression algorithms are known to be unreliable. If
    // the verify_compression flag is set then try to de-compress the
    // compressed data and compare to the input.
    if (*type != kNoCompression && r->table_options.verify_compression) {
      // Retrieve the uncompressed contents into a new buffer
      const UncompressionDict* verify_dict;
      if (!is_data_block || r->verify_dict == nullptr) {
        verify_dict = &UncompressionDict::GetEmptyDict();
      } else {
        verify_dict = r->verify_dict.get();
      }
      assert(verify_dict != nullptr);
      BlockContents contents;
      UncompressionInfo uncompression_info(*verify_ctx, *verify_dict,
                                           r->compression_type);
      Status stat = UncompressBlockContentsForCompressionType(
          uncompression_info, block_contents->data(), block_contents->size(),
          &contents, r->table_options.format_version, r->ioptions);

      if (stat.ok()) {
        bool compressed_ok = contents.data.compare(raw_block_contents) == 0;
        if (!compressed_ok) {
          // The result of the compression was invalid. abort.
          abort_compression = true;
          ROCKS_LOG_ERROR(r->ioptions.logger,
                          "Decompressed block did not match raw block");
          *out_status =
              Status::Corruption("Decompressed block did not match raw block");
        }
      } else {
        // Decompression reported an error. abort.
        *out_status = Status::Corruption(std::string("Could not decompress: ") +
                                         stat.getState());
        abort_compression = true;
      }
    }
  } else {
    // Block is too big to be compressed.
    if (is_data_block) {
      r->uncompressible_input_data_bytes.fetch_add(raw_block_contents.size(),
                                                   std::memory_order_relaxed);
    }
    abort_compression = true;
  }
  if (is_data_block) {
    r->uncompressible_input_data_bytes.fetch_add(kBlockTrailerSize,
                                                 std::memory_order_relaxed);
  }

  // Abort compression if the block is too big, or did not pass
  // verification.
  if (abort_compression) {
    RecordTick(r->ioptions.stats, NUMBER_BLOCK_NOT_COMPRESSED);
    *type = kNoCompression;
    *block_contents = raw_block_contents;
  } else if (*type != kNoCompression) {
    if (ShouldReportDetailedTime(r->ioptions.env, r->ioptions.stats)) {
      RecordTimeToHistogram(r->ioptions.stats, COMPRESSION_TIMES_NANOS,
                            timer.ElapsedNanos());
    }
    RecordInHistogram(r->ioptions.stats, BYTES_COMPRESSED,
                      raw_block_contents.size());
    RecordTick(r->ioptions.stats, NUMBER_BLOCK_COMPRESSED);
  } else if (*type != r->compression_type) {
    RecordTick(r->ioptions.stats, NUMBER_BLOCK_NOT_COMPRESSED);
  }
}

void BlockBasedTableBuilder::WriteRawBlock(const Slice& block_contents,
                                           CompressionType type,
                                           BlockHandle* handle,
                                           bool is_data_block,
                                           const Slice* raw_block_contents) {
  Rep* r = rep_;
  Status s = Status::OK();
  IOStatus io_s = IOStatus::OK();
  StopWatch sw(r->ioptions.clock, r->ioptions.stats, WRITE_RAW_BLOCK_MICROS);
  handle->set_offset(r->get_offset());
  handle->set_size(block_contents.size());
  assert(status().ok());
  assert(io_status().ok());
  io_s = r->file->Append(block_contents);
  if (io_s.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t checksum = 0;
    switch (r->table_options.checksum) {
      case kNoChecksum:
        break;
      case kCRC32c: {
        uint32_t crc =
            crc32c::Value(block_contents.data(), block_contents.size());
        // Extend to cover compression type
        crc = crc32c::Extend(crc, trailer, 1);
        checksum = crc32c::Mask(crc);
        break;
      }
      case kxxHash: {
        XXH32_state_t* const state = XXH32_createState();
        XXH32_reset(state, 0);
        XXH32_update(state, block_contents.data(), block_contents.size());
        // Extend to cover compression type
        XXH32_update(state, trailer, 1);
        checksum = XXH32_digest(state);
        XXH32_freeState(state);
        break;
      }
      case kxxHash64: {
        XXH64_state_t* const state = XXH64_createState();
        XXH64_reset(state, 0);
        XXH64_update(state, block_contents.data(), block_contents.size());
        // Extend to cover compression type
        XXH64_update(state, trailer, 1);
        checksum = Lower32of64(XXH64_digest(state));
        XXH64_freeState(state);
        break;
      }
      default:
        assert(false);
        break;
    }
    EncodeFixed32(trailer + 1, checksum);
    assert(io_s.ok());
    TEST_SYNC_POINT_CALLBACK(
        "BlockBasedTableBuilder::WriteRawBlock:TamperWithChecksum",
        static_cast<char*>(trailer));
    io_s = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (io_s.ok()) {
      assert(s.ok());
      if (is_data_block &&
          r->table_options.prepopulate_block_cache ==
              BlockBasedTableOptions::PrepopulateBlockCache::kFlushOnly) {
        if (type == kNoCompression) {
          s = InsertBlockInCache(block_contents, handle);
        } else if (raw_block_contents != nullptr) {
          s = InsertBlockInCache(*raw_block_contents, handle);
        }
        if (!s.ok()) {
          r->SetStatus(s);
        }
      }
      // TODO:: Should InsertBlockInCompressedCache take into account error from
      // InsertBlockInCache or ignore and overwrite it.
      s = InsertBlockInCompressedCache(block_contents, type, handle);
      if (!s.ok()) {
        r->SetStatus(s);
      }
    } else {
      r->SetIOStatus(io_s);
    }
    if (s.ok() && io_s.ok()) {
      r->set_offset(r->get_offset() + block_contents.size() +
                    kBlockTrailerSize);
      if (r->table_options.block_align && is_data_block) {
        size_t pad_bytes =
            (r->alignment - ((block_contents.size() + kBlockTrailerSize) &
                             (r->alignment - 1))) &
            (r->alignment - 1);
        io_s = r->file->Pad(pad_bytes);
        if (io_s.ok()) {
          r->set_offset(r->get_offset() + pad_bytes);
        } else {
          r->SetIOStatus(io_s);
        }
      }
      if (r->IsParallelCompressionEnabled()) {
        if (is_data_block) {
          r->pc_rep->file_size_estimator.ReapBlock(block_contents.size(),
                                                   r->get_offset());
        } else {
          r->pc_rep->file_size_estimator.SetEstimatedFileSize(r->get_offset());
        }
      }
    }
  } else {
    r->SetIOStatus(io_s);
  }
  if (!io_s.ok() && s.ok()) {
    r->SetStatus(io_s);
  }
}

void BlockBasedTableBuilder::BGWorkWriteRawBlock() {
  Rep* r = rep_;
  ParallelCompressionRep::BlockRepSlot* slot = nullptr;
  ParallelCompressionRep::BlockRep* block_rep = nullptr;
  while (r->pc_rep->write_queue.pop(slot)) {
    assert(slot != nullptr);
    slot->Take(block_rep);
    assert(block_rep != nullptr);
    if (!block_rep->status.ok()) {
      r->SetStatus(block_rep->status);
      // Reap block so that blocked Flush() can finish
      // if there is one, and Flush() will notice !ok() next time.
      block_rep->status = Status::OK();
      r->pc_rep->ReapBlock(block_rep);
      continue;
    }

    for (size_t i = 0; i < block_rep->keys->Size(); i++) {
      auto& key = (*block_rep->keys)[i];
      if (r->filter_builder != nullptr) {
        size_t ts_sz =
            r->internal_comparator.user_comparator()->timestamp_size();
        r->filter_builder->Add(ExtractUserKeyAndStripTimestamp(key, ts_sz));
      }
      r->index_builder->OnKeyAdded(key);
    }

    r->pc_rep->file_size_estimator.SetCurrBlockRawSize(block_rep->data->size());

    WriteRawBlock(block_rep->compressed_contents, block_rep->compression_type,
                  &r->pending_handle, true /* is_data_block*/,
                  &block_rep->contents);
    if (!ok()) {
      break;
    }

    if (r->filter_builder != nullptr) {
      r->filter_builder->StartBlock(r->get_offset());
    }
    r->props.data_size = r->get_offset();
    ++r->props.num_data_blocks;

    if (block_rep->first_key_in_next_block == nullptr) {
      r->index_builder->AddIndexEntry(&(block_rep->keys->Back()), nullptr,
                                      r->pending_handle);
    } else {
      Slice first_key_in_next_block =
          Slice(*block_rep->first_key_in_next_block);
      r->index_builder->AddIndexEntry(&(block_rep->keys->Back()),
                                      &first_key_in_next_block,
                                      r->pending_handle);
    }

    r->pc_rep->ReapBlock(block_rep);
  }
}

void BlockBasedTableBuilder::StartParallelCompression() {
  rep_->pc_rep.reset(
      new ParallelCompressionRep(rep_->compression_opts.parallel_threads));
  rep_->pc_rep->compress_thread_pool.reserve(
      rep_->compression_opts.parallel_threads);
  for (uint32_t i = 0; i < rep_->compression_opts.parallel_threads; i++) {
    rep_->pc_rep->compress_thread_pool.emplace_back([this, i] {
      BGWorkCompression(*(rep_->compression_ctxs[i]),
                        rep_->verify_ctxs[i].get());
    });
  }
  rep_->pc_rep->write_thread.reset(
      new port::Thread([this] { BGWorkWriteRawBlock(); }));
}

void BlockBasedTableBuilder::StopParallelCompression() {
  rep_->pc_rep->compress_queue.finish();
  for (auto& thread : rep_->pc_rep->compress_thread_pool) {
    thread.join();
  }
  rep_->pc_rep->write_queue.finish();
  rep_->pc_rep->write_thread->join();
}

Status BlockBasedTableBuilder::status() const { return rep_->GetStatus(); }

IOStatus BlockBasedTableBuilder::io_status() const {
  return rep_->GetIOStatus();
}

namespace {
// Delete the entry resided in the cache.
template <class Entry>
void DeleteEntryCached(const Slice& /*key*/, void* value) {
  auto entry = reinterpret_cast<Entry*>(value);
  delete entry;
}
}  // namespace

// Helper function to setup the cache key's prefix for the Table.
void BlockBasedTableBuilder::SetupCacheKeyPrefix(
    const TableBuilderOptions& tbo) {
  if (rep_->table_options.block_cache.get() != nullptr) {
    BlockBasedTable::GenerateCachePrefix<Cache, FSWritableFile>(
        rep_->table_options.block_cache.get(), rep_->file->writable_file(),
        &rep_->cache_key_prefix[0], &rep_->cache_key_prefix_size,
        tbo.db_session_id, tbo.cur_file_num);
  }
  if (rep_->table_options.block_cache_compressed.get() != nullptr) {
    BlockBasedTable::GenerateCachePrefix<Cache, FSWritableFile>(
        rep_->table_options.block_cache_compressed.get(),
        rep_->file->writable_file(), &rep_->compressed_cache_key_prefix[0],
        &rep_->compressed_cache_key_prefix_size, tbo.db_session_id,
        tbo.cur_file_num);
  }
}

//
// Make a copy of the block contents and insert into compressed block cache
//
Status BlockBasedTableBuilder::InsertBlockInCompressedCache(
    const Slice& block_contents, const CompressionType type,
    const BlockHandle* handle) {
  Rep* r = rep_;
  Cache* block_cache_compressed = r->table_options.block_cache_compressed.get();
  Status s;
  if (type != kNoCompression && block_cache_compressed != nullptr) {
    size_t size = block_contents.size();

    auto ubuf =
        AllocateBlock(size + 1, block_cache_compressed->memory_allocator());
    memcpy(ubuf.get(), block_contents.data(), size);
    ubuf[size] = type;

    BlockContents* block_contents_to_cache =
        new BlockContents(std::move(ubuf), size);
#ifndef NDEBUG
    block_contents_to_cache->is_raw_block = true;
#endif  // NDEBUG

    // make cache key by appending the file offset to the cache prefix id
    char* end = EncodeVarint64(
        r->compressed_cache_key_prefix + r->compressed_cache_key_prefix_size,
        handle->offset());
    Slice key(r->compressed_cache_key_prefix,
              static_cast<size_t>(end - r->compressed_cache_key_prefix));

    s = block_cache_compressed->Insert(
        key, block_contents_to_cache,
        block_contents_to_cache->ApproximateMemoryUsage(),
        &DeleteEntryCached<BlockContents>);
    if (s.ok()) {
      RecordTick(rep_->ioptions.stats, BLOCK_CACHE_COMPRESSED_ADD);
    } else {
      RecordTick(rep_->ioptions.stats, BLOCK_CACHE_COMPRESSED_ADD_FAILURES);
    }
    // Invalidate OS cache.
    r->file->InvalidateCache(static_cast<size_t>(r->get_offset()), size)
        .PermitUncheckedError();
  }
  return s;
}

Status BlockBasedTableBuilder::InsertBlockInCache(const Slice& block_contents,
                                                  const BlockHandle* handle) {
  // Uncompressed regular block cache
  Cache* block_cache = rep_->table_options.block_cache.get();
  Status s;
  if (block_cache != nullptr) {
    size_t size = block_contents.size();
    auto buf = AllocateBlock(size, block_cache->memory_allocator());
    memcpy(buf.get(), block_contents.data(), size);
    BlockContents results(std::move(buf), size);

    char
        cache_key[BlockBasedTable::kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    Slice key = BlockBasedTable::GetCacheKey(rep_->cache_key_prefix,
                                             rep_->cache_key_prefix_size,
                                             *handle, cache_key);

    const size_t read_amp_bytes_per_bit =
        rep_->table_options.read_amp_bytes_per_bit;
    Block* block = new Block(std::move(results), read_amp_bytes_per_bit);
    size_t charge = block->ApproximateMemoryUsage();
    s = block_cache->Insert(key, block, charge, &DeleteEntryCached<Block>);
    if (s.ok()) {
      BlockBasedTable::UpdateCacheInsertionMetrics(
          BlockType::kData, nullptr /*get_context*/, charge,
          s.IsOkOverwritten(), rep_->ioptions.stats);
    } else {
      RecordTick(rep_->ioptions.stats, BLOCK_CACHE_ADD_FAILURES);
    }
  }
  return s;
}

void BlockBasedTableBuilder::WriteFilterBlock(
    MetaIndexBuilder* meta_index_builder) {
  BlockHandle filter_block_handle;
  bool empty_filter_block =
      (rep_->filter_builder == nullptr || rep_->filter_builder->IsEmpty());
  if (ok() && !empty_filter_block) {
    rep_->props.num_filter_entries +=
        rep_->filter_builder->EstimateEntriesAdded();
    Status s = Status::Incomplete();
    while (ok() && s.IsIncomplete()) {
      Slice filter_content =
          rep_->filter_builder->Finish(filter_block_handle, &s);
      assert(s.ok() || s.IsIncomplete());
      rep_->props.filter_size += filter_content.size();
      WriteRawBlock(filter_content, kNoCompression, &filter_block_handle);
    }
  }
  if (ok() && !empty_filter_block) {
    // Add mapping from "<filter_block_prefix>.Name" to location
    // of filter data.
    std::string key;
    if (rep_->filter_builder->IsBlockBased()) {
      key = BlockBasedTable::kFilterBlockPrefix;
    } else {
      key = rep_->table_options.partition_filters
                ? BlockBasedTable::kPartitionedFilterBlockPrefix
                : BlockBasedTable::kFullFilterBlockPrefix;
    }
    key.append(rep_->table_options.filter_policy->Name());
    meta_index_builder->Add(key, filter_block_handle);
  }
}

void BlockBasedTableBuilder::WriteIndexBlock(
    MetaIndexBuilder* meta_index_builder, BlockHandle* index_block_handle) {
  IndexBuilder::IndexBlocks index_blocks;
  auto index_builder_status = rep_->index_builder->Finish(&index_blocks);
  if (index_builder_status.IsIncomplete()) {
    // We we have more than one index partition then meta_blocks are not
    // supported for the index. Currently meta_blocks are used only by
    // HashIndexBuilder which is not multi-partition.
    assert(index_blocks.meta_blocks.empty());
  } else if (ok() && !index_builder_status.ok()) {
    rep_->SetStatus(index_builder_status);
  }
  if (ok()) {
    for (const auto& item : index_blocks.meta_blocks) {
      BlockHandle block_handle;
      WriteBlock(item.second, &block_handle, false /* is_data_block */);
      if (!ok()) {
        break;
      }
      meta_index_builder->Add(item.first, block_handle);
    }
  }
  if (ok()) {
    if (rep_->table_options.enable_index_compression) {
      WriteBlock(index_blocks.index_block_contents, index_block_handle, false);
    } else {
      WriteRawBlock(index_blocks.index_block_contents, kNoCompression,
                    index_block_handle);
    }
  }
  // If there are more index partitions, finish them and write them out
  if (index_builder_status.IsIncomplete()) {
    Status s = Status::Incomplete();
    while (ok() && s.IsIncomplete()) {
      s = rep_->index_builder->Finish(&index_blocks, *index_block_handle);
      if (!s.ok() && !s.IsIncomplete()) {
        rep_->SetStatus(s);
        return;
      }
      if (rep_->table_options.enable_index_compression) {
        WriteBlock(index_blocks.index_block_contents, index_block_handle,
                   false);
      } else {
        WriteRawBlock(index_blocks.index_block_contents, kNoCompression,
                      index_block_handle);
      }
      // The last index_block_handle will be for the partition index block
    }
  }
}

void BlockBasedTableBuilder::WritePropertiesBlock(
    MetaIndexBuilder* meta_index_builder) {
  BlockHandle properties_block_handle;
  if (ok()) {
    PropertyBlockBuilder property_block_builder;
    rep_->props.column_family_id = rep_->column_family_id;
    rep_->props.column_family_name = rep_->column_family_name;
    rep_->props.filter_policy_name =
        rep_->table_options.filter_policy != nullptr
            ? rep_->table_options.filter_policy->Name()
            : "";
    rep_->props.index_size =
        rep_->index_builder->IndexSize() + kBlockTrailerSize;
    rep_->props.comparator_name = rep_->ioptions.user_comparator != nullptr
                                      ? rep_->ioptions.user_comparator->Name()
                                      : "nullptr";
    rep_->props.merge_operator_name =
        rep_->ioptions.merge_operator != nullptr
            ? rep_->ioptions.merge_operator->Name()
            : "nullptr";
    rep_->props.compression_name =
        CompressionTypeToString(rep_->compression_type);
    rep_->props.compression_options =
        CompressionOptionsToString(rep_->compression_opts);
    rep_->props.prefix_extractor_name =
        rep_->moptions.prefix_extractor != nullptr
            ? rep_->moptions.prefix_extractor->Name()
            : "nullptr";

    std::string property_collectors_names = "[";
    for (size_t i = 0;
         i < rep_->ioptions.table_properties_collector_factories.size(); ++i) {
      if (i != 0) {
        property_collectors_names += ",";
      }
      property_collectors_names +=
          rep_->ioptions.table_properties_collector_factories[i]->Name();
    }
    property_collectors_names += "]";
    rep_->props.property_collectors_names = property_collectors_names;
    if (rep_->table_options.index_type ==
        BlockBasedTableOptions::kTwoLevelIndexSearch) {
      assert(rep_->p_index_builder_ != nullptr);
      rep_->props.index_partitions = rep_->p_index_builder_->NumPartitions();
      rep_->props.top_level_index_size =
          rep_->p_index_builder_->TopLevelIndexSize(rep_->offset);
    }
    rep_->props.index_key_is_user_key =
        !rep_->index_builder->seperator_is_key_plus_seq();
    rep_->props.index_value_is_delta_encoded =
        rep_->use_delta_encoding_for_index_values;
    rep_->props.creation_time = rep_->creation_time;
    rep_->props.oldest_key_time = rep_->oldest_key_time;
    rep_->props.file_creation_time = rep_->file_creation_time;
    if (rep_->sampled_input_data_bytes > 0) {
      rep_->props.slow_compression_estimated_data_size = static_cast<uint64_t>(
          static_cast<double>(rep_->sampled_output_slow_data_bytes) /
              rep_->sampled_input_data_bytes *
              rep_->compressible_input_data_bytes +
          rep_->uncompressible_input_data_bytes + 0.5);
      rep_->props.fast_compression_estimated_data_size = static_cast<uint64_t>(
          static_cast<double>(rep_->sampled_output_fast_data_bytes) /
              rep_->sampled_input_data_bytes *
              rep_->compressible_input_data_bytes +
          rep_->uncompressible_input_data_bytes + 0.5);
    } else if (rep_->sample_for_compression > 0) {
      // We tried to sample but none were found. Assume worst-case (compression
      // ratio 1.0) so data is complete and aggregatable.
      rep_->props.slow_compression_estimated_data_size =
          rep_->compressible_input_data_bytes +
          rep_->uncompressible_input_data_bytes;
      rep_->props.fast_compression_estimated_data_size =
          rep_->compressible_input_data_bytes +
          rep_->uncompressible_input_data_bytes;
    }
    rep_->props.db_id = rep_->db_id;
    rep_->props.db_session_id = rep_->db_session_id;
    rep_->props.db_host_id = rep_->db_host_id;

    // Add basic properties
    property_block_builder.AddTableProperty(rep_->props);

    // Add use collected properties
    NotifyCollectTableCollectorsOnFinish(rep_->table_properties_collectors,
                                         rep_->ioptions.logger,
                                         &property_block_builder);

    WriteRawBlock(property_block_builder.Finish(), kNoCompression,
                  &properties_block_handle);
  }
  if (ok()) {
#ifndef NDEBUG
    {
      uint64_t props_block_offset = properties_block_handle.offset();
      uint64_t props_block_size = properties_block_handle.size();
      TEST_SYNC_POINT_CALLBACK(
          "BlockBasedTableBuilder::WritePropertiesBlock:GetPropsBlockOffset",
          &props_block_offset);
      TEST_SYNC_POINT_CALLBACK(
          "BlockBasedTableBuilder::WritePropertiesBlock:GetPropsBlockSize",
          &props_block_size);
    }
#endif  // !NDEBUG
    meta_index_builder->Add(kPropertiesBlock, properties_block_handle);
  }
}

void BlockBasedTableBuilder::WriteCompressionDictBlock(
    MetaIndexBuilder* meta_index_builder) {
  if (rep_->compression_dict != nullptr &&
      rep_->compression_dict->GetRawDict().size()) {
    BlockHandle compression_dict_block_handle;
    if (ok()) {
      WriteRawBlock(rep_->compression_dict->GetRawDict(), kNoCompression,
                    &compression_dict_block_handle);
#ifndef NDEBUG
      Slice compression_dict = rep_->compression_dict->GetRawDict();
      TEST_SYNC_POINT_CALLBACK(
          "BlockBasedTableBuilder::WriteCompressionDictBlock:RawDict",
          &compression_dict);
#endif  // NDEBUG
    }
    if (ok()) {
      meta_index_builder->Add(kCompressionDictBlock,
                              compression_dict_block_handle);
    }
  }
}

void BlockBasedTableBuilder::WriteRangeDelBlock(
    MetaIndexBuilder* meta_index_builder) {
  if (ok() && !rep_->range_del_block.empty()) {
    BlockHandle range_del_block_handle;
    WriteRawBlock(rep_->range_del_block.Finish(), kNoCompression,
                  &range_del_block_handle);
    meta_index_builder->Add(kRangeDelBlock, range_del_block_handle);
  }
}

void BlockBasedTableBuilder::WriteFooter(BlockHandle& metaindex_block_handle,
                                         BlockHandle& index_block_handle) {
  Rep* r = rep_;
  // No need to write out new footer if we're using default checksum.
  // We're writing legacy magic number because we want old versions of RocksDB
  // be able to read files generated with new release (just in case if
  // somebody wants to roll back after an upgrade)
  // TODO(icanadi) at some point in the future, when we're absolutely sure
  // nobody will roll back to RocksDB 2.x versions, retire the legacy magic
  // number and always write new table files with new magic number
  bool legacy = (r->table_options.format_version == 0);
  // this is guaranteed by BlockBasedTableBuilder's constructor
  assert(r->table_options.checksum == kCRC32c ||
         r->table_options.format_version != 0);
  Footer footer(
      legacy ? kLegacyBlockBasedTableMagicNumber : kBlockBasedTableMagicNumber,
      r->table_options.format_version);
  footer.set_metaindex_handle(metaindex_block_handle);
  footer.set_index_handle(index_block_handle);
  footer.set_checksum(r->table_options.checksum);
  std::string footer_encoding;
  footer.EncodeTo(&footer_encoding);
  assert(ok());
  IOStatus ios = r->file->Append(footer_encoding);
  if (ios.ok()) {
    r->set_offset(r->get_offset() + footer_encoding.size());
  } else {
    r->SetIOStatus(ios);
    r->SetStatus(ios);
  }
}

void BlockBasedTableBuilder::EnterUnbuffered() {
  Rep* r = rep_;
  assert(r->state == Rep::State::kBuffered);
  r->state = Rep::State::kUnbuffered;
  const size_t kSampleBytes = r->compression_opts.zstd_max_train_bytes > 0
                                  ? r->compression_opts.zstd_max_train_bytes
                                  : r->compression_opts.max_dict_bytes;
  const size_t kNumBlocksBuffered = r->data_block_buffers.size();
  if (kNumBlocksBuffered == 0) {
    // The below code is neither safe nor necessary for handling zero data
    // blocks.
    return;
  }

  // Abstract algebra teaches us that a finite cyclic group (such as the
  // additive group of integers modulo N) can be generated by a number that is
  // coprime with N. Since N is variable (number of buffered data blocks), we
  // must then pick a prime number in order to guarantee coprimeness with any N.
  //
  // One downside of this approach is the spread will be poor when
  // `kPrimeGeneratorRemainder` is close to zero or close to
  // `kNumBlocksBuffered`.
  //
  // Picked a random number between one and one trillion and then chose the
  // next prime number greater than or equal to it.
  const uint64_t kPrimeGenerator = 545055921143ull;
  // Can avoid repeated division by just adding the remainder repeatedly.
  const size_t kPrimeGeneratorRemainder = static_cast<size_t>(
      kPrimeGenerator % static_cast<uint64_t>(kNumBlocksBuffered));
  const size_t kInitSampleIdx = kNumBlocksBuffered / 2;

  std::string compression_dict_samples;
  std::vector<size_t> compression_dict_sample_lens;
  size_t buffer_idx = kInitSampleIdx;
  for (size_t i = 0;
       i < kNumBlocksBuffered && compression_dict_samples.size() < kSampleBytes;
       ++i) {
    size_t copy_len = std::min(kSampleBytes - compression_dict_samples.size(),
                               r->data_block_buffers[buffer_idx].size());
    compression_dict_samples.append(r->data_block_buffers[buffer_idx], 0,
                                    copy_len);
    compression_dict_sample_lens.emplace_back(copy_len);

    buffer_idx += kPrimeGeneratorRemainder;
    if (buffer_idx >= kNumBlocksBuffered) {
      buffer_idx -= kNumBlocksBuffered;
    }
  }

  // final data block flushed, now we can generate dictionary from the samples.
  // OK if compression_dict_samples is empty, we'll just get empty dictionary.
  std::string dict;
  if (r->compression_opts.zstd_max_train_bytes > 0) {
    dict = ZSTD_TrainDictionary(compression_dict_samples,
                                compression_dict_sample_lens,
                                r->compression_opts.max_dict_bytes);
  } else {
    dict = std::move(compression_dict_samples);
  }
  r->compression_dict.reset(new CompressionDict(dict, r->compression_type,
                                                r->compression_opts.level));
  r->verify_dict.reset(new UncompressionDict(
      dict, r->compression_type == kZSTD ||
                r->compression_type == kZSTDNotFinalCompression));

  auto get_iterator_for_block = [&r](size_t i) {
    auto& data_block = r->data_block_buffers[i];
    assert(!data_block.empty());

    Block reader{BlockContents{data_block}};
    DataBlockIter* iter = reader.NewDataIterator(
        r->internal_comparator.user_comparator(), kDisableGlobalSequenceNumber);

    iter->SeekToFirst();
    assert(iter->Valid());
    return std::unique_ptr<DataBlockIter>(iter);
  };

  std::unique_ptr<DataBlockIter> iter = nullptr, next_block_iter = nullptr;

  for (size_t i = 0; ok() && i < r->data_block_buffers.size(); ++i) {
    if (iter == nullptr) {
      iter = get_iterator_for_block(i);
      assert(iter != nullptr);
    };

    if (i + 1 < r->data_block_buffers.size()) {
      next_block_iter = get_iterator_for_block(i + 1);
    }

    auto& data_block = r->data_block_buffers[i];

    if (r->IsParallelCompressionEnabled()) {
      Slice first_key_in_next_block;
      const Slice* first_key_in_next_block_ptr = &first_key_in_next_block;
      if (i + 1 < r->data_block_buffers.size()) {
        assert(next_block_iter != nullptr);
        first_key_in_next_block = next_block_iter->key();
      } else {
        first_key_in_next_block_ptr = r->first_key_in_next_block;
      }

      std::vector<std::string> keys;
      for (; iter->Valid(); iter->Next()) {
        keys.emplace_back(iter->key().ToString());
      }

      ParallelCompressionRep::BlockRep* block_rep = r->pc_rep->PrepareBlock(
          r->compression_type, first_key_in_next_block_ptr, &data_block, &keys);

      assert(block_rep != nullptr);
      r->pc_rep->file_size_estimator.EmitBlock(block_rep->data->size(),
                                               r->get_offset());
      r->pc_rep->EmitBlock(block_rep);
    } else {
      for (; iter->Valid(); iter->Next()) {
        Slice key = iter->key();
        if (r->filter_builder != nullptr) {
          size_t ts_sz =
              r->internal_comparator.user_comparator()->timestamp_size();
          r->filter_builder->Add(ExtractUserKeyAndStripTimestamp(key, ts_sz));
        }
        r->index_builder->OnKeyAdded(key);
      }
      WriteBlock(Slice(data_block), &r->pending_handle,
                 true /* is_data_block */);
      if (ok() && i + 1 < r->data_block_buffers.size()) {
        assert(next_block_iter != nullptr);
        Slice first_key_in_next_block = next_block_iter->key();

        Slice* first_key_in_next_block_ptr = &first_key_in_next_block;

        iter->SeekToLast();
        std::string last_key = iter->key().ToString();
        r->index_builder->AddIndexEntry(&last_key, first_key_in_next_block_ptr,
                                        r->pending_handle);
      }
    }

    std::swap(iter, next_block_iter);
  }
  r->data_block_buffers.clear();
}

Status BlockBasedTableBuilder::Finish() {
  Rep* r = rep_;
  assert(r->state != Rep::State::kClosed);
  bool empty_data_block = r->data_block.empty();
  r->first_key_in_next_block = nullptr;
  Flush();
  if (r->state == Rep::State::kBuffered) {
    EnterUnbuffered();
  }
  if (r->IsParallelCompressionEnabled()) {
    StopParallelCompression();
#ifndef NDEBUG
    for (const auto& br : r->pc_rep->block_rep_buf) {
      assert(br.status.ok());
    }
#endif  // !NDEBUG
  } else {
    // To make sure properties block is able to keep the accurate size of index
    // block, we will finish writing all index entries first.
    if (ok() && !empty_data_block) {
      r->index_builder->AddIndexEntry(
          &r->last_key, nullptr /* no next data block */, r->pending_handle);
    }
  }

  // Write meta blocks, metaindex block and footer in the following order.
  //    1. [meta block: filter]
  //    2. [meta block: index]
  //    3. [meta block: compression dictionary]
  //    4. [meta block: range deletion tombstone]
  //    5. [meta block: properties]
  //    6. [metaindex block]
  //    7. Footer
  BlockHandle metaindex_block_handle, index_block_handle;
  MetaIndexBuilder meta_index_builder;
  WriteFilterBlock(&meta_index_builder);
  WriteIndexBlock(&meta_index_builder, &index_block_handle);
  WriteCompressionDictBlock(&meta_index_builder);
  WriteRangeDelBlock(&meta_index_builder);
  WritePropertiesBlock(&meta_index_builder);
  if (ok()) {
    // flush the meta index block
    WriteRawBlock(meta_index_builder.Finish(), kNoCompression,
                  &metaindex_block_handle);
  }
  if (ok()) {
    WriteFooter(metaindex_block_handle, index_block_handle);
  }
  r->state = Rep::State::kClosed;
  r->SetStatus(r->CopyIOStatus());
  Status ret_status = r->CopyStatus();
  assert(!ret_status.ok() || io_status().ok());
  return ret_status;
}

void BlockBasedTableBuilder::Abandon() {
  assert(rep_->state != Rep::State::kClosed);
  if (rep_->IsParallelCompressionEnabled()) {
    StopParallelCompression();
  }
  rep_->state = Rep::State::kClosed;
  rep_->CopyStatus().PermitUncheckedError();
  rep_->CopyIOStatus().PermitUncheckedError();
}

uint64_t BlockBasedTableBuilder::NumEntries() const {
  return rep_->props.num_entries;
}

bool BlockBasedTableBuilder::IsEmpty() const {
  return rep_->props.num_entries == 0 && rep_->props.num_range_deletions == 0;
}

uint64_t BlockBasedTableBuilder::FileSize() const { return rep_->offset; }

uint64_t BlockBasedTableBuilder::EstimatedFileSize() const {
  if (rep_->IsParallelCompressionEnabled()) {
    // Use compression ratio so far and inflight raw bytes to estimate
    // final SST size.
    return rep_->pc_rep->file_size_estimator.GetEstimatedFileSize();
  } else {
    return FileSize();
  }
}

bool BlockBasedTableBuilder::NeedCompact() const {
  for (const auto& collector : rep_->table_properties_collectors) {
    if (collector->NeedCompact()) {
      return true;
    }
  }
  return false;
}

TableProperties BlockBasedTableBuilder::GetTableProperties() const {
  TableProperties ret = rep_->props;
  for (const auto& collector : rep_->table_properties_collectors) {
    for (const auto& prop : collector->GetReadableProperties()) {
      ret.readable_properties.insert(prop);
    }
    collector->Finish(&ret.user_collected_properties).PermitUncheckedError();
  }
  return ret;
}

std::string BlockBasedTableBuilder::GetFileChecksum() const {
  if (rep_->file != nullptr) {
    return rep_->file->GetFileChecksum();
  } else {
    return kUnknownFileChecksum;
  }
}

const char* BlockBasedTableBuilder::GetFileChecksumFuncName() const {
  if (rep_->file != nullptr) {
    return rep_->file->GetFileChecksumFuncName();
  } else {
    return kUnknownFileChecksumFuncName;
  }
}

const std::string BlockBasedTable::kFilterBlockPrefix = "filter.";
const std::string BlockBasedTable::kFullFilterBlockPrefix = "fullfilter.";
const std::string BlockBasedTable::kPartitionedFilterBlockPrefix =
    "partitionedfilter.";
}  // namespace ROCKSDB_NAMESPACE
