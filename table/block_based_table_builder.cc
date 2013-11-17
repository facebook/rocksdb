//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/block_based_table_builder.h"

#include <assert.h>
#include <map>

#include "rocksdb/flush_block_policy.h"
#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/table.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "table/block_based_table_reader.h"
#include "table/block.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"

namespace rocksdb {

namespace {

struct BytewiseLessThan {
  bool operator()(const std::string& key1, const std::string& key2) const {
    // smaller entries will be placed in front.
    return comparator->Compare(key1, key2) <= 0;
  }
  const Comparator* comparator = BytewiseComparator();
};

// When writing to a block that requires entries to be sorted by
// `BytewiseComparator`, we can buffer the content to `BytewiseSortedMap`
// before writng to store.
typedef std::map<std::string, std::string, BytewiseLessThan> BytewiseSortedMap;

void AddStats(BytewiseSortedMap& stats, std::string name, uint64_t val) {
  assert(stats.find(name) == stats.end());

  std::string dst;
  PutVarint64(&dst, val);

  stats.insert(
      std::make_pair(name, dst)
  );
}

static bool GoodCompressionRatio(size_t compressed_size, size_t raw_size) {
  // Check to see if compressed less than 12.5%
  return compressed_size < raw_size - (raw_size / 8u);
}

// Were we encounter any error occurs during user-defined statistics collection,
// we'll write the warning message to info log.
void LogStatsCollectionError(
    Logger* info_log, const std::string& method, const std::string& name) {
  assert(method == "Add" || method == "Finish");

  std::string msg =
    "[Warning] encountered error when calling TableStatsCollector::" +
    method + "() with collector name: " + name;
  Log(info_log, "%s", msg.c_str());
}

}  // anonymous namespace

struct BlockBasedTableBuilder::Rep {
  Options options;
  WritableFile* file;
  uint64_t offset = 0;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  CompressionType compression_type;

  uint64_t num_entries = 0;
  uint64_t num_data_blocks = 0;
  uint64_t raw_key_size = 0;
  uint64_t raw_value_size = 0;
  uint64_t data_size = 0;

  bool closed = false;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;
  char compressed_cache_key_prefix[BlockBasedTable::kMaxCacheKeyPrefixSize];
  size_t compressed_cache_key_prefix_size;


  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
  std::unique_ptr<FlushBlockPolicy> flush_block_policy;

  Rep(const Options& opt, WritableFile* f, CompressionType compression_type)
      : options(opt),
        file(f),
        data_block(options),
        // To avoid linear scan, we make the block_restart_interval to be `1`
        // in index block builder
        index_block(1 /* block_restart_interval */, options.comparator),
        compression_type(compression_type),
        filter_block(opt.filter_policy == nullptr ? nullptr
                     : new FilterBlockBuilder(opt)) {
    assert(options.flush_block_policy_factory);
    auto factory = options.flush_block_policy_factory;
    flush_block_policy.reset(factory->NewFlushBlockPolicy(data_block));
  }
};

BlockBasedTableBuilder::BlockBasedTableBuilder(const Options& options,
                                               WritableFile* file,
                                               CompressionType compression_type)
    : rep_(new Rep(options, file, compression_type)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
  if (options.block_cache_compressed.get() != nullptr) {
    BlockBasedTable::GenerateCachePrefix(options.block_cache_compressed, file,
                               &rep_->compressed_cache_key_prefix[0],
                               &rep_->compressed_cache_key_prefix_size);
  }
}

BlockBasedTableBuilder::~BlockBasedTableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

void BlockBasedTableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  auto should_flush = r->flush_block_policy->Update(key, value);
  if (should_flush) {
    assert(!r->data_block.empty());
    Flush();

    // Add item to index block.
    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block.  This allows us to use shorter
    // keys in the index block.  For example, consider a block boundary
    // between the keys "the quick brown fox" and "the who".  We can use
    // "the r" as the key for the index block entry since it is >= all
    // entries in the first block and < all entries in subsequent
    // blocks.
    if (ok()) {
      r->options.comparator->FindShortestSeparator(&r->last_key, key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
    }
  }

  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  r->last_key.assign(key.data(), key.size());
  r->data_block.Add(key, value);
  r->num_entries++;
  r->raw_key_size += key.size();
  r->raw_value_size += value.size();

  for (auto collector : r->options.table_stats_collectors) {
    Status s = collector->Add(key, value);
    if (!s.ok()) {
      LogStatsCollectionError(
          r->options.info_log.get(),
          "Add", /* method */
          collector->Name()
      );
    }
  }
}

void BlockBasedTableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
  r->data_size = r->offset;
  ++r->num_data_blocks;
}

void BlockBasedTableBuilder::WriteBlock(BlockBuilder* block,
                                        BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  std::string* compressed = &r->compressed_output;
  CompressionType type = r->compression_type;
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(r->options.compression_opts, raw.data(),
                                raw.size(), compressed) &&
          GoodCompressionRatio(compressed->size(), raw.size())) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or not good compression ratio, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
    case kZlibCompression:
      if (port::Zlib_Compress(r->options.compression_opts, raw.data(),
                              raw.size(), compressed) &&
          GoodCompressionRatio(compressed->size(), raw.size())) {
        block_contents = *compressed;
      } else {
        // Zlib not supported, or not good compression ratio, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    case kBZip2Compression:
      if (port::BZip2_Compress(r->options.compression_opts, raw.data(),
                               raw.size(), compressed) &&
          GoodCompressionRatio(compressed->size(), raw.size())) {
        block_contents = *compressed;
      } else {
        // BZip not supported, or not good compression ratio, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

void BlockBasedTableBuilder::WriteRawBlock(const Slice& block_contents,
                                           CompressionType type,
                                           BlockHandle* handle) {
  Rep* r = rep_;
  StopWatch sw(r->options.env, r->options.statistics, WRITE_RAW_BLOCK_MICROS);
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer+1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->status = InsertBlockInCache(block_contents, type, handle);
    }
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status BlockBasedTableBuilder::status() const {
  return rep_->status;
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

//
// Make a copy of the block contents and insert into compressed block cache
//
Status BlockBasedTableBuilder::InsertBlockInCache(const Slice& block_contents,
                                 const CompressionType type,
                                 const BlockHandle* handle) {
  Rep* r = rep_;
  Cache* block_cache_compressed = r->options.block_cache_compressed.get();

  if (type != kNoCompression && block_cache_compressed != nullptr) {

    Cache::Handle* cache_handle = nullptr;
    size_t size = block_contents.size();

    char* ubuf = new char[size];             // make a new copy
    memcpy(ubuf, block_contents.data(), size);

    BlockContents results;
    Slice sl(ubuf, size);
    results.data = sl;
    results.cachable = true; // XXX
    results.heap_allocated = true;
    results.compression_type = type;

    Block* block = new Block(results);

    // make cache key by appending the file offset to the cache prefix id
    char* end = EncodeVarint64(
                  r->compressed_cache_key_prefix +
                  r->compressed_cache_key_prefix_size,
                  handle->offset());
    Slice key(r->compressed_cache_key_prefix, static_cast<size_t>
              (end - r->compressed_cache_key_prefix));

    // Insert into compressed block cache.
    cache_handle = block_cache_compressed->Insert(key, block, block->size(),
                                                  &DeleteCachedBlock);
    block_cache_compressed->Release(cache_handle);

    // Invalidate OS cache.
    r->file->InvalidateCache(r->offset, size);
  }
  return Status::OK();
}

Status BlockBasedTableBuilder::Finish() {
  Rep* r = rep_;
  bool empty_data_block = r->data_block.empty();
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle,
              metaindex_block_handle,
              index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // To make sure stats block is able to keep the accurate size of index
  // block, we will finish writing all index entries here and flush them
  // to storage after metaindex block is written.
  if (ok() && !empty_data_block) {
    r->options.comparator->FindShortSuccessor(&r->last_key);

    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    r->index_block.Add(r->last_key, handle_encoding);
  }

  // Write meta blocks and metaindex block with the following order.
  //    1. [meta block: filter]
  //    2. [meta block: stats]
  //    3. [metaindex block]
  if (ok()) {
    // We use `BytewiseComparator` as the comparator for meta block.
    BlockBuilder meta_index_block(
        r->options.block_restart_interval,
        BytewiseComparator()
    );
    // Key: meta block name
    // Value: block handle to that meta block
    BytewiseSortedMap meta_block_handles;

    // Write filter block.
    if (r->filter_block != nullptr) {
      // Add mapping from "<filter_block_prefix>.Name" to location
      // of filter data.
      std::string key = BlockBasedTable::kFilterBlockPrefix;
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_block_handles.insert(
          std::make_pair(key, handle_encoding)
      );
    }

    // Write stats block.
    {
      BlockBuilder stats_block(
          r->options.block_restart_interval,
          BytewiseComparator()
      );

      BytewiseSortedMap stats;

      // Add basic stats
      AddStats(stats, BlockBasedTableStatsNames::kRawKeySize, r->raw_key_size);
      AddStats(stats, BlockBasedTableStatsNames::kRawValueSize,
               r->raw_value_size);
      AddStats(stats, BlockBasedTableStatsNames::kDataSize, r->data_size);
      AddStats(
          stats,
          BlockBasedTableStatsNames::kIndexSize,
          r->index_block.CurrentSizeEstimate() + kBlockTrailerSize
      );
      AddStats(stats, BlockBasedTableStatsNames::kNumEntries, r->num_entries);
      AddStats(stats, BlockBasedTableStatsNames::kNumDataBlocks,
               r->num_data_blocks);
      if (r->filter_block != nullptr) {
        stats.insert(std::make_pair(
              BlockBasedTableStatsNames::kFilterPolicy,
              r->options.filter_policy->Name()
        ));
      }

      for (auto collector : r->options.table_stats_collectors) {
        TableStats::UserCollectedStats user_collected_stats;
        Status s =
          collector->Finish(&user_collected_stats);

        if (!s.ok()) {
          LogStatsCollectionError(
              r->options.info_log.get(),
              "Finish", /* method */
              collector->Name()
          );
        } else {
          stats.insert(
              user_collected_stats.begin(),
              user_collected_stats.end()
          );
        }
      }

      for (const auto& stat : stats) {
        stats_block.Add(stat.first, stat.second);
      }

      BlockHandle stats_block_handle;
      WriteBlock(&stats_block, &stats_block_handle);

      std::string handle_encoding;
      stats_block_handle.EncodeTo(&handle_encoding);
      meta_block_handles.insert(
          std::make_pair(BlockBasedTable::kStatsBlock, handle_encoding)
      );
    }  // end of stats block writing

    for (const auto& metablock : meta_block_handles) {
      meta_index_block.Add(metablock.first, metablock.second);
    }

    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }  // meta blocks and metaindex block.

  // Write index block
  if (ok()) {
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void BlockBasedTableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t BlockBasedTableBuilder::NumEntries() const {
  return rep_->num_entries;
}

uint64_t BlockBasedTableBuilder::FileSize() const {
  return rep_->offset;
}

}  // namespace rocksdb
