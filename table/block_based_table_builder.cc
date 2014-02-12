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
#include <inttypes.h>
#include <map>
#include <stdio.h>

#include "rocksdb/flush_block_policy.h"
#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "table/table_builder.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "db/dbformat.h"
#include "table/block_based_table_reader.h"
#include "table/block.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"

namespace rocksdb {

namespace {

static bool GoodCompressionRatio(size_t compressed_size, size_t raw_size) {
  // Check to see if compressed less than 12.5%
  return compressed_size < raw_size - (raw_size / 8u);
}

}  // anonymous namespace

// kBlockBasedTableMagicNumber was picked by running
//    echo http://code.google.com/p/leveldb/ | sha1sum
// and taking the leading 64 bits.
// Please note that kBlockBasedTableMagicNumber may also be accessed by
// other .cc files so it have to be explicitly declared with "extern".
extern const uint64_t kBlockBasedTableMagicNumber
    = 0xdb4775248b80fb57ull;

struct BlockBasedTableBuilder::Rep {
  Options options;
  const InternalKeyComparator& internal_comparator;
  WritableFile* file;
  uint64_t offset = 0;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  CompressionType compression_type;
  TableProperties props;

  bool closed = false;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;
  char compressed_cache_key_prefix[BlockBasedTable::kMaxCacheKeyPrefixSize];
  size_t compressed_cache_key_prefix_size;

  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
  std::unique_ptr<FlushBlockPolicy> flush_block_policy;

  Rep(const Options& opt, const InternalKeyComparator& icomparator,
      WritableFile* f, FlushBlockPolicyFactory* flush_block_policy_factory,
      CompressionType compression_type)
      : options(opt),
        internal_comparator(icomparator),
        file(f),
        data_block(options, &internal_comparator),
        // To avoid linear scan, we make the block_restart_interval to be `1`
        // in index block builder
        index_block(1 /* block_restart_interval */, &internal_comparator),
        compression_type(compression_type),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt, &internal_comparator)),
        flush_block_policy(
            flush_block_policy_factory->NewFlushBlockPolicy(data_block)) {}
};

BlockBasedTableBuilder::BlockBasedTableBuilder(
    const Options& options, const InternalKeyComparator& internal_comparator,
    WritableFile* file, FlushBlockPolicyFactory* flush_block_policy_factory,
    CompressionType compression_type)
    : rep_(new Rep(options, internal_comparator, file,
                   flush_block_policy_factory, compression_type)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
  if (options.block_cache_compressed.get() != nullptr) {
    BlockBasedTable::GenerateCachePrefix(
        options.block_cache_compressed.get(), file,
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
  if (r->props.num_entries > 0) {
    assert(r->internal_comparator.Compare(key, Slice(r->last_key)) > 0);
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
      r->internal_comparator.FindShortestSeparator(&r->last_key, key);
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
  r->props.num_entries++;
  r->props.raw_key_size += key.size();
  r->props.raw_value_size += value.size();

  NotifyCollectTableCollectorsOnAdd(
      key,
      value,
      r->options.table_properties_collectors,
      r->options.info_log.get()
  );
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
  r->props.data_size = r->offset;
  ++r->props.num_data_blocks;
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
    case kLZ4Compression:
      if (port::LZ4_Compress(r->options.compression_opts, raw.data(),
                             raw.size(), compressed) &&
          GoodCompressionRatio(compressed->size(), raw.size())) {
        block_contents = *compressed;
      } else {
        // LZ4 not supported, or not good compression ratio, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    case kLZ4HCCompression:
      if (port::LZ4HC_Compress(r->options.compression_opts, raw.data(),
                               raw.size(), compressed) &&
          GoodCompressionRatio(compressed->size(), raw.size())) {
        block_contents = *compressed;
      } else {
        // LZ4 not supported, or not good compression ratio, so just
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
  StopWatch sw(r->options.env, r->options.statistics.get(),
               WRITE_RAW_BLOCK_MICROS);
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
    auto filter_contents = r->filter_block->Finish();
    r->props.filter_size = filter_contents.size();
    WriteRawBlock(filter_contents, kNoCompression, &filter_block_handle);
  }

  // To make sure properties block is able to keep the accurate size of index
  // block, we will finish writing all index entries here and flush them
  // to storage after metaindex block is written.
  if (ok() && !empty_data_block) {
    r->internal_comparator.FindShortSuccessor(&r->last_key);

    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    r->index_block.Add(r->last_key, handle_encoding);
  }

  // Write meta blocks and metaindex block with the following order.
  //    1. [meta block: filter]
  //    2. [meta block: properties]
  //    3. [metaindex block]
  if (ok()) {
    MetaIndexBuilder meta_index_builer;

    // Write filter block.
    if (r->filter_block != nullptr) {
      // Add mapping from "<filter_block_prefix>.Name" to location
      // of filter data.
      std::string key = BlockBasedTable::kFilterBlockPrefix;
      key.append(r->options.filter_policy->Name());
      meta_index_builer.Add(key, filter_block_handle);
    }

    // Write properties block.
    {
      PropertyBlockBuilder property_block_builder;
      std::vector<std::string> failed_user_prop_collectors;
      r->props.filter_policy_name = r->options.filter_policy != nullptr ?
          r->options.filter_policy->Name() : "";
      r->props.index_size =
        r->index_block.CurrentSizeEstimate() + kBlockTrailerSize;

      // Add basic properties
      property_block_builder.AddTableProperty(r->props);

      NotifyCollectTableCollectorsOnFinish(
          r->options.table_properties_collectors,
          r->options.info_log.get(),
          &property_block_builder
      );

      BlockHandle properties_block_handle;
      WriteRawBlock(
          property_block_builder.Finish(),
          kNoCompression,
          &properties_block_handle
      );

      meta_index_builer.Add(kPropertiesBlock,
                            properties_block_handle);
    }  // end of properties block writing

    WriteRawBlock(
        meta_index_builer.Finish(),
        kNoCompression,
        &metaindex_block_handle
    );
  }  // meta blocks and metaindex block.

  // Write index block
  if (ok()) {
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer(kBlockBasedTableMagicNumber);
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }

  // Print out the table stats
  if (ok()) {
    // user collected properties
    std::string user_collected;
    user_collected.reserve(1024);
    for (auto collector : r->options.table_properties_collectors) {
      for (const auto& prop : collector->GetReadableProperties()) {
        user_collected.append(prop.first);
        user_collected.append("=");
        user_collected.append(prop.second);
        user_collected.append("; ");
      }
    }

    Log(
        r->options.info_log,
        "Table was constructed:\n"
        "  [basic properties]: %s\n"
        "  [user collected properties]: %s",
        r->props.ToString().c_str(),
        user_collected.c_str()
    );
  }

  return r->status;
}

void BlockBasedTableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t BlockBasedTableBuilder::NumEntries() const {
  return rep_->props.num_entries;
}

uint64_t BlockBasedTableBuilder::FileSize() const {
  return rep_->offset;
}

const std::string BlockBasedTable::kFilterBlockPrefix =
    "filter.";

}  // namespace rocksdb
