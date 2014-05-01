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
#include <stdio.h>

#include <map>
#include <memory>

#include "db/dbformat.h"

#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

#include "table/block.h"
#include "table/block_based_table_reader.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/table_builder.h"

#include "util/coding.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"
#include "util/xxhash.h"

namespace rocksdb {

namespace {

typedef BlockBasedTableOptions::IndexType IndexType;

// The interface for building index.
// Instruction for adding a new concrete IndexBuilder:
//  1. Create a subclass instantiated from IndexBuilder.
//  2. Add a new entry associated with that subclass in TableOptions::IndexType.
//  3. Add a create function for the new subclass in CreateIndexBuilder.
// Note: we can devise more advanced design to simplify the process for adding
// new subclass, which will, on the other hand, increase the code complexity and
// catch unwanted attention from readers. Given that we won't add/change
// indexes frequently, it makes sense to just embrace a more straightforward
// design that just works.
class IndexBuilder {
 public:
  explicit IndexBuilder(const Comparator* comparator)
      : comparator_(comparator) {}

  virtual ~IndexBuilder() {}

  // Add a new index entry to index block.
  // To allow further optimization, we provide `last_key_in_current_block` and
  // `first_key_in_next_block`, based on which the specific implementation can
  // determine the best index key to be used for the index block.
  // @last_key_in_current_block: this parameter maybe overridden with the value
  //                             "substitute key".
  // @first_key_in_next_block: it will be nullptr if the entry being added is
  //                           the last one in the table
  //
  // REQUIRES: Finish() has not yet been called.
  virtual void AddEntry(std::string* last_key_in_current_block,
                        const Slice* first_key_in_next_block,
                        const BlockHandle& block_handle) = 0;

  // Inform the index builder that all entries has been written. Block builder
  // may therefore perform any operation required for block finalization.
  //
  // REQUIRES: Finish() has not yet been called.
  virtual Slice Finish() = 0;

  // Get the estimated size for index block.
  virtual size_t EstimatedSize() const = 0;

 protected:
  const Comparator* comparator_;
};

// This index builder builds space-efficient index block.
//
// Optimizations:
//  1. Made block's `block_restart_interval` to be 1, which will avoid linear
//     search when doing index lookup.
//  2. Shorten the key length for index block. Other than honestly using the
//     last key in the data block as the index key, we instead find a shortest
//     substitute key that serves the same function.
class ShortenedIndexBuilder : public IndexBuilder {
 public:
  explicit ShortenedIndexBuilder(const Comparator* comparator)
      : IndexBuilder(comparator),
        index_block_builder_(1 /* block_restart_interval == 1 */, comparator) {}

  virtual void AddEntry(std::string* last_key_in_current_block,
                        const Slice* first_key_in_next_block,
                        const BlockHandle& block_handle) override {
    if (first_key_in_next_block != nullptr) {
      comparator_->FindShortestSeparator(last_key_in_current_block,
                                         *first_key_in_next_block);
    } else {
      comparator_->FindShortSuccessor(last_key_in_current_block);
    }

    std::string handle_encoding;
    block_handle.EncodeTo(&handle_encoding);
    index_block_builder_.Add(*last_key_in_current_block, handle_encoding);
  }

  virtual Slice Finish() override { return index_block_builder_.Finish(); }

  virtual size_t EstimatedSize() const {
    return index_block_builder_.CurrentSizeEstimate();
  }

 private:
  BlockBuilder index_block_builder_;
};

// FullKeyIndexBuilder is also based on BlockBuilder. It works pretty much like
// ShortenedIndexBuilder, but preserves the full key instead the substitude key.
class FullKeyIndexBuilder : public IndexBuilder {
 public:
  explicit FullKeyIndexBuilder(const Comparator* comparator)
      : IndexBuilder(comparator),
        index_block_builder_(1 /* block_restart_interval == 1 */, comparator) {}

  virtual void AddEntry(std::string* last_key_in_current_block,
                        const Slice* first_key_in_next_block,
                        const BlockHandle& block_handle) override {
    std::string handle_encoding;
    block_handle.EncodeTo(&handle_encoding);
    index_block_builder_.Add(*last_key_in_current_block, handle_encoding);
  }

  virtual Slice Finish() override { return index_block_builder_.Finish(); }

  virtual size_t EstimatedSize() const {
    return index_block_builder_.CurrentSizeEstimate();
  }

 private:
  BlockBuilder index_block_builder_;
};

// Create a index builder based on its type.
IndexBuilder* CreateIndexBuilder(IndexType type, const Comparator* comparator) {
  switch (type) {
    case BlockBasedTableOptions::kBinarySearch: {
      return new ShortenedIndexBuilder(comparator);
    }
    default: {
      assert(!"Do not recognize the index type ");
      return nullptr;
    }
  }
  // impossible.
  assert(false);
  return nullptr;
}

bool GoodCompressionRatio(size_t compressed_size, size_t raw_size) {
  // Check to see if compressed less than 12.5%
  return compressed_size < raw_size - (raw_size / 8u);
}

Slice CompressBlock(const Slice& raw,
                    const CompressionOptions& compression_options,
                    CompressionType* type, std::string* compressed_output) {
  if (*type == kNoCompression) {
    return raw;
  }

  // Will return compressed block contents if (1) the compression method is
  // supported in this platform and (2) the compression rate is "good enough".
  switch (*type) {
    case kSnappyCompression:
      if (port::Snappy_Compress(compression_options, raw.data(), raw.size(),
                                compressed_output) &&
          GoodCompressionRatio(compressed_output->size(), raw.size())) {
        return *compressed_output;
      }
      break;  // fall back to no compression.
    case kZlibCompression:
      if (port::Zlib_Compress(compression_options, raw.data(), raw.size(),
                              compressed_output) &&
          GoodCompressionRatio(compressed_output->size(), raw.size())) {
        return *compressed_output;
      }
      break;  // fall back to no compression.
    case kBZip2Compression:
      if (port::BZip2_Compress(compression_options, raw.data(), raw.size(),
                               compressed_output) &&
          GoodCompressionRatio(compressed_output->size(), raw.size())) {
        return *compressed_output;
      }
      break;  // fall back to no compression.
    case kLZ4Compression:
      if (port::LZ4_Compress(compression_options, raw.data(), raw.size(),
                             compressed_output) &&
          GoodCompressionRatio(compressed_output->size(), raw.size())) {
        return *compressed_output;
      }
      break;  // fall back to no compression.
    case kLZ4HCCompression:
      if (port::LZ4HC_Compress(compression_options, raw.data(), raw.size(),
                               compressed_output) &&
          GoodCompressionRatio(compressed_output->size(), raw.size())) {
        return *compressed_output;
      }
      break;     // fall back to no compression.
    default: {}  // Do not recognize this compression type
  }

  // Compression method is not supported, or not good compression ratio, so just
  // fall back to uncompressed form.
  *type = kNoCompression;
  return raw;
}

}  // anonymous namespace

// kBlockBasedTableMagicNumber was picked by running
//    echo rocksdb.table.block_based | sha1sum
// and taking the leading 64 bits.
// Please note that kBlockBasedTableMagicNumber may also be accessed by
// other .cc files so it have to be explicitly declared with "extern".
extern const uint64_t kBlockBasedTableMagicNumber = 0x88e241b785f4cff7ull;
// We also support reading and writing legacy block based table format (for
// backwards compatibility)
extern const uint64_t kLegacyBlockBasedTableMagicNumber = 0xdb4775248b80fb57ull;

// A collector that collects properties of interest to block-based table.
// For now this class looks heavy-weight since we only write one additional
// property.
// But in the forseeable future, we will add more and more properties that are
// specific to block-based table.
class BlockBasedTableBuilder::BlockBasedTablePropertiesCollector
    : public TablePropertiesCollector {
 public:
  BlockBasedTablePropertiesCollector(
      BlockBasedTableOptions::IndexType index_type)
      : index_type_(index_type) {}

  virtual Status Add(const Slice& key, const Slice& value) {
    // Intentionally left blank. Have no interest in collecting stats for
    // individual key/value pairs.
    return Status::OK();
  }

  virtual Status Finish(UserCollectedProperties* properties) {
    std::string val;
    PutFixed32(&val, static_cast<uint32_t>(index_type_));
    properties->insert({BlockBasedTablePropertyNames::kIndexType, val});

    return Status::OK();
  }

  // The name of the properties collector can be used for debugging purpose.
  virtual const char* Name() const {
    return "BlockBasedTablePropertiesCollector";
  }

  virtual UserCollectedProperties GetReadableProperties() const {
    // Intentionally left blank.
    return UserCollectedProperties();
  }

 private:
  BlockBasedTableOptions::IndexType index_type_;
};

struct BlockBasedTableBuilder::Rep {
  Options options;
  const InternalKeyComparator& internal_comparator;
  WritableFile* file;
  uint64_t offset = 0;
  Status status;
  BlockBuilder data_block;
  std::unique_ptr<IndexBuilder> index_builder;

  std::string last_key;
  CompressionType compression_type;
  ChecksumType checksum_type;
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
      CompressionType compression_type, IndexType index_block_type,
      ChecksumType checksum_type)
      : options(opt),
        internal_comparator(icomparator),
        file(f),
        data_block(options, &internal_comparator),
        index_builder(
            CreateIndexBuilder(index_block_type, &internal_comparator)),
        compression_type(compression_type),
        checksum_type(checksum_type),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt, &internal_comparator)),
        flush_block_policy(flush_block_policy_factory->NewFlushBlockPolicy(
            options, data_block)) {
    options.table_properties_collectors.push_back(
        std::make_shared<BlockBasedTablePropertiesCollector>(index_block_type));
  }
};

// TODO(sdong): Currently only write out binary search index. In
// BlockBasedTableReader, Hash index will be built using binary search index.
BlockBasedTableBuilder::BlockBasedTableBuilder(
    const Options& options, const BlockBasedTableOptions& table_options,
    const InternalKeyComparator& internal_comparator, WritableFile* file,
    CompressionType compression_type)
    : rep_(new Rep(options, internal_comparator, file,
                   table_options.flush_block_policy_factory.get(),
                   compression_type,
                   BlockBasedTableOptions::IndexType::kBinarySearch,
                   table_options.checksum)) {
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
      r->index_builder->AddEntry(&r->last_key, &key, r->pending_handle);
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
  WriteBlock(block->Finish(), handle);
  block->Reset();
}

void BlockBasedTableBuilder::WriteBlock(const Slice& raw_block_contents,
                                        BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;

  auto type = r->compression_type;
  auto block_contents =
      CompressBlock(raw_block_contents, r->options.compression_opts, &type,
                    &r->compressed_output);
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
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
    char* trailer_without_type = trailer + 1;
    switch (r->checksum_type) {
      case kNoChecksum:
        // we don't support no checksum yet
        assert(false);
        // intentional fallthrough in release binary
      case kCRC32c: {
        auto crc = crc32c::Value(block_contents.data(), block_contents.size());
        crc = crc32c::Extend(crc, trailer, 1);  // Extend to cover block type
        EncodeFixed32(trailer_without_type, crc32c::Mask(crc));
        break;
      }
      case kxxHash: {
        void* xxh = XXH32_init(0);
        XXH32_update(xxh, block_contents.data(), block_contents.size());
        XXH32_update(xxh, trailer, 1);  // Extend  to cover block type
        EncodeFixed32(trailer_without_type, XXH32_digest(xxh));
        break;
      }
    }

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
    r->index_builder->AddEntry(&r->last_key, nullptr /* no next data block */,
                               r->pending_handle);
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
          r->index_builder->EstimatedSize() + kBlockTrailerSize;

      // Add basic properties
      property_block_builder.AddTableProperty(r->props);

      // Add use collected properties
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
    WriteBlock(r->index_builder->Finish(), &index_block_handle);
  }

  // Write footer
  if (ok()) {
    // No need to write out new footer if we're using default checksum.
    // We're writing legacy magic number because we want old versions of RocksDB
    // be able to read files generated with new release (just in case if
    // somebody wants to roll back after an upgrade)
    // TODO(icanadi) at some point in the future, when we're absolutely sure
    // nobody will roll back to RocksDB 2.x versions, retire the legacy magic
    // number and always write new table files with new magic number
    bool legacy = (r->checksum_type == kCRC32c);
    Footer footer(legacy ? kLegacyBlockBasedTableMagicNumber
                         : kBlockBasedTableMagicNumber);
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    footer.set_checksum(r->checksum_type);
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
