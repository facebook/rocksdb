//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "table/block_based_table_reader.h"

#include <algorithm>
#include <limits>
#include <string>
#include <utility>
#include <vector>


#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"

#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"

#include "table/block.h"
#include "table/block_based_filter_block.h"
#include "table/block_based_table_factory.h"
#include "table/block_based_table_request.h"
#include "table/block_prefix_index.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/full_filter_block.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
#include "table/partitioned_filter_block.h"
#include "table/persistent_cache_helper.h"
#include "table/sst_file_writer_collectors.h"
#include "table/two_level_iterator.h"

#include "monitoring/perf_context_imp.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/random_read_context.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/sync_point.h"

namespace rocksdb {

extern const uint64_t kBlockBasedTableMagicNumber;
extern const std::string kHashIndexPrefixesBlock;
extern const std::string kHashIndexPrefixesMetadataBlock;
using std::unique_ptr;

typedef BlockBasedTable::IndexReader IndexReader;

BlockBasedTable::~BlockBasedTable() {
  Close();
  delete rep_;
}

namespace {
// Read the block identified by "handle" from "file".
// The only relevant option is options.verify_checksums for now.
// On failure return non-OK.
// On success fill *result and return OK - caller owns *result
// @param compression_dict Data for presetting the compression library's
//    dictionary.
Status ReadBlockFromFile(RandomAccessFileReader* file, const Footer& footer,
                         const ReadOptions& options, const BlockHandle& handle,
                         std::unique_ptr<Block>* result,
                         const ImmutableCFOptions& ioptions, bool do_uncompress,
                         const Slice& compression_dict,
                         const PersistentCacheOptions& cache_options,
                         SequenceNumber global_seqno,
                         size_t read_amp_bytes_per_bit) {
  BlockContents contents;
  Status s = ReadBlockContents(file, footer, options, handle, &contents, ioptions,
                               do_uncompress, compression_dict, cache_options);
  if (s.ok()) {
    result->reset(new Block(std::move(contents), global_seqno,
                            read_amp_bytes_per_bit, ioptions.statistics));
  }

  return s;
}

// Delete the entry resided in the cache.
template <class Entry>
void DeleteCachedEntry(const Slice& key, void* value) {
  auto entry = reinterpret_cast<Entry*>(value);
  delete entry;
}
}  // namespace

   // Release the cached entry and decrement its ref count.
void BlockBasedTable::ReleaseCachedEntry(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

Slice BlockBasedTable::GetCacheKeyFromOffset(const char* cache_key_prefix,
                            size_t cache_key_prefix_size, uint64_t offset,
                            char* cache_key) {
  assert(cache_key != nullptr);
  assert(cache_key_prefix_size != 0);
  assert(cache_key_prefix_size <= BlockBasedTable::kMaxCacheKeyPrefixSize);
  memcpy(cache_key, cache_key_prefix, cache_key_prefix_size);
  char* end = EncodeVarint64(cache_key + cache_key_prefix_size, offset);
  return Slice(cache_key, static_cast<size_t>(end - cache_key));
}

Cache::Handle* BlockBasedTable::GetEntryFromCache(Cache* block_cache, const Slice& key,
                                 Tickers block_cache_miss_ticker,
                                 Tickers block_cache_hit_ticker,
                                 Statistics* statistics) {
  auto cache_handle = block_cache->Lookup(key, statistics);
  if (cache_handle != nullptr) {
    PERF_COUNTER_ADD(block_cache_hit_count, 1);
    // overall cache hit
    RecordTick(statistics, BLOCK_CACHE_HIT);
    // total bytes read from cache
    RecordTick(statistics, BLOCK_CACHE_BYTES_READ,
               block_cache->GetUsage(cache_handle));
    // block-type specific cache hit
    RecordTick(statistics, block_cache_hit_ticker);
  } else {
    // overall cache miss
    RecordTick(statistics, BLOCK_CACHE_MISS);
    // block-type specific cache miss
    RecordTick(statistics, block_cache_miss_ticker);
  }

  return cache_handle;
}

PartitionIndexReader::~PartitionIndexReader() {
}

Status PartitionIndexReader::Create(BlockBasedTable* table, RandomAccessFileReader* file,
  const Footer& footer, const BlockHandle& index_handle,
  const ImmutableCFOptions& ioptions,
  const Comparator* comparator, IndexReader** index_reader,
  const PersistentCacheOptions& cache_options,
  const int level) {
  std::unique_ptr<Block> index_block;
  auto s = ReadBlockFromFile(
    file, footer, ReadOptions(), index_handle, &index_block, ioptions,
    true /* decompress */, Slice() /*compression dict*/, cache_options,
    kDisableGlobalSequenceNumber, 0 /* read_amp_bytes_per_bit */);

    if (s.ok()) {
      *index_reader =
          new PartitionIndexReader(table, comparator, std::move(index_block),
                                   ioptions.statistics, level);
    }

    return s;
  }


// return a two-level iterator: first level is on the partition index
InternalIterator* PartitionIndexReader::NewIterator(BlockIter* iter,
                                                    bool dont_care ) {
  // Filters are already checked before seeking the index
  const bool skip_filters = true;
  const bool is_index = true;
  Cleanable* block_cache_cleaner = nullptr;
  const bool pin_cached_indexes =
    level_ == 0 &&
    table_->rep_->table_options.pin_l0_filter_and_index_blocks_in_cache;
  if (pin_cached_indexes) {
    // Keep partition indexes into the cache as long as the partition index
    // reader object is alive
    block_cache_cleaner = this;
  }
  return NewTwoLevelIterator(
    new BlockBasedTable::BlockEntryIteratorState(
      table_, ReadOptions(), skip_filters, is_index, block_cache_cleaner),
    index_block_->NewIterator(comparator_, nullptr, true));
  // TODO(myabandeh): Update TwoLevelIterator to be able to make use of
  // on-stack
  // BlockIter while the state is on heap
}

size_t PartitionIndexReader::size() const { return index_block_->size(); }

size_t PartitionIndexReader::usable_size() const {
  return index_block_->usable_size();
}

size_t PartitionIndexReader::ApproximateMemoryUsage() const {
  assert(index_block_);
  return index_block_->ApproximateMemoryUsage();
}

BinarySearchIndexReader::~BinarySearchIndexReader() {
}

Status BinarySearchIndexReader::Create(RandomAccessFileReader* file, const Footer& footer,
  const BlockHandle& index_handle,
  const ImmutableCFOptions &ioptions,
  const Comparator* comparator, IndexReader** index_reader,
  const PersistentCacheOptions& cache_options) {
  std::unique_ptr<Block> index_block;
  auto s = ReadBlockFromFile(
    file, footer, ReadOptions(), index_handle, &index_block, ioptions,
    true /* decompress */, Slice() /*compression dict*/, cache_options,
    kDisableGlobalSequenceNumber, 0 /* read_amp_bytes_per_bit */);

    if (s.ok()) {
      *index_reader = new BinarySearchIndexReader(
          comparator, std::move(index_block), ioptions.statistics);
    }

    return s;
  }

InternalIterator* BinarySearchIndexReader::NewIterator(BlockIter* iter,
                                                      bool dont_care) {
  return index_block_->NewIterator(comparator_, iter, true);
}

size_t BinarySearchIndexReader::size() const { 
  return index_block_->size();
}

size_t BinarySearchIndexReader::usable_size() const {
  return index_block_->usable_size();
}

size_t BinarySearchIndexReader::ApproximateMemoryUsage() const {
  assert(index_block_);
  return index_block_->ApproximateMemoryUsage();
}

HashIndexReader::~HashIndexReader() {
}

void HashIndexReader::SetBlockPrefixIndex(BlockPrefixIndex* prefix_index) {
  index_block_->SetBlockPrefixIndex(prefix_index);
}


InternalIterator* HashIndexReader::NewIterator(BlockIter* iter,
                                                bool total_order_seek) {
  return index_block_->NewIterator(comparator_, iter, total_order_seek);
}

size_t HashIndexReader::size() const { return index_block_->size(); }

size_t HashIndexReader::usable_size() const {
  return index_block_->usable_size();
}

size_t HashIndexReader::ApproximateMemoryUsage() const {
  assert(index_block_);
  return index_block_->ApproximateMemoryUsage() +
    prefixes_contents_.data.size();
}

Status HashIndexReader::Create(const SliceTransform* hash_key_extractor,
                       const Footer& footer, RandomAccessFileReader* file,
                       const ImmutableCFOptions& ioptions,
                       const Comparator* comparator,
                       const BlockHandle& index_handle,
                       InternalIterator* meta_index_iter,
                       IndexReader** index_reader,
                       bool hash_index_allow_collision,
                       const PersistentCacheOptions& cache_options) {

    std::unique_ptr<Block> index_block;
    auto s = ReadBlockFromFile(
        file, footer, ReadOptions(), index_handle, &index_block, ioptions,
        true /* decompress */, Slice() /*compression dict*/, cache_options,
        kDisableGlobalSequenceNumber, 0 /* read_amp_bytes_per_bit */);

    if (!s.ok()) {
      return s;
    }

    // Note, failure to create prefix hash index does not need to be a
    // hard error. We can still fall back to the original binary search index.
    // So, Create will succeed regardless, from this point on.

    auto new_index_reader =
        new HashIndexReader(comparator, std::move(index_block),
          ioptions.statistics);
    *index_reader = new_index_reader;

    // Get prefixes block
    BlockHandle prefixes_handle;
    s = FindMetaBlock(meta_index_iter, kHashIndexPrefixesBlock,
                      &prefixes_handle);
    if (!s.ok()) {
      // TODO: log error
      return Status::OK();
    }

    // Get index metadata block
    BlockHandle prefixes_meta_handle;
    s = FindMetaBlock(meta_index_iter, kHashIndexPrefixesMetadataBlock,
                      &prefixes_meta_handle);
    if (!s.ok()) {
      // TODO: log error
      return Status::OK();
    }

    // Read contents for the blocks
    BlockContents prefixes_contents;
    s = ReadBlockContents(file, footer, ReadOptions(), prefixes_handle,
                          &prefixes_contents, ioptions, true /* decompress */,
                          Slice() /*compression dict*/, cache_options);
    if (!s.ok()) {
      return s;
    }
    BlockContents prefixes_meta_contents;
    s = ReadBlockContents(file, footer, ReadOptions(), prefixes_meta_handle,
                          &prefixes_meta_contents, ioptions, true /* decompress */,
                          Slice() /*compression dict*/, cache_options);
    if (!s.ok()) {
      // TODO: log error
      return Status::OK();
    }

    BlockPrefixIndex* prefix_index = nullptr;
    s = BlockPrefixIndex::Create(hash_key_extractor, prefixes_contents.data,
                                 prefixes_meta_contents.data, &prefix_index);
    // TODO: log error
    if (s.ok()) {
      new_index_reader->index_block_->SetBlockPrefixIndex(prefix_index);
    }

    return Status::OK();
  }

// Helper function to setup the cache key's prefix for the Table.
void BlockBasedTable::SetupCacheKeyPrefix(Rep* rep, uint64_t file_size) {
  assert(kMaxCacheKeyPrefixSize >= 10);
  rep->cache_key_prefix_size = 0;
  rep->compressed_cache_key_prefix_size = 0;
  if (rep->table_options.block_cache != nullptr) {
    GenerateCachePrefix(rep->table_options.block_cache.get(), rep->file->file(),
                        &rep->cache_key_prefix[0], &rep->cache_key_prefix_size);
    // Create dummy offset of index reader which is beyond the file size.
    rep->dummy_index_reader_offset =
        file_size + rep->table_options.block_cache->NewId();
  }
  if (rep->table_options.persistent_cache != nullptr) {
    GenerateCachePrefix(/*cache=*/nullptr, rep->file->file(),
                        &rep->persistent_cache_key_prefix[0],
                        &rep->persistent_cache_key_prefix_size);
  }
  if (rep->table_options.block_cache_compressed != nullptr) {
    GenerateCachePrefix(rep->table_options.block_cache_compressed.get(),
                        rep->file->file(), &rep->compressed_cache_key_prefix[0],
                        &rep->compressed_cache_key_prefix_size);
  }
}

void BlockBasedTable::GenerateCachePrefix(Cache* cc,
    RandomAccessFile* file, char* buffer, size_t* size) {

  // generate an id from the file
  *size = file->GetUniqueId(buffer, kMaxCacheKeyPrefixSize);

  // If the prefix wasn't generated or was too long,
  // create one from the cache.
  if (cc && *size == 0) {
    char* end = EncodeVarint64(buffer, cc->NewId());
    *size = static_cast<size_t>(end - buffer);
  }
}

void BlockBasedTable::GenerateCachePrefix(Cache* cc,
    WritableFile* file, char* buffer, size_t* size) {

  // generate an id from the file
  *size = file->GetUniqueId(buffer, kMaxCacheKeyPrefixSize);

  // If the prefix wasn't generated or was too long,
  // create one from the cache.
  if (*size == 0) {
    char* end = EncodeVarint64(buffer, cc->NewId());
    *size = static_cast<size_t>(end - buffer);
  }
}


// Return True if table_properties has `user_prop_name` has a `true` value
// or it doesn't contain this property (for backward compatible).
bool BlockBasedTable::IsFeatureSupported(const TableProperties& table_properties,
                        const std::string& user_prop_name, Logger* info_log) {
  auto& props = table_properties.user_collected_properties;
  auto pos = props.find(user_prop_name);
  // Older version doesn't have this value set. Skip this check.
  if (pos != props.end()) {
    if (pos->second == kPropFalse) {
      return false;
    } else if (pos->second != kPropTrue) {
      ROCKS_LOG_WARN(info_log, "Property %s has invalidate value %s",
                     user_prop_name.c_str(), pos->second.c_str());
    }
  }
  return true;
}

SequenceNumber BlockBasedTable::GetGlobalSequenceNumber(const TableProperties& table_properties,
                                       Logger* info_log) {
  auto& props = table_properties.user_collected_properties;

  auto version_pos = props.find(ExternalSstFilePropertyNames::kVersion);
  auto seqno_pos = props.find(ExternalSstFilePropertyNames::kGlobalSeqno);

  if (version_pos == props.end()) {
    if (seqno_pos != props.end()) {
      // This is not an external sst file, global_seqno is not supported.
      assert(false);
      ROCKS_LOG_ERROR(
          info_log,
          "A non-external sst file have global seqno property with value %s",
          seqno_pos->second.c_str());
    }
    return kDisableGlobalSequenceNumber;
  }

  uint32_t version = DecodeFixed32(version_pos->second.c_str());
  if (version < 2) {
    if (seqno_pos != props.end() || version != 1) {
      // This is a v1 external sst file, global_seqno is not supported.
      assert(false);
      ROCKS_LOG_ERROR(
          info_log,
          "An external sst file with version %u have global seqno property "
          "with value %s",
          version, seqno_pos->second.c_str());
    }
    return kDisableGlobalSequenceNumber;
  }

  SequenceNumber global_seqno = DecodeFixed64(seqno_pos->second.c_str());

  if (global_seqno > kMaxSequenceNumber) {
    assert(false);
    ROCKS_LOG_ERROR(
        info_log,
        "An external sst file with version %u have global seqno property "
        "with value %llu, which is greater than kMaxSequenceNumber",
        version, global_seqno);
  }

  return global_seqno;
}

Slice BlockBasedTable::GetCacheKey(const char* cache_key_prefix,
                                   size_t cache_key_prefix_size,
                                   const BlockHandle& handle, char* cache_key) {
  assert(cache_key != nullptr);
  assert(cache_key_prefix_size != 0);
  assert(cache_key_prefix_size <= kMaxCacheKeyPrefixSize);
  memcpy(cache_key, cache_key_prefix, cache_key_prefix_size);
  char* end =
      EncodeVarint64(cache_key + cache_key_prefix_size, handle.offset());
  return Slice(cache_key, static_cast<size_t>(end - cache_key));
}

Status BlockBasedTable::Open(const ImmutableCFOptions& ioptions,
                             const EnvOptions& env_options,
                             const BlockBasedTableOptions& table_options,
                             const InternalKeyComparator& internal_comparator,
                             unique_ptr<RandomAccessFileReader>&& file,
                             uint64_t file_size,
                             unique_ptr<TableReader>* table_reader,
                             const bool prefetch_index_and_filter_in_cache,
                             const bool skip_filters, const int level) {
  table_reader->reset();

  return async::TableOpenRequestContext::Open(ioptions, env_options,
        table_options,
        internal_comparator,
        std::move(file), file_size,
        table_reader,
        prefetch_index_and_filter_in_cache,
        skip_filters, level);
}

void BlockBasedTable::SetupForCompaction() {
  switch (rep_->ioptions.access_hint_on_compaction_start) {
    case Options::NONE:
      break;
    case Options::NORMAL:
      rep_->file->file()->Hint(RandomAccessFile::NORMAL);
      break;
    case Options::SEQUENTIAL:
      rep_->file->file()->Hint(RandomAccessFile::SEQUENTIAL);
      break;
    case Options::WILLNEED:
      rep_->file->file()->Hint(RandomAccessFile::WILLNEED);
      break;
    default:
      assert(false);
  }
  compaction_optimized_ = true;
}

std::shared_ptr<const TableProperties> BlockBasedTable::GetTableProperties()
    const {
  return rep_->table_properties;
}

size_t BlockBasedTable::ApproximateMemoryUsage() const {
  size_t usage = 0;
  if (rep_->filter) {
    usage += rep_->filter->ApproximateMemoryUsage();
  }
  if (rep_->index_reader) {
    usage += rep_->index_reader->ApproximateMemoryUsage();
  }
  return usage;
}

// Load the meta-block from the file. On success, return the loaded meta block
// and its iterator.
Status BlockBasedTable::ReadMetaBlock(Rep* rep,
                                      std::unique_ptr<Block>* meta_block,
                                      std::unique_ptr<InternalIterator>* iter) {
  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  //  TODO: we never really verify check sum for meta index block
  std::unique_ptr<Block> meta;
  Status s = ReadBlockFromFile(
      rep->file.get(), rep->footer, ReadOptions(),
      rep->footer.metaindex_handle(), &meta, rep->ioptions,
      true /* decompress */, Slice() /*compression dict*/,
      rep->persistent_cache_options, kDisableGlobalSequenceNumber,
      0 /* read_amp_bytes_per_bit */);

  if (!s.ok()) {
    ROCKS_LOG_ERROR(rep->ioptions.info_log,
                    "Encountered error while reading data from properties"
                    " block %s",
                    s.ToString().c_str());
    return s;
  }

  *meta_block = std::move(meta);
  // meta block uses bytewise comparator.
  iter->reset(meta_block->get()->NewIterator(BytewiseComparator()));
  return Status::OK();
}

Status BlockBasedTable::GetDataBlockFromCache(
    const Slice& block_cache_key, const Slice& compressed_block_cache_key,
    Cache* block_cache, Cache* block_cache_compressed,
    const ImmutableCFOptions& ioptions, const ReadOptions& read_options,
    BlockBasedTable::CachableEntry<Block>* block, uint32_t format_version,
    const Slice& compression_dict, size_t read_amp_bytes_per_bit,
    bool is_index) {
  Status s;
  Block* compressed_block = nullptr;
  Cache::Handle* block_cache_compressed_handle = nullptr;
  Statistics* statistics = ioptions.statistics;

  // Lookup uncompressed cache first
  if (block_cache != nullptr) {
    block->cache_handle = GetEntryFromCache(
        block_cache, block_cache_key,
        is_index ? BLOCK_CACHE_INDEX_MISS : BLOCK_CACHE_DATA_MISS,
        is_index ? BLOCK_CACHE_INDEX_HIT : BLOCK_CACHE_DATA_HIT, statistics);
    if (block->cache_handle != nullptr) {
      block->value =
          reinterpret_cast<Block*>(block_cache->Value(block->cache_handle));
      return s;
    }
  }

  // If not found, search from the compressed block cache.
  assert(block->cache_handle == nullptr && block->value == nullptr);

  if (block_cache_compressed == nullptr) {
    return s;
  }

  assert(!compressed_block_cache_key.empty());
  block_cache_compressed_handle =
      block_cache_compressed->Lookup(compressed_block_cache_key);
  // if we found in the compressed cache, then uncompress and insert into
  // uncompressed cache
  if (block_cache_compressed_handle == nullptr) {
    RecordTick(statistics, BLOCK_CACHE_COMPRESSED_MISS);
    return s;
  }

  // found compressed block
  RecordTick(statistics, BLOCK_CACHE_COMPRESSED_HIT);
  compressed_block = reinterpret_cast<Block*>(
      block_cache_compressed->Value(block_cache_compressed_handle));
  assert(compressed_block->compression_type() != kNoCompression);

  // Retrieve the uncompressed contents into a new buffer
  BlockContents contents;
  s = UncompressBlockContents(compressed_block->data(),
                              compressed_block->size(), &contents,
                              format_version, compression_dict,
                              ioptions);

  // Insert uncompressed block into block cache
  if (s.ok()) {
    block->value =
        new Block(std::move(contents), compressed_block->global_seqno(),
                  read_amp_bytes_per_bit,
                  statistics);  // uncompressed block
    assert(block->value->compression_type() == kNoCompression);
    if (block_cache != nullptr && block->value->cachable() &&
        read_options.fill_cache) {
      s = block_cache->Insert(
          block_cache_key, block->value, block->value->usable_size(),
          &DeleteCachedEntry<Block>, &(block->cache_handle));
      if (s.ok()) {
        RecordTick(statistics, BLOCK_CACHE_ADD);
        if (is_index) {
          RecordTick(statistics, BLOCK_CACHE_INDEX_ADD);
          RecordTick(statistics, BLOCK_CACHE_INDEX_BYTES_INSERT,
                     block->value->usable_size());
        } else {
          RecordTick(statistics, BLOCK_CACHE_DATA_ADD);
          RecordTick(statistics, BLOCK_CACHE_DATA_BYTES_INSERT,
                     block->value->usable_size());
        }
        RecordTick(statistics, BLOCK_CACHE_BYTES_WRITE,
                   block->value->usable_size());
      } else {
        RecordTick(statistics, BLOCK_CACHE_ADD_FAILURES);
        delete block->value;
        block->value = nullptr;
      }
    }
  }

  // Release hold on compressed cache entry
  block_cache_compressed->Release(block_cache_compressed_handle);
  return s;
}

Status BlockBasedTable::PutDataBlockToCache(
    const Slice& block_cache_key, const Slice& compressed_block_cache_key,
    Cache* block_cache, Cache* block_cache_compressed,
    const ReadOptions& read_options, const ImmutableCFOptions& ioptions,
    CachableEntry<Block>* block, std::unique_ptr<Block>& raw_block,
    std::unique_ptr<Block>& uncompressed_block, uint32_t format_version,
    const Slice& compression_dict, size_t read_amp_bytes_per_bit, bool is_index,
    Cache::Priority priority) {
  assert(raw_block);
  assert(raw_block->compression_type() == kNoCompression ||
         block_cache_compressed != nullptr);

  uncompressed_block.reset();

  Status s;
  // Retrieve the uncompressed contents into a new buffer
  BlockContents contents;
  Statistics* statistics = ioptions.statistics;
  if (raw_block->compression_type() != kNoCompression) {
    s = UncompressBlockContents(raw_block->data(), raw_block->size(), &contents,
                                format_version, compression_dict, ioptions);
  }
  if (!s.ok()) {
    raw_block.reset();
    return s;
  }

  if (raw_block->compression_type() != kNoCompression) {
    uncompressed_block.reset(new Block(std::move(contents), raw_block->global_seqno(),
                             read_amp_bytes_per_bit,
                             statistics));  // uncompressed block
  } else {
    // No need to uncompress
    uncompressed_block = std::move(raw_block);
  }

  // Insert compressed block into compressed block cache.
  // Release the hold on the compressed cache entry immediately.
  if (block_cache_compressed != nullptr && raw_block != nullptr &&
      raw_block->cachable()) {
    s = block_cache_compressed->Insert(compressed_block_cache_key, raw_block.get(),
                                       raw_block->usable_size(),
                                       &DeleteCachedEntry<Block>);
    if (s.ok()) {
      // Avoid the following code to delete this cached block.
      raw_block.release();
      RecordTick(statistics, BLOCK_CACHE_COMPRESSED_ADD);
    } else {
      raw_block.reset();
      RecordTick(statistics, BLOCK_CACHE_COMPRESSED_ADD_FAILURES);
    }
  }

  // insert into uncompressed block cache

  // If we succeed inserting into the cache then we release() uncompressed_block
  // However, if the block is not cacheable or the insert into the cache fails,
  // we keep uncompressed_block in-tact so the caller may still take advantage
  // of the block we spent time reading.
  assert((uncompressed_block->compression_type() == kNoCompression));
  if (block_cache != nullptr && uncompressed_block->cachable()) {
    block->value = uncompressed_block.get();
    s = block_cache->Insert(
        block_cache_key, block->value, block->value->usable_size(),
        &DeleteCachedEntry<Block>, &(block->cache_handle), priority);
    if (s.ok()) {
      uncompressed_block.release();
      assert(block->cache_handle != nullptr);
      RecordTick(statistics, BLOCK_CACHE_ADD);
      if (is_index) {
        RecordTick(statistics, BLOCK_CACHE_INDEX_ADD);
        RecordTick(statistics, BLOCK_CACHE_INDEX_BYTES_INSERT,
                   block->value->usable_size());
      } else {
        RecordTick(statistics, BLOCK_CACHE_DATA_ADD);
        RecordTick(statistics, BLOCK_CACHE_DATA_BYTES_INSERT,
                   block->value->usable_size());
      }
      RecordTick(statistics, BLOCK_CACHE_BYTES_WRITE,
                 block->value->usable_size());
      assert(reinterpret_cast<Block*>(
                 block_cache->Value(block->cache_handle)) == block->value);
    } else {
      RecordTick(statistics, BLOCK_CACHE_ADD_FAILURES);
      block->value = nullptr;
    }
  } else {
    block->value = uncompressed_block.release();
  }

  return s;
}

FilterBlockReader* BlockBasedTable::ReadFilter(
    const BlockHandle& filter_handle, const bool is_a_filter_partition) const {

  using namespace async;
  ReadFilterHelper::ReadFilterCallback empty_cb;

  ReadFilterHelper read_helper(this, is_a_filter_partition);
  Status s = read_helper.Read(empty_cb, filter_handle);
  s = read_helper.OnFilterReadComplete(s);

  if (s.ok()) {
    return read_helper.GetReader();
  }

  return nullptr;
}

BlockBasedTable::CachableEntry<FilterBlockReader> BlockBasedTable::GetFilter(
                                                          bool no_io) const {
  const BlockHandle& filter_blk_handle = rep_->filter_handle;
  const bool is_a_filter_partition = true;
  return GetFilter(filter_blk_handle, !is_a_filter_partition, no_io);
}

BlockBasedTable::CachableEntry<FilterBlockReader> BlockBasedTable::GetFilter(
    const BlockHandle& filter_blk_handle, const bool is_a_filter_partition,
    bool no_io) const {

  async::GetFilterHelper::GetFilterCallback empty_cb;
  BlockBasedTable::CachableEntry<FilterBlockReader> result;
  async::GetFilterHelper helper(this, filter_blk_handle, is_a_filter_partition, no_io);

  Status s = helper.GetFilter(empty_cb);

  // No IO possible due to no_io flag
  if (s.IsIncomplete()) {
    result = helper.GetEntry();
    return result;
  }

  helper.OnGetFilterComplete(s);

  result = helper.GetEntry();
  return result;
}

InternalIterator* BlockBasedTable::NewIndexIterator(
    const ReadOptions& read_options, BlockIter* input_iter,
    CachableEntry<IndexReader>* index_entry) {

  InternalIterator* result = nullptr;
  async::NewIndexIteratorContext::Create(this, read_options,
                                         nullptr, input_iter, index_entry, &result);

  return result;
}

InternalIterator* BlockBasedTable::NewDataBlockIterator(
    Rep* rep, const ReadOptions& ro, const Slice& index_value,
    BlockIter* input_iter, bool is_index) {
  BlockHandle handle;
  Slice input = index_value;
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.
  Status s = handle.DecodeFrom(&input);
  return NewDataBlockIterator(rep, ro, handle, input_iter, is_index, s);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// If input_iter is null, new a iterator
// If input_iter is not null, update this iter and return it
InternalIterator* BlockBasedTable::NewDataBlockIterator(
    Rep* rep, const ReadOptions& ro, const BlockHandle& handle,
    BlockIter* input_iter, bool is_index, Status s) {

  using namespace async;
  NewDataBlockIteratorHelper helper(rep, ro, is_index);
  if (!s.ok()) {
    helper.StatusToIterator(input_iter, s);
    return helper.GetResult();
  }

  NewDataBlockIteratorHelper::ReadDataBlockCallback empty_cb;
  s = helper.Create(empty_cb, handle, input_iter);

  assert(!s.IsIOPending());

  helper.OnCreateComplete(s);

  // Status is returned via iterator
  return helper.GetResult();
}

Status BlockBasedTable::MaybeLoadDataBlockToCache(
  Rep* rep, const ReadOptions& ro, const BlockHandle& handle,
  Slice compression_dict, CachableEntry<Block>* block_entry, bool is_index) {

  using namespace async;

  Status s;

  MaybeLoadDataBlockToCacheHelper helper(is_index, rep);

  if (!helper.IsCacheEnabled(rep)) {
    return s;
  }

  s = helper.GetBlockFromCache(rep, ro, handle, compression_dict,
                               block_entry);

  if (s.ok() && block_entry->value != nullptr) {
    return s;
  }

  if(s.IsNotFound() && helper.ShouldRead(ro)) {

    MaybeLoadDataBlockToCacheHelper::BlockContCallback empty_cb;
    BlockContents block_contents;

    const bool do_uncompress = (nullptr ==
                                rep->table_options.block_cache_compressed);

    s = helper.RequestCachebableBlock(empty_cb, rep, ro, handle, &block_contents,
                                      do_uncompress);
    s = helper.OnBlockReadComplete(s, rep, ro, std::move(block_contents),
                                   compression_dict, block_entry);
  } 

  if(s.IsNotFound()) {
    s = Status::OK();
  }

  return s;
}

BlockBasedTable::BlockEntryIteratorState::BlockEntryIteratorState(
    BlockBasedTable* table, const ReadOptions& read_options, bool skip_filters,
    bool is_index, Cleanable* block_cache_cleaner)
    : TwoLevelIteratorState(table->rep_->ioptions.prefix_extractor != nullptr),
      table_(table),
      read_options_(read_options),
      skip_filters_(skip_filters),
      is_index_(is_index),
      block_cache_cleaner_(block_cache_cleaner) {}

InternalIterator*
BlockBasedTable::BlockEntryIteratorState::NewSecondaryIterator(
    const Slice& index_value) {
  // Return a block iterator on the index partition
  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  auto iter = NewDataBlockIterator(table_->rep_, read_options_, handle, nullptr,
                                   is_index_, s);
  if (block_cache_cleaner_) {
    uint64_t offset = handle.offset();
    {
      ReadLock rl(&cleaner_mu);
      if (cleaner_set.find(offset) != cleaner_set.end()) {
        // already have a refernce to the block cache objects
        return iter;
      }
    }
    WriteLock wl(&cleaner_mu);
    cleaner_set.insert(offset);
    // Keep the data into cache until the cleaner cleansup
    iter->DelegateCleanupsTo(block_cache_cleaner_);
  }
  return iter;
}

bool BlockBasedTable::BlockEntryIteratorState::PrefixMayMatch(
    const Slice& internal_key) {
  if (read_options_.total_order_seek || skip_filters_) {
    return true;
  }
  return table_->PrefixMayMatch(internal_key);
}

// This will be broken if the user specifies an unusual implementation
// of Options.comparator, or if the user specifies an unusual
// definition of prefixes in BlockBasedTableOptions.filter_policy.
// In particular, we require the following three properties:
//
// 1) key.starts_with(prefix(key))
// 2) Compare(prefix(key), key) <= 0.
// 3) If Compare(key1, key2) <= 0, then Compare(prefix(key1), prefix(key2)) <= 0
//
// Otherwise, this method guarantees no I/O will be incurred.
//
// REQUIRES: this method shouldn't be called while the DB lock is held.
bool BlockBasedTable::PrefixMayMatch(const Slice& internal_key) {
  if (!rep_->filter_policy) {
    return true;
  }

  assert(rep_->ioptions.prefix_extractor != nullptr);
  auto user_key = ExtractUserKey(internal_key);
  if (!rep_->ioptions.prefix_extractor->InDomain(user_key) ||
      rep_->table_properties->prefix_extractor_name.compare(
          rep_->ioptions.prefix_extractor->Name()) != 0) {
    return true;
  }
  auto prefix = rep_->ioptions.prefix_extractor->Transform(user_key);

  bool may_match = true;
  Status s;

  // First, try check with full filter
  const bool no_io = true;
  auto filter_entry = GetFilter(no_io);
  FilterBlockReader* filter = filter_entry.value;
  if (filter != nullptr) {
    if (!filter->IsBlockBased()) {
      const Slice* const const_ikey_ptr = &internal_key;
      may_match =
          filter->PrefixMayMatch(prefix, kNotValid, no_io, const_ikey_ptr);
    } else {
      InternalKey internal_key_prefix(prefix, kMaxSequenceNumber, kTypeValue);
      auto internal_prefix = internal_key_prefix.Encode();

      // To prevent any io operation in this method, we set `read_tier` to make
      // sure we always read index or filter only when they have already been
      // loaded to memory.
      ReadOptions no_io_read_options;
      no_io_read_options.read_tier = kBlockCacheTier;

      // Then, try find it within each block
      unique_ptr<InternalIterator> iiter(NewIndexIterator(no_io_read_options));
      iiter->Seek(internal_prefix);

      if (!iiter->Valid()) {
        // we're past end of file
        // if it's incomplete, it means that we avoided I/O
        // and we're not really sure that we're past the end
        // of the file
        may_match = iiter->status().IsIncomplete();
      } else if (ExtractUserKey(iiter->key())
                     .starts_with(ExtractUserKey(internal_prefix))) {
        // we need to check for this subtle case because our only
        // guarantee is that "the key is a string >= last key in that data
        // block" according to the doc/table_format.txt spec.
        //
        // Suppose iiter->key() starts with the desired prefix; it is not
        // necessarily the case that the corresponding data block will
        // contain the prefix, since iiter->key() need not be in the
        // block.  However, the next data block may contain the prefix, so
        // we return true to play it safe.
        may_match = true;
      } else if (filter->IsBlockBased()) {
        // iiter->key() does NOT start with the desired prefix.  Because
        // Seek() finds the first key that is >= the seek target, this
        // means that iiter->key() > prefix.  Thus, any data blocks coming
        // after the data block corresponding to iiter->key() cannot
        // possibly contain the key.  Thus, the corresponding data block
        // is the only on could potentially contain the prefix.
        Slice handle_value = iiter->value();
        BlockHandle handle;
        s = handle.DecodeFrom(&handle_value);
        assert(s.ok());
        may_match = filter->PrefixMayMatch(prefix, handle.offset());
      }
    }
  }

  Statistics* statistics = rep_->ioptions.statistics;
  RecordTick(statistics, BLOOM_FILTER_PREFIX_CHECKED);
  if (!may_match) {
    RecordTick(statistics, BLOOM_FILTER_PREFIX_USEFUL);
  }

  // if rep_->filter_entry is not set, we should call Release(); otherwise
  // don't call, in this case we have a local copy in rep_->filter_entry,
  // it's pinned to the cache and will be released in the destructor
  if (!rep_->filter_entry.IsSet()) {
    filter_entry.Release(rep_->table_options.block_cache.get());
  }

  return may_match;
}

InternalIterator* BlockBasedTable::NewIterator(const ReadOptions& read_options,
                                               Arena* arena,
                                               bool skip_filters) {
  return NewTwoLevelIterator(
      new BlockEntryIteratorState(this, read_options, skip_filters),
      NewIndexIterator(read_options), arena);
}

InternalIterator* BlockBasedTable::NewRangeTombstoneIterator(
    const ReadOptions& read_options) {

  using namespace async;

  InternalIterator* result = nullptr;
  if (!NewRangeTombstoneIterContext::IsPresent(rep_)) {
    return result;
  }

  NewRangeTombstoneIterContext::CreateIterator(rep_, read_options, &result);
  return result;
}

bool BlockBasedTable::FullFilterKeyMayMatch(const ReadOptions& read_options,
                                            FilterBlockReader* filter,
                                            const Slice& internal_key,
                                            const bool no_io) const {
  if (filter == nullptr || filter->IsBlockBased()) {
    return true;
  }
  Slice user_key = ExtractUserKey(internal_key);
  const Slice* const const_ikey_ptr = &internal_key;
  if (filter->whole_key_filtering()) {
    return filter->KeyMayMatch(user_key, kNotValid, no_io, const_ikey_ptr);
  }
  if (!read_options.total_order_seek && rep_->ioptions.prefix_extractor &&
      rep_->table_properties->prefix_extractor_name.compare(
          rep_->ioptions.prefix_extractor->Name()) == 0 &&
      rep_->ioptions.prefix_extractor->InDomain(user_key) &&
      !filter->PrefixMayMatch(
          rep_->ioptions.prefix_extractor->Transform(user_key), kNotValid,
          false, const_ikey_ptr)) {
    return false;
  }
  return true;
}

Status BlockBasedTable::Get(const ReadOptions& read_options, const Slice& key,
                            GetContext* get_context, bool skip_filters) {

  return async::BlockBasedGetContext::Get(this, read_options, key, get_context,
                                          skip_filters);
}

Status BlockBasedTable::Prefetch(const Slice* const begin,
                                 const Slice* const end) {
  auto& comparator = rep_->internal_comparator;
  // pre-condition
  if (begin && end && comparator.Compare(*begin, *end) > 0) {
    return Status::InvalidArgument(*begin, *end);
  }

  BlockIter iiter_on_stack;
  auto iiter = NewIndexIterator(ReadOptions(), &iiter_on_stack);
  std::unique_ptr<InternalIterator> iiter_unique_ptr;
  if (iiter != &iiter_on_stack) {
    iiter_unique_ptr = std::unique_ptr<InternalIterator>(iiter);
  }

  if (!iiter->status().ok()) {
    // error opening index iterator
    return iiter->status();
  }

  // indicates if we are on the last page that need to be pre-fetched
  bool prefetching_boundary_page = false;

  for (begin ? iiter->Seek(*begin) : iiter->SeekToFirst(); iiter->Valid();
       iiter->Next()) {
    Slice block_handle = iiter->value();

    if (end && comparator.Compare(iiter->key(), *end) >= 0) {
      if (prefetching_boundary_page) {
        break;
      }

      // The index entry represents the last key in the data block.
      // We should load this page into memory as well, but no more
      prefetching_boundary_page = true;
    }

    // Load the block specified by the block_handle into the block cache
    BlockIter biter;
    NewDataBlockIterator(rep_, ReadOptions(), block_handle, &biter);

    if (!biter.status().ok()) {
      // there was an unexpected error while pre-fetching
      return biter.status();
    }
  }

  return Status::OK();
}

bool BlockBasedTable::TEST_KeyInCache(const ReadOptions& options,
                                      const Slice& key) {
  std::unique_ptr<InternalIterator> iiter(NewIndexIterator(options));
  iiter->Seek(key);
  assert(iiter->Valid());
  CachableEntry<Block> block;

  BlockHandle handle;
  Slice input = iiter->value();
  Status s = handle.DecodeFrom(&input);
  assert(s.ok());
  Cache* block_cache = rep_->table_options.block_cache.get();
  assert(block_cache != nullptr);

  char cache_key_storage[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  Slice cache_key =
      GetCacheKey(rep_->cache_key_prefix, rep_->cache_key_prefix_size,
                  handle, cache_key_storage);
  Slice ckey;

  s = GetDataBlockFromCache(
      cache_key, ckey, block_cache, nullptr, rep_->ioptions, options, &block,
      rep_->table_options.format_version,
      rep_->compression_dict_block ? rep_->compression_dict_block->data
                                   : Slice(),
      0 /* read_amp_bytes_per_bit */);
  assert(s.ok());
  bool in_cache = block.value != nullptr;
  if (in_cache) {
    ReleaseCachedEntry(block_cache, block.cache_handle);
  }
  return in_cache;
}

// REQUIRES: The following fields of rep_ should have already been populated:
//  1. file
//  2. index_handle,
//  3. options
//  4. internal_comparator
//  5. index_type
Status BlockBasedTable::CreateIndexReader(
  IndexReader** index_reader, InternalIterator* preloaded_meta_index_iter,
  int level) {

  ReadOptions ro;
  return async::CreateIndexReaderContext::CreateReader(this, ro,
         preloaded_meta_index_iter, index_reader, level);
}

uint64_t BlockBasedTable::ApproximateOffsetOf(const Slice& key) {
  unique_ptr<InternalIterator> index_iter(NewIndexIterator(ReadOptions()));

  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->footer.metaindex_handle().offset();
    }
  } else {
    // key is past the last key in the file. If table_properties is not
    // available, approximate the offset by returning the offset of the
    // metaindex block (which is right near the end of the file).
    result = 0;
    if (rep_->table_properties) {
      result = rep_->table_properties->data_size;
    }
    // table_properties is not present in the table.
    if (result == 0) {
      result = rep_->footer.metaindex_handle().offset();
    }
  }
  return result;
}

bool BlockBasedTable::TEST_filter_block_preloaded() const {
  return rep_->filter != nullptr;
}

bool BlockBasedTable::TEST_index_reader_preloaded() const {
  return rep_->index_reader != nullptr;
}

Status BlockBasedTable::GetKVPairsFromDataBlocks(
    std::vector<KVPairBlock>* kv_pair_blocks) {
  std::unique_ptr<InternalIterator> blockhandles_iter(
      NewIndexIterator(ReadOptions()));

  Status s = blockhandles_iter->status();
  if (!s.ok()) {
    // Cannot read Index Block
    return s;
  }

  for (blockhandles_iter->SeekToFirst(); blockhandles_iter->Valid();
       blockhandles_iter->Next()) {
    s = blockhandles_iter->status();

    if (!s.ok()) {
      break;
    }

    std::unique_ptr<InternalIterator> datablock_iter;
    datablock_iter.reset(
        NewDataBlockIterator(rep_, ReadOptions(), blockhandles_iter->value()));
    s = datablock_iter->status();

    if (!s.ok()) {
      // Error reading the block - Skipped
      continue;
    }

    KVPairBlock kv_pair_block;
    for (datablock_iter->SeekToFirst(); datablock_iter->Valid();
         datablock_iter->Next()) {
      s = datablock_iter->status();
      if (!s.ok()) {
        // Error reading the block - Skipped
        break;
      }
      const Slice& key = datablock_iter->key();
      const Slice& value = datablock_iter->value();
      std::string key_copy = std::string(key.data(), key.size());
      std::string value_copy = std::string(value.data(), value.size());

      kv_pair_block.push_back(
          std::make_pair(std::move(key_copy), std::move(value_copy)));
    }
    kv_pair_blocks->push_back(std::move(kv_pair_block));
  }
  return Status::OK();
}

Status BlockBasedTable::DumpTable(WritableFile* out_file) {
  // Output Footer
  out_file->Append(
      "Footer Details:\n"
      "--------------------------------------\n"
      "  ");
  out_file->Append(rep_->footer.ToString().c_str());
  out_file->Append("\n");

  // Output MetaIndex
  out_file->Append(
      "Metaindex Details:\n"
      "--------------------------------------\n");
  std::unique_ptr<Block> meta;
  std::unique_ptr<InternalIterator> meta_iter;
  Status s = ReadMetaBlock(rep_, &meta, &meta_iter);
  if (s.ok()) {
    for (meta_iter->SeekToFirst(); meta_iter->Valid(); meta_iter->Next()) {
      s = meta_iter->status();
      if (!s.ok()) {
        return s;
      }
      if (meta_iter->key() == rocksdb::kPropertiesBlock) {
        out_file->Append("  Properties block handle: ");
        out_file->Append(meta_iter->value().ToString(true).c_str());
        out_file->Append("\n");
      } else if (meta_iter->key() == rocksdb::kCompressionDictBlock) {
        out_file->Append("  Compression dictionary block handle: ");
        out_file->Append(meta_iter->value().ToString(true).c_str());
        out_file->Append("\n");
      } else if (strstr(meta_iter->key().ToString().c_str(),
                        "filter.rocksdb.") != nullptr) {
        out_file->Append("  Filter block handle: ");
        out_file->Append(meta_iter->value().ToString(true).c_str());
        out_file->Append("\n");
      } else if (meta_iter->key() == rocksdb::kRangeDelBlock) {
        out_file->Append("  Range deletion block handle: ");
        out_file->Append(meta_iter->value().ToString(true).c_str());
        out_file->Append("\n");
      }
    }
    out_file->Append("\n");
  } else {
    return s;
  }

  // Output TableProperties
  const rocksdb::TableProperties* table_properties;
  table_properties = rep_->table_properties.get();

  if (table_properties != nullptr) {
    out_file->Append(
        "Table Properties:\n"
        "--------------------------------------\n"
        "  ");
    out_file->Append(table_properties->ToString("\n  ", ": ").c_str());
    out_file->Append("\n");
  }

  // Output Filter blocks
  if (!rep_->filter && !table_properties->filter_policy_name.empty()) {
    // Support only BloomFilter as off now
    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(1));
    if (table_properties->filter_policy_name.compare(
            table_options.filter_policy->Name()) == 0) {
      std::string filter_block_key = kFilterBlockPrefix;
      filter_block_key.append(table_properties->filter_policy_name);
      BlockHandle handle;
      if (FindMetaBlock(meta_iter.get(), filter_block_key, &handle).ok()) {
        BlockContents block;
        if (ReadBlockContents(
                rep_->file.get(), rep_->footer, ReadOptions(), handle, &block,
                rep_->ioptions, false /*decompress*/,
                Slice() /*compression dict*/, rep_->persistent_cache_options)
                .ok()) {
          rep_->filter.reset(new BlockBasedFilterBlockReader(
              rep_->ioptions.prefix_extractor, table_options,
              table_options.whole_key_filtering, std::move(block),
              rep_->ioptions.statistics));
        }
      }
    }
  }
  if (rep_->filter) {
    out_file->Append(
        "Filter Details:\n"
        "--------------------------------------\n"
        "  ");
    out_file->Append(rep_->filter->ToString().c_str());
    out_file->Append("\n");
  }

  // Output Index block
  s = DumpIndexBlock(out_file);
  if (!s.ok()) {
    return s;
  }

  // Output compression dictionary
  if (rep_->compression_dict_block != nullptr) {
    auto compression_dict = rep_->compression_dict_block->data;
    out_file->Append(
        "Compression Dictionary:\n"
        "--------------------------------------\n");
    out_file->Append("  size (bytes): ");
    out_file->Append(rocksdb::ToString(compression_dict.size()));
    out_file->Append("\n\n");
    out_file->Append("  HEX    ");
    out_file->Append(compression_dict.ToString(true).c_str());
    out_file->Append("\n\n");
  }

  // Output range deletions block
  auto* range_del_iter = NewRangeTombstoneIterator(ReadOptions());
  if (range_del_iter != nullptr) {
    range_del_iter->SeekToFirst();
    if (range_del_iter->Valid()) {
      out_file->Append(
          "Range deletions:\n"
          "--------------------------------------\n"
          "  ");
      for (; range_del_iter->Valid(); range_del_iter->Next()) {
        DumpKeyValue(range_del_iter->key(), range_del_iter->value(), out_file);
      }
      out_file->Append("\n");
    }
    delete range_del_iter;
  }
  // Output Data blocks
  s = DumpDataBlocks(out_file);

  return s;
}

void BlockBasedTable::Close() {
  rep_->filter_entry.Release(rep_->table_options.block_cache.get());
  rep_->index_entry.Release(rep_->table_options.block_cache.get());
  rep_->range_del_entry.Release(rep_->table_options.block_cache.get());
  // cleanup index and filter blocks to avoid accessing dangling pointer
  if (!rep_->table_options.no_block_cache && rep_->table_options.block_cache) {
    char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    // Get the filter block key
    auto key = GetCacheKey(rep_->cache_key_prefix, rep_->cache_key_prefix_size,
                           rep_->footer.metaindex_handle(), cache_key);
    rep_->table_options.block_cache.get()->Erase(key);
    // Get the index block key
    key = GetCacheKeyFromOffset(rep_->cache_key_prefix,
                                rep_->cache_key_prefix_size,
                                rep_->dummy_index_reader_offset, cache_key);
    rep_->table_options.block_cache.get()->Erase(key);
  }
}

Status BlockBasedTable::DumpIndexBlock(WritableFile* out_file) {
  out_file->Append(
      "Index Details:\n"
      "--------------------------------------\n");

  std::unique_ptr<InternalIterator> blockhandles_iter(
      NewIndexIterator(ReadOptions()));
  Status s = blockhandles_iter->status();
  if (!s.ok()) {
    out_file->Append("Can not read Index Block \n\n");
    return s;
  }

  out_file->Append("  Block key hex dump: Data block handle\n");
  out_file->Append("  Block key ascii\n\n");
  for (blockhandles_iter->SeekToFirst(); blockhandles_iter->Valid();
       blockhandles_iter->Next()) {
    s = blockhandles_iter->status();
    if (!s.ok()) {
      break;
    }
    Slice key = blockhandles_iter->key();
    InternalKey ikey;
    ikey.DecodeFrom(key);

    out_file->Append("  HEX    ");
    out_file->Append(ikey.user_key().ToString(true).c_str());
    out_file->Append(": ");
    out_file->Append(blockhandles_iter->value().ToString(true).c_str());
    out_file->Append("\n");

    std::string str_key = ikey.user_key().ToString();
    std::string res_key("");
    char cspace = ' ';
    for (size_t i = 0; i < str_key.size(); i++) {
      res_key.append(&str_key[i], 1);
      res_key.append(1, cspace);
    }
    out_file->Append("  ASCII  ");
    out_file->Append(res_key.c_str());
    out_file->Append("\n  ------\n");
  }
  out_file->Append("\n");
  return Status::OK();
}

Status BlockBasedTable::DumpDataBlocks(WritableFile* out_file) {
  std::unique_ptr<InternalIterator> blockhandles_iter(
      NewIndexIterator(ReadOptions()));
  Status s = blockhandles_iter->status();
  if (!s.ok()) {
    out_file->Append("Can not read Index Block \n\n");
    return s;
  }

  uint64_t datablock_size_min = std::numeric_limits<uint64_t>::max();
  uint64_t datablock_size_max = 0;
  uint64_t datablock_size_sum = 0;

  size_t block_id = 1;
  for (blockhandles_iter->SeekToFirst(); blockhandles_iter->Valid();
       block_id++, blockhandles_iter->Next()) {
    s = blockhandles_iter->status();
    if (!s.ok()) {
      break;
    }

    Slice bh_val = blockhandles_iter->value();
    BlockHandle bh;
    bh.DecodeFrom(&bh_val);
    uint64_t datablock_size = bh.size();
    datablock_size_min = std::min(datablock_size_min, datablock_size);
    datablock_size_max = std::max(datablock_size_max, datablock_size);
    datablock_size_sum += datablock_size;

    out_file->Append("Data Block # ");
    out_file->Append(rocksdb::ToString(block_id));
    out_file->Append(" @ ");
    out_file->Append(blockhandles_iter->value().ToString(true).c_str());
    out_file->Append("\n");
    out_file->Append("--------------------------------------\n");

    std::unique_ptr<InternalIterator> datablock_iter;
    datablock_iter.reset(
        NewDataBlockIterator(rep_, ReadOptions(), blockhandles_iter->value()));
    s = datablock_iter->status();

    if (!s.ok()) {
      out_file->Append("Error reading the block - Skipped \n\n");
      continue;
    }

    for (datablock_iter->SeekToFirst(); datablock_iter->Valid();
         datablock_iter->Next()) {
      s = datablock_iter->status();
      if (!s.ok()) {
        out_file->Append("Error reading the block - Skipped \n");
        break;
      }
      DumpKeyValue(datablock_iter->key(), datablock_iter->value(), out_file);
    }
    out_file->Append("\n");
  }

  uint64_t num_datablocks = block_id - 1;
  if (num_datablocks) {
    double datablock_size_avg =
        static_cast<double>(datablock_size_sum) / num_datablocks;
    out_file->Append("Data Block Summary:\n");
    out_file->Append("--------------------------------------");
    out_file->Append("\n  # data blocks: ");
    out_file->Append(rocksdb::ToString(num_datablocks));
    out_file->Append("\n  min data block size: ");
    out_file->Append(rocksdb::ToString(datablock_size_min));
    out_file->Append("\n  max data block size: ");
    out_file->Append(rocksdb::ToString(datablock_size_max));
    out_file->Append("\n  avg data block size: ");
    out_file->Append(rocksdb::ToString(datablock_size_avg));
    out_file->Append("\n");
  }

  return Status::OK();
}

void BlockBasedTable::DumpKeyValue(const Slice& key, const Slice& value,
                                   WritableFile* out_file) {
  InternalKey ikey;
  ikey.DecodeFrom(key);

  out_file->Append("  HEX    ");
  out_file->Append(ikey.user_key().ToString(true).c_str());
  out_file->Append(": ");
  out_file->Append(value.ToString(true).c_str());
  out_file->Append("\n");

  std::string str_key = ikey.user_key().ToString();
  std::string str_value = value.ToString();
  std::string res_key(""), res_value("");
  char cspace = ' ';
  for (size_t i = 0; i < str_key.size(); i++) {
    res_key.append(&str_key[i], 1);
    res_key.append(1, cspace);
  }
  for (size_t i = 0; i < str_value.size(); i++) {
    res_value.append(&str_value[i], 1);
    res_value.append(1, cspace);
  }

  out_file->Append("  ASCII  ");
  out_file->Append(res_key.c_str());
  out_file->Append(": ");
  out_file->Append(res_value.c_str());
  out_file->Append("\n  ------\n");
}

void BlockBasedTable::DeleteCachedFilterEntry(const Slice& key, void* value) {
  FilterBlockReader* filter = reinterpret_cast<FilterBlockReader*>(value);
  if (filter->statistics() != nullptr) {
    RecordTick(filter->statistics(), BLOCK_CACHE_FILTER_BYTES_EVICT,
               filter->size());
  }
  delete filter;
}

void BlockBasedTable::DeleteCachedIndexEntry(const Slice& key, void* value) {
  IndexReader* index_reader = reinterpret_cast<IndexReader*>(value);
  if (index_reader->statistics() != nullptr) {
    RecordTick(index_reader->statistics(), BLOCK_CACHE_INDEX_BYTES_EVICT,
               index_reader->usable_size());
  }
  delete index_reader;
}

}  // namespace rocksdb
