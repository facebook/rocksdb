//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
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
#include "table/block_fetcher.h"
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

std::atomic<uint64_t> BlockBasedTable::next_cache_key_id_(0);

namespace {
// Read the block identified by "handle" from "file".
// The only relevant option is options.verify_checksums for now.
// On failure return non-OK.
// On success fill *result and return OK - caller owns *result
// @param compression_dict Data for presetting the compression library's
//    dictionary.
Status ReadBlockFromFile(
    RandomAccessFileReader* file, FilePrefetchBuffer* prefetch_buffer,
    const Footer& footer, const ReadOptions& options, const BlockHandle& handle,
    std::unique_ptr<Block>* result, const ImmutableCFOptions& ioptions,
    bool do_uncompress, const Slice& compression_dict,
    const PersistentCacheOptions& cache_options, SequenceNumber global_seqno,
    size_t read_amp_bytes_per_bit) {
  BlockContents contents;
  BlockFetcher block_fetcher(file, prefetch_buffer, footer, options, handle,
                             &contents, ioptions, do_uncompress,
                             compression_dict, cache_options);
  Status s = block_fetcher.ReadBlockContents();
  if (s.ok()) {
    result->reset(new Block(std::move(contents), global_seqno,
                            read_amp_bytes_per_bit, ioptions.statistics));
  }

  return s;
}

// Delete the resource that is held by the iterator.
template <class ResourceType>
void DeleteHeldResource(void* arg, void* /*ignored*/) {
  delete reinterpret_cast<ResourceType*>(arg);
}

// Delete the entry resided in the cache.
template <class Entry>
void DeleteCachedEntry(const Slice& /*key*/, void* value) {
  auto entry = reinterpret_cast<Entry*>(value);
  delete entry;
}

void DeleteCachedFilterEntry(const Slice& key, void* value);
void DeleteCachedIndexEntry(const Slice& key, void* value);

// Release the cached entry and decrement its ref count.
void ReleaseCachedEntry(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Release the cached entry and decrement its ref count.
void ForceReleaseCachedEntry(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle, true /* force_erase */);
}

Slice GetCacheKeyFromOffset(const char* cache_key_prefix,
                            size_t cache_key_prefix_size, uint64_t offset,
                            char* cache_key) {
  assert(cache_key != nullptr);
  assert(cache_key_prefix_size != 0);
  assert(cache_key_prefix_size <= BlockBasedTable::kMaxCacheKeyPrefixSize);
  memcpy(cache_key, cache_key_prefix, cache_key_prefix_size);
  char* end = EncodeVarint64(cache_key + cache_key_prefix_size, offset);
  return Slice(cache_key, static_cast<size_t>(end - cache_key));
}

Cache::Handle* GetEntryFromCache(Cache* block_cache, const Slice& key,
                                 Tickers block_cache_miss_ticker,
                                 Tickers block_cache_hit_ticker,
                                 Statistics* statistics,
                                 GetContext* get_context) {
  auto cache_handle = block_cache->Lookup(key, statistics);
  if (cache_handle != nullptr) {
    PERF_COUNTER_ADD(block_cache_hit_count, 1);
    if (get_context != nullptr) {
      // overall cache hit
      get_context->RecordCounters(BLOCK_CACHE_HIT, 1);
      // total bytes read from cache
      get_context->RecordCounters(BLOCK_CACHE_BYTES_READ,
                                  block_cache->GetUsage(cache_handle));
      // block-type specific cache hit
      get_context->RecordCounters(block_cache_hit_ticker, 1);
    } else {
      // overall cache hit
      RecordTick(statistics, BLOCK_CACHE_HIT);
      // total bytes read from cache
      RecordTick(statistics, BLOCK_CACHE_BYTES_READ,
                 block_cache->GetUsage(cache_handle));
      RecordTick(statistics, block_cache_hit_ticker);
    }
  } else {
    if (get_context != nullptr) {
      // overall cache miss
      get_context->RecordCounters(BLOCK_CACHE_MISS, 1);
      // block-type specific cache miss
      get_context->RecordCounters(block_cache_miss_ticker, 1);
    } else {
      RecordTick(statistics, BLOCK_CACHE_MISS);
      RecordTick(statistics, block_cache_miss_ticker);
    }
  }

  return cache_handle;
}

}  // namespace

// Index that allows binary search lookup in a two-level index structure.
class PartitionIndexReader : public IndexReader, public Cleanable {
 public:
  // Read the partition index from the file and create an instance for
  // `PartitionIndexReader`.
  // On success, index_reader will be populated; otherwise it will remain
  // unmodified.
  static Status Create(BlockBasedTable* table, RandomAccessFileReader* file,
                       FilePrefetchBuffer* prefetch_buffer,
                       const Footer& footer, const BlockHandle& index_handle,
                       const ImmutableCFOptions& ioptions,
                       const InternalKeyComparator* icomparator,
                       IndexReader** index_reader,
                       const PersistentCacheOptions& cache_options,
                       const int level) {
    std::unique_ptr<Block> index_block;
    auto s = ReadBlockFromFile(
        file, prefetch_buffer, footer, ReadOptions(), index_handle,
        &index_block, ioptions, true /* decompress */,
        Slice() /*compression dict*/, cache_options,
        kDisableGlobalSequenceNumber, 0 /* read_amp_bytes_per_bit */);

    if (s.ok()) {
      *index_reader =
          new PartitionIndexReader(table, icomparator, std::move(index_block),
                                   ioptions.statistics, level);
    }

    return s;
  }

  // return a two-level iterator: first level is on the partition index
  virtual InternalIterator* NewIterator(BlockIter* /*iter*/ = nullptr,
                                        bool /*dont_care*/ = true,
                                        bool fill_cache = true) override {
    // Filters are already checked before seeking the index
    if (!partition_map_.empty()) {
      return NewTwoLevelIterator(
          new BlockBasedTable::PartitionedIndexIteratorState(
              table_, partition_map_.size() ? &partition_map_ : nullptr),
          index_block_->NewIterator(icomparator_, nullptr, true));
    } else {
      auto ro = ReadOptions();
      ro.fill_cache = fill_cache;
      return new BlockBasedTableIterator(
          table_, ro, *icomparator_,
          index_block_->NewIterator(icomparator_, nullptr, true), false);
    }
    // TODO(myabandeh): Update TwoLevelIterator to be able to make use of
    // on-stack BlockIter while the state is on heap. Currentlly it assumes
    // the first level iter is always on heap and will attempt to delete it
    // in its destructor.
  }

  virtual void CacheDependencies(bool pin) override {
    // Before read partitions, prefetch them to avoid lots of IOs
    auto rep = table_->rep_;
    BlockIter biter;
    BlockHandle handle;
    index_block_->NewIterator(icomparator_, &biter, true);
    // Index partitions are assumed to be consecuitive. Prefetch them all.
    // Read the first block offset
    biter.SeekToFirst();
    Slice input = biter.value();
    Status s = handle.DecodeFrom(&input);
    assert(s.ok());
    if (!s.ok()) {
      ROCKS_LOG_WARN(rep->ioptions.info_log,
                     "Could not read first index partition");
      return;
    }
    uint64_t prefetch_off = handle.offset();

    // Read the last block's offset
    biter.SeekToLast();
    input = biter.value();
    s = handle.DecodeFrom(&input);
    assert(s.ok());
    if (!s.ok()) {
      ROCKS_LOG_WARN(rep->ioptions.info_log,
                     "Could not read last index partition");
      return;
    }
    uint64_t last_off = handle.offset() + handle.size() + kBlockTrailerSize;
    uint64_t prefetch_len = last_off - prefetch_off;
    std::unique_ptr<FilePrefetchBuffer> prefetch_buffer;
    auto& file = table_->rep_->file;
    prefetch_buffer.reset(new FilePrefetchBuffer());
    s = prefetch_buffer->Prefetch(file.get(), prefetch_off,
      static_cast<size_t>(prefetch_len));

    // After prefetch, read the partitions one by one
    biter.SeekToFirst();
    auto ro = ReadOptions();
    Cache* block_cache = rep->table_options.block_cache.get();
    for (; biter.Valid(); biter.Next()) {
      input = biter.value();
      s = handle.DecodeFrom(&input);
      assert(s.ok());
      if (!s.ok()) {
        ROCKS_LOG_WARN(rep->ioptions.info_log,
                       "Could not read index partition");
        continue;
      }

      BlockBasedTable::CachableEntry<Block> block;
      Slice compression_dict;
      if (rep->compression_dict_block) {
        compression_dict = rep->compression_dict_block->data;
      }
      const bool is_index = true;
      // TODO: Support counter batch update for partitioned index and
      // filter blocks
      s = table_->MaybeLoadDataBlockToCache(
          prefetch_buffer.get(), rep, ro, handle, compression_dict, &block,
          is_index, nullptr /* get_context */);

      assert(s.ok() || block.value == nullptr);
      if (s.ok() && block.value != nullptr) {
        if (block.cache_handle != nullptr) {
          if (pin) {
            partition_map_[handle.offset()] = block;
            RegisterCleanup(&ReleaseCachedEntry, block_cache,
                            block.cache_handle);
          } else {
            block_cache->Release(block.cache_handle);
          }
        } else {
          delete block.value;
        }
      }
    }
  }

  virtual size_t size() const override { return index_block_->size(); }
  virtual size_t usable_size() const override {
    return index_block_->usable_size();
  }

  virtual size_t ApproximateMemoryUsage() const override {
    assert(index_block_);
    return index_block_->ApproximateMemoryUsage();
  }

 private:
  PartitionIndexReader(BlockBasedTable* table,
                       const InternalKeyComparator* icomparator,
                       std::unique_ptr<Block>&& index_block, Statistics* stats,
                       const int /*level*/)
      : IndexReader(icomparator, stats),
        table_(table),
        index_block_(std::move(index_block)) {
    assert(index_block_ != nullptr);
  }
  BlockBasedTable* table_;
  std::unique_ptr<Block> index_block_;
  std::unordered_map<uint64_t, BlockBasedTable::CachableEntry<Block>>
      partition_map_;
};

// Index that allows binary search lookup for the first key of each block.
// This class can be viewed as a thin wrapper for `Block` class which already
// supports binary search.
class BinarySearchIndexReader : public IndexReader {
 public:
  // Read index from the file and create an intance for
  // `BinarySearchIndexReader`.
  // On success, index_reader will be populated; otherwise it will remain
  // unmodified.
  static Status Create(RandomAccessFileReader* file,
                       FilePrefetchBuffer* prefetch_buffer,
                       const Footer& footer, const BlockHandle& index_handle,
                       const ImmutableCFOptions& ioptions,
                       const InternalKeyComparator* icomparator,
                       IndexReader** index_reader,
                       const PersistentCacheOptions& cache_options) {
    std::unique_ptr<Block> index_block;
    auto s = ReadBlockFromFile(
        file, prefetch_buffer, footer, ReadOptions(), index_handle,
        &index_block, ioptions, true /* decompress */,
        Slice() /*compression dict*/, cache_options,
        kDisableGlobalSequenceNumber, 0 /* read_amp_bytes_per_bit */);

    if (s.ok()) {
      *index_reader = new BinarySearchIndexReader(
          icomparator, std::move(index_block), ioptions.statistics);
    }

    return s;
  }

  virtual InternalIterator* NewIterator(BlockIter* iter = nullptr,
                                        bool /*dont_care*/ = true,
                                        bool /*dont_care*/ = true) override {
    return index_block_->NewIterator(icomparator_, iter, true);
  }

  virtual size_t size() const override { return index_block_->size(); }
  virtual size_t usable_size() const override {
    return index_block_->usable_size();
  }

  virtual size_t ApproximateMemoryUsage() const override {
    assert(index_block_);
    return index_block_->ApproximateMemoryUsage();
  }

 private:
  BinarySearchIndexReader(const InternalKeyComparator* icomparator,
                          std::unique_ptr<Block>&& index_block,
                          Statistics* stats)
      : IndexReader(icomparator, stats), index_block_(std::move(index_block)) {
    assert(index_block_ != nullptr);
  }
  std::unique_ptr<Block> index_block_;
};

// Index that leverages an internal hash table to quicken the lookup for a given
// key.
class HashIndexReader : public IndexReader {
 public:
  static Status Create(const SliceTransform* hash_key_extractor,
                       const Footer& footer, RandomAccessFileReader* file,
                       FilePrefetchBuffer* prefetch_buffer,
                       const ImmutableCFOptions& ioptions,
                       const InternalKeyComparator* icomparator,
                       const BlockHandle& index_handle,
                       InternalIterator* meta_index_iter,
                       IndexReader** index_reader,
                       bool /*hash_index_allow_collision*/,
                       const PersistentCacheOptions& cache_options) {
    std::unique_ptr<Block> index_block;
    auto s = ReadBlockFromFile(
        file, prefetch_buffer, footer, ReadOptions(), index_handle,
        &index_block, ioptions, true /* decompress */,
        Slice() /*compression dict*/, cache_options,
        kDisableGlobalSequenceNumber, 0 /* read_amp_bytes_per_bit */);

    if (!s.ok()) {
      return s;
    }

    // Note, failure to create prefix hash index does not need to be a
    // hard error. We can still fall back to the original binary search index.
    // So, Create will succeed regardless, from this point on.

    auto new_index_reader =
        new HashIndexReader(icomparator, std::move(index_block),
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

    Slice dummy_comp_dict;
    // Read contents for the blocks
    BlockContents prefixes_contents;
    BlockFetcher prefixes_block_fetcher(
        file, prefetch_buffer, footer, ReadOptions(), prefixes_handle,
        &prefixes_contents, ioptions, true /* decompress */,
        dummy_comp_dict /*compression dict*/, cache_options);
    s = prefixes_block_fetcher.ReadBlockContents();
    if (!s.ok()) {
      return s;
    }
    BlockContents prefixes_meta_contents;
    BlockFetcher prefixes_meta_block_fetcher(
        file, prefetch_buffer, footer, ReadOptions(), prefixes_meta_handle,
        &prefixes_meta_contents, ioptions, true /* decompress */,
        dummy_comp_dict /*compression dict*/, cache_options);
    prefixes_meta_block_fetcher.ReadBlockContents();
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

  virtual InternalIterator* NewIterator(BlockIter* iter = nullptr,
                                        bool total_order_seek = true,
                                        bool /*dont_care*/ = true) override {
    return index_block_->NewIterator(icomparator_, iter, total_order_seek);
  }

  virtual size_t size() const override { return index_block_->size(); }
  virtual size_t usable_size() const override {
    return index_block_->usable_size();
  }

  virtual size_t ApproximateMemoryUsage() const override {
    assert(index_block_);
    return index_block_->ApproximateMemoryUsage() +
           prefixes_contents_.data.size();
  }

 private:
  HashIndexReader(const InternalKeyComparator* icomparator,
                  std::unique_ptr<Block>&& index_block, Statistics* stats)
      : IndexReader(icomparator, stats), index_block_(std::move(index_block)) {
    assert(index_block_ != nullptr);
  }

  ~HashIndexReader() {
  }

  std::unique_ptr<Block> index_block_;
  BlockContents prefixes_contents_;
};

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

namespace {
// Return True if table_properties has `user_prop_name` has a `true` value
// or it doesn't contain this property (for backward compatible).
bool IsFeatureSupported(const TableProperties& table_properties,
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

SequenceNumber GetGlobalSequenceNumber(const TableProperties& table_properties,
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
}  // namespace

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

  Footer footer;

  std::unique_ptr<FilePrefetchBuffer> prefetch_buffer;

  // Before read footer, readahead backwards to prefetch data
  const size_t kTailPrefetchSize = 512 * 1024;
  size_t prefetch_off;
  size_t prefetch_len;
  if (file_size < kTailPrefetchSize) {
    prefetch_off = 0;
    prefetch_len = static_cast<size_t>(file_size);
  } else {
    prefetch_off = static_cast<size_t>(file_size - kTailPrefetchSize);
    prefetch_len = kTailPrefetchSize;
  }
  Status s;
  // TODO should not have this special logic in the future.
  if (!file->use_direct_io()) {
    s = file->Prefetch(prefetch_off, prefetch_len);
  } else {
    prefetch_buffer.reset(new FilePrefetchBuffer());
    s = prefetch_buffer->Prefetch(file.get(), prefetch_off, prefetch_len);
  }
  s = ReadFooterFromFile(file.get(), prefetch_buffer.get(), file_size, &footer,
                         kBlockBasedTableMagicNumber);
  if (!s.ok()) {
    return s;
  }
  if (!BlockBasedTableSupportedVersion(footer.version())) {
    return Status::Corruption(
        "Unknown Footer version. Maybe this file was created with newer "
        "version of RocksDB?");
  }

  // We've successfully read the footer. We are ready to serve requests.
  // Better not mutate rep_ after the creation. eg. internal_prefix_transform
  // raw pointer will be used to create HashIndexReader, whose reset may
  // access a dangling pointer.
  Rep* rep = new BlockBasedTable::Rep(ioptions, env_options, table_options,
                                      internal_comparator, skip_filters);
  rep->file = std::move(file);
  rep->footer = footer;
  rep->index_type = table_options.index_type;
  rep->hash_index_allow_collision = table_options.hash_index_allow_collision;
  // We need to wrap data with internal_prefix_transform to make sure it can
  // handle prefix correctly.
  rep->internal_prefix_transform.reset(
      new InternalKeySliceTransform(rep->ioptions.prefix_extractor));
  SetupCacheKeyPrefix(rep, file_size);
  unique_ptr<BlockBasedTable> new_table(new BlockBasedTable(rep));

  // page cache options
  rep->persistent_cache_options =
      PersistentCacheOptions(rep->table_options.persistent_cache,
                             std::string(rep->persistent_cache_key_prefix,
                                         rep->persistent_cache_key_prefix_size),
                                         rep->ioptions.statistics);

  // Read meta index
  std::unique_ptr<Block> meta;
  std::unique_ptr<InternalIterator> meta_iter;
  s = ReadMetaBlock(rep, prefetch_buffer.get(), &meta, &meta_iter);
  if (!s.ok()) {
    return s;
  }

  // Find filter handle and filter type
  if (rep->filter_policy) {
    for (auto filter_type :
         {Rep::FilterType::kFullFilter, Rep::FilterType::kPartitionedFilter,
          Rep::FilterType::kBlockFilter}) {
      std::string prefix;
      switch (filter_type) {
        case Rep::FilterType::kFullFilter:
          prefix = kFullFilterBlockPrefix;
          break;
        case Rep::FilterType::kPartitionedFilter:
          prefix = kPartitionedFilterBlockPrefix;
          break;
        case Rep::FilterType::kBlockFilter:
          prefix = kFilterBlockPrefix;
          break;
        default:
          assert(0);
      }
      std::string filter_block_key = prefix;
      filter_block_key.append(rep->filter_policy->Name());
      if (FindMetaBlock(meta_iter.get(), filter_block_key, &rep->filter_handle)
              .ok()) {
        rep->filter_type = filter_type;
        break;
      }
    }
  }

  // Read the properties
  bool found_properties_block = true;
  s = SeekToPropertiesBlock(meta_iter.get(), &found_properties_block);

  if (!s.ok()) {
    ROCKS_LOG_WARN(rep->ioptions.info_log,
                   "Error when seeking to properties block from file: %s",
                   s.ToString().c_str());
  } else if (found_properties_block) {
    s = meta_iter->status();
    TableProperties* table_properties = nullptr;
    if (s.ok()) {
      s = ReadProperties(meta_iter->value(), rep->file.get(),
                         prefetch_buffer.get(), rep->footer, rep->ioptions,
                         &table_properties);
    }

    if (!s.ok()) {
      ROCKS_LOG_WARN(rep->ioptions.info_log,
                     "Encountered error while reading data from properties "
                     "block %s",
                     s.ToString().c_str());
    } else {
      assert(table_properties != nullptr);
      rep->table_properties.reset(table_properties);
      rep->blocks_maybe_compressed = rep->table_properties->compression_name !=
                                     CompressionTypeToString(kNoCompression);
    }
  } else {
    ROCKS_LOG_ERROR(rep->ioptions.info_log,
                    "Cannot find Properties block from file.");
  }

  // Read the compression dictionary meta block
  bool found_compression_dict;
  BlockHandle compression_dict_handle;
  s = SeekToCompressionDictBlock(meta_iter.get(), &found_compression_dict,
    &compression_dict_handle);
  if (!s.ok()) {
    ROCKS_LOG_WARN(
        rep->ioptions.info_log,
        "Error when seeking to compression dictionary block from file: %s",
        s.ToString().c_str());
  } else if (found_compression_dict && !compression_dict_handle.IsNull()) {
    // TODO(andrewkr): Add to block cache if cache_index_and_filter_blocks is
    // true.
    std::unique_ptr<BlockContents> compression_dict_cont{new BlockContents()};
    PersistentCacheOptions cache_options;
    ReadOptions read_options;
    read_options.verify_checksums = false;
    BlockFetcher compression_block_fetcher(
      rep->file.get(), prefetch_buffer.get(), rep->footer, read_options,
      compression_dict_handle, compression_dict_cont.get(), rep->ioptions, false /* decompress */,
      Slice() /*compression dict*/, cache_options);
    s = compression_block_fetcher.ReadBlockContents();

    if (!s.ok()) {
      ROCKS_LOG_WARN(
          rep->ioptions.info_log,
          "Encountered error while reading data from compression dictionary "
          "block %s",
          s.ToString().c_str());
    } else {
      rep->compression_dict_block = std::move(compression_dict_cont);
    }
  }

  // Read the range del meta block
  bool found_range_del_block;
  s = SeekToRangeDelBlock(meta_iter.get(), &found_range_del_block,
                          &rep->range_del_handle);
  if (!s.ok()) {
    ROCKS_LOG_WARN(
        rep->ioptions.info_log,
        "Error when seeking to range delete tombstones block from file: %s",
        s.ToString().c_str());
  } else {
    if (found_range_del_block && !rep->range_del_handle.IsNull()) {
      ReadOptions read_options;
      s = MaybeLoadDataBlockToCache(
          prefetch_buffer.get(), rep, read_options, rep->range_del_handle,
          Slice() /* compression_dict */, &rep->range_del_entry,
          false /* is_index */, nullptr /* get_context */);
      if (!s.ok()) {
        ROCKS_LOG_WARN(
            rep->ioptions.info_log,
            "Encountered error while reading data from range del block %s",
            s.ToString().c_str());
      }
    }
  }

  // Determine whether whole key filtering is supported.
  if (rep->table_properties) {
    rep->whole_key_filtering &=
        IsFeatureSupported(*(rep->table_properties),
                           BlockBasedTablePropertyNames::kWholeKeyFiltering,
                           rep->ioptions.info_log);
    rep->prefix_filtering &= IsFeatureSupported(
        *(rep->table_properties),
        BlockBasedTablePropertyNames::kPrefixFiltering, rep->ioptions.info_log);

    rep->global_seqno = GetGlobalSequenceNumber(*(rep->table_properties),
                                                rep->ioptions.info_log);
  }

  const bool pin =
      rep->table_options.pin_l0_filter_and_index_blocks_in_cache && level == 0;
  // pre-fetching of blocks is turned on
  // Will use block cache for index/filter blocks access
  // Always prefetch index and filter for level 0
  if (table_options.cache_index_and_filter_blocks) {
    if (prefetch_index_and_filter_in_cache || level == 0) {
      assert(table_options.block_cache != nullptr);
      // Hack: Call NewIndexIterator() to implicitly add index to the
      // block_cache

      CachableEntry<IndexReader> index_entry;
      unique_ptr<InternalIterator> iter(
          new_table->NewIndexIterator(ReadOptions(), nullptr, &index_entry));
      s = iter->status();
      if (s.ok()) {
        // This is the first call to NewIndexIterator() since we're in Open().
        // On success it should give us ownership of the `CachableEntry` by
        // populating `index_entry`.
        assert(index_entry.value != nullptr);
        index_entry.value->CacheDependencies(pin);
        if (pin) {
          rep->index_entry = std::move(index_entry);
        } else {
          index_entry.Release(table_options.block_cache.get());
        }

        // Hack: Call GetFilter() to implicitly add filter to the block_cache
        auto filter_entry = new_table->GetFilter();
        if (filter_entry.value != nullptr) {
          filter_entry.value->CacheDependencies(pin);
        }
        // if pin_l0_filter_and_index_blocks_in_cache is true, and this is
        // a level0 file, then save it in rep_->filter_entry; it will be
        // released in the destructor only, hence it will be pinned in the
        // cache while this reader is alive
        if (pin) {
          rep->filter_entry = filter_entry;
        } else {
          filter_entry.Release(table_options.block_cache.get());
        }
      }
    }
  } else {
    // If we don't use block cache for index/filter blocks access, we'll
    // pre-load these blocks, which will kept in member variables in Rep
    // and with a same life-time as this table object.
    IndexReader* index_reader = nullptr;
    s = new_table->CreateIndexReader(prefetch_buffer.get(), &index_reader,
                                     meta_iter.get(), level);
    if (s.ok()) {
      rep->index_reader.reset(index_reader);
      // The partitions of partitioned index are always stored in cache. They
      // are hence follow the configuration for pin and prefetch regardless of
      // the value of cache_index_and_filter_blocks
      if (prefetch_index_and_filter_in_cache || level == 0) {
        rep->index_reader->CacheDependencies(pin);
      }

      // Set filter block
      if (rep->filter_policy) {
        const bool is_a_filter_partition = true;
        auto filter = new_table->ReadFilter(
            prefetch_buffer.get(), rep->filter_handle, !is_a_filter_partition);
        rep->filter.reset(filter);
        // Refer to the comment above about paritioned indexes always being
        // cached
        if (filter && (prefetch_index_and_filter_in_cache || level == 0)) {
          filter->CacheDependencies(pin);
        }
      }
    } else {
      delete index_reader;
    }
  }

  if (s.ok()) {
    *table_reader = std::move(new_table);
  }

  return s;
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
                                      FilePrefetchBuffer* prefetch_buffer,
                                      std::unique_ptr<Block>* meta_block,
                                      std::unique_ptr<InternalIterator>* iter) {
  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  std::unique_ptr<Block> meta;
  Status s = ReadBlockFromFile(
      rep->file.get(), prefetch_buffer, rep->footer, ReadOptions(),
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
    const Slice& compression_dict, size_t read_amp_bytes_per_bit, bool is_index,
    GetContext* get_context) {
  Status s;
  Block* compressed_block = nullptr;
  Cache::Handle* block_cache_compressed_handle = nullptr;
  Statistics* statistics = ioptions.statistics;

  // Lookup uncompressed cache first
  if (block_cache != nullptr) {
    block->cache_handle = GetEntryFromCache(
        block_cache, block_cache_key,
        is_index ? BLOCK_CACHE_INDEX_MISS : BLOCK_CACHE_DATA_MISS,
        is_index ? BLOCK_CACHE_INDEX_HIT : BLOCK_CACHE_DATA_HIT, statistics,
        get_context);
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
      block_cache->TEST_mark_as_data_block(block_cache_key,
                                           block->value->usable_size());
      if (s.ok()) {
        if (get_context != nullptr) {
          get_context->RecordCounters(BLOCK_CACHE_ADD, 1);
          get_context->RecordCounters(BLOCK_CACHE_BYTES_WRITE,
                                      block->value->usable_size());
        } else {
          RecordTick(statistics, BLOCK_CACHE_ADD);
          RecordTick(statistics, BLOCK_CACHE_BYTES_WRITE,
                     block->value->usable_size());
        }
        if (is_index) {
          if (get_context != nullptr) {
            get_context->RecordCounters(BLOCK_CACHE_INDEX_ADD, 1);
            get_context->RecordCounters(BLOCK_CACHE_INDEX_BYTES_INSERT,
                                        block->value->usable_size());
          } else {
            RecordTick(statistics, BLOCK_CACHE_INDEX_ADD);
            RecordTick(statistics, BLOCK_CACHE_INDEX_BYTES_INSERT,
                       block->value->usable_size());
          }
        } else {
          if (get_context != nullptr) {
            get_context->RecordCounters(BLOCK_CACHE_DATA_ADD, 1);
            get_context->RecordCounters(BLOCK_CACHE_DATA_BYTES_INSERT,
                                        block->value->usable_size());
          } else {
            RecordTick(statistics, BLOCK_CACHE_DATA_ADD);
            RecordTick(statistics, BLOCK_CACHE_DATA_BYTES_INSERT,
                       block->value->usable_size());
          }
        }
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
    const ReadOptions& /*read_options*/, const ImmutableCFOptions& ioptions,
    CachableEntry<Block>* block, Block* raw_block, uint32_t format_version,
    const Slice& compression_dict, size_t read_amp_bytes_per_bit, bool is_index,
    Cache::Priority priority, GetContext* get_context) {
  assert(raw_block->compression_type() == kNoCompression ||
         block_cache_compressed != nullptr);

  Status s;
  // Retrieve the uncompressed contents into a new buffer
  BlockContents contents;
  Statistics* statistics = ioptions.statistics;
  if (raw_block->compression_type() != kNoCompression) {
    s = UncompressBlockContents(raw_block->data(), raw_block->size(), &contents,
                                format_version, compression_dict, ioptions);
  }
  if (!s.ok()) {
    delete raw_block;
    return s;
  }

  if (raw_block->compression_type() != kNoCompression) {
    block->value = new Block(std::move(contents), raw_block->global_seqno(),
                             read_amp_bytes_per_bit,
                             statistics);  // uncompressed block
  } else {
    block->value = raw_block;
    raw_block = nullptr;
  }

  // Insert compressed block into compressed block cache.
  // Release the hold on the compressed cache entry immediately.
  if (block_cache_compressed != nullptr && raw_block != nullptr &&
      raw_block->cachable()) {
    s = block_cache_compressed->Insert(compressed_block_cache_key, raw_block,
                                       raw_block->usable_size(),
                                       &DeleteCachedEntry<Block>);
    if (s.ok()) {
      // Avoid the following code to delete this cached block.
      raw_block = nullptr;
      RecordTick(statistics, BLOCK_CACHE_COMPRESSED_ADD);
    } else {
      RecordTick(statistics, BLOCK_CACHE_COMPRESSED_ADD_FAILURES);
    }
  }
  delete raw_block;

  // insert into uncompressed block cache
  assert((block->value->compression_type() == kNoCompression));
  if (block_cache != nullptr && block->value->cachable()) {
    s = block_cache->Insert(
        block_cache_key, block->value, block->value->usable_size(),
        &DeleteCachedEntry<Block>, &(block->cache_handle), priority);
    block_cache->TEST_mark_as_data_block(block_cache_key,
                                         block->value->usable_size());
    if (s.ok()) {
      assert(block->cache_handle != nullptr);
      if (get_context != nullptr) {
        get_context->RecordCounters(BLOCK_CACHE_ADD, 1);
        get_context->RecordCounters(BLOCK_CACHE_BYTES_WRITE,
                                    block->value->usable_size());
      } else {
        RecordTick(statistics, BLOCK_CACHE_ADD);
        RecordTick(statistics, BLOCK_CACHE_BYTES_WRITE,
                   block->value->usable_size());
      }
      if (is_index) {
        if (get_context != nullptr) {
          get_context->RecordCounters(BLOCK_CACHE_INDEX_ADD, 1);
          get_context->RecordCounters(BLOCK_CACHE_INDEX_BYTES_INSERT,
                                      block->value->usable_size());
        } else {
          RecordTick(statistics, BLOCK_CACHE_INDEX_ADD);
          RecordTick(statistics, BLOCK_CACHE_INDEX_BYTES_INSERT,
                     block->value->usable_size());
        }
      } else {
        if (get_context != nullptr) {
          get_context->RecordCounters(BLOCK_CACHE_DATA_ADD, 1);
          get_context->RecordCounters(BLOCK_CACHE_DATA_BYTES_INSERT,
                                      block->value->usable_size());
        } else {
          RecordTick(statistics, BLOCK_CACHE_DATA_ADD);
          RecordTick(statistics, BLOCK_CACHE_DATA_BYTES_INSERT,
                     block->value->usable_size());
        }
      }
      assert(reinterpret_cast<Block*>(
                 block_cache->Value(block->cache_handle)) == block->value);
    } else {
      RecordTick(statistics, BLOCK_CACHE_ADD_FAILURES);
      delete block->value;
      block->value = nullptr;
    }
  }

  return s;
}

FilterBlockReader* BlockBasedTable::ReadFilter(
    FilePrefetchBuffer* prefetch_buffer, const BlockHandle& filter_handle,
    const bool is_a_filter_partition) const {
  auto& rep = rep_;
  // TODO: We might want to unify with ReadBlockFromFile() if we start
  // requiring checksum verification in Table::Open.
  if (rep->filter_type == Rep::FilterType::kNoFilter) {
    return nullptr;
  }
  BlockContents block;

  Slice dummy_comp_dict;

  BlockFetcher block_fetcher(rep->file.get(), prefetch_buffer, rep->footer,
                             ReadOptions(), filter_handle, &block,
                             rep->ioptions, false /* decompress */,
                             dummy_comp_dict, rep->persistent_cache_options);
  Status s = block_fetcher.ReadBlockContents();

  if (!s.ok()) {
    // Error reading the block
    return nullptr;
  }

  assert(rep->filter_policy);

  auto filter_type = rep->filter_type;
  if (rep->filter_type == Rep::FilterType::kPartitionedFilter &&
      is_a_filter_partition) {
    filter_type = Rep::FilterType::kFullFilter;
  }

  switch (filter_type) {
    case Rep::FilterType::kPartitionedFilter: {
      return new PartitionedFilterBlockReader(
          rep->prefix_filtering ? rep->ioptions.prefix_extractor : nullptr,
          rep->whole_key_filtering, std::move(block), nullptr,
          rep->ioptions.statistics, rep->internal_comparator, this);
    }

    case Rep::FilterType::kBlockFilter:
      return new BlockBasedFilterBlockReader(
          rep->prefix_filtering ? rep->ioptions.prefix_extractor : nullptr,
          rep->table_options, rep->whole_key_filtering, std::move(block),
          rep->ioptions.statistics);

    case Rep::FilterType::kFullFilter: {
      auto filter_bits_reader =
          rep->filter_policy->GetFilterBitsReader(block.data);
      assert(filter_bits_reader != nullptr);
      return new FullFilterBlockReader(
          rep->prefix_filtering ? rep->ioptions.prefix_extractor : nullptr,
          rep->whole_key_filtering, std::move(block), filter_bits_reader,
          rep->ioptions.statistics);
    }

    default:
      // filter_type is either kNoFilter (exited the function at the first if),
      // or it must be covered in this switch block
      assert(false);
      return nullptr;
  }
}

BlockBasedTable::CachableEntry<FilterBlockReader> BlockBasedTable::GetFilter(
    FilePrefetchBuffer* prefetch_buffer, bool no_io,
    GetContext* get_context) const {
  const BlockHandle& filter_blk_handle = rep_->filter_handle;
  const bool is_a_filter_partition = true;
  return GetFilter(prefetch_buffer, filter_blk_handle, !is_a_filter_partition,
                   no_io, get_context);
}

BlockBasedTable::CachableEntry<FilterBlockReader> BlockBasedTable::GetFilter(
    FilePrefetchBuffer* prefetch_buffer, const BlockHandle& filter_blk_handle,
    const bool is_a_filter_partition, bool no_io,
    GetContext* get_context) const {
  // If cache_index_and_filter_blocks is false, filter should be pre-populated.
  // We will return rep_->filter anyway. rep_->filter can be nullptr if filter
  // read fails at Open() time. We don't want to reload again since it will
  // most probably fail again.
  if (!is_a_filter_partition &&
      !rep_->table_options.cache_index_and_filter_blocks) {
    return {rep_->filter.get(), nullptr /* cache handle */};
  }

  Cache* block_cache = rep_->table_options.block_cache.get();
  if (rep_->filter_policy == nullptr /* do not use filter */ ||
      block_cache == nullptr /* no block cache at all */) {
    return {nullptr /* filter */, nullptr /* cache handle */};
  }

  if (!is_a_filter_partition && rep_->filter_entry.IsSet()) {
    return rep_->filter_entry;
  }

  PERF_TIMER_GUARD(read_filter_block_nanos);

  // Fetching from the cache
  char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  auto key = GetCacheKey(rep_->cache_key_prefix, rep_->cache_key_prefix_size,
                         filter_blk_handle, cache_key);

  Statistics* statistics = rep_->ioptions.statistics;
  auto cache_handle =
      GetEntryFromCache(block_cache, key, BLOCK_CACHE_FILTER_MISS,
                        BLOCK_CACHE_FILTER_HIT, statistics, get_context);

  FilterBlockReader* filter = nullptr;
  if (cache_handle != nullptr) {
    filter = reinterpret_cast<FilterBlockReader*>(
        block_cache->Value(cache_handle));
  } else if (no_io) {
    // Do not invoke any io.
    return CachableEntry<FilterBlockReader>();
  } else {
    filter =
        ReadFilter(prefetch_buffer, filter_blk_handle, is_a_filter_partition);
    if (filter != nullptr) {
      Status s = block_cache->Insert(
          key, filter, filter->size(), &DeleteCachedFilterEntry, &cache_handle,
          rep_->table_options.cache_index_and_filter_blocks_with_high_priority
              ? Cache::Priority::HIGH
              : Cache::Priority::LOW);
      if (s.ok()) {
        if (get_context != nullptr) {
          get_context->RecordCounters(BLOCK_CACHE_ADD, 1);
          get_context->RecordCounters(BLOCK_CACHE_BYTES_WRITE, filter->size());
          get_context->RecordCounters(BLOCK_CACHE_FILTER_ADD, 1);
          get_context->RecordCounters(BLOCK_CACHE_FILTER_BYTES_INSERT,
                                      filter->size());
        } else {
          RecordTick(statistics, BLOCK_CACHE_ADD);
          RecordTick(statistics, BLOCK_CACHE_BYTES_WRITE, filter->size());
          RecordTick(statistics, BLOCK_CACHE_FILTER_ADD);
          RecordTick(statistics, BLOCK_CACHE_FILTER_BYTES_INSERT,
                     filter->size());
        }
      } else {
        RecordTick(statistics, BLOCK_CACHE_ADD_FAILURES);
        delete filter;
        return CachableEntry<FilterBlockReader>();
      }
    }
  }

  return { filter, cache_handle };
}

InternalIterator* BlockBasedTable::NewIndexIterator(
    const ReadOptions& read_options, BlockIter* input_iter,
    CachableEntry<IndexReader>* index_entry, GetContext* get_context) {
  // index reader has already been pre-populated.
  if (rep_->index_reader) {
    return rep_->index_reader->NewIterator(
        input_iter, read_options.total_order_seek, read_options.fill_cache);
  }
  // we have a pinned index block
  if (rep_->index_entry.IsSet()) {
    return rep_->index_entry.value->NewIterator(
        input_iter, read_options.total_order_seek, read_options.fill_cache);
  }

  PERF_TIMER_GUARD(read_index_block_nanos);

  const bool no_io = read_options.read_tier == kBlockCacheTier;
  Cache* block_cache = rep_->table_options.block_cache.get();
  char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  auto key =
      GetCacheKeyFromOffset(rep_->cache_key_prefix, rep_->cache_key_prefix_size,
                            rep_->dummy_index_reader_offset, cache_key);
  Statistics* statistics = rep_->ioptions.statistics;
  auto cache_handle =
      GetEntryFromCache(block_cache, key, BLOCK_CACHE_INDEX_MISS,
                        BLOCK_CACHE_INDEX_HIT, statistics, get_context);

  if (cache_handle == nullptr && no_io) {
    if (input_iter != nullptr) {
      input_iter->Invalidate(Status::Incomplete("no blocking io"));
      return input_iter;
    } else {
      return NewErrorInternalIterator(Status::Incomplete("no blocking io"));
    }
  }

  IndexReader* index_reader = nullptr;
  if (cache_handle != nullptr) {
    index_reader =
        reinterpret_cast<IndexReader*>(block_cache->Value(cache_handle));
  } else {
    // Create index reader and put it in the cache.
    Status s;
    TEST_SYNC_POINT("BlockBasedTable::NewIndexIterator::thread2:2");
    s = CreateIndexReader(nullptr /* prefetch_buffer */, &index_reader);
    TEST_SYNC_POINT("BlockBasedTable::NewIndexIterator::thread1:1");
    TEST_SYNC_POINT("BlockBasedTable::NewIndexIterator::thread2:3");
    TEST_SYNC_POINT("BlockBasedTable::NewIndexIterator::thread1:4");
    if (s.ok()) {
      assert(index_reader != nullptr);
      s = block_cache->Insert(
          key, index_reader, index_reader->usable_size(),
          &DeleteCachedIndexEntry, &cache_handle,
          rep_->table_options.cache_index_and_filter_blocks_with_high_priority
              ? Cache::Priority::HIGH
              : Cache::Priority::LOW);
    }

    if (s.ok()) {
      size_t usable_size = index_reader->usable_size();
      if (get_context != nullptr) {
        get_context->RecordCounters(BLOCK_CACHE_ADD, 1);
        get_context->RecordCounters(BLOCK_CACHE_BYTES_WRITE, usable_size);
      } else {
        RecordTick(statistics, BLOCK_CACHE_ADD);
        RecordTick(statistics, BLOCK_CACHE_BYTES_WRITE, usable_size);
      }
      RecordTick(statistics, BLOCK_CACHE_INDEX_ADD);
      RecordTick(statistics, BLOCK_CACHE_INDEX_BYTES_INSERT, usable_size);
    } else {
      if (index_reader != nullptr) {
        delete index_reader;
      }
      RecordTick(statistics, BLOCK_CACHE_ADD_FAILURES);
      // make sure if something goes wrong, index_reader shall remain intact.
      if (input_iter != nullptr) {
        input_iter->Invalidate(s);
        return input_iter;
      } else {
        return NewErrorInternalIterator(s);
      }
    }

  }

  assert(cache_handle);
  auto* iter = index_reader->NewIterator(
      input_iter, read_options.total_order_seek);

  // the caller would like to take ownership of the index block
  // don't call RegisterCleanup() in this case, the caller will take care of it
  if (index_entry != nullptr) {
    *index_entry = {index_reader, cache_handle};
  } else {
    iter->RegisterCleanup(&ReleaseCachedEntry, block_cache, cache_handle);
  }

  return iter;
}

BlockIter* BlockBasedTable::NewDataBlockIterator(
    Rep* rep, const ReadOptions& ro, const Slice& index_value,
    BlockIter* input_iter, bool is_index, GetContext* get_context) {
  BlockHandle handle;
  Slice input = index_value;
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.
  Status s = handle.DecodeFrom(&input);
  return NewDataBlockIterator(rep, ro, handle, input_iter, is_index,
                              get_context, s);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// If input_iter is null, new a iterator
// If input_iter is not null, update this iter and return it
BlockIter* BlockBasedTable::NewDataBlockIterator(
    Rep* rep, const ReadOptions& ro, const BlockHandle& handle,
    BlockIter* input_iter, bool is_index, GetContext* get_context, Status s) {
  PERF_TIMER_GUARD(new_table_block_iter_nanos);

  const bool no_io = (ro.read_tier == kBlockCacheTier);
  Cache* block_cache = rep->table_options.block_cache.get();
  CachableEntry<Block> block;
  Slice compression_dict;
  if (s.ok()) {
    if (rep->compression_dict_block) {
      compression_dict = rep->compression_dict_block->data;
    }
    s = MaybeLoadDataBlockToCache(nullptr /*prefetch_buffer*/, rep, ro, handle,
                                  compression_dict, &block, is_index,
                                  get_context);
  }

  BlockIter* iter;
  if (input_iter != nullptr) {
    iter = input_iter;
  } else {
    iter = new BlockIter;
  }
  // Didn't get any data from block caches.
  if (s.ok() && block.value == nullptr) {
    if (no_io) {
      // Could not read from block_cache and can't do IO
      iter->Invalidate(Status::Incomplete("no blocking io"));
      return iter;
    }
    std::unique_ptr<Block> block_value;
    {
      StopWatch sw(rep->ioptions.env, rep->ioptions.statistics,
                   READ_BLOCK_GET_MICROS);
      s = ReadBlockFromFile(rep->file.get(), nullptr /* prefetch_buffer */,
                            rep->footer, ro, handle, &block_value, rep->ioptions,
                            rep->blocks_maybe_compressed, compression_dict,
                            rep->persistent_cache_options, rep->global_seqno,
                            rep->table_options.read_amp_bytes_per_bit);
    }
    if (s.ok()) {
      block.value = block_value.release();
    }
  }

  if (s.ok()) {
    assert(block.value != nullptr);
    iter = block.value->NewIterator(&rep->internal_comparator, iter, true,
                                    rep->ioptions.statistics);
    if (block.cache_handle != nullptr) {
      iter->RegisterCleanup(&ReleaseCachedEntry, block_cache,
                            block.cache_handle);
    } else {
      if (!ro.fill_cache && rep->cache_key_prefix_size != 0) {
        // insert a dummy record to block cache to track the memory usage
        Cache::Handle* cache_handle;
        // There are two other types of cache keys: 1) SST cache key added in
        // `MaybeLoadDataBlockToCache` 2) dummy cache key added in
        // `write_buffer_manager`. Use longer prefix (41 bytes) to differentiate
        // from SST cache key(31 bytes), and use non-zero prefix to
        // differentiate from `write_buffer_manager`
        const size_t kExtraCacheKeyPrefix = kMaxVarint64Length * 4 + 1;
        char cache_key[kExtraCacheKeyPrefix + kMaxVarint64Length];
        // Prefix: use rep->cache_key_prefix padded by 0s
        memset(cache_key, 0, kExtraCacheKeyPrefix + kMaxVarint64Length);
        assert(rep->cache_key_prefix_size != 0);
        assert(rep->cache_key_prefix_size <= kExtraCacheKeyPrefix);
        memcpy(cache_key, rep->cache_key_prefix, rep->cache_key_prefix_size);
        char* end = EncodeVarint64(cache_key + kExtraCacheKeyPrefix,
                                   next_cache_key_id_++);
        assert(end - cache_key <=
               static_cast<int>(kExtraCacheKeyPrefix + kMaxVarint64Length));
        Slice unique_key =
            Slice(cache_key, static_cast<size_t>(end - cache_key));
        s = block_cache->Insert(unique_key, nullptr, block.value->usable_size(),
                                nullptr, &cache_handle);
        if (s.ok()) {
          if (cache_handle != nullptr) {
            iter->RegisterCleanup(&ForceReleaseCachedEntry, block_cache,
                                  cache_handle);
          }
        }
      }
      iter->RegisterCleanup(&DeleteHeldResource<Block>, block.value, nullptr);
    }
  } else {
    assert(block.value == nullptr);
    iter->Invalidate(s);
  }
  return iter;
}

Status BlockBasedTable::MaybeLoadDataBlockToCache(
    FilePrefetchBuffer* prefetch_buffer, Rep* rep, const ReadOptions& ro,
    const BlockHandle& handle, Slice compression_dict,
    CachableEntry<Block>* block_entry, bool is_index, GetContext* get_context) {
  assert(block_entry != nullptr);
  const bool no_io = (ro.read_tier == kBlockCacheTier);
  Cache* block_cache = rep->table_options.block_cache.get();
  Cache* block_cache_compressed =
      rep->table_options.block_cache_compressed.get();

  // If either block cache is enabled, we'll try to read from it.
  Status s;
  if (block_cache != nullptr || block_cache_compressed != nullptr) {
    Statistics* statistics = rep->ioptions.statistics;
    char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    char compressed_cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    Slice key, /* key to the block cache */
        ckey /* key to the compressed block cache */;

    // create key for block cache
    if (block_cache != nullptr) {
      key = GetCacheKey(rep->cache_key_prefix, rep->cache_key_prefix_size,
                        handle, cache_key);
    }

    if (block_cache_compressed != nullptr) {
      ckey = GetCacheKey(rep->compressed_cache_key_prefix,
                         rep->compressed_cache_key_prefix_size, handle,
                         compressed_cache_key);
    }

    s = GetDataBlockFromCache(
        key, ckey, block_cache, block_cache_compressed, rep->ioptions, ro,
        block_entry, rep->table_options.format_version, compression_dict,
        rep->table_options.read_amp_bytes_per_bit, is_index, get_context);

    if (block_entry->value == nullptr && !no_io && ro.fill_cache) {
      std::unique_ptr<Block> raw_block;
      {
        StopWatch sw(rep->ioptions.env, statistics, READ_BLOCK_GET_MICROS);
        s = ReadBlockFromFile(
            rep->file.get(), prefetch_buffer, rep->footer, ro, handle,
            &raw_block, rep->ioptions,
            block_cache_compressed == nullptr && rep->blocks_maybe_compressed,
            compression_dict, rep->persistent_cache_options, rep->global_seqno,
            rep->table_options.read_amp_bytes_per_bit);
      }

      if (s.ok()) {
        s = PutDataBlockToCache(
            key, ckey, block_cache, block_cache_compressed, ro, rep->ioptions,
            block_entry, raw_block.release(), rep->table_options.format_version,
            compression_dict, rep->table_options.read_amp_bytes_per_bit,
            is_index,
            is_index && rep->table_options
                            .cache_index_and_filter_blocks_with_high_priority
                ? Cache::Priority::HIGH
                : Cache::Priority::LOW,
            get_context);
      }
    }
  }
  assert(s.ok() || block_entry->value == nullptr);
  return s;
}

BlockBasedTable::PartitionedIndexIteratorState::PartitionedIndexIteratorState(
    BlockBasedTable* table,
    std::unordered_map<uint64_t, CachableEntry<Block>>* block_map)
    : table_(table), block_map_(block_map) {}

const size_t BlockBasedTableIterator::kMaxReadaheadSize = 256 * 1024;

InternalIterator*
BlockBasedTable::PartitionedIndexIteratorState::NewSecondaryIterator(
    const Slice& index_value) {
  // Return a block iterator on the index partition
  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  auto rep = table_->get_rep();
  auto block = block_map_->find(handle.offset());
  // This is a possible scenario since block cache might not have had space
  // for the partition
  if (block != block_map_->end()) {
    PERF_COUNTER_ADD(block_cache_hit_count, 1);
    RecordTick(rep->ioptions.statistics, BLOCK_CACHE_INDEX_HIT);
    RecordTick(rep->ioptions.statistics, BLOCK_CACHE_HIT);
    Cache* block_cache = rep->table_options.block_cache.get();
    assert(block_cache);
    RecordTick(rep->ioptions.statistics, BLOCK_CACHE_BYTES_READ,
               block_cache->GetUsage(block->second.cache_handle));
    return block->second.value->NewIterator(&rep->internal_comparator, nullptr,
                                            true, rep->ioptions.statistics);
  }
  // Create an empty iterator
  return new BlockIter();
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
  auto filter_entry = GetFilter();
  FilterBlockReader* filter = filter_entry.value;
  if (filter != nullptr) {
    if (!filter->IsBlockBased()) {
      const Slice* const const_ikey_ptr = &internal_key;
      may_match =
          filter->PrefixMayMatch(prefix, kNotValid, false, const_ikey_ptr);
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

void BlockBasedTableIterator::Seek(const Slice& target) {
  if (!CheckPrefixMayMatch(target)) {
    ResetDataIter();
    return;
  }

  SavePrevIndexValue();

  index_iter_->Seek(target);

  if (!index_iter_->Valid()) {
    ResetDataIter();
    return;
  }

  InitDataBlock();

  data_block_iter_.Seek(target);

  FindKeyForward();
  assert(!data_block_iter_.Valid() ||
         icomp_.Compare(target, data_block_iter_.key()) <= 0);
}

void BlockBasedTableIterator::SeekForPrev(const Slice& target) {
  if (!CheckPrefixMayMatch(target)) {
    ResetDataIter();
    return;
  }

  SavePrevIndexValue();

  // Call Seek() rather than SeekForPrev() in the index block, because the
  // target data block will likely to contain the position for `target`, the
  // same as Seek(), rather than than before.
  // For example, if we have three data blocks, each containing two keys:
  //   [2, 4]  [6, 8] [10, 12]
  //  (the keys in the index block would be [4, 8, 12])
  // and the user calls SeekForPrev(7), we need to go to the second block,
  // just like if they call Seek(7).
  // The only case where the block is difference is when they seek to a position
  // in the boundary. For example, if they SeekForPrev(5), we should go to the
  // first block, rather than the second. However, we don't have the information
  // to distinguish the two unless we read the second block. In this case, we'll
  // end up with reading two blocks.
  index_iter_->Seek(target);

  if (!index_iter_->Valid()) {
    index_iter_->SeekToLast();
    if (!index_iter_->Valid()) {
      ResetDataIter();
      block_iter_points_to_real_block_ = false;
      return;
    }
  }

  InitDataBlock();

  data_block_iter_.SeekForPrev(target);

  FindKeyBackward();
  assert(!data_block_iter_.Valid() ||
         icomp_.Compare(target, data_block_iter_.key()) >= 0);
}

void BlockBasedTableIterator::SeekToFirst() {
  SavePrevIndexValue();
  index_iter_->SeekToFirst();
  if (!index_iter_->Valid()) {
    ResetDataIter();
    return;
  }
  InitDataBlock();
  data_block_iter_.SeekToFirst();
  FindKeyForward();
}

void BlockBasedTableIterator::SeekToLast() {
  SavePrevIndexValue();
  index_iter_->SeekToLast();
  if (!index_iter_->Valid()) {
    ResetDataIter();
    return;
  }
  InitDataBlock();
  data_block_iter_.SeekToLast();
  FindKeyBackward();
}

void BlockBasedTableIterator::Next() {
  assert(block_iter_points_to_real_block_);
  data_block_iter_.Next();
  FindKeyForward();
}

void BlockBasedTableIterator::Prev() {
  assert(block_iter_points_to_real_block_);
  data_block_iter_.Prev();
  FindKeyBackward();
}

void BlockBasedTableIterator::InitDataBlock() {
  BlockHandle data_block_handle;
  Slice handle_slice = index_iter_->value();
  if (!block_iter_points_to_real_block_ ||
      handle_slice.compare(prev_index_value_) != 0 ||
      // if previous attempt of reading the block missed cache, try again
      data_block_iter_.status().IsIncomplete()) {
    if (block_iter_points_to_real_block_) {
      ResetDataIter();
    }
    Status s = data_block_handle.DecodeFrom(&handle_slice);
    auto* rep = table_->get_rep();

    // Automatically prefetch additional data when a range scan (iterator) does
    // more than 2 sequential IOs. This is enabled only when
    // ReadOptions.readahead_size is 0.
    if (read_options_.readahead_size == 0) {
      if (num_file_reads_ < 2) {
        num_file_reads_++;
      } else if (data_block_handle.offset() +
                     static_cast<size_t>(data_block_handle.size()) +
                     kBlockTrailerSize >
                 readahead_limit_) {
        num_file_reads_++;
        // Do not readahead more than kMaxReadaheadSize.
        readahead_size_ = std::min(kMaxReadaheadSize, readahead_size_);
        table_->get_rep()->file->Prefetch(data_block_handle.offset(),
                                          readahead_size_);
        readahead_limit_ = static_cast<size_t>(data_block_handle.offset()
          + readahead_size_);
        // Keep exponentially increasing readahead size until kMaxReadaheadSize.
        readahead_size_ *= 2;
      }
    }

    BlockBasedTable::NewDataBlockIterator(rep, read_options_, data_block_handle,
                                          &data_block_iter_, false,
                                          /* get_context */ nullptr, s);
    block_iter_points_to_real_block_ = true;
  }
}

void BlockBasedTableIterator::FindKeyForward() {
  is_out_of_bound_ = false;
  // TODO the while loop inherits from two-level-iterator. We don't know
  // whether a block can be empty so it can be replaced by an "if".
  while (!data_block_iter_.Valid()) {
    if (!data_block_iter_.status().ok()) {
      return;
    }
    ResetDataIter();
    // We used to check the current index key for upperbound.
    // It will only save a data reading for a small percentage of use cases,
    // so for code simplicity, we removed it. We can add it back if there is a
    // significnat performance regression.
    index_iter_->Next();

    if (index_iter_->Valid()) {
      InitDataBlock();
      data_block_iter_.SeekToFirst();
    } else {
      return;
    }
  }

  // Check upper bound on the current key
  bool reached_upper_bound =
      (read_options_.iterate_upper_bound != nullptr &&
       block_iter_points_to_real_block_ && data_block_iter_.Valid() &&
       icomp_.user_comparator()->Compare(ExtractUserKey(data_block_iter_.key()),
                                         *read_options_.iterate_upper_bound) >=
           0);
  TEST_SYNC_POINT_CALLBACK(
      "BlockBasedTable::BlockEntryIteratorState::KeyReachedUpperBound",
      &reached_upper_bound);
  if (reached_upper_bound) {
    is_out_of_bound_ = true;
    ResetDataIter();
    return;
  }
}

void BlockBasedTableIterator::FindKeyBackward() {
  while (!data_block_iter_.Valid()) {
    if (!data_block_iter_.status().ok()) {
      return;
    }

    ResetDataIter();
    index_iter_->Prev();

    if (index_iter_->Valid()) {
      InitDataBlock();
      data_block_iter_.SeekToLast();
    } else {
      return;
    }
  }

  // We could have check lower bound here too, but we opt not to do it for
  // code simplicity.
}

InternalIterator* BlockBasedTable::NewIterator(const ReadOptions& read_options,
                                               Arena* arena,
                                               bool skip_filters) {
  if (arena == nullptr) {
    return new BlockBasedTableIterator(
        this, read_options, rep_->internal_comparator,
        NewIndexIterator(read_options),
        !skip_filters && !read_options.total_order_seek &&
            rep_->ioptions.prefix_extractor != nullptr);
  } else {
    auto* mem = arena->AllocateAligned(sizeof(BlockBasedTableIterator));
    return new (mem) BlockBasedTableIterator(
        this, read_options, rep_->internal_comparator,
        NewIndexIterator(read_options),
        !skip_filters && !read_options.total_order_seek &&
            rep_->ioptions.prefix_extractor != nullptr);
  }
}

InternalIterator* BlockBasedTable::NewRangeTombstoneIterator(
    const ReadOptions& read_options) {
  if (rep_->range_del_handle.IsNull()) {
    // The block didn't exist, nullptr indicates no range tombstones.
    return nullptr;
  }
  if (rep_->range_del_entry.cache_handle != nullptr) {
    // We have a handle to an uncompressed block cache entry that's held for
    // this table's lifetime. Increment its refcount before returning an
    // iterator based on it since the returned iterator may outlive this table
    // reader.
    assert(rep_->range_del_entry.value != nullptr);
    Cache* block_cache = rep_->table_options.block_cache.get();
    assert(block_cache != nullptr);
    if (block_cache->Ref(rep_->range_del_entry.cache_handle)) {
      auto iter = rep_->range_del_entry.value->NewIterator(
          &rep_->internal_comparator, nullptr /* iter */,
          true /* total_order_seek */, rep_->ioptions.statistics);
      iter->RegisterCleanup(&ReleaseCachedEntry, block_cache,
                            rep_->range_del_entry.cache_handle);
      return iter;
    }
  }
  std::string str;
  rep_->range_del_handle.EncodeTo(&str);
  // The meta-block exists but isn't in uncompressed block cache (maybe
  // because it is disabled), so go through the full lookup process.
  return NewDataBlockIterator(rep_, read_options, Slice(str));
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
  bool may_match = true;
  if (filter->whole_key_filtering()) {
    may_match = filter->KeyMayMatch(user_key, kNotValid, no_io, const_ikey_ptr);
  } else if (!read_options.total_order_seek &&
             rep_->ioptions.prefix_extractor &&
             rep_->table_properties->prefix_extractor_name.compare(
                 rep_->ioptions.prefix_extractor->Name()) == 0 &&
             rep_->ioptions.prefix_extractor->InDomain(user_key) &&
             !filter->PrefixMayMatch(
                 rep_->ioptions.prefix_extractor->Transform(user_key),
                 kNotValid, false, const_ikey_ptr)) {
    may_match = false;
  }
  if (may_match) {
    RecordTick(rep_->ioptions.statistics, BLOOM_FILTER_FULL_POSITIVE);
  }
  return may_match;
}

Status BlockBasedTable::Get(const ReadOptions& read_options, const Slice& key,
                            GetContext* get_context, bool skip_filters) {
  Status s;
  const bool no_io = read_options.read_tier == kBlockCacheTier;
  CachableEntry<FilterBlockReader> filter_entry;
  if (!skip_filters) {
    filter_entry =
        GetFilter(/*prefetch_buffer*/ nullptr,
                  read_options.read_tier == kBlockCacheTier, get_context);
  }
  FilterBlockReader* filter = filter_entry.value;

  // First check the full filter
  // If full filter not useful, Then go into each block
  if (!FullFilterKeyMayMatch(read_options, filter, key, no_io)) {
    RecordTick(rep_->ioptions.statistics, BLOOM_FILTER_USEFUL);
  } else {
    BlockIter iiter_on_stack;
    auto iiter = NewIndexIterator(read_options, &iiter_on_stack,
                                  /* index_entry */ nullptr, get_context);
    std::unique_ptr<InternalIterator> iiter_unique_ptr;
    if (iiter != &iiter_on_stack) {
      iiter_unique_ptr.reset(iiter);
    }

    bool matched = false;  // if such user key mathced a key in SST
    bool done = false;
    for (iiter->Seek(key); iiter->Valid() && !done; iiter->Next()) {
      Slice handle_value = iiter->value();

      BlockHandle handle;
      bool not_exist_in_filter =
          filter != nullptr && filter->IsBlockBased() == true &&
          handle.DecodeFrom(&handle_value).ok() &&
          !filter->KeyMayMatch(ExtractUserKey(key), handle.offset(), no_io);

      if (not_exist_in_filter) {
        // Not found
        // TODO: think about interaction with Merge. If a user key cannot
        // cross one data block, we should be fine.
        RecordTick(rep_->ioptions.statistics, BLOOM_FILTER_USEFUL);
        break;
      } else {
        BlockIter biter;
        NewDataBlockIterator(rep_, read_options, iiter->value(), &biter, false,
                             get_context);

        if (read_options.read_tier == kBlockCacheTier &&
            biter.status().IsIncomplete()) {
          // couldn't get block from block_cache
          // Update Saver.state to Found because we are only looking for
          // whether we can guarantee the key is not there when "no_io" is set
          get_context->MarkKeyMayExist();
          break;
        }
        if (!biter.status().ok()) {
          s = biter.status();
          break;
        }

        // Call the *saver function on each entry/block until it returns false
        for (biter.Seek(key); biter.Valid(); biter.Next()) {
          ParsedInternalKey parsed_key;
          if (!ParseInternalKey(biter.key(), &parsed_key)) {
            s = Status::Corruption(Slice());
          }

          if (!get_context->SaveValue(parsed_key, biter.value(), &matched,
                                      &biter)) {
            done = true;
            break;
          }
        }
        s = biter.status();
      }
      if (done) {
        // Avoid the extra Next which is expensive in two-level indexes
        break;
      }
    }
    if (matched && filter != nullptr && !filter->IsBlockBased()) {
      RecordTick(rep_->ioptions.statistics, BLOOM_FILTER_FULL_TRUE_POSITIVE);
    }
    if (s.ok()) {
      s = iiter->status();
    }
  }

  // if rep_->filter_entry is not set, we should call Release(); otherwise
  // don't call, in this case we have a local copy in rep_->filter_entry,
  // it's pinned to the cache and will be released in the destructor
  if (!rep_->filter_entry.IsSet()) {
    filter_entry.Release(rep_->table_options.block_cache.get());
  }
  return s;
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

Status BlockBasedTable::VerifyChecksum() {
  Status s;
  // Check Meta blocks
  std::unique_ptr<Block> meta;
  std::unique_ptr<InternalIterator> meta_iter;
  s = ReadMetaBlock(rep_, nullptr /* prefetch buffer */, &meta, &meta_iter);
  if (s.ok()) {
    s = VerifyChecksumInBlocks(meta_iter.get());
    if (!s.ok()) {
      return s;
    }
  } else {
    return s;
  }
  // Check Data blocks
  BlockIter iiter_on_stack;
  InternalIterator* iiter = NewIndexIterator(ReadOptions(), &iiter_on_stack);
  std::unique_ptr<InternalIterator> iiter_unique_ptr;
  if (iiter != &iiter_on_stack) {
    iiter_unique_ptr = std::unique_ptr<InternalIterator>(iiter);
  }
  if (!iiter->status().ok()) {
    // error opening index iterator
    return iiter->status();
  }
  s = VerifyChecksumInBlocks(iiter);
  return s;
}

Status BlockBasedTable::VerifyChecksumInBlocks(InternalIterator* index_iter) {
  Status s;
  for (index_iter->SeekToFirst(); index_iter->Valid(); index_iter->Next()) {
    s = index_iter->status();
    if (!s.ok()) {
      break;
    }
    BlockHandle handle;
    Slice input = index_iter->value();
    s = handle.DecodeFrom(&input);
    if (!s.ok()) {
      break;
    }
    BlockContents contents;
    Slice dummy_comp_dict;
    BlockFetcher block_fetcher(rep_->file.get(), nullptr /* prefetch buffer */,
                               rep_->footer, ReadOptions(), handle, &contents,
                               rep_->ioptions, false /* decompress */,
                               dummy_comp_dict /*compression dict*/,
                               rep_->persistent_cache_options);
    s = block_fetcher.ReadBlockContents();
    if (!s.ok()) {
      break;
    }
  }
  return s;
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
      GetCacheKey(rep_->cache_key_prefix, rep_->cache_key_prefix_size, handle,
                  cache_key_storage);
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
    FilePrefetchBuffer* prefetch_buffer, IndexReader** index_reader,
    InternalIterator* preloaded_meta_index_iter, int level) {
  // Some old version of block-based tables don't have index type present in
  // table properties. If that's the case we can safely use the kBinarySearch.
  auto index_type_on_file = BlockBasedTableOptions::kBinarySearch;
  if (rep_->table_properties) {
    auto& props = rep_->table_properties->user_collected_properties;
    auto pos = props.find(BlockBasedTablePropertyNames::kIndexType);
    if (pos != props.end()) {
      index_type_on_file = static_cast<BlockBasedTableOptions::IndexType>(
          DecodeFixed32(pos->second.c_str()));
    }
  }

  auto file = rep_->file.get();
  const InternalKeyComparator* icomparator = &rep_->internal_comparator;
  const Footer& footer = rep_->footer;
  if (index_type_on_file == BlockBasedTableOptions::kHashSearch &&
      rep_->ioptions.prefix_extractor == nullptr) {
    ROCKS_LOG_WARN(rep_->ioptions.info_log,
                   "BlockBasedTableOptions::kHashSearch requires "
                   "options.prefix_extractor to be set."
                   " Fall back to binary search index.");
    index_type_on_file = BlockBasedTableOptions::kBinarySearch;
  }

  switch (index_type_on_file) {
    case BlockBasedTableOptions::kTwoLevelIndexSearch: {
      return PartitionIndexReader::Create(
          this, file, prefetch_buffer, footer, footer.index_handle(),
          rep_->ioptions, icomparator, index_reader,
          rep_->persistent_cache_options, level);
    }
    case BlockBasedTableOptions::kBinarySearch: {
      return BinarySearchIndexReader::Create(
          file, prefetch_buffer, footer, footer.index_handle(), rep_->ioptions,
          icomparator, index_reader, rep_->persistent_cache_options);
    }
    case BlockBasedTableOptions::kHashSearch: {
      std::unique_ptr<Block> meta_guard;
      std::unique_ptr<InternalIterator> meta_iter_guard;
      auto meta_index_iter = preloaded_meta_index_iter;
      if (meta_index_iter == nullptr) {
        auto s =
            ReadMetaBlock(rep_, prefetch_buffer, &meta_guard, &meta_iter_guard);
        if (!s.ok()) {
          // we simply fall back to binary search in case there is any
          // problem with prefix hash index loading.
          ROCKS_LOG_WARN(rep_->ioptions.info_log,
                         "Unable to read the metaindex block."
                         " Fall back to binary search index.");
          return BinarySearchIndexReader::Create(
              file, prefetch_buffer, footer, footer.index_handle(),
              rep_->ioptions, icomparator, index_reader,
              rep_->persistent_cache_options);
        }
        meta_index_iter = meta_iter_guard.get();
      }

      return HashIndexReader::Create(
          rep_->internal_prefix_transform.get(), footer, file, prefetch_buffer,
          rep_->ioptions, icomparator, footer.index_handle(), meta_index_iter,
          index_reader, rep_->hash_index_allow_collision,
          rep_->persistent_cache_options);
    }
    default: {
      std::string error_message =
          "Unrecognized index type: " + ToString(index_type_on_file);
      return Status::InvalidArgument(error_message.c_str());
    }
  }
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
  Status s =
      ReadMetaBlock(rep_, nullptr /* prefetch_buffer */, &meta, &meta_iter);
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
        Slice dummy_comp_dict;
        BlockFetcher block_fetcher(
            rep_->file.get(), nullptr /* prefetch_buffer */, rep_->footer,
            ReadOptions(), handle, &block, rep_->ioptions, false /*decompress*/,
            dummy_comp_dict /*compression dict*/,
            rep_->persistent_cache_options);
        s = block_fetcher.ReadBlockContents();
        if (!s.ok()) {
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
  if (rep_->closed) {
    return;
  }
  rep_->filter_entry.Release(rep_->table_options.block_cache.get());
  rep_->index_entry.Release(rep_->table_options.block_cache.get());
  rep_->range_del_entry.Release(rep_->table_options.block_cache.get());
  // cleanup index and filter blocks to avoid accessing dangling pointer
  if (!rep_->table_options.no_block_cache) {
    char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    // Get the filter block key
    auto key = GetCacheKey(rep_->cache_key_prefix, rep_->cache_key_prefix_size,
                           rep_->filter_handle, cache_key);
    rep_->table_options.block_cache.get()->Erase(key);
    // Get the index block key
    key = GetCacheKeyFromOffset(rep_->cache_key_prefix,
                                rep_->cache_key_prefix_size,
                                rep_->dummy_index_reader_offset, cache_key);
    rep_->table_options.block_cache.get()->Erase(key);
  }
  rep_->closed = true;
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
    if (str_key[i] == '\0') {
      res_key.append("\\0", 2);
    } else {
      res_key.append(&str_key[i], 1);
    }
    res_key.append(1, cspace);
  }
  for (size_t i = 0; i < str_value.size(); i++) {
    if (str_value[i] == '\0') {
      res_value.append("\\0", 2);
    } else {
      res_value.append(&str_value[i], 1);
    }
    res_value.append(1, cspace);
  }

  out_file->Append("  ASCII  ");
  out_file->Append(res_key.c_str());
  out_file->Append(": ");
  out_file->Append(res_value.c_str());
  out_file->Append("\n  ------\n");
}

namespace {

void DeleteCachedFilterEntry(const Slice& /*key*/, void* value) {
  FilterBlockReader* filter = reinterpret_cast<FilterBlockReader*>(value);
  if (filter->statistics() != nullptr) {
    RecordTick(filter->statistics(), BLOCK_CACHE_FILTER_BYTES_EVICT,
               filter->size());
  }
  delete filter;
}

void DeleteCachedIndexEntry(const Slice& /*key*/, void* value) {
  IndexReader* index_reader = reinterpret_cast<IndexReader*>(value);
  if (index_reader->statistics() != nullptr) {
    RecordTick(index_reader->statistics(), BLOCK_CACHE_INDEX_BYTES_EVICT,
               index_reader->usable_size());
  }
  delete index_reader;
}

}  // anonymous namespace

}  // namespace rocksdb
