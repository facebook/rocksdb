//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/block_based_table_reader.h"

#include "db/dbformat.h"

#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"

#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"

#include "util/coding.h"
#include "util/perf_context_imp.h"
#include "util/stop_watch.h"
#include "table/block_based_table_options.h"

namespace rocksdb {

// The longest the prefix of the cache key used to identify blocks can be.
// We are using the fact that we know for Posix files the unique ID is three
// varints.
const size_t kMaxCacheKeyPrefixSize = kMaxVarint64Length*3+1;
using std::unique_ptr;

struct BlockBasedTable::Rep {
  Rep(const EnvOptions& storage_options) :
    soptions(storage_options) {
  }

  Options options;
  const EnvOptions& soptions;
  Status status;
  unique_ptr<RandomAccessFile> file;
  char cache_key_prefix[kMaxCacheKeyPrefixSize];
  size_t cache_key_prefix_size = 0;
  char compressed_cache_key_prefix[kMaxCacheKeyPrefixSize];
  size_t compressed_cache_key_prefix_size = 0;

  // Handle to metaindex_block: saved from footer
  BlockHandle metaindex_handle;
  // Handle to index: saved from footer
  BlockHandle index_handle;
  // index_block will be populated and used only when options.block_cache is
  // NULL; otherwise we will get the index block via the block cache.
  unique_ptr<Block> index_block;
  unique_ptr<FilterBlockReader> filter;

  TableProperties table_properties;
};

BlockBasedTable::~BlockBasedTable() {
  delete rep_;
}

// CachableEntry represents the entries that *may* be fetched from block cache.
//  field `value` is the item we want to get.
//  field `cache_handle` is the cache handle to the block cache. If the value
//    was not read from cache, `cache_handle` will be nullptr.
template <class TValue>
struct BlockBasedTable::CachableEntry {
  CachableEntry(TValue* value, Cache::Handle* cache_handle)
    : value(value)
    , cache_handle(cache_handle) {
  }
  CachableEntry(): CachableEntry(nullptr, nullptr) { }
  void Release(Cache* cache) {
    if (cache_handle) {
      cache->Release(cache_handle);
      value = nullptr;
      cache_handle = nullptr;
    }
  }

  TValue* value = nullptr;
  // if the entry is from the cache, cache_handle will be populated.
  Cache::Handle* cache_handle = nullptr;
};

// Helper function to setup the cache key's prefix for the Table.
void BlockBasedTable::SetupCacheKeyPrefix(Rep* rep) {
  assert(kMaxCacheKeyPrefixSize >= 10);
  rep->cache_key_prefix_size = 0;
  rep->compressed_cache_key_prefix_size = 0;
  if (rep->options.block_cache != nullptr) {
    GenerateCachePrefix(rep->options.block_cache.get(), rep->file.get(),
                        &rep->cache_key_prefix[0],
                        &rep->cache_key_prefix_size);
  }
  if (rep->options.block_cache_compressed != nullptr) {
    GenerateCachePrefix(rep->options.block_cache_compressed.get(),
                        rep->file.get(), &rep->compressed_cache_key_prefix[0],
                        &rep->compressed_cache_key_prefix_size);
  }
}

void BlockBasedTable::GenerateCachePrefix(Cache* cc,
    RandomAccessFile* file, char* buffer, size_t* size) {

  // generate an id from the file
  *size = file->GetUniqueId(buffer, kMaxCacheKeyPrefixSize);

  // If the prefix wasn't generated or was too long,
  // create one from the cache.
  if (*size == 0) {
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

namespace {  // anonymous namespace, not visible externally

// Read the block identified by "handle" from "file".
// The only relevant option is options.verify_checksums for now.
// Set *didIO to true if didIO is not null.
// On failure return non-OK.
// On success fill *result and return OK - caller owns *result
Status ReadBlockFromFile(
    RandomAccessFile* file,
    const ReadOptions& options,
    const BlockHandle& handle,
    Block** result,
    Env* env,
    bool* didIO = nullptr,
    bool do_uncompress = true) {
  BlockContents contents;
  Status s = ReadBlockContents(file, options, handle, &contents,
                               env, do_uncompress);
  if (s.ok()) {
    *result = new Block(contents);
  }

  if (didIO) {
    *didIO = true;
  }
  return s;
}

void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

void DeleteCachedFilter(const Slice& key, void* value) {
  auto filter = reinterpret_cast<FilterBlockReader*>(value);
  delete filter;
}

void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

Slice GetCacheKey(const char* cache_key_prefix,
                  size_t cache_key_prefix_size,
                  const BlockHandle& handle,
                  char* cache_key) {
  assert(cache_key != nullptr);
  assert(cache_key_prefix_size != 0);
  assert(cache_key_prefix_size <= kMaxCacheKeyPrefixSize);
  memcpy(cache_key, cache_key_prefix, cache_key_prefix_size);
  char* end = EncodeVarint64(cache_key + cache_key_prefix_size,
                             handle.offset());
  return Slice(cache_key, static_cast<size_t>(end - cache_key));
}

Cache::Handle* GetFromBlockCache(
    Cache* block_cache,
    const Slice& key,
    Tickers block_cache_miss_ticker,
    Tickers block_cache_hit_ticker,
    Statistics* statistics) {
  auto cache_handle = block_cache->Lookup(key);
  if (cache_handle != nullptr) {
    BumpPerfCount(&perf_context.block_cache_hit_count);
    // overall cache hit
    RecordTick(statistics, BLOCK_CACHE_HIT);
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

} // end of anonymous namespace

Status BlockBasedTable::Open(const Options& options, const EnvOptions& soptions,
                             const BlockBasedTableOptions& table_options,
                             unique_ptr<RandomAccessFile>&& file,
                             uint64_t file_size,
                             unique_ptr<TableReader>* table_reader) {
  table_reader->reset();

  if (file_size < Footer::kEncodedLength) {
    return Status::InvalidArgument("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(file_size - Footer::kEncodedLength,
                        Footer::kEncodedLength, &footer_input, footer_space);
  if (!s.ok()) return s;

  // Check that we actually read the whole footer from the file. It may be
  // that size isn't correct.
  if (footer_input.size() != Footer::kEncodedLength) {
    return Status::InvalidArgument("file is too short to be an sstable");
  }

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // We've successfully read the footer and the index block: we're
  // ready to serve requests.
  Rep* rep = new BlockBasedTable::Rep(soptions);
  rep->options = options;
  rep->file = std::move(file);
  rep->metaindex_handle = footer.metaindex_handle();
  rep->index_handle = footer.index_handle();
  SetupCacheKeyPrefix(rep);
  unique_ptr<BlockBasedTable> new_table(new BlockBasedTable(rep));

  // Read meta index
  std::unique_ptr<Block> meta;
  std::unique_ptr<Iterator> meta_iter;
  s = ReadMetaBlock(rep, &meta, &meta_iter);

  // Read the properties
  meta_iter->Seek(kPropertiesBlock);
  if (meta_iter->Valid() && meta_iter->key() == Slice(kPropertiesBlock)) {
    s = meta_iter->status();
    if (s.ok()) {
      s = ReadProperties(meta_iter->value(), rep, &rep->table_properties);
    }

    if (!s.ok()) {
      auto err_msg =
        "[Warning] Encountered error while reading data from properties "
        "block " + s.ToString();
      Log(rep->options.info_log, "%s", err_msg.c_str());
    }
  }

  // Will use block cache for index/filter blocks access?
  if (options.block_cache && table_options.cache_index_and_filter_blocks) {
    // Call IndexBlockReader() to implicitly add index to the block_cache
    unique_ptr<Iterator> iter(new_table->IndexBlockReader(ReadOptions()));
    s = iter->status();

    if (s.ok()) {
      // Call GetFilter() to implicitly add filter to the block_cache
      auto filter_entry = new_table->GetFilter();
      filter_entry.Release(options.block_cache.get());
    }
  } else {
    // If we don't use block cache for index/filter blocks access, we'll
    // pre-load these blocks, which will kept in member variables in Rep
    // and with a same life-time as this table object.
    Block* index_block = nullptr;
    // TODO: we never really verify check sum for index block
    s = ReadBlockFromFile(
        rep->file.get(),
        ReadOptions(),
        footer.index_handle(),
        &index_block,
        options.env
    );

    if (s.ok()) {
      assert(index_block->compressionType() == kNoCompression);
      rep->index_block.reset(index_block);

      // Set index block
      if (rep->options.filter_policy) {
        std::string key = kFilterBlockPrefix;
        key.append(rep->options.filter_policy->Name());
        meta_iter->Seek(key);

        if (meta_iter->Valid() && meta_iter->key() == Slice(key)) {
          rep->filter.reset(ReadFilter(meta_iter->value(), rep));
        }
      }
    } else {
      delete index_block;
    }

  }

  if (s.ok()) {
    *table_reader = std::move(new_table);
  }

  return s;
}

void BlockBasedTable::SetupForCompaction() {
  switch (rep_->options.access_hint_on_compaction_start) {
    case Options::NONE:
      break;
    case Options::NORMAL:
      rep_->file->Hint(RandomAccessFile::NORMAL);
      break;
    case Options::SEQUENTIAL:
      rep_->file->Hint(RandomAccessFile::SEQUENTIAL);
      break;
    case Options::WILLNEED:
      rep_->file->Hint(RandomAccessFile::WILLNEED);
      break;
    default:
      assert(false);
  }
  compaction_optimized_ = true;
}

TableProperties& BlockBasedTable::GetTableProperties() {
  return rep_->table_properties;
}

// Load the meta-block from the file. On success, return the loaded meta block
// and its iterator.
Status BlockBasedTable::ReadMetaBlock(
    Rep* rep,
    std::unique_ptr<Block>* meta_block,
    std::unique_ptr<Iterator>* iter) {
  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  //  TODO: we never really verify check sum for meta index block
  Block* meta = nullptr;
  Status s = ReadBlockFromFile(
      rep->file.get(),
      ReadOptions(),
      rep->metaindex_handle,
      &meta,
      rep->options.env);

    if (!s.ok()) {
      auto err_msg =
        "[Warning] Encountered error while reading data from properties"
        "block " + s.ToString();
      Log(rep->options.info_log, "%s", err_msg.c_str());
    }
  if (!s.ok()) {
    delete meta;
    return s;
  }

  meta_block->reset(meta);
  // meta block uses bytewise comparator.
  iter->reset(meta->NewIterator(BytewiseComparator()));
  return Status::OK();
}

FilterBlockReader* BlockBasedTable::ReadFilter (
    const Slice& filter_handle_value,
    BlockBasedTable::Rep* rep,
    size_t* filter_size) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return nullptr;
  }

  // TODO: We might want to unify with ReadBlockFromFile() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  BlockContents block;
  if (!ReadBlockContents(rep->file.get(), opt, filter_handle, &block,
                        rep->options.env, false).ok()) {
    return nullptr;
  }

  if (filter_size) {
    *filter_size = block.data.size();
  }

  return new FilterBlockReader(
       rep->options, block.data, block.heap_allocated);
}

Status BlockBasedTable::ReadProperties(
    const Slice& handle_value, Rep* rep, TableProperties* table_properties) {
  assert(table_properties);

  Slice v = handle_value;
  BlockHandle handle;
  if (!handle.DecodeFrom(&v).ok()) {
    return Status::InvalidArgument("Failed to decode properties block handle");
  }

  BlockContents block_contents;
  Status s = ReadBlockContents(
      rep->file.get(),
      ReadOptions(),
      handle,
      &block_contents,
      rep->options.env,
      false
  );

  if (!s.ok()) {
    return s;
  }

  Block properties_block(block_contents);
  std::unique_ptr<Iterator> iter(
      properties_block.NewIterator(BytewiseComparator())
  );

  // All pre-defined properties of type uint64_t
  std::unordered_map<std::string, uint64_t*> predefined_uint64_properties = {
    { BlockBasedTablePropertiesNames::kDataSize,
      &table_properties->data_size },
    { BlockBasedTablePropertiesNames::kIndexSize,
      &table_properties->index_size },
    { BlockBasedTablePropertiesNames::kFilterSize,
      &table_properties->filter_size },
    { BlockBasedTablePropertiesNames::kRawKeySize,
      &table_properties->raw_key_size },
    { BlockBasedTablePropertiesNames::kRawValueSize,
      &table_properties->raw_value_size },
    { BlockBasedTablePropertiesNames::kNumDataBlocks,
      &table_properties->num_data_blocks },
    { BlockBasedTablePropertiesNames::kNumEntries,
      &table_properties->num_entries },
  };

  std::string last_key;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    s = iter->status();
    if (!s.ok()) {
      break;
    }

    auto key = iter->key().ToString();
    // properties block is strictly sorted with no duplicate key.
    assert(
        last_key.empty() ||
        BytewiseComparator()->Compare(key, last_key) > 0
    );
    last_key = key;

    auto raw_val = iter->value();
    auto pos = predefined_uint64_properties.find(key);

    if (pos != predefined_uint64_properties.end()) {
      // handle predefined rocksdb properties
      uint64_t val;
      if (!GetVarint64(&raw_val, &val)) {
        // skip malformed value
        auto error_msg =
          "[Warning] detect malformed value in properties meta-block:"
          "\tkey: " + key + "\tval: " + raw_val.ToString();
        Log(rep->options.info_log, "%s", error_msg.c_str());
        continue;
      }
      *(pos->second) = val;
    } else if (key == BlockBasedTablePropertiesNames::kFilterPolicy) {
      table_properties->filter_policy_name = raw_val.ToString();
    } else {
      // handle user-collected
      table_properties->user_collected_properties.insert(
          std::make_pair(key, raw_val.ToString())
      );
    }
  }

  return s;
}

Status BlockBasedTable::GetBlock(
    const BlockBasedTable* table,
    const BlockHandle& handle,
    const ReadOptions& options,
    const bool for_compaction,
    const Tickers block_cache_miss_ticker,
    const Tickers block_cache_hit_ticker,
    bool* didIO,
    CachableEntry<Block>* entry) {
  bool no_io = options.read_tier == kBlockCacheTier;
  Cache* block_cache = table->rep_->options.block_cache.get();
  Statistics* statistics = table->rep_->options.statistics.get();
  Status s;

  if (block_cache != nullptr) {
    char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    auto key = GetCacheKey(
        table->rep_->cache_key_prefix,
        table->rep_->cache_key_prefix_size,
        handle,
        cache_key
    );

    entry->cache_handle = GetFromBlockCache(
        block_cache,
        key,
        block_cache_miss_ticker,
        block_cache_hit_ticker,
        statistics
    );

    if (entry->cache_handle != nullptr) {
      entry->value =
        reinterpret_cast<Block*>(block_cache->Value(entry->cache_handle));
    } else if (no_io) {
      // Did not find in block_cache and can't do IO
      return Status::Incomplete("no blocking io");
    } else {
      Histograms histogram = for_compaction ?
        READ_BLOCK_COMPACTION_MICROS : READ_BLOCK_GET_MICROS;
      {
        // block for stop watch
        StopWatch sw(table->rep_->options.env, statistics, histogram);
        s = ReadBlockFromFile(
              table->rep_->file.get(),
              options,
              handle,
              &entry->value,
              table->rep_->options.env,
              didIO
            );
      }
      if (s.ok()) {
        if (options.fill_cache && entry->value->isCachable()) {
          entry->cache_handle = block_cache->Insert(
            key, entry->value, entry->value->size(), &DeleteCachedBlock);
          RecordTick(statistics, BLOCK_CACHE_ADD);
        }
      }
    }
  } else if (no_io) {
    // Could not read from block_cache and can't do IO
    return Status::Incomplete("no blocking io");
  } else {
    s = ReadBlockFromFile(
        table->rep_->file.get(),
        options,
        handle,
        &entry->value,
        table->rep_->options.env,
        didIO
      );
  }

  return s;
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator* BlockBasedTable::BlockReader(void* arg,
                                       const ReadOptions& options,
                                       const Slice& index_value,
                                       bool* didIO,
                                       bool for_compaction) {
  const bool no_io = (options.read_tier == kBlockCacheTier);
  BlockBasedTable* table = reinterpret_cast<BlockBasedTable*>(arg);
  Cache* block_cache = table->rep_->options.block_cache.get();
  Cache* block_cache_compressed = table->rep_->options.
                                    block_cache_compressed.get();
  Statistics* statistics = table->rep_->options.statistics.get();
  Block* block = nullptr;
  Block* cblock = nullptr;
  Cache::Handle* cache_handle = nullptr;
  Cache::Handle* compressed_cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  if (block_cache != nullptr || block_cache_compressed != nullptr) {
    char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    char compressed_cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    Slice key,  /* key to the block cache */
          ckey /* key to the compressed block cache */ ;

    // create key for block cache
    if (block_cache != nullptr) {
      key = GetCacheKey(
          table->rep_->cache_key_prefix,
          table->rep_->cache_key_prefix_size,
          handle,
          cache_key
      );
    }

    if (block_cache_compressed != nullptr) {
      ckey = GetCacheKey(
          table->rep_->compressed_cache_key_prefix,
          table->rep_->compressed_cache_key_prefix_size,
          handle,
          compressed_cache_key
      );
    }

    // Lookup uncompressed cache first
    if (block_cache != nullptr) {
      assert(!key.empty());
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
        RecordTick(statistics, BLOCK_CACHE_HIT);
        RecordTick(statistics, BLOCK_CACHE_DATA_HIT);
      } else {
        RecordTick(statistics, BLOCK_CACHE_MISS);
        RecordTick(statistics, BLOCK_CACHE_DATA_MISS);
      }
    }

    // If not found in uncompressed cache, lookup compressed cache
    if (block == nullptr && block_cache_compressed != nullptr) {
      assert(!ckey.empty());
      compressed_cache_handle = block_cache_compressed->Lookup(ckey);

      // if we found in the compressed cache, then uncompress and
      // insert into uncompressed cache
      if (compressed_cache_handle != nullptr) {
        // found compressed block
        cblock = reinterpret_cast<Block*>(block_cache_compressed->
                        Value(compressed_cache_handle));
        assert(cblock->compressionType() != kNoCompression);

        // Retrieve the uncompressed contents into a new buffer
        BlockContents contents;
        s = UncompressBlockContents(cblock->data(), cblock->size(),
                                    &contents);

        // Insert uncompressed block into block cache
        if (s.ok()) {
          block = new Block(contents); // uncompressed block
          assert(block->compressionType() == kNoCompression);
          if (block_cache != nullptr && block->isCachable() &&
              options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
            assert(reinterpret_cast<Block*>(block_cache->Value(cache_handle))
                   == block);
          }
        }
        // Release hold on compressed cache entry
        block_cache_compressed->Release(compressed_cache_handle);
        RecordTick(statistics, BLOCK_CACHE_COMPRESSED_HIT);
      }
    }

    if (block != nullptr) {
      BumpPerfCount(&perf_context.block_cache_hit_count);
    } else if (no_io) {
      // Did not find in block_cache and can't do IO
      return NewErrorIterator(Status::Incomplete("no blocking io"));
    } else {
      Histograms histogram = for_compaction ?
        READ_BLOCK_COMPACTION_MICROS : READ_BLOCK_GET_MICROS;
      {  // block for stop watch
        StopWatch sw(table->rep_->options.env, statistics, histogram);
        s = ReadBlockFromFile(
              table->rep_->file.get(),
              options,
              handle,
              &cblock,
              table->rep_->options.env,
              didIO,
              block_cache_compressed == nullptr
            );
      }
      if (s.ok()) {
        assert(cblock->compressionType() == kNoCompression ||
               block_cache_compressed != nullptr);

        // Retrieve the uncompressed contents into a new buffer
        BlockContents contents;
        if (cblock->compressionType() != kNoCompression) {
          s = UncompressBlockContents(cblock->data(), cblock->size(),
                                      &contents);
        }
        if (s.ok()) {
          if (cblock->compressionType() != kNoCompression) {
            block = new Block(contents); // uncompressed block
          } else {
            block = cblock;
            cblock = nullptr;
          }
          if (block->isCachable() && options.fill_cache) {
            // Insert compressed block into compressed block cache.
            // Release the hold on the compressed cache entry immediately.
            if (block_cache_compressed != nullptr && cblock != nullptr) {
              compressed_cache_handle = block_cache_compressed->Insert(
                          ckey, cblock, cblock->size(), &DeleteCachedBlock);
              block_cache_compressed->Release(compressed_cache_handle);
              RecordTick(statistics, BLOCK_CACHE_COMPRESSED_MISS);
              cblock = nullptr;
            }
            // insert into uncompressed block cache
            assert((block->compressionType() == kNoCompression));
            if (block_cache != nullptr) {
              cache_handle = block_cache->Insert(
                key, block, block->size(), &DeleteCachedBlock);
              RecordTick(statistics, BLOCK_CACHE_ADD);
              assert(reinterpret_cast<Block*>(block_cache->Value(
                     cache_handle))== block);
            }
          }
        }
      }
      if (cblock != nullptr) {
        delete cblock;
      }
    }
  } else if (no_io) {
    // Could not read from block_cache and can't do IO
    return NewErrorIterator(Status::Incomplete("no blocking io"));
  } else {
    s = ReadBlockFromFile(
        table->rep_->file.get(),
        options,
        handle,
        &block,
        table->rep_->options.env,
        didIO
      );
  }

  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle != nullptr) {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    } else {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

BlockBasedTable::CachableEntry<FilterBlockReader>
BlockBasedTable::GetFilter(bool no_io) const {
  if (!rep_->options.filter_policy || !rep_->options.block_cache) {
    return {rep_->filter.get(), nullptr};
  }

  // Fetching from the cache
  Cache* block_cache = rep_->options.block_cache.get();
  char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  auto key = GetCacheKey(
      rep_->cache_key_prefix,
      rep_->cache_key_prefix_size,
      rep_->metaindex_handle,
      cache_key
  );

  Statistics* statistics = rep_->options.statistics.get();
  auto cache_handle = GetFromBlockCache(
    block_cache,
    key,
    BLOCK_CACHE_FILTER_MISS,
    BLOCK_CACHE_FILTER_HIT,
    statistics
  );

  FilterBlockReader* filter = nullptr;
  if (cache_handle != nullptr) {
     filter = reinterpret_cast<FilterBlockReader*>(
         block_cache->Value(cache_handle));
  } else if (no_io) {
    // Do not invoke any io.
    return CachableEntry<FilterBlockReader>();
  } else {
    size_t filter_size = 0;
    std::unique_ptr<Block> meta;
    std::unique_ptr<Iterator> iter;
    auto s = ReadMetaBlock(rep_, &meta, &iter);

    if (s.ok()) {
      std::string filter_block_key = kFilterBlockPrefix;
      filter_block_key.append(rep_->options.filter_policy->Name());
      iter->Seek(filter_block_key);

      if (iter->Valid() && iter->key() == Slice(filter_block_key)) {
        filter = ReadFilter(iter->value(), rep_, &filter_size);
        assert(filter);
        assert(filter_size > 0);

        cache_handle = block_cache->Insert(
          key, filter, filter_size, &DeleteCachedFilter);
        RecordTick(statistics, BLOCK_CACHE_ADD);
      }
    }
  }

  return { filter, cache_handle };
}

// Get the iterator from the index block.
Iterator* BlockBasedTable::IndexBlockReader(const ReadOptions& options) const {
  if (rep_->index_block) {
    return rep_->index_block->NewIterator(rep_->options.comparator);
  }

  // get index block from cache
  assert (rep_->options.block_cache);
  bool didIO = false;
  CachableEntry<Block> entry;

  auto s = GetBlock(
      this,
      rep_->index_handle,
      options,
      false,  /* for compaction */
      BLOCK_CACHE_INDEX_MISS,
      BLOCK_CACHE_INDEX_HIT,
      &didIO,
      &entry
  );

  Iterator* iter;
  if (entry.value != nullptr) {
    iter = entry.value->NewIterator(rep_->options.comparator);
    if (entry.cache_handle) {
      iter->RegisterCleanup(
          &ReleaseBlock, rep_->options.block_cache.get(), entry.cache_handle
      );
    } else {
      iter->RegisterCleanup(&DeleteBlock, entry.value, nullptr);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* BlockBasedTable::BlockReader(void* arg,
                                       const ReadOptions& options,
                                       const EnvOptions& soptions,
                                       const Slice& index_value,
                                       bool for_compaction) {
  return BlockReader(arg, options, index_value, nullptr, for_compaction);
}

// This will be broken if the user specifies an unusual implementation
// of Options.comparator, or if the user specifies an unusual
// definition of prefixes in Options.filter_policy.  In particular, we
// require the following three properties:
//
// 1) key.starts_with(prefix(key))
// 2) Compare(prefix(key), key) <= 0.
// 3) If Compare(key1, key2) <= 0, then Compare(prefix(key1), prefix(key2)) <= 0
//
// Otherwise, this method guarantees no I/O will be incurred.
//
// REQUIRES: this method shouldn't be called while the DB lock is held.
bool BlockBasedTable::PrefixMayMatch(const Slice& internal_prefix) {
  bool may_match = true;
  Status s;

  if (!rep_->options.filter_policy) {
    return true;
  }

  // To prevent any io operation in this method, we set `read_tier` to make
  // sure we always read index or filter only when they have already been
  // loaded to memory.
  ReadOptions no_io_read_options;
  no_io_read_options.read_tier = kBlockCacheTier;
  unique_ptr<Iterator> iiter(
      IndexBlockReader(no_io_read_options)
  );
  iiter->Seek(internal_prefix);

  if (!iiter->Valid()) {
    // we're past end of file
    // if it's incomplete, it means that we avoided I/O
    // and we're not really sure that we're past the end
    // of the file
    may_match = iiter->status().IsIncomplete();
  } else if (ExtractUserKey(iiter->key()).starts_with(
              ExtractUserKey(internal_prefix))) {
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
  } else {
    // iiter->key() does NOT start with the desired prefix.  Because
    // Seek() finds the first key that is >= the seek target, this
    // means that iiter->key() > prefix.  Thus, any data blocks coming
    // after the data block corresponding to iiter->key() cannot
    // possibly contain the key.  Thus, the corresponding data block
    // is the only one which could potentially contain the prefix.
    Slice handle_value = iiter->value();
    BlockHandle handle;
    s = handle.DecodeFrom(&handle_value);
    assert(s.ok());
    auto filter_entry = GetFilter(true /* no io */);
    may_match =
      filter_entry.value == nullptr ||
      filter_entry.value->PrefixMayMatch(handle.offset(), internal_prefix);
    filter_entry.Release(rep_->options.block_cache.get());
  }

  Statistics* statistics = rep_->options.statistics.get();
  RecordTick(statistics, BLOOM_FILTER_PREFIX_CHECKED);
  if (!may_match) {
    RecordTick(statistics, BLOOM_FILTER_PREFIX_USEFUL);
  }

  return may_match;
}

Iterator* BlockBasedTable::NewIterator(const ReadOptions& options) {
  if (options.prefix) {
    InternalKey internal_prefix(*options.prefix, 0, kTypeValue);
    if (!PrefixMayMatch(internal_prefix.Encode())) {
      // nothing in this file can match the prefix, so we should not
      // bother doing I/O to this file when iterating.
      return NewEmptyIterator();
    }
  }

  return NewTwoLevelIterator(
           IndexBlockReader(options),
           &BlockBasedTable::BlockReader,
           const_cast<BlockBasedTable*>(this),
           options,
           rep_->soptions
         );
}

Status BlockBasedTable::Get(
    const ReadOptions& readOptions,
    const Slice& key,
    void* handle_context,
    bool (*result_handler)(void* handle_context, const Slice& k,
                           const Slice& v, bool didIO),
    void (*mark_key_may_exist_handler)(void* handle_context)) {
  Status s;
  Iterator* iiter = IndexBlockReader(readOptions);
  auto filter_entry = GetFilter(readOptions.read_tier == kBlockCacheTier);
  FilterBlockReader* filter = filter_entry.value;
  bool done = false;
  for (iiter->Seek(key); iiter->Valid() && !done; iiter->Next()) {
    Slice handle_value = iiter->value();

    BlockHandle handle;
    bool may_not_exist_in_filter =
      filter != nullptr &&
      handle.DecodeFrom(&handle_value).ok() &&
      !filter->KeyMayMatch(handle.offset(), key);

    if (may_not_exist_in_filter) {
      // Not found
      // TODO: think about interaction with Merge. If a user key cannot
      // cross one data block, we should be fine.
      RecordTick(rep_->options.statistics.get(), BLOOM_FILTER_USEFUL);
      break;
    } else {
      bool didIO = false;
      unique_ptr<Iterator> block_iter(
        BlockReader(this, readOptions, iiter->value(), &didIO));

      if (readOptions.read_tier && block_iter->status().IsIncomplete()) {
        // couldn't get block from block_cache
        // Update Saver.state to Found because we are only looking for whether
        // we can guarantee the key is not there when "no_io" is set
        (*mark_key_may_exist_handler)(handle_context);
        break;
      }

      // Call the *saver function on each entry/block until it returns false
      for (block_iter->Seek(key); block_iter->Valid(); block_iter->Next()) {
        if (!(*result_handler)(handle_context, block_iter->key(),
                               block_iter->value(), didIO)) {
          done = true;
          break;
        }
      }
      s = block_iter->status();
    }
  }

  filter_entry.Release(rep_->options.block_cache.get());
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

bool SaveDidIO(void* arg, const Slice& key, const Slice& value, bool didIO) {
  *reinterpret_cast<bool*>(arg) = didIO;
  return false;
}
bool BlockBasedTable::TEST_KeyInCache(const ReadOptions& options,
                                      const Slice& key) {
  // We use Get() as it has logic that checks whether we read the
  // block from the disk or not.
  bool didIO = false;
  Status s = Get(options, key, &didIO, SaveDidIO);
  assert(s.ok());
  return !didIO;
}

uint64_t BlockBasedTable::ApproximateOffsetOf(const Slice& key) {
  Iterator* index_iter = IndexBlockReader(ReadOptions());

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
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

const std::string BlockBasedTable::kFilterBlockPrefix =
    "filter.";
const std::string BlockBasedTable::kPropertiesBlock =
    "rocksdb.properties";
const std::string BlockBasedTablePropertiesNames::kDataSize  =
    "rocksdb.data.size";
const std::string BlockBasedTablePropertiesNames::kIndexSize =
    "rocksdb.index.size";
const std::string BlockBasedTablePropertiesNames::kFilterSize =
    "rocksdb.filter.size";
const std::string BlockBasedTablePropertiesNames::kRawKeySize =
    "rocksdb.raw.key.size";
const std::string BlockBasedTablePropertiesNames::kRawValueSize =
    "rocksdb.raw.value.size";
const std::string BlockBasedTablePropertiesNames::kNumDataBlocks =
    "rocksdb.num.data.blocks";
const std::string BlockBasedTablePropertiesNames::kNumEntries =
    "rocksdb.num.entries";
const std::string BlockBasedTablePropertiesNames::kFilterPolicy =
    "rocksdb.filter.policy";

}  // namespace rocksdb
