//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/block_based_table_reader.h"

#include <string>
#include <utility>

#include "db/dbformat.h"

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
#include "table/filter_block.h"
#include "table/block_hash_index.h"
#include "table/block_prefix_index.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/two_level_iterator.h"

#include "util/coding.h"
#include "util/perf_context_imp.h"
#include "util/stop_watch.h"

namespace rocksdb {

extern const uint64_t kBlockBasedTableMagicNumber;
extern const std::string kHashIndexPrefixesBlock;
extern const std::string kHashIndexPrefixesMetadataBlock;
using std::unique_ptr;

typedef BlockBasedTable::IndexReader IndexReader;

namespace {

// The longest the prefix of the cache key used to identify blocks can be.
// We are using the fact that we know for Posix files the unique ID is three
// varints.
// For some reason, compiling for iOS complains that this variable is unused
const size_t kMaxCacheKeyPrefixSize __attribute__((unused)) =
    kMaxVarint64Length * 3 + 1;

// Read the block identified by "handle" from "file".
// The only relevant option is options.verify_checksums for now.
// On failure return non-OK.
// On success fill *result and return OK - caller owns *result
Status ReadBlockFromFile(RandomAccessFile* file, const Footer& footer,
                         const ReadOptions& options, const BlockHandle& handle,
                         Block** result, Env* env, bool do_uncompress = true) {
  BlockContents contents;
  Status s = ReadBlockContents(file, footer, options, handle, &contents, env,
                               do_uncompress);
  if (s.ok()) {
    *result = new Block(contents);
  }

  return s;
}

// Delete the resource that is held by the iterator.
template <class ResourceType>
void DeleteHeldResource(void* arg, void* ignored) {
  delete reinterpret_cast<ResourceType*>(arg);
}

// Delete the entry resided in the cache.
template <class Entry>
void DeleteCachedEntry(const Slice& key, void* value) {
  auto entry = reinterpret_cast<Entry*>(value);
  delete entry;
}

// Release the cached entry and decrement its ref count.
void ReleaseCachedEntry(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

Slice GetCacheKey(const char* cache_key_prefix, size_t cache_key_prefix_size,
                  const BlockHandle& handle, char* cache_key) {
  assert(cache_key != nullptr);
  assert(cache_key_prefix_size != 0);
  assert(cache_key_prefix_size <= kMaxCacheKeyPrefixSize);
  memcpy(cache_key, cache_key_prefix, cache_key_prefix_size);
  char* end =
      EncodeVarint64(cache_key + cache_key_prefix_size, handle.offset());
  return Slice(cache_key, static_cast<size_t>(end - cache_key));
}

Cache::Handle* GetEntryFromCache(Cache* block_cache, const Slice& key,
                                 Tickers block_cache_miss_ticker,
                                 Tickers block_cache_hit_ticker,
                                 Statistics* statistics) {
  auto cache_handle = block_cache->Lookup(key);
  if (cache_handle != nullptr) {
    PERF_COUNTER_ADD(block_cache_hit_count, 1);
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

}  // namespace

// -- IndexReader and its subclasses
// IndexReader is the interface that provide the functionality for index access.
class BlockBasedTable::IndexReader {
 public:
  explicit IndexReader(const Comparator* comparator)
      : comparator_(comparator) {}

  virtual ~IndexReader() {}

  // Create an iterator for index access.
  // An iter is passed in, if it is not null, update this one and return it
  // If it is null, create a new Iterator
  virtual Iterator* NewIterator(
      BlockIter* iter = nullptr, bool total_order_seek = true) = 0;

  // The size of the index.
  virtual size_t size() const = 0;

  // Report an approximation of how much memory has been used other than memory
  // that was allocated in block cache.
  virtual size_t ApproximateMemoryUsage() const = 0;

 protected:
  const Comparator* comparator_;
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
  static Status Create(RandomAccessFile* file, const Footer& footer,
                       const BlockHandle& index_handle, Env* env,
                       const Comparator* comparator,
                       IndexReader** index_reader) {
    Block* index_block = nullptr;
    auto s = ReadBlockFromFile(file, footer, ReadOptions(), index_handle,
                               &index_block, env);

    if (s.ok()) {
      *index_reader = new BinarySearchIndexReader(comparator, index_block);
    }

    return s;
  }

  virtual Iterator* NewIterator(
      BlockIter* iter = nullptr, bool dont_care = true) override {
    return index_block_->NewIterator(comparator_, iter, true);
  }

  virtual size_t size() const override { return index_block_->size(); }

  virtual size_t ApproximateMemoryUsage() const override {
    assert(index_block_);
    return index_block_->ApproximateMemoryUsage();
  }

 private:
  BinarySearchIndexReader(const Comparator* comparator, Block* index_block)
      : IndexReader(comparator), index_block_(index_block) {
    assert(index_block_ != nullptr);
  }
  std::unique_ptr<Block> index_block_;
};

// Index that leverages an internal hash table to quicken the lookup for a given
// key.
class HashIndexReader : public IndexReader {
 public:
  static Status Create(const SliceTransform* hash_key_extractor,
                       const Footer& footer, RandomAccessFile* file, Env* env,
                       const Comparator* comparator,
                       const BlockHandle& index_handle,
                       Iterator* meta_index_iter, IndexReader** index_reader,
                       bool hash_index_allow_collision) {
    Block* index_block = nullptr;
    auto s = ReadBlockFromFile(file, footer, ReadOptions(), index_handle,
                               &index_block, env);

    if (!s.ok()) {
      return s;
    }

    // Note, failure to create prefix hash index does not need to be a
    // hard error. We can still fall back to the original binary search index.
    // So, Create will succeed regardless, from this point on.

    auto new_index_reader =
        new HashIndexReader(comparator, index_block);
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
                          &prefixes_contents, env, true /* do decompression */);
    if (!s.ok()) {
      return s;
    }
    BlockContents prefixes_meta_contents;
    s = ReadBlockContents(file, footer, ReadOptions(), prefixes_meta_handle,
                          &prefixes_meta_contents, env,
                          true /* do decompression */);
    if (!s.ok()) {
      if (prefixes_contents.heap_allocated) {
        delete[] prefixes_contents.data.data();
      }
      // TODO: log error
      return Status::OK();
    }

    if (!hash_index_allow_collision) {
      // TODO: deprecate once hash_index_allow_collision proves to be stable.
      BlockHashIndex* hash_index = nullptr;
      s = CreateBlockHashIndex(hash_key_extractor,
                               prefixes_contents.data,
                               prefixes_meta_contents.data,
                               &hash_index);
      // TODO: log error
      if (s.ok()) {
        new_index_reader->index_block_->SetBlockHashIndex(hash_index);
        new_index_reader->OwnPrefixesContents(prefixes_contents);
      }
    } else {
      BlockPrefixIndex* prefix_index = nullptr;
      s = BlockPrefixIndex::Create(hash_key_extractor,
                                   prefixes_contents.data,
                                   prefixes_meta_contents.data,
                                   &prefix_index);
      // TODO: log error
      if (s.ok()) {
        new_index_reader->index_block_->SetBlockPrefixIndex(prefix_index);
      }
    }

    // Always release prefix meta block
    if (prefixes_meta_contents.heap_allocated) {
      delete[] prefixes_meta_contents.data.data();
    }

    // Release prefix content block if we don't own it.
    if (!new_index_reader->own_prefixes_contents_) {
      if (prefixes_contents.heap_allocated) {
        delete[] prefixes_contents.data.data();
      }
    }

    return Status::OK();
  }

  virtual Iterator* NewIterator(
      BlockIter* iter = nullptr, bool total_order_seek = true) override {
    return index_block_->NewIterator(comparator_, iter, total_order_seek);
  }

  virtual size_t size() const override { return index_block_->size(); }

  virtual size_t ApproximateMemoryUsage() const override {
    assert(index_block_);
    return index_block_->ApproximateMemoryUsage() +
           prefixes_contents_.data.size();
  }

 private:
  HashIndexReader(const Comparator* comparator, Block* index_block)
      : IndexReader(comparator),
        index_block_(index_block),
        own_prefixes_contents_(false) {
    assert(index_block_ != nullptr);
  }

  ~HashIndexReader() {
    if (own_prefixes_contents_ && prefixes_contents_.heap_allocated) {
      delete[] prefixes_contents_.data.data();
    }
  }

  void OwnPrefixesContents(const BlockContents& prefixes_contents) {
    prefixes_contents_ = prefixes_contents;
    own_prefixes_contents_ = true;
  }

  std::unique_ptr<Block> index_block_;
  bool own_prefixes_contents_;
  BlockContents prefixes_contents_;
};


struct BlockBasedTable::Rep {
  Rep(const EnvOptions& storage_options,
      const BlockBasedTableOptions& table_opt,
      const InternalKeyComparator& internal_comparator)
      : soptions(storage_options), table_options(table_opt),
        filter_policy(table_opt.filter_policy.get()),
        internal_comparator(internal_comparator) {}

  Options options;
  const EnvOptions& soptions;
  const BlockBasedTableOptions& table_options;
  const FilterPolicy* const filter_policy;
  const InternalKeyComparator& internal_comparator;
  Status status;
  unique_ptr<RandomAccessFile> file;
  char cache_key_prefix[kMaxCacheKeyPrefixSize];
  size_t cache_key_prefix_size = 0;
  char compressed_cache_key_prefix[kMaxCacheKeyPrefixSize];
  size_t compressed_cache_key_prefix_size = 0;

  // Footer contains the fixed table information
  Footer footer;
  // index_reader and filter will be populated and used only when
  // options.block_cache is nullptr; otherwise we will get the index block via
  // the block cache.
  unique_ptr<IndexReader> index_reader;
  unique_ptr<FilterBlockReader> filter;

  std::shared_ptr<const TableProperties> table_properties;
  BlockBasedTableOptions::IndexType index_type;
  bool hash_index_allow_collision;
  // TODO(kailiu) It is very ugly to use internal key in table, since table
  // module should not be relying on db module. However to make things easier
  // and compatible with existing code, we introduce a wrapper that allows
  // block to extract prefix without knowing if a key is internal or not.
  unique_ptr<SliceTransform> internal_prefix_transform;
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
  if (rep->table_options.block_cache != nullptr) {
    GenerateCachePrefix(rep->table_options.block_cache.get(), rep->file.get(),
                        &rep->cache_key_prefix[0],
                        &rep->cache_key_prefix_size);
  }
  if (rep->table_options.block_cache_compressed != nullptr) {
    GenerateCachePrefix(rep->table_options.block_cache_compressed.get(),
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

Status BlockBasedTable::Open(const Options& options, const EnvOptions& soptions,
                             const BlockBasedTableOptions& table_options,
                             const InternalKeyComparator& internal_comparator,
                             unique_ptr<RandomAccessFile>&& file,
                             uint64_t file_size,
                             unique_ptr<TableReader>* table_reader) {
  table_reader->reset();

  Footer footer(kBlockBasedTableMagicNumber);
  auto s = ReadFooterFromFile(file.get(), file_size, &footer);
  if (!s.ok()) return s;

  // We've successfully read the footer and the index block: we're
  // ready to serve requests.
  Rep* rep = new BlockBasedTable::Rep(
      soptions, table_options, internal_comparator);
  rep->options = options;
  rep->file = std::move(file);
  rep->footer = footer;
  rep->index_type = table_options.index_type;
  rep->hash_index_allow_collision = table_options.hash_index_allow_collision;
  SetupCacheKeyPrefix(rep);
  unique_ptr<BlockBasedTable> new_table(new BlockBasedTable(rep));

  // Read meta index
  std::unique_ptr<Block> meta;
  std::unique_ptr<Iterator> meta_iter;
  s = ReadMetaBlock(rep, &meta, &meta_iter);

  // Read the properties
  bool found_properties_block = true;
  s = SeekToPropertiesBlock(meta_iter.get(), &found_properties_block);

  if (found_properties_block) {
    s = meta_iter->status();
    TableProperties* table_properties = nullptr;
    if (s.ok()) {
      s = ReadProperties(meta_iter->value(), rep->file.get(), rep->footer,
                         rep->options.env, rep->options.info_log.get(),
                         &table_properties);
    }

    if (!s.ok()) {
      auto err_msg =
        "[Warning] Encountered error while reading data from properties "
        "block " + s.ToString();
      Log(rep->options.info_log, "%s", err_msg.c_str());
    } else {
      rep->table_properties.reset(table_properties);
    }
  } else {
    Log(WARN_LEVEL, rep->options.info_log,
        "Cannot find Properties block from file.");
  }

  // Will use block cache for index/filter blocks access?
  if (table_options.block_cache &&
      table_options.cache_index_and_filter_blocks) {
    // Hack: Call NewIndexIterator() to implicitly add index to the block_cache
    unique_ptr<Iterator> iter(new_table->NewIndexIterator(ReadOptions()));
    s = iter->status();

    if (s.ok()) {
      // Hack: Call GetFilter() to implicitly add filter to the block_cache
      auto filter_entry = new_table->GetFilter();
      filter_entry.Release(table_options.block_cache.get());
    }
  } else {
    // If we don't use block cache for index/filter blocks access, we'll
    // pre-load these blocks, which will kept in member variables in Rep
    // and with a same life-time as this table object.
    IndexReader* index_reader = nullptr;
    // TODO: we never really verify check sum for index block
    s = new_table->CreateIndexReader(&index_reader, meta_iter.get());

    if (s.ok()) {
      rep->index_reader.reset(index_reader);

      // Set filter block
      if (rep->filter_policy) {
        std::string key = kFilterBlockPrefix;
        key.append(rep->filter_policy->Name());
        BlockHandle handle;
        if (FindMetaBlock(meta_iter.get(), key, &handle).ok()) {
          rep->filter.reset(ReadFilter(handle, rep));
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
      rep->footer,
      ReadOptions(),
      rep->footer.metaindex_handle(),
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

Status BlockBasedTable::GetDataBlockFromCache(
    const Slice& block_cache_key, const Slice& compressed_block_cache_key,
    Cache* block_cache, Cache* block_cache_compressed, Statistics* statistics,
    const ReadOptions& read_options,
    BlockBasedTable::CachableEntry<Block>* block) {
  Status s;
  Block* compressed_block = nullptr;
  Cache::Handle* block_cache_compressed_handle = nullptr;

  // Lookup uncompressed cache first
  if (block_cache != nullptr) {
    block->cache_handle =
        GetEntryFromCache(block_cache, block_cache_key, BLOCK_CACHE_DATA_MISS,
                          BLOCK_CACHE_DATA_HIT, statistics);
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
                              compressed_block->size(), &contents);

  // Insert uncompressed block into block cache
  if (s.ok()) {
    block->value = new Block(contents);  // uncompressed block
    assert(block->value->compression_type() == kNoCompression);
    if (block_cache != nullptr && block->value->cachable() &&
        read_options.fill_cache) {
      block->cache_handle =
          block_cache->Insert(block_cache_key, block->value,
                              block->value->size(), &DeleteCachedEntry<Block>);
      assert(reinterpret_cast<Block*>(
                 block_cache->Value(block->cache_handle)) == block->value);
    }
  }

  // Release hold on compressed cache entry
  block_cache_compressed->Release(block_cache_compressed_handle);
  return s;
}

Status BlockBasedTable::PutDataBlockToCache(
    const Slice& block_cache_key, const Slice& compressed_block_cache_key,
    Cache* block_cache, Cache* block_cache_compressed,
    const ReadOptions& read_options, Statistics* statistics,
    CachableEntry<Block>* block, Block* raw_block) {
  assert(raw_block->compression_type() == kNoCompression ||
         block_cache_compressed != nullptr);

  Status s;
  // Retrieve the uncompressed contents into a new buffer
  BlockContents contents;
  if (raw_block->compression_type() != kNoCompression) {
    s = UncompressBlockContents(raw_block->data(), raw_block->size(),
                                &contents);
  }
  if (!s.ok()) {
    delete raw_block;
    return s;
  }

  if (raw_block->compression_type() != kNoCompression) {
    block->value = new Block(contents);  // uncompressed block
  } else {
    block->value = raw_block;
    raw_block = nullptr;
  }

  // Insert compressed block into compressed block cache.
  // Release the hold on the compressed cache entry immediately.
  if (block_cache_compressed != nullptr && raw_block != nullptr &&
      raw_block->cachable()) {
    auto cache_handle = block_cache_compressed->Insert(
        compressed_block_cache_key, raw_block, raw_block->size(),
        &DeleteCachedEntry<Block>);
    block_cache_compressed->Release(cache_handle);
    RecordTick(statistics, BLOCK_CACHE_COMPRESSED_MISS);
    // Avoid the following code to delete this cached block.
    raw_block = nullptr;
  }
  delete raw_block;

  // insert into uncompressed block cache
  assert((block->value->compression_type() == kNoCompression));
  if (block_cache != nullptr && block->value->cachable()) {
    block->cache_handle =
        block_cache->Insert(block_cache_key, block->value, block->value->size(),
                            &DeleteCachedEntry<Block>);
    RecordTick(statistics, BLOCK_CACHE_ADD);
    assert(reinterpret_cast<Block*>(block_cache->Value(block->cache_handle)) ==
           block->value);
  }

  return s;
}

FilterBlockReader* BlockBasedTable::ReadFilter(const BlockHandle& filter_handle,
                                               BlockBasedTable::Rep* rep,
                                               size_t* filter_size) {
  // TODO: We might want to unify with ReadBlockFromFile() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  BlockContents block;
  if (!ReadBlockContents(rep->file.get(), rep->footer, opt, filter_handle,
                         &block, rep->options.env, false).ok()) {
    return nullptr;
  }

  if (filter_size) {
    *filter_size = block.data.size();
  }

  return new FilterBlockReader(
       rep->options, rep->table_options, block.data, block.heap_allocated);
}

BlockBasedTable::CachableEntry<FilterBlockReader> BlockBasedTable::GetFilter(
    bool no_io) const {
  // filter pre-populated
  if (rep_->filter != nullptr) {
    return {rep_->filter.get(), nullptr /* cache handle */};
  }

  Cache* block_cache = rep_->table_options.block_cache.get();
  if (rep_->filter_policy == nullptr /* do not use filter */ ||
      block_cache == nullptr /* no block cache at all */) {
    return {nullptr /* filter */, nullptr /* cache handle */};
  }

  // Fetching from the cache
  char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  auto key = GetCacheKey(
      rep_->cache_key_prefix,
      rep_->cache_key_prefix_size,
      rep_->footer.metaindex_handle(),
      cache_key
  );

  Statistics* statistics = rep_->options.statistics.get();
  auto cache_handle =
      GetEntryFromCache(block_cache, key, BLOCK_CACHE_FILTER_MISS,
                        BLOCK_CACHE_FILTER_HIT, statistics);

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
      filter_block_key.append(rep_->filter_policy->Name());
      BlockHandle handle;
      if (FindMetaBlock(iter.get(), filter_block_key, &handle).ok()) {
        filter = ReadFilter(handle, rep_, &filter_size);
        assert(filter);
        assert(filter_size > 0);

        cache_handle = block_cache->Insert(
            key, filter, filter_size, &DeleteCachedEntry<FilterBlockReader>);
        RecordTick(statistics, BLOCK_CACHE_ADD);
      }
    }
  }

  return { filter, cache_handle };
}

Iterator* BlockBasedTable::NewIndexIterator(const ReadOptions& read_options,
        BlockIter* input_iter) {
  // index reader has already been pre-populated.
  if (rep_->index_reader) {
    return rep_->index_reader->NewIterator(
        input_iter, read_options.total_order_seek);
  }

  bool no_io = read_options.read_tier == kBlockCacheTier;
  Cache* block_cache = rep_->table_options.block_cache.get();
  char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  auto key = GetCacheKey(rep_->cache_key_prefix, rep_->cache_key_prefix_size,
                         rep_->footer.index_handle(), cache_key);
  Statistics* statistics = rep_->options.statistics.get();
  auto cache_handle =
      GetEntryFromCache(block_cache, key, BLOCK_CACHE_INDEX_MISS,
                        BLOCK_CACHE_INDEX_HIT, statistics);

  if (cache_handle == nullptr && no_io) {
    if (input_iter != nullptr) {
      input_iter->SetStatus(Status::Incomplete("no blocking io"));
      return input_iter;
    } else {
      return NewErrorIterator(Status::Incomplete("no blocking io"));
    }
  }

  IndexReader* index_reader = nullptr;
  if (cache_handle != nullptr) {
    index_reader =
        reinterpret_cast<IndexReader*>(block_cache->Value(cache_handle));
  } else {
    // Create index reader and put it in the cache.
    Status s;
    s = CreateIndexReader(&index_reader);

    if (!s.ok()) {
      // make sure if something goes wrong, index_reader shall remain intact.
      assert(index_reader == nullptr);
      if (input_iter != nullptr) {
        input_iter->SetStatus(s);
        return input_iter;
      } else {
        return NewErrorIterator(s);
      }
    }

    cache_handle = block_cache->Insert(key, index_reader, index_reader->size(),
                                       &DeleteCachedEntry<IndexReader>);
    RecordTick(statistics, BLOCK_CACHE_ADD);
  }

  assert(cache_handle);
  auto* iter = index_reader->NewIterator(
      input_iter, read_options.total_order_seek);
  iter->RegisterCleanup(&ReleaseCachedEntry, block_cache, cache_handle);
  return iter;
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// If input_iter is null, new a iterator
// If input_iter is not null, update this iter and return it
Iterator* BlockBasedTable::NewDataBlockIterator(Rep* rep,
    const ReadOptions& ro, const Slice& index_value,
    BlockIter* input_iter) {
  const bool no_io = (ro.read_tier == kBlockCacheTier);
  Cache* block_cache = rep->table_options.block_cache.get();
  Cache* block_cache_compressed =
      rep->table_options.block_cache_compressed.get();
  CachableEntry<Block> block;

  BlockHandle handle;
  Slice input = index_value;
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.
  Status s = handle.DecodeFrom(&input);

  if (!s.ok()) {
    if (input_iter != nullptr) {
      input_iter->SetStatus(s);
      return input_iter;
    } else {
      return NewErrorIterator(s);
    }
  }

  // If either block cache is enabled, we'll try to read from it.
  if (block_cache != nullptr || block_cache_compressed != nullptr) {
    Statistics* statistics = rep->options.statistics.get();
    char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    char compressed_cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    Slice key, /* key to the block cache */
        ckey /* key to the compressed block cache */;

    // create key for block cache
    if (block_cache != nullptr) {
      key = GetCacheKey(rep->cache_key_prefix,
                        rep->cache_key_prefix_size, handle, cache_key);
    }

    if (block_cache_compressed != nullptr) {
      ckey = GetCacheKey(rep->compressed_cache_key_prefix,
                         rep->compressed_cache_key_prefix_size, handle,
                         compressed_cache_key);
    }

    s = GetDataBlockFromCache(key, ckey, block_cache, block_cache_compressed,
                              statistics, ro, &block);

    if (block.value == nullptr && !no_io && ro.fill_cache) {
      Block* raw_block = nullptr;
      {
        StopWatch sw(rep->options.env, statistics, READ_BLOCK_GET_MICROS);
        s = ReadBlockFromFile(rep->file.get(), rep->footer, ro, handle,
                              &raw_block, rep->options.env,
                              block_cache_compressed == nullptr);
      }

      if (s.ok()) {
        s = PutDataBlockToCache(key, ckey, block_cache, block_cache_compressed,
                                ro, statistics, &block, raw_block);
      }
    }
  }

  // Didn't get any data from block caches.
  if (block.value == nullptr) {
    if (no_io) {
      // Could not read from block_cache and can't do IO
      if (input_iter != nullptr) {
        input_iter->SetStatus(Status::Incomplete("no blocking io"));
        return input_iter;
      } else {
        return NewErrorIterator(Status::Incomplete("no blocking io"));
      }
    }
    s = ReadBlockFromFile(rep->file.get(), rep->footer, ro, handle,
                          &block.value, rep->options.env);
  }

  Iterator* iter;
  if (block.value != nullptr) {
    iter = block.value->NewIterator(&rep->internal_comparator, input_iter);
    if (block.cache_handle != nullptr) {
      iter->RegisterCleanup(&ReleaseCachedEntry, block_cache,
          block.cache_handle);
    } else {
      iter->RegisterCleanup(&DeleteHeldResource<Block>, block.value, nullptr);
    }
  } else {
    if (input_iter != nullptr) {
      input_iter->SetStatus(s);
      iter = input_iter;
    } else {
      iter = NewErrorIterator(s);
    }
  }
  return iter;
}

class BlockBasedTable::BlockEntryIteratorState : public TwoLevelIteratorState {
 public:
  BlockEntryIteratorState(BlockBasedTable* table,
                          const ReadOptions& read_options)
      : TwoLevelIteratorState(table->rep_->options.prefix_extractor != nullptr),
        table_(table),
        read_options_(read_options) {}

  Iterator* NewSecondaryIterator(const Slice& index_value) override {
    return NewDataBlockIterator(table_->rep_, read_options_, index_value);
  }

  bool PrefixMayMatch(const Slice& internal_key) override {
    if (read_options_.total_order_seek) {
      return true;
    }
    return table_->PrefixMayMatch(internal_key);
  }

 private:
  // Don't own table_
  BlockBasedTable* table_;
  const ReadOptions read_options_;
};

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

  assert(rep_->options.prefix_extractor != nullptr);
  auto prefix = rep_->options.prefix_extractor->Transform(
      ExtractUserKey(internal_key));
  InternalKey internal_key_prefix(prefix, 0, kTypeValue);
  auto internal_prefix = internal_key_prefix.Encode();

  bool may_match = true;
  Status s;

  // To prevent any io operation in this method, we set `read_tier` to make
  // sure we always read index or filter only when they have already been
  // loaded to memory.
  ReadOptions no_io_read_options;
  no_io_read_options.read_tier = kBlockCacheTier;
  unique_ptr<Iterator> iiter(NewIndexIterator(no_io_read_options));
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
    may_match = filter_entry.value == nullptr ||
                filter_entry.value->PrefixMayMatch(handle.offset(), prefix);
    filter_entry.Release(rep_->table_options.block_cache.get());
  }

  Statistics* statistics = rep_->options.statistics.get();
  RecordTick(statistics, BLOOM_FILTER_PREFIX_CHECKED);
  if (!may_match) {
    RecordTick(statistics, BLOOM_FILTER_PREFIX_USEFUL);
  }

  return may_match;
}

Iterator* BlockBasedTable::NewIterator(const ReadOptions& read_options,
                                       Arena* arena) {
  return NewTwoLevelIterator(new BlockEntryIteratorState(this, read_options),
                             NewIndexIterator(read_options), arena);
}

Status BlockBasedTable::Get(
    const ReadOptions& read_options, const Slice& key, void* handle_context,
    bool (*result_handler)(void* handle_context, const ParsedInternalKey& k,
                           const Slice& v),
    void (*mark_key_may_exist_handler)(void* handle_context)) {
  Status s;
  BlockIter iiter;
  NewIndexIterator(read_options, &iiter);

  auto filter_entry = GetFilter(read_options.read_tier == kBlockCacheTier);
  FilterBlockReader* filter = filter_entry.value;
  bool done = false;
  for (iiter.Seek(key); iiter.Valid() && !done; iiter.Next()) {
    Slice handle_value = iiter.value();

    BlockHandle handle;
    bool may_not_exist_in_filter =
        filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), ExtractUserKey(key));

    if (may_not_exist_in_filter) {
      // Not found
      // TODO: think about interaction with Merge. If a user key cannot
      // cross one data block, we should be fine.
      RecordTick(rep_->options.statistics.get(), BLOOM_FILTER_USEFUL);
      break;
    } else {
      BlockIter biter;
      NewDataBlockIterator(rep_, read_options, iiter.value(), &biter);

      if (read_options.read_tier && biter.status().IsIncomplete()) {
        // couldn't get block from block_cache
        // Update Saver.state to Found because we are only looking for whether
        // we can guarantee the key is not there when "no_io" is set
        (*mark_key_may_exist_handler)(handle_context);
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

        if (!(*result_handler)(handle_context, parsed_key,
                               biter.value())) {
          done = true;
          break;
        }
      }
      s = biter.status();
    }
  }

  filter_entry.Release(rep_->table_options.block_cache.get());
  if (s.ok()) {
    s = iiter.status();
  }

  return s;
}

bool BlockBasedTable::TEST_KeyInCache(const ReadOptions& options,
                                      const Slice& key) {
  std::unique_ptr<Iterator> iiter(NewIndexIterator(options));
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

  s = GetDataBlockFromCache(cache_key, ckey, block_cache, nullptr, nullptr,
                            options, &block);
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
Status BlockBasedTable::CreateIndexReader(IndexReader** index_reader,
                                          Iterator* preloaded_meta_index_iter) {
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
  auto env = rep_->options.env;
  auto comparator = &rep_->internal_comparator;
  const Footer& footer = rep_->footer;

  if (index_type_on_file == BlockBasedTableOptions::kHashSearch &&
      rep_->options.prefix_extractor == nullptr) {
    Log(rep_->options.info_log,
        "BlockBasedTableOptions::kHashSearch requires "
        "options.prefix_extractor to be set."
        " Fall back to binary seach index.");
    index_type_on_file = BlockBasedTableOptions::kBinarySearch;
  }

  switch (index_type_on_file) {
    case BlockBasedTableOptions::kBinarySearch: {
      return BinarySearchIndexReader::Create(
          file, footer, footer.index_handle(), env, comparator, index_reader);
    }
    case BlockBasedTableOptions::kHashSearch: {
      std::unique_ptr<Block> meta_guard;
      std::unique_ptr<Iterator> meta_iter_guard;
      auto meta_index_iter = preloaded_meta_index_iter;
      if (meta_index_iter == nullptr) {
        auto s = ReadMetaBlock(rep_, &meta_guard, &meta_iter_guard);
        if (!s.ok()) {
          // we simply fall back to binary search in case there is any
          // problem with prefix hash index loading.
          Log(rep_->options.info_log,
              "Unable to read the metaindex block."
              " Fall back to binary seach index.");
          return BinarySearchIndexReader::Create(
            file, footer, footer.index_handle(), env, comparator, index_reader);
        }
        meta_index_iter = meta_iter_guard.get();
      }

      // We need to wrap data with internal_prefix_transform to make sure it can
      // handle prefix correctly.
      rep_->internal_prefix_transform.reset(
          new InternalKeySliceTransform(rep_->options.prefix_extractor.get()));
      return HashIndexReader::Create(
          rep_->internal_prefix_transform.get(), footer, file, env, comparator,
          footer.index_handle(), meta_index_iter, index_reader,
          rep_->hash_index_allow_collision);
    }
    default: {
      std::string error_message =
          "Unrecognized index type: " + std::to_string(rep_->index_type);
      return Status::InvalidArgument(error_message.c_str());
    }
  }
}

uint64_t BlockBasedTable::ApproximateOffsetOf(const Slice& key) {
  unique_ptr<Iterator> index_iter(NewIndexIterator(ReadOptions()));

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

}  // namespace rocksdb
