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

namespace rocksdb {

// The longest the prefix of the cache key used to identify blocks can be.
// We are using the fact that we know for Posix files the unique ID is three
// varints.
const size_t kMaxCacheKeyPrefixSize = kMaxVarint64Length*3+1;

struct BlockBasedTable::Rep {
  ~Rep() {
    delete filter;
    delete [] filter_data;
    delete index_block;
  }
  Rep(const EnvOptions& storage_options) :
    soptions(storage_options) {
  }

  Options options;
  const EnvOptions& soptions;
  Status status;
  unique_ptr<RandomAccessFile> file;
  char cache_key_prefix[kMaxCacheKeyPrefixSize];
  size_t cache_key_prefix_size;
  char compressed_cache_key_prefix[kMaxCacheKeyPrefixSize];
  size_t compressed_cache_key_prefix_size;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
  TableStats table_stats;
};

BlockBasedTable::~BlockBasedTable() {
  delete rep_;
}

// Helper function to setup the cache key's prefix for the Table.
void BlockBasedTable::SetupCacheKeyPrefix(Rep* rep) {
  assert(kMaxCacheKeyPrefixSize >= 10);
  rep->cache_key_prefix_size = 0;
  rep->compressed_cache_key_prefix_size = 0;
  if (rep->options.block_cache != nullptr) {
    GenerateCachePrefix(rep->options.block_cache, rep->file.get(),
                        &rep->cache_key_prefix[0],
                        &rep->cache_key_prefix_size);
  }
  if (rep->options.block_cache_compressed != nullptr) {
    GenerateCachePrefix(rep->options.block_cache_compressed, rep->file.get(),
                        &rep->compressed_cache_key_prefix[0],
                        &rep->compressed_cache_key_prefix_size);
  }
}

void BlockBasedTable::GenerateCachePrefix(shared_ptr<Cache> cc,
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

void BlockBasedTable::GenerateCachePrefix(shared_ptr<Cache> cc,
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
Status ReadBlock(RandomAccessFile* file,
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

} // end of anonymous namespace

Status BlockBasedTable::Open(const Options& options,
                             const EnvOptions& soptions,
                             unique_ptr<RandomAccessFile> && file,
                             uint64_t size,
                             unique_ptr<TableReader>* table_reader) {
  table_reader->reset();
  if (size < Footer::kEncodedLength) {
    return Status::InvalidArgument("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  // Check that we actually read the whole footer from the file. It may be
  // that size isn't correct.
  if (footer_input.size() != Footer::kEncodedLength) {
    return Status::InvalidArgument("file is too short to be an sstable");
  }

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  Block* index_block = nullptr;
  // TODO: we never really verify check sum for index block
  s = ReadBlock(file.get(), ReadOptions(), footer.index_handle(), &index_block,
                options.env);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    assert(index_block->compressionType() == kNoCompression);
    BlockBasedTable::Rep* rep = new BlockBasedTable::Rep(soptions);
    rep->options = options;
    rep->file = std::move(file);
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    SetupCacheKeyPrefix(rep);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    table_reader->reset(new BlockBasedTable(rep));
    ((BlockBasedTable*) (table_reader->get()))->ReadMeta(footer);
  } else {
    if (index_block) delete index_block;
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

TableStats& BlockBasedTable::GetTableStats() {
  return rep_->table_stats;
}

void BlockBasedTable::ReadMeta(const Footer& footer) {
  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  //  TODO: we never really verify check sum for meta index block
  Block* meta = nullptr;
  if (!ReadBlock(rep_->file.get(), ReadOptions(), footer.metaindex_handle(),
                 &meta, rep_->options.env).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  assert(meta->compressionType() == kNoCompression);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  // read filter
  if (rep_->options.filter_policy) {
    std::string key = kFilterBlockPrefix;
    key.append(rep_->options.filter_policy->Name());
    iter->Seek(key);

    if (iter->Valid() && iter->key() == Slice(key)) {
      ReadFilter(iter->value());
    }
  }

  // read stats
  iter->Seek(kStatsBlock);
  if (iter->Valid() && iter->key() == Slice(kStatsBlock)) {
    auto s = iter->status();
    if (s.ok()) {
      s = ReadStats(iter->value(), rep_);
    }

    if (!s.ok()) {
      auto err_msg =
        "[Warning] Encountered error while reading data from stats block " +
        s.ToString();
      Log(rep_->options.info_log, err_msg.c_str());
    }
  }

  delete iter;
  delete meta;
}

void BlockBasedTable::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // TODO: We might want to unify with ReadBlock() if we start
  // requiring checksum verification in BlockBasedTable::Open.
  ReadOptions opt;
  BlockContents block;
  if (!ReadBlockContents(rep_->file.get(), opt, filter_handle, &block,
                        rep_->options.env, false).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();     // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options, block.data);
}

Status BlockBasedTable::ReadStats(const Slice& handle_value, Rep* rep) {
  Slice v = handle_value;
  BlockHandle handle;
  if (!handle.DecodeFrom(&v).ok()) {
    return Status::InvalidArgument("Failed to decode stats block handle");
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

  Block stats_block(block_contents);
  std::unique_ptr<Iterator> iter(
      stats_block.NewIterator(BytewiseComparator())
  );

  auto& table_stats = rep->table_stats;
  // All pre-defined stats of type uint64_t
  std::unordered_map<std::string, uint64_t*> predefined_uint64_stats = {
    { BlockBasedTableStatsNames::kDataSize,      &table_stats.data_size      },
    { BlockBasedTableStatsNames::kIndexSize,     &table_stats.index_size     },
    { BlockBasedTableStatsNames::kRawKeySize,    &table_stats.raw_key_size   },
    { BlockBasedTableStatsNames::kRawValueSize,  &table_stats.raw_value_size },
    { BlockBasedTableStatsNames::kNumDataBlocks, &table_stats.num_data_blocks},
    { BlockBasedTableStatsNames::kNumEntries,    &table_stats.num_entries    },
  };

  std::string last_key;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    s = iter->status();
    if (!s.ok()) {
      break;
    }

    auto key = iter->key().ToString();
    // stats block is strictly sorted with no duplicate key.
    assert(
        last_key.empty() ||
        BytewiseComparator()->Compare(key, last_key) > 0
    );
    last_key = key;

    auto raw_val = iter->value();
    auto pos = predefined_uint64_stats.find(key);

    if (pos != predefined_uint64_stats.end()) {
      // handle predefined rocksdb stats
      uint64_t val;
      if (!GetVarint64(&raw_val, &val)) {
        // skip malformed value
        auto error_msg =
          "[Warning] detect malformed value in stats meta-block:"
          "\tkey: " + key + "\tval: " + raw_val.ToString();
        Log(rep->options.info_log, error_msg.c_str());
        continue;
      }
      *(pos->second) = val;
    } else if (key == BlockBasedTableStatsNames::kFilterPolicy) {
      table_stats.filter_policy_name = raw_val.ToString();
    } else {
      // handle user-collected
      table_stats.user_collected_stats.insert(
          std::make_pair(iter->key().ToString(), raw_val.ToString())
      );
    }
  }

  return s;
}

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
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
  std::shared_ptr<Statistics> statistics = table->rep_->options.statistics;
  Block* block = nullptr;
  Block* cblock = nullptr;
  Cache::Handle* cache_handle = nullptr;
  Cache::Handle* compressed_cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    if (block_cache != nullptr || block_cache_compressed != nullptr) {
      char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
      char compressed_cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
      char* end = cache_key;

      // create key for block cache
      if (block_cache != nullptr) {
        assert(table->rep_->cache_key_prefix_size != 0);
        assert(table->rep_->cache_key_prefix_size <= kMaxCacheKeyPrefixSize);
        memcpy(cache_key, table->rep_->cache_key_prefix,
               table->rep_->cache_key_prefix_size);
        end = EncodeVarint64(cache_key + table->rep_->cache_key_prefix_size,
                             handle.offset());
      }
      Slice key(cache_key, static_cast<size_t>(end - cache_key));

      // create key for compressed block cache
      end = compressed_cache_key;
      if (block_cache_compressed != nullptr) {
        assert(table->rep_->compressed_cache_key_prefix_size != 0);
        assert(table->rep_->compressed_cache_key_prefix_size <=
               kMaxCacheKeyPrefixSize);
        memcpy(compressed_cache_key, table->rep_->compressed_cache_key_prefix,
               table->rep_->compressed_cache_key_prefix_size);
        end = EncodeVarint64(compressed_cache_key +
                             table->rep_->compressed_cache_key_prefix_size,
                             handle.offset());
      }
      Slice ckey(compressed_cache_key, static_cast<size_t>
                                      (end - compressed_cache_key));

      // Lookup uncompressed cache first
      if (block_cache != nullptr) {
        cache_handle = block_cache->Lookup(key);
        if (cache_handle != nullptr) {
          block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
          RecordTick(statistics, BLOCK_CACHE_HIT);
        }
      }

      // If not found in uncompressed cache, lookup compressed cache
      if (block == nullptr && block_cache_compressed != nullptr) {
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
          s = ReadBlock(
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
                RecordTick(statistics, BLOCK_CACHE_MISS);
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
      s = ReadBlock(table->rep_->file.get(), options, handle, &block,
                    table->rep_->options.env, didIO);
    }
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
// TODO(tylerharter): right now, this won't cause I/O since blooms are
// in memory.  When blooms may need to be paged in, we should refactor so that
// this is only ever called lazily.  In particular, this shouldn't be called
// while the DB lock is held like it is now.
bool BlockBasedTable::PrefixMayMatch(const Slice& internal_prefix) {
  FilterBlockReader* filter = rep_->filter;
  bool may_match = true;
  Status s;

  if (filter == nullptr) {
    return true;
  }

  std::unique_ptr<Iterator> iiter(rep_->index_block->NewIterator(
                                           rep_->options.comparator));
  iiter->Seek(internal_prefix);
  if (!iiter->Valid()) {
    // we're past end of file
    may_match = false;
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
    may_match = filter->PrefixMayMatch(handle.offset(), internal_prefix);
  }

  RecordTick(rep_->options.statistics, BLOOM_FILTER_PREFIX_CHECKED);
  if (!may_match) {
    RecordTick(rep_->options.statistics, BLOOM_FILTER_PREFIX_USEFUL);
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
      rep_->index_block->NewIterator(rep_->options.comparator),
      &BlockBasedTable::BlockReader, const_cast<BlockBasedTable*>(this),
      options, rep_->soptions);
}

Status BlockBasedTable::Get(
    const ReadOptions& readOptions,
    const Slice& key,
    void* handle_context,
    bool (*result_handler)(void* handle_context, const Slice& k,
                           const Slice& v, bool didIO),
    void (*mark_key_may_exist_handler)(void* handle_context)) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  bool done = false;
  for (iiter->Seek(key); iiter->Valid() && !done; iiter->Next()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != nullptr &&
        handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), key)) {
      // Not found
      // TODO: think about interaction with Merge. If a user key cannot
      // cross one data block, we should be fine.
      RecordTick(rep_->options.statistics, BLOOM_FILTER_USEFUL);
      break;
    } else {
      bool didIO = false;
      std::unique_ptr<Iterator> block_iter(
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
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
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

const std::string BlockBasedTable::kFilterBlockPrefix = "filter.";
const std::string BlockBasedTable::kStatsBlock = "rocksdb.stats";

const std::string BlockBasedTableStatsNames::kDataSize  = "rocksdb.data.size";
const std::string BlockBasedTableStatsNames::kIndexSize = "rocksdb.index.size";
const std::string BlockBasedTableStatsNames::kRawKeySize =
    "rocksdb.raw.key.size";
const std::string BlockBasedTableStatsNames::kRawValueSize =
    "rocksdb.raw.value.size";
const std::string BlockBasedTableStatsNames::kNumDataBlocks =
    "rocksdb.num.data.blocks";
const std::string BlockBasedTableStatsNames::kNumEntries =
    "rocksdb.num.entries";
const std::string BlockBasedTableStatsNames::kFilterPolicy =
    "rocksdb.filter.policy";

}  // namespace rocksdb
