// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "leveldb/statistics.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/table.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

// The longest the prefix of the cache key used to identify blocks can be.
// We are using the fact that we know for Posix files the unique ID is three
// varints.
const size_t kMaxCacheKeyPrefixSize = kMaxVarint64Length*3+1;

struct Table::Rep {
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
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

// Helper function to setup the cache key's prefix for the Table.
void Table::SetupCacheKeyPrefix(Rep* rep) {
  assert(kMaxCacheKeyPrefixSize >= 10);
  rep->cache_key_prefix_size = 0;
  if (rep->options.block_cache) {
    rep->cache_key_prefix_size = rep->file->GetUniqueId(rep->cache_key_prefix,
                                                        kMaxCacheKeyPrefixSize);

    if (rep->cache_key_prefix_size == 0) {
      // If the prefix wasn't generated or was too long, we create one from the
      // cache.
      char* end = EncodeVarint64(rep->cache_key_prefix,
                                 rep->options.block_cache->NewId());
      rep->cache_key_prefix_size =
            static_cast<size_t>(end - rep->cache_key_prefix);
    }
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
                 bool* didIO = nullptr) {
  BlockContents contents;
  Status s = ReadBlockContents(file, options, handle, &contents);
  if (s.ok()) {
    *result = new Block(contents);
  }

  if (didIO) {
    *didIO = true;
  }
  return s;
}

} // end of anonymous namespace

Status Table::Open(const Options& options,
                   const EnvOptions& soptions,
                   unique_ptr<RandomAccessFile>&& file,
                   uint64_t size,
                   unique_ptr<Table>* table) {
  table->reset();
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
  s = ReadBlock(file.get(), ReadOptions(), footer.index_handle(), &index_block);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Rep* rep = new Table::Rep(soptions);
    rep->options = options;
    rep->file = std::move(file);
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    SetupCacheKeyPrefix(rep);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    table->reset(new Table(rep));
    (*table)->ReadMeta(footer);
  } else {
    if (index_block) delete index_block;
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  //  TODO: we never really verify check sum for meta index block
  Block* meta = nullptr;
  if (!ReadBlock(rep_->file.get(), ReadOptions(), footer.metaindex_handle(),
                 &meta).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // TODO: We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  BlockContents block;
  if (!ReadBlockContents(rep_->file.get(), opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();     // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() {
  delete rep_;
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
Iterator* Table::BlockReader(void* arg,
                             const ReadOptions& options,
                             const Slice& index_value,
                             bool* didIO) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache.get();
  std::shared_ptr<Statistics> statistics = table->rep_->options.statistics;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    if (block_cache != nullptr) {
      char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
      const size_t cache_key_prefix_size = table->rep_->cache_key_prefix_size;
      assert(cache_key_prefix_size != 0);
      assert(cache_key_prefix_size <= kMaxCacheKeyPrefixSize);
      memcpy(cache_key, table->rep_->cache_key_prefix,
             cache_key_prefix_size);
      char* end = EncodeVarint64(cache_key + cache_key_prefix_size,
                                 handle.offset());
      Slice key(cache_key, static_cast<size_t>(end-cache_key));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));

        RecordTick(statistics, BLOCK_CACHE_HIT);
      } else {
        s = ReadBlock(table->rep_->file.get(), options, handle, &block, didIO);
        if (s.ok()) {
          if (block->isCachable() && options.fill_cache) {
            cache_handle = block_cache->Insert(
              key, block, block->size(), &DeleteCachedBlock);
          }
        }

        RecordTick(statistics, BLOCK_CACHE_MISS);
      }
    } else {
      s = ReadBlock(table->rep_->file.get(), options, handle, &block, didIO);
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::BlockReader(void* arg,
                             const ReadOptions& options,
                             const EnvOptions& soptions,
                             const Slice& index_value) {
  return BlockReader(arg, options, index_value, nullptr);
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options, rep_->soptions);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k,
                          void* arg,
                          bool (*saver)(void*, const Slice&, const Slice&, bool)) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  bool done = false;
  for (iiter->Seek(k); iiter->Valid() && !done; iiter->Next()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != nullptr &&
        handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
      // TODO: think about interaction with Merge. If a user key cannot
      // cross one data block, we should be fine.
      RecordTick(rep_->options.statistics, BLOOM_FILTER_USEFUL);
      break;
    } else {
      bool didIO = false;
      Iterator* block_iter = BlockReader(this, options, iiter->value(),
                                         &didIO);

      for (block_iter->Seek(k); block_iter->Valid(); block_iter->Next()) {
        if (!(*saver)(arg, block_iter->key(), block_iter->value(), didIO)) {
          done = true;
          break;
        }
      }
      s = block_iter->status();
      delete block_iter;
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
bool Table::TEST_KeyInCache(const ReadOptions& options, const Slice& key) {
  // We use InternalGet() as it has logic that checks whether we read the
  // block from the disk or not.
  bool didIO = false;
  Status s = InternalGet(options, key, &didIO, SaveDidIO);
  assert(s.ok());
  return !didIO;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
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

}  // namespace leveldb
