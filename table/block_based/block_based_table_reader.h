//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <stdint.h>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/range_tombstone_fragmenter.h"
#include "file/filename.h"
#include "options/cf_options.h"
#include "rocksdb/options.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_type.h"
#include "table/block_based/cachable_entry.h"
#include "table/block_based/filter_block.h"
#include "table/format.h"
#include "table/get_context.h"
#include "table/multiget_context.h"
#include "table/persistent_cache_helper.h"
#include "table/table_properties_internal.h"
#include "table/table_reader.h"
#include "table/two_level_iterator.h"
#include "trace_replay/block_cache_tracer.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/user_comparator_wrapper.h"

namespace rocksdb {

class Cache;
class FilterBlockReader;
class BlockBasedFilterBlockReader;
class FullFilterBlockReader;
class Footer;
class InternalKeyComparator;
class Iterator;
class RandomAccessFile;
class TableCache;
class TableReader;
class WritableFile;
struct BlockBasedTableOptions;
struct EnvOptions;
struct ReadOptions;
class GetContext;

typedef std::vector<std::pair<std::string, std::string>> KVPairBlock;

// Reader class for BlockBasedTable format.
// For the format of BlockBasedTable refer to
// https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format.
// This is the default table type. Data is chucked into fixed size blocks and
// each block in-turn stores entries. When storing data, we can compress and/or
// encode data efficiently within a block, which often results in a much smaller
// data size compared with the raw data size. As for the record retrieval, we'll
// first locate the block where target record may reside, then read the block to
// memory, and finally search that record within the block. Of course, to avoid
// frequent reads of the same block, we introduced the block cache to keep the
// loaded blocks in the memory.
class BlockBasedTable : public TableReader {
 public:
  static const std::string kFilterBlockPrefix;
  static const std::string kFullFilterBlockPrefix;
  static const std::string kPartitionedFilterBlockPrefix;
  // The longest prefix of the cache key used to identify blocks.
  // For Posix files the unique ID is three varints.
  static const size_t kMaxCacheKeyPrefixSize = kMaxVarint64Length * 3 + 1;

  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table_reader" to the newly opened
  // table.  The client should delete "*table_reader" when no longer needed.
  // If there was an error while initializing the table, sets "*table_reader"
  // to nullptr and returns a non-ok status.
  //
  // @param file must remain live while this Table is in use.
  // @param prefetch_index_and_filter_in_cache can be used to disable
  // prefetching of
  //    index and filter blocks into block cache at startup
  // @param skip_filters Disables loading/accessing the filter block. Overrides
  //    prefetch_index_and_filter_in_cache, so filter will be skipped if both
  //    are set.
  static Status Open(const ImmutableCFOptions& ioptions,
                     const EnvOptions& env_options,
                     const BlockBasedTableOptions& table_options,
                     const InternalKeyComparator& internal_key_comparator,
                     std::unique_ptr<RandomAccessFileReader>&& file,
                     uint64_t file_size,
                     std::unique_ptr<TableReader>* table_reader,
                     const SliceTransform* prefix_extractor = nullptr,
                     bool prefetch_index_and_filter_in_cache = true,
                     bool skip_filters = false, int level = -1,
                     const bool immortal_table = false,
                     const SequenceNumber largest_seqno = 0,
                     TailPrefetchStats* tail_prefetch_stats = nullptr,
                     BlockCacheTracer* const block_cache_tracer = nullptr);

  bool PrefixMayMatch(const Slice& internal_key,
                      const ReadOptions& read_options,
                      const SliceTransform* options_prefix_extractor,
                      const bool need_upper_bound_check,
                      BlockCacheLookupContext* lookup_context) const;

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  // @param skip_filters Disables loading/accessing the filter block
  // compaction_readahead_size: its value will only be used if caller =
  // kCompaction.
  InternalIterator* NewIterator(const ReadOptions&,
                                const SliceTransform* prefix_extractor,
                                Arena* arena, bool skip_filters,
                                TableReaderCaller caller,
                                size_t compaction_readahead_size = 0) override;

  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions& read_options) override;

  // @param skip_filters Disables loading/accessing the filter block
  Status Get(const ReadOptions& readOptions, const Slice& key,
             GetContext* get_context, const SliceTransform* prefix_extractor,
             bool skip_filters = false) override;

  void MultiGet(const ReadOptions& readOptions,
                const MultiGetContext::Range* mget_range,
                const SliceTransform* prefix_extractor,
                bool skip_filters = false) override;

  // Pre-fetch the disk blocks that correspond to the key range specified by
  // (kbegin, kend). The call will return error status in the event of
  // IO or iteration error.
  Status Prefetch(const Slice* begin, const Slice* end) override;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  uint64_t ApproximateOffsetOf(const Slice& key,
                               TableReaderCaller caller) override;

  bool TEST_BlockInCache(const BlockHandle& handle) const;

  // Returns true if the block for the specified key is in cache.
  // REQUIRES: key is in this table && block cache enabled
  bool TEST_KeyInCache(const ReadOptions& options, const Slice& key);

  // Set up the table for Compaction. Might change some parameters with
  // posix_fadvise
  void SetupForCompaction() override;

  std::shared_ptr<const TableProperties> GetTableProperties() const override;

  size_t ApproximateMemoryUsage() const override;

  // convert SST file to a human readable form
  Status DumpTable(WritableFile* out_file,
                   const SliceTransform* prefix_extractor = nullptr) override;

  Status VerifyChecksum(TableReaderCaller caller) override;

  void Close() override;

  ~BlockBasedTable();

  bool TEST_filter_block_preloaded() const;
  bool TEST_IndexBlockInCache() const;

  // IndexReader is the interface that provides the functionality for index
  // access.
  class IndexReader {
   public:
    virtual ~IndexReader() = default;

    // Create an iterator for index access. If iter is null, then a new object
    // is created on the heap, and the callee will have the ownership.
    // If a non-null iter is passed in, it will be used, and the returned value
    // is either the same as iter or a new on-heap object that
    // wraps the passed iter. In the latter case the return value points
    // to a different object then iter, and the callee has the ownership of the
    // returned object.
    virtual InternalIteratorBase<IndexValue>* NewIterator(
        const ReadOptions& read_options, bool disable_prefix_seek,
        IndexBlockIter* iter, GetContext* get_context,
        BlockCacheLookupContext* lookup_context) = 0;

    // Report an approximation of how much memory has been used other than
    // memory that was allocated in block cache.
    virtual size_t ApproximateMemoryUsage() const = 0;
    // Cache the dependencies of the index reader (e.g. the partitions
    // of a partitioned index).
    virtual void CacheDependencies(bool /* pin */) {}
  };

  class IndexReaderCommon;

  static Slice GetCacheKey(const char* cache_key_prefix,
                           size_t cache_key_prefix_size,
                           const BlockHandle& handle, char* cache_key);

  // Retrieve all key value pairs from data blocks in the table.
  // The key retrieved are internal keys.
  Status GetKVPairsFromDataBlocks(std::vector<KVPairBlock>* kv_pair_blocks);

  struct Rep;

  Rep* get_rep() { return rep_; }
  const Rep* get_rep() const { return rep_; }

  // input_iter: if it is not null, update this one and return it as Iterator
  template <typename TBlockIter>
  TBlockIter* NewDataBlockIterator(
      const ReadOptions& ro, const BlockHandle& block_handle,
      TBlockIter* input_iter, BlockType block_type, GetContext* get_context,
      BlockCacheLookupContext* lookup_context, Status s,
      FilePrefetchBuffer* prefetch_buffer, bool for_compaction = false) const;

  // input_iter: if it is not null, update this one and return it as Iterator
  template <typename TBlockIter>
  TBlockIter* NewDataBlockIterator(const ReadOptions& ro,
                                   CachableEntry<Block>& block,
                                   TBlockIter* input_iter, Status s) const;

  class PartitionedIndexIteratorState;

  friend class PartitionIndexReader;

 protected:
  Rep* rep_;
  explicit BlockBasedTable(Rep* rep, BlockCacheTracer* const block_cache_tracer)
      : rep_(rep), block_cache_tracer_(block_cache_tracer) {}

 private:
  friend class MockedBlockBasedTable;
  static std::atomic<uint64_t> next_cache_key_id_;
  BlockCacheTracer* const block_cache_tracer_;

  void UpdateCacheHitMetrics(BlockType block_type, GetContext* get_context,
                             size_t usage) const;
  void UpdateCacheMissMetrics(BlockType block_type,
                              GetContext* get_context) const;
  void UpdateCacheInsertionMetrics(BlockType block_type,
                                   GetContext* get_context, size_t usage) const;
  Cache::Handle* GetEntryFromCache(Cache* block_cache, const Slice& key,
                                   BlockType block_type,
                                   GetContext* get_context) const;

  // Either Block::NewDataIterator() or Block::NewIndexIterator().
  template <typename TBlockIter>
  static TBlockIter* InitBlockIterator(const Rep* rep, Block* block,
                                       TBlockIter* input_iter,
                                       bool block_contents_pinned);

  // If block cache enabled (compressed or uncompressed), looks for the block
  // identified by handle in (1) uncompressed cache, (2) compressed cache, and
  // then (3) file. If found, inserts into the cache(s) that were searched
  // unsuccessfully (e.g., if found in file, will add to both uncompressed and
  // compressed caches if they're enabled).
  //
  // @param block_entry value is set to the uncompressed block if found. If
  //    in uncompressed block cache, also sets cache_handle to reference that
  //    block.
  Status MaybeReadBlockAndLoadToCache(
      FilePrefetchBuffer* prefetch_buffer, const ReadOptions& ro,
      const BlockHandle& handle, const UncompressionDict& uncompression_dict,
      CachableEntry<Block>* block_entry, BlockType block_type,
      GetContext* get_context, BlockCacheLookupContext* lookup_context,
      BlockContents* contents) const;

  // Similar to the above, with one crucial difference: it will retrieve the
  // block from the file even if there are no caches configured (assuming the
  // read options allow I/O).
  Status RetrieveBlock(FilePrefetchBuffer* prefetch_buffer,
                       const ReadOptions& ro, const BlockHandle& handle,
                       const UncompressionDict& uncompression_dict,
                       CachableEntry<Block>* block_entry, BlockType block_type,
                       GetContext* get_context,
                       BlockCacheLookupContext* lookup_context,
                       bool for_compaction = false) const;

  Status GetDataBlockFromCache(
      const ReadOptions& ro, const BlockHandle& handle,
      const UncompressionDict& uncompression_dict,
      CachableEntry<Block>* block_entry, BlockType block_type,
      GetContext* get_context) const;

  void MaybeLoadBlocksToCache(
      const ReadOptions& options, const MultiGetRange* batch,
      const autovector<BlockHandle, MultiGetContext::MAX_BATCH_SIZE>*  handles,
      autovector<Status, MultiGetContext::MAX_BATCH_SIZE>* statuses,
      autovector<
        CachableEntry<Block>, MultiGetContext::MAX_BATCH_SIZE>* results,
      char* scratch, const UncompressionDict& uncompression_dict) const;

  // For the following two functions:
  // if `no_io == true`, we will not try to read filter/index from sst file
  // were they not present in cache yet.
  CachableEntry<FilterBlockReader> GetFilter(
      const SliceTransform* prefix_extractor,
      FilePrefetchBuffer* prefetch_buffer, bool no_io, GetContext* get_context,
      BlockCacheLookupContext* lookup_context) const;
  virtual CachableEntry<FilterBlockReader> GetFilter(
      FilePrefetchBuffer* prefetch_buffer, const BlockHandle& filter_blk_handle,
      const bool is_a_filter_partition, bool no_io, GetContext* get_context,
      BlockCacheLookupContext* lookup_context,
      const SliceTransform* prefix_extractor) const;

  CachableEntry<UncompressionDict> GetUncompressionDict(
      FilePrefetchBuffer* prefetch_buffer, bool no_io, GetContext* get_context,
      BlockCacheLookupContext* lookup_context) const;

  // Get the iterator from the index reader.
  // If input_iter is not set, return new Iterator
  // If input_iter is set, update it and return it as Iterator
  //
  // Note: ErrorIterator with Status::Incomplete shall be returned if all the
  // following conditions are met:
  //  1. We enabled table_options.cache_index_and_filter_blocks.
  //  2. index is not present in block cache.
  //  3. We disallowed any io to be performed, that is, read_options ==
  //     kBlockCacheTier
  InternalIteratorBase<IndexValue>* NewIndexIterator(
      const ReadOptions& read_options, bool need_upper_bound_check,
      IndexBlockIter* input_iter, GetContext* get_context,
      BlockCacheLookupContext* lookup_context) const;

  // Read block cache from block caches (if set): block_cache and
  // block_cache_compressed.
  // On success, Status::OK with be returned and @block will be populated with
  // pointer to the block as well as its block handle.
  // @param uncompression_dict Data for presetting the compression library's
  //    dictionary.
  Status GetDataBlockFromCache(
      const Slice& block_cache_key, const Slice& compressed_block_cache_key,
      Cache* block_cache, Cache* block_cache_compressed,
      const ReadOptions& read_options, CachableEntry<Block>* block,
      const UncompressionDict& uncompression_dict, BlockType block_type,
      GetContext* get_context = nullptr) const;

  // Put a raw block (maybe compressed) to the corresponding block caches.
  // This method will perform decompression against raw_block if needed and then
  // populate the block caches.
  // On success, Status::OK will be returned; also @block will be populated with
  // uncompressed block and its cache handle.
  //
  // Allocated memory managed by raw_block_contents will be transferred to
  // PutDataBlockToCache(). After the call, the object will be invalid.
  // @param uncompression_dict Data for presetting the compression library's
  //    dictionary.
  Status PutDataBlockToCache(
      const Slice& block_cache_key, const Slice& compressed_block_cache_key,
      Cache* block_cache, Cache* block_cache_compressed,
      CachableEntry<Block>* cached_block, BlockContents* raw_block_contents,
      CompressionType raw_block_comp_type,
      const UncompressionDict& uncompression_dict, SequenceNumber seq_no,
      MemoryAllocator* memory_allocator, BlockType block_type,
      GetContext* get_context) const;

  // Calls (*handle_result)(arg, ...) repeatedly, starting with the entry found
  // after a call to Seek(key), until handle_result returns false.
  // May not make such a call if filter policy says that key is not present.
  friend class TableCache;
  friend class BlockBasedTableBuilder;

  // Create a index reader based on the index type stored in the table.
  // Optionally, user can pass a preloaded meta_index_iter for the index that
  // need to access extra meta blocks for index construction. This parameter
  // helps avoid re-reading meta index block if caller already created one.
  Status CreateIndexReader(FilePrefetchBuffer* prefetch_buffer,
                           InternalIterator* preloaded_meta_index_iter,
                           bool use_cache, bool prefetch, bool pin,
                           IndexReader** index_reader,
                           BlockCacheLookupContext* lookup_context);

  bool FullFilterKeyMayMatch(const ReadOptions& read_options,
                             FilterBlockReader* filter, const Slice& user_key,
                             const bool no_io,
                             const SliceTransform* prefix_extractor,
                             BlockCacheLookupContext* lookup_context) const;

  void FullFilterKeysMayMatch(const ReadOptions& read_options,
                              FilterBlockReader* filter, MultiGetRange* range,
                              const bool no_io,
                              const SliceTransform* prefix_extractor,
                              BlockCacheLookupContext* lookup_context) const;

  static Status PrefetchTail(
      RandomAccessFileReader* file, uint64_t file_size,
      TailPrefetchStats* tail_prefetch_stats, const bool prefetch_all,
      const bool preload_all,
      std::unique_ptr<FilePrefetchBuffer>* prefetch_buffer);
  Status ReadMetaBlock(FilePrefetchBuffer* prefetch_buffer,
                       std::unique_ptr<Block>* meta_block,
                       std::unique_ptr<InternalIterator>* iter);
  Status TryReadPropertiesWithGlobalSeqno(FilePrefetchBuffer* prefetch_buffer,
                                          const Slice& handle_value,
                                          TableProperties** table_properties);
  Status ReadPropertiesBlock(FilePrefetchBuffer* prefetch_buffer,
                             InternalIterator* meta_iter,
                             const SequenceNumber largest_seqno);
  Status ReadRangeDelBlock(FilePrefetchBuffer* prefetch_buffer,
                           InternalIterator* meta_iter,
                           const InternalKeyComparator& internal_comparator,
                           BlockCacheLookupContext* lookup_context);
  Status ReadCompressionDictBlock(
      FilePrefetchBuffer* prefetch_buffer,
      std::unique_ptr<const BlockContents>* compression_dict_block) const;
  Status PrefetchIndexAndFilterBlocks(
      FilePrefetchBuffer* prefetch_buffer, InternalIterator* meta_iter,
      BlockBasedTable* new_table, bool prefetch_all,
      const BlockBasedTableOptions& table_options, const int level,
      BlockCacheLookupContext* lookup_context);

  static BlockType GetBlockTypeForMetaBlockByName(const Slice& meta_block_name);

  Status VerifyChecksumInMetaBlocks(InternalIteratorBase<Slice>* index_iter);
  Status VerifyChecksumInBlocks(InternalIteratorBase<IndexValue>* index_iter);

  // Create the filter from the filter block.
  virtual FilterBlockReader* ReadFilter(
      FilePrefetchBuffer* prefetch_buffer, const BlockHandle& filter_handle,
      const bool is_a_filter_partition,
      const SliceTransform* prefix_extractor = nullptr) const;

  static void SetupCacheKeyPrefix(Rep* rep);

  // Generate a cache key prefix from the file
  static void GenerateCachePrefix(Cache* cc, RandomAccessFile* file,
                                  char* buffer, size_t* size);
  static void GenerateCachePrefix(Cache* cc, WritableFile* file, char* buffer,
                                  size_t* size);

  // Helper functions for DumpTable()
  Status DumpIndexBlock(WritableFile* out_file);
  Status DumpDataBlocks(WritableFile* out_file);
  void DumpKeyValue(const Slice& key, const Slice& value,
                    WritableFile* out_file);

  // No copying allowed
  explicit BlockBasedTable(const TableReader&) = delete;
  void operator=(const TableReader&) = delete;

  friend class PartitionedFilterBlockReader;
  friend class PartitionedFilterBlockTest;
};

// Maitaning state of a two-level iteration on a partitioned index structure.
class BlockBasedTable::PartitionedIndexIteratorState
    : public TwoLevelIteratorState {
 public:
  PartitionedIndexIteratorState(
      const BlockBasedTable* table,
      std::unordered_map<uint64_t, CachableEntry<Block>>* block_map);
  InternalIteratorBase<IndexValue>* NewSecondaryIterator(
      const BlockHandle& index_value) override;

 private:
  // Don't own table_
  const BlockBasedTable* table_;
  std::unordered_map<uint64_t, CachableEntry<Block>>* block_map_;
};

// Stores all the properties associated with a BlockBasedTable.
// These are immutable.
struct BlockBasedTable::Rep {
  Rep(const ImmutableCFOptions& _ioptions, const EnvOptions& _env_options,
      const BlockBasedTableOptions& _table_opt,
      const InternalKeyComparator& _internal_comparator, bool skip_filters,
      int _level, const bool _immortal_table)
      : ioptions(_ioptions),
        env_options(_env_options),
        table_options(_table_opt),
        filter_policy(skip_filters ? nullptr : _table_opt.filter_policy.get()),
        internal_comparator(_internal_comparator),
        filter_type(FilterType::kNoFilter),
        index_type(BlockBasedTableOptions::IndexType::kBinarySearch),
        hash_index_allow_collision(false),
        whole_key_filtering(_table_opt.whole_key_filtering),
        prefix_filtering(true),
        global_seqno(kDisableGlobalSequenceNumber),
        level(_level),
        immortal_table(_immortal_table) {}

  const ImmutableCFOptions& ioptions;
  const EnvOptions& env_options;
  const BlockBasedTableOptions table_options;
  const FilterPolicy* const filter_policy;
  const InternalKeyComparator& internal_comparator;
  Status status;
  std::unique_ptr<RandomAccessFileReader> file;
  char cache_key_prefix[kMaxCacheKeyPrefixSize];
  size_t cache_key_prefix_size = 0;
  char persistent_cache_key_prefix[kMaxCacheKeyPrefixSize];
  size_t persistent_cache_key_prefix_size = 0;
  char compressed_cache_key_prefix[kMaxCacheKeyPrefixSize];
  size_t compressed_cache_key_prefix_size = 0;
  PersistentCacheOptions persistent_cache_options;

  // Footer contains the fixed table information
  Footer footer;
  // `filter` and `uncompression_dict` will be populated (i.e., non-nullptr)
  // and used only when options.block_cache is nullptr or when
  // `cache_index_and_filter_blocks == false`. Otherwise, we will get the
  // filter and compression dictionary blocks via the block cache. In that case,
  // `filter_handle`, and `compression_dict_handle` are used to lookup these
  // meta-blocks in block cache.
  //
  // Note: the IndexReader object is always stored in this member variable;
  // the index block itself, however, may or may not be in the block cache
  // based on the settings above. We plan to change the handling of the
  // filter and compression dictionary similarly.
  std::unique_ptr<IndexReader> index_reader;
  std::unique_ptr<FilterBlockReader> filter;
  std::unique_ptr<UncompressionDict> uncompression_dict;

  enum class FilterType {
    kNoFilter,
    kFullFilter,
    kBlockFilter,
    kPartitionedFilter,
  };
  FilterType filter_type;
  BlockHandle filter_handle;
  BlockHandle compression_dict_handle;

  std::shared_ptr<const TableProperties> table_properties;
  BlockBasedTableOptions::IndexType index_type;
  bool hash_index_allow_collision;
  bool whole_key_filtering;
  bool prefix_filtering;
  // TODO(kailiu) It is very ugly to use internal key in table, since table
  // module should not be relying on db module. However to make things easier
  // and compatible with existing code, we introduce a wrapper that allows
  // block to extract prefix without knowing if a key is internal or not.
  std::unique_ptr<SliceTransform> internal_prefix_transform;
  std::shared_ptr<const SliceTransform> table_prefix_extractor;

  // only used in level 0 files when pin_l0_filter_and_index_blocks_in_cache is
  // true or in all levels when pin_top_level_index_and_filter is set in
  // combination with partitioned filters: then we do use the LRU cache,
  // but we always keep the filter block's handle checked out here (=we
  // don't call Release()), plus the parsed out objects the LRU cache will never
  // push flush them out, hence they're pinned
  CachableEntry<FilterBlockReader> filter_entry;
  std::shared_ptr<const FragmentedRangeTombstoneList> fragmented_range_dels;

  // If global_seqno is used, all Keys in this file will have the same
  // seqno with value `global_seqno`.
  //
  // A value of kDisableGlobalSequenceNumber means that this feature is disabled
  // and every key have it's own seqno.
  SequenceNumber global_seqno;

  // the level when the table is opened, could potentially change when trivial
  // move is involved
  int level;

  // If false, blocks in this file are definitely all uncompressed. Knowing this
  // before reading individual blocks enables certain optimizations.
  bool blocks_maybe_compressed = true;

  // If true, data blocks in this file are definitely ZSTD compressed. If false
  // they might not be. When false we skip creating a ZSTD digested
  // uncompression dictionary. Even if we get a false negative, things should
  // still work, just not as quickly.
  bool blocks_definitely_zstd_compressed = false;

  // These describe how index is encoded.
  bool index_has_first_key = false;
  bool index_key_includes_seq = true;
  bool index_value_is_full = true;

  bool closed = false;
  const bool immortal_table;

  SequenceNumber get_global_seqno(BlockType block_type) const {
    return (block_type == BlockType::kFilter ||
            block_type == BlockType::kCompressionDictionary)
               ? kDisableGlobalSequenceNumber
               : global_seqno;
  }

  uint64_t cf_id_for_tracing() const {
    return table_properties ? table_properties->column_family_id
                            : rocksdb::TablePropertiesCollectorFactory::
                                  Context::kUnknownColumnFamily;
  }

  Slice cf_name_for_tracing() const {
    return table_properties ? table_properties->column_family_name
                            : BlockCacheTraceHelper::kUnknownColumnFamilyName;
  }

  uint32_t level_for_tracing() const { return level >= 0 ? level : UINT32_MAX; }

  uint64_t sst_number_for_tracing() const {
    return file ? TableFileNameToNumber(file->file_name()) : UINT64_MAX;
  }
};

// Iterates over the contents of BlockBasedTable.
template <class TBlockIter, typename TValue = Slice>
class BlockBasedTableIterator : public InternalIteratorBase<TValue> {
  // compaction_readahead_size: its value will only be used if for_compaction =
  // true
 public:
  BlockBasedTableIterator(const BlockBasedTable* table,
                          const ReadOptions& read_options,
                          const InternalKeyComparator& icomp,
                          InternalIteratorBase<IndexValue>* index_iter,
                          bool check_filter, bool need_upper_bound_check,
                          const SliceTransform* prefix_extractor,
                          BlockType block_type, TableReaderCaller caller,
                          size_t compaction_readahead_size = 0)
      : InternalIteratorBase<TValue>(false),
        table_(table),
        read_options_(read_options),
        icomp_(icomp),
        user_comparator_(icomp.user_comparator()),
        index_iter_(index_iter),
        pinned_iters_mgr_(nullptr),
        block_iter_points_to_real_block_(false),
        check_filter_(check_filter),
        need_upper_bound_check_(need_upper_bound_check),
        prefix_extractor_(prefix_extractor),
        block_type_(block_type),
        lookup_context_(caller),
        compaction_readahead_size_(compaction_readahead_size) {}

  ~BlockBasedTableIterator() { delete index_iter_; }

  void Seek(const Slice& target) override;
  void SeekForPrev(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() final override;
  bool NextAndGetResult(IterateResult* result) override;
  void Prev() override;
  bool Valid() const override {
    return !is_out_of_bound_ &&
           (is_at_first_key_from_index_ ||
            (block_iter_points_to_real_block_ && block_iter_.Valid()));
  }
  Slice key() const override {
    assert(Valid());
    if (is_at_first_key_from_index_) {
      return index_iter_->value().first_internal_key;
    } else {
      return block_iter_.key();
    }
  }
  Slice user_key() const override {
    assert(Valid());
    if (is_at_first_key_from_index_) {
      return ExtractUserKey(index_iter_->value().first_internal_key);
    } else {
      return block_iter_.user_key();
    }
  }
  TValue value() const override {
    assert(Valid());

    // Load current block if not loaded.
    if (is_at_first_key_from_index_ &&
        !const_cast<BlockBasedTableIterator*>(this)
             ->MaterializeCurrentBlock()) {
      // Oops, index is not consistent with block contents, but we have
      // no good way to report error at this point. Let's return empty value.
      return TValue();
    }

    return block_iter_.value();
  }
  Status status() const override {
    if (!index_iter_->status().ok()) {
      return index_iter_->status();
    } else if (block_iter_points_to_real_block_) {
      return block_iter_.status();
    } else {
      return Status::OK();
    }
  }

  // Whether iterator invalidated for being out of bound.
  bool IsOutOfBound() override { return is_out_of_bound_; }

  inline bool MayBeOutOfUpperBound() override {
    assert(Valid());
    return !data_block_within_upper_bound_;
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }
  bool IsKeyPinned() const override {
    // Our key comes either from block_iter_'s current key
    // or index_iter_'s current *value*.
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           ((is_at_first_key_from_index_ && index_iter_->IsValuePinned()) ||
            (block_iter_points_to_real_block_ && block_iter_.IsKeyPinned()));
  }
  bool IsValuePinned() const override {
    // Load current block if not loaded.
    if (is_at_first_key_from_index_) {
      const_cast<BlockBasedTableIterator*>(this)->MaterializeCurrentBlock();
    }
    // BlockIter::IsValuePinned() is always true. No need to check
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           block_iter_points_to_real_block_;
  }

  bool CheckPrefixMayMatch(const Slice& ikey) {
    if (check_filter_ &&
        !table_->PrefixMayMatch(ikey, read_options_, prefix_extractor_,
                                need_upper_bound_check_, &lookup_context_)) {
      // TODO remember the iterator is invalidated because of prefix
      // match. This can avoid the upper level file iterator to falsely
      // believe the position is the end of the SST file and move to
      // the first key of the next file.
      ResetDataIter();
      return false;
    }
    return true;
  }

  void ResetDataIter() {
    if (block_iter_points_to_real_block_) {
      if (pinned_iters_mgr_ != nullptr && pinned_iters_mgr_->PinningEnabled()) {
        block_iter_.DelegateCleanupsTo(pinned_iters_mgr_);
      }
      block_iter_.Invalidate(Status::OK());
      block_iter_points_to_real_block_ = false;
    }
  }

  void SavePrevIndexValue() {
    if (block_iter_points_to_real_block_) {
      // Reseek. If they end up with the same data block, we shouldn't re-fetch
      // the same data block.
      prev_block_offset_ = index_iter_->value().handle.offset();
    }
  }

 private:
  const BlockBasedTable* table_;
  const ReadOptions read_options_;
  const InternalKeyComparator& icomp_;
  UserComparatorWrapper user_comparator_;
  InternalIteratorBase<IndexValue>* index_iter_;
  PinnedIteratorsManager* pinned_iters_mgr_;
  TBlockIter block_iter_;

  // True if block_iter_ is initialized and points to the same block
  // as index iterator.
  bool block_iter_points_to_real_block_;
  // See InternalIteratorBase::IsOutOfBound().
  bool is_out_of_bound_ = false;
  // Whether current data block being fully within iterate upper bound.
  bool data_block_within_upper_bound_ = false;
  // True if we're standing at the first key of a block, and we haven't loaded
  // that block yet. A call to value() will trigger loading the block.
  bool is_at_first_key_from_index_ = false;
  bool check_filter_;
  // TODO(Zhongyi): pick a better name
  bool need_upper_bound_check_;
  const SliceTransform* prefix_extractor_;
  BlockType block_type_;
  uint64_t prev_block_offset_ = std::numeric_limits<uint64_t>::max();
  BlockCacheLookupContext lookup_context_;
  // Readahead size used in compaction, its value is used only if
  // lookup_context_.caller = kCompaction.
  size_t compaction_readahead_size_;

  // All the below fields control iterator readahead
  static const size_t kInitAutoReadaheadSize = 8 * 1024;
  // Found that 256 KB readahead size provides the best performance, based on
  // experiments, for auto readahead. Experiment data is in PR #3282.
  static const size_t kMaxAutoReadaheadSize;
  static const int kMinNumFileReadsToStartAutoReadahead = 2;
  size_t readahead_size_ = kInitAutoReadaheadSize;
  size_t readahead_limit_ = 0;
  int64_t num_file_reads_ = 0;
  std::unique_ptr<FilePrefetchBuffer> prefetch_buffer_;

  // If `target` is null, seek to first.
  void SeekImpl(const Slice* target);

  void InitDataBlock();
  bool MaterializeCurrentBlock();
  void FindKeyForward();
  void FindBlockForward();
  void FindKeyBackward();
  void CheckOutOfBound();

  // Check if data block is fully within iterate_upper_bound.
  //
  // Note MyRocks may update iterate bounds between seek. To workaround it,
  // we need to check and update data_block_within_upper_bound_ accordingly.
  void CheckDataBlockWithinUpperBound();
};

}  // namespace rocksdb
