//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cstdint>
#include <memory>

#include "cache/cache_entry_roles.h"
#include "cache/cache_key.h"
#include "cache/cache_reservation_manager.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/seqno_to_time_mapping.h"
#include "file/filename.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table_properties.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_cache.h"
#include "table/block_based/block_type.h"
#include "table/block_based/cachable_entry.h"
#include "table/block_based/filter_block.h"
#include "table/block_based/uncompression_dict_reader.h"
#include "table/format.h"
#include "table/persistent_cache_options.h"
#include "table/table_properties_internal.h"
#include "table/table_reader.h"
#include "table/two_level_iterator.h"
#include "trace_replay/block_cache_tracer.h"
#include "util/atomic.h"
#include "util/coro_utils.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

class Cache;
class FilterBlockReader;
class FullFilterBlockReader;
class Footer;
class InternalKeyComparator;
class Iterator;
class FSRandomAccessFile;
class TableCache;
class TableReader;
class WritableFile;
struct BlockBasedTableOptions;
struct EnvOptions;
struct ReadOptions;
class GetContext;

using KVPairBlock = std::vector<std::pair<std::string, std::string>>;

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
  static const std::string kObsoleteFilterBlockPrefix;
  static const std::string kFullFilterBlockPrefix;
  static const std::string kPartitionedFilterBlockPrefix;

  // 1-byte compression type + 32-bit checksum
  static constexpr size_t kBlockTrailerSize = 5;

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
  // @param force_direct_prefetch if true, always prefetching to RocksDB
  //    buffer, rather than calling RandomAccessFile::Prefetch().
  static Status Open(
      const ReadOptions& ro, const ImmutableOptions& ioptions,
      const EnvOptions& env_options,
      const BlockBasedTableOptions& table_options,
      const InternalKeyComparator& internal_key_comparator,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      uint8_t block_protection_bytes_per_key,
      std::unique_ptr<TableReader>* table_reader, uint64_t tail_size,
      std::shared_ptr<CacheReservationManager> table_reader_cache_res_mgr =
          nullptr,
      const std::shared_ptr<const SliceTransform>& prefix_extractor = nullptr,
      bool prefetch_index_and_filter_in_cache = true, bool skip_filters = false,
      int level = -1, const bool immortal_table = false,
      const SequenceNumber largest_seqno = 0,
      bool force_direct_prefetch = false,
      TailPrefetchStats* tail_prefetch_stats = nullptr,
      BlockCacheTracer* const block_cache_tracer = nullptr,
      size_t max_file_size_for_l0_meta_pin = 0,
      const std::string& cur_db_session_id = "", uint64_t cur_file_num = 0,
      UniqueId64x2 expected_unique_id = {},
      const bool user_defined_timestamps_persisted = true);

  bool PrefixRangeMayMatch(const Slice& internal_key,
                           const ReadOptions& read_options,
                           const SliceTransform* options_prefix_extractor,
                           const bool need_upper_bound_check,
                           BlockCacheLookupContext* lookup_context,
                           bool* filter_checked) const;

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  // @param read_options Must outlive the returned iterator.
  // @param skip_filters Disables loading/accessing the filter block
  // compaction_readahead_size: its value will only be used if caller =
  // kCompaction.
  InternalIterator* NewIterator(const ReadOptions&,
                                const SliceTransform* prefix_extractor,
                                Arena* arena, bool skip_filters,
                                TableReaderCaller caller,
                                size_t compaction_readahead_size = 0,
                                bool allow_unprepared_value = false) override;

  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions& read_options) override;

  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      SequenceNumber read_seqno, const Slice* timestamp) override;

  // @param skip_filters Disables loading/accessing the filter block
  Status Get(const ReadOptions& readOptions, const Slice& key,
             GetContext* get_context, const SliceTransform* prefix_extractor,
             bool skip_filters = false) override;

  Status MultiGetFilter(const ReadOptions& read_options,
                        const SliceTransform* prefix_extractor,
                        MultiGetRange* mget_range) override;

  DECLARE_SYNC_AND_ASYNC_OVERRIDE(void, MultiGet,
                                  const ReadOptions& readOptions,
                                  const MultiGetContext::Range* mget_range,
                                  const SliceTransform* prefix_extractor,
                                  bool skip_filters = false);

  // Pre-fetch the disk blocks that correspond to the key range specified by
  // (kbegin, kend). The call will return error status in the event of
  // IO or iteration error.
  Status Prefetch(const ReadOptions& read_options, const Slice* begin,
                  const Slice* end) override;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file). The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  uint64_t ApproximateOffsetOf(const ReadOptions& read_options,
                               const Slice& key,
                               TableReaderCaller caller) override;

  // Given start and end keys, return the approximate data size in the file
  // between the keys. The returned value is in terms of file bytes, and so
  // includes effects like compression of the underlying data.
  // The start key must not be greater than the end key.
  uint64_t ApproximateSize(const ReadOptions& read_options, const Slice& start,
                           const Slice& end, TableReaderCaller caller) override;

  Status ApproximateKeyAnchors(const ReadOptions& read_options,
                               std::vector<Anchor>& anchors) override;

  bool EraseFromCache(const BlockHandle& handle) const;

  bool TEST_BlockInCache(const BlockHandle& handle) const;

  // Returns true if the block for the specified key is in cache.
  // REQUIRES: key is in this table && block cache enabled
  bool TEST_KeyInCache(const ReadOptions& options, const Slice& key);

  void TEST_GetDataBlockHandle(const ReadOptions& options, const Slice& key,
                               BlockHandle& handle);

  // Set up the table for Compaction. Might change some parameters with
  // posix_fadvise
  void SetupForCompaction() override;

  std::shared_ptr<const TableProperties> GetTableProperties() const override;

  const SeqnoToTimeMapping& GetSeqnoToTimeMapping() const;

  size_t ApproximateMemoryUsage() const override;

  // convert SST file to a human readable form
  Status DumpTable(WritableFile* out_file) override;

  Status VerifyChecksum(const ReadOptions& readOptions,
                        TableReaderCaller caller) override;

  void MarkObsolete(uint32_t uncache_aggressiveness) override;

  ~BlockBasedTable();

  bool TEST_FilterBlockInCache() const;
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
    virtual Status CacheDependencies(
        const ReadOptions& /*ro*/, bool /* pin */,
        FilePrefetchBuffer* /* tail_prefetch_buffer */) {
      return Status::OK();
    }
    virtual void EraseFromCacheBeforeDestruction(
        uint32_t /*uncache_aggressiveness*/) {}
  };

  class IndexReaderCommon;

  static void SetupBaseCacheKey(const TableProperties* properties,
                                const std::string& cur_db_session_id,
                                uint64_t cur_file_number,
                                OffsetableCacheKey* out_base_cache_key,
                                bool* out_is_stable = nullptr);

  static CacheKey GetCacheKey(const OffsetableCacheKey& base_cache_key,
                              const BlockHandle& handle);

  static void UpdateCacheInsertionMetrics(BlockType block_type,
                                          GetContext* get_context, size_t usage,
                                          bool redundant,
                                          Statistics* const statistics);

  Statistics* GetStatistics() const;
  bool IsLastLevel() const;

  // Get the size to read from storage for a BlockHandle. size_t because we
  // are about to load into memory.
  static inline size_t BlockSizeWithTrailer(const BlockHandle& handle) {
    return static_cast<size_t>(handle.size() + kBlockTrailerSize);
  }

  // It is the caller's responsibility to make sure that this is called with
  // block-based table serialized block contents, which contains the compression
  // byte in the trailer after `block_size`.
  static inline CompressionType GetBlockCompressionType(const char* block_data,
                                                        size_t block_size) {
    return static_cast<CompressionType>(block_data[block_size]);
  }
  static inline CompressionType GetBlockCompressionType(
      const BlockContents& contents) {
    assert(contents.has_trailer);
    return GetBlockCompressionType(contents.data.data(), contents.data.size());
  }

  // Retrieve all key value pairs from data blocks in the table.
  // The key retrieved are internal keys.
  Status GetKVPairsFromDataBlocks(const ReadOptions& read_options,
                                  std::vector<KVPairBlock>* kv_pair_blocks);

  template <typename TBlocklike>
  Status LookupAndPinBlocksInCache(
      const ReadOptions& ro, const BlockHandle& handle,
      CachableEntry<TBlocklike>* out_parsed_block) const;

  struct Rep;

  Rep* get_rep() { return rep_; }
  const Rep* get_rep() const { return rep_; }

  // input_iter: if it is not null, update this one and return it as Iterator
  template <typename TBlockIter>
  TBlockIter* NewDataBlockIterator(
      const ReadOptions& ro, const BlockHandle& block_handle,
      TBlockIter* input_iter, BlockType block_type, GetContext* get_context,
      BlockCacheLookupContext* lookup_context,
      FilePrefetchBuffer* prefetch_buffer, bool for_compaction, bool async_read,
      Status& s, bool use_block_cache_for_lookup) const;

  // input_iter: if it is not null, update this one and return it as Iterator
  template <typename TBlockIter>
  TBlockIter* NewDataBlockIterator(const ReadOptions& ro,
                                   CachableEntry<Block>& block,
                                   TBlockIter* input_iter, Status s) const;

  class PartitionedIndexIteratorState;

  template <typename TBlocklike>
  friend class FilterBlockReaderCommon;

  friend class PartitionIndexReader;

  friend class UncompressionDictReader;

 protected:
  Rep* rep_;
  explicit BlockBasedTable(Rep* rep, BlockCacheTracer* const block_cache_tracer)
      : rep_(rep), block_cache_tracer_(block_cache_tracer) {}
  // No copying allowed
  explicit BlockBasedTable(const TableReader&) = delete;
  void operator=(const TableReader&) = delete;

 private:
  friend class MockedBlockBasedTable;
  friend class BlockBasedTableReaderTestVerifyChecksum_ChecksumMismatch_Test;
  BlockCacheTracer* const block_cache_tracer_;

  void UpdateCacheHitMetrics(BlockType block_type, GetContext* get_context,
                             size_t usage) const;
  void UpdateCacheMissMetrics(BlockType block_type,
                              GetContext* get_context) const;

  // Either Block::NewDataIterator() or Block::NewIndexIterator().
  template <typename TBlockIter>
  static TBlockIter* InitBlockIterator(const Rep* rep, Block* block,
                                       BlockType block_type,
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
  template <typename TBlocklike>
  WithBlocklikeCheck<Status, TBlocklike> MaybeReadBlockAndLoadToCache(
      FilePrefetchBuffer* prefetch_buffer, const ReadOptions& ro,
      const BlockHandle& handle, const UncompressionDict& uncompression_dict,
      bool for_compaction, CachableEntry<TBlocklike>* block_entry,
      GetContext* get_context, BlockCacheLookupContext* lookup_context,
      BlockContents* contents, bool async_read,
      bool use_block_cache_for_lookup) const;

  // Similar to the above, with one crucial difference: it will retrieve the
  // block from the file even if there are no caches configured (assuming the
  // read options allow I/O).
  template <typename TBlocklike>
  WithBlocklikeCheck<Status, TBlocklike> RetrieveBlock(
      FilePrefetchBuffer* prefetch_buffer, const ReadOptions& ro,
      const BlockHandle& handle, const UncompressionDict& uncompression_dict,
      CachableEntry<TBlocklike>* block_entry, GetContext* get_context,
      BlockCacheLookupContext* lookup_context, bool for_compaction,
      bool use_cache, bool async_read, bool use_block_cache_for_lookup) const;

  template <typename TBlocklike>
  WithBlocklikeCheck<void, TBlocklike> SaveLookupContextOrTraceRecord(
      const Slice& block_key, bool is_cache_hit, const ReadOptions& ro,
      const TBlocklike* parsed_block_value,
      BlockCacheLookupContext* lookup_context) const;

  void FinishTraceRecord(const BlockCacheLookupContext& lookup_context,
                         const Slice& block_key, const Slice& referenced_key,
                         bool does_referenced_key_exist,
                         uint64_t referenced_data_size) const;

  DECLARE_SYNC_AND_ASYNC_CONST(
      void, RetrieveMultipleBlocks, const ReadOptions& options,
      const MultiGetRange* batch,
      const autovector<BlockHandle, MultiGetContext::MAX_BATCH_SIZE>* handles,
      Status* statuses, CachableEntry<Block_kData>* results, char* scratch,
      const UncompressionDict& uncompression_dict, bool use_fs_scratch);

  // Get the iterator from the index reader.
  //
  // If input_iter is not set, return a new Iterator.
  // If input_iter is set, try to update it and return it as Iterator.
  // However note that in some cases the returned iterator may be different
  // from input_iter. In such case the returned iterator should be freed.
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

  template <typename TBlocklike>
  Cache::Priority GetCachePriority() const;

  // Read block cache from block caches (if set): block_cache.
  // On success, Status::OK with be returned and @block will be populated with
  // pointer to the block as well as its block handle.
  // @param uncompression_dict Data for presetting the compression library's
  //    dictionary.
  template <typename TBlocklike>
  WithBlocklikeCheck<Status, TBlocklike> GetDataBlockFromCache(
      const Slice& cache_key, BlockCacheInterface<TBlocklike> block_cache,
      CachableEntry<TBlocklike>* block, GetContext* get_context,
      const UncompressionDict* dict) const;

  // Put a maybe compressed block to the corresponding block caches.
  // This method will perform decompression against block_contents if needed
  // and then populate the block caches.
  // On success, Status::OK will be returned; also @block will be populated with
  // uncompressed block and its cache handle.
  //
  // Allocated memory managed by block_contents will be transferred to
  // PutDataBlockToCache(). After the call, the object will be invalid.
  // @param uncompression_dict Data for presetting the compression library's
  //    dictionary.
  template <typename TBlocklike>
  WithBlocklikeCheck<Status, TBlocklike> PutDataBlockToCache(
      const Slice& cache_key, BlockCacheInterface<TBlocklike> block_cache,
      CachableEntry<TBlocklike>* cached_block,
      BlockContents&& uncompressed_block_contents,
      BlockContents&& compressed_block_contents,
      CompressionType block_comp_type,
      const UncompressionDict& uncompression_dict,
      MemoryAllocator* memory_allocator, GetContext* get_context) const;

  // Calls (*handle_result)(arg, ...) repeatedly, starting with the entry found
  // after a call to Seek(key), until handle_result returns false.
  // May not make such a call if filter policy says that key is not present.
  friend class TableCache;
  friend class BlockBasedTableBuilder;

  // Create a index reader based on the index type stored in the table.
  // Optionally, user can pass a preloaded meta_index_iter for the index that
  // need to access extra meta blocks for index construction. This parameter
  // helps avoid re-reading meta index block if caller already created one.
  Status CreateIndexReader(const ReadOptions& ro,
                           FilePrefetchBuffer* prefetch_buffer,
                           InternalIterator* preloaded_meta_index_iter,
                           bool use_cache, bool prefetch, bool pin,
                           BlockCacheLookupContext* lookup_context,
                           std::unique_ptr<IndexReader>* index_reader);

  bool FullFilterKeyMayMatch(FilterBlockReader* filter, const Slice& user_key,
                             const SliceTransform* prefix_extractor,
                             GetContext* get_context,
                             BlockCacheLookupContext* lookup_context,
                             const ReadOptions& read_options) const;

  void FullFilterKeysMayMatch(FilterBlockReader* filter, MultiGetRange* range,
                              const SliceTransform* prefix_extractor,
                              BlockCacheLookupContext* lookup_context,
                              const ReadOptions& read_options) const;

  // If force_direct_prefetch is true, always prefetching to RocksDB
  //    buffer, rather than calling RandomAccessFile::Prefetch().
  static Status PrefetchTail(
      const ReadOptions& ro, const ImmutableOptions& ioptions,
      RandomAccessFileReader* file, uint64_t file_size,
      bool force_direct_prefetch, TailPrefetchStats* tail_prefetch_stats,
      const bool prefetch_all, const bool preload_all,
      std::unique_ptr<FilePrefetchBuffer>* prefetch_buffer, Statistics* stats,
      uint64_t tail_size, Logger* const logger);
  Status ReadMetaIndexBlock(const ReadOptions& ro,
                            FilePrefetchBuffer* prefetch_buffer,
                            std::unique_ptr<Block>* metaindex_block,
                            std::unique_ptr<InternalIterator>* iter);
  Status ReadPropertiesBlock(const ReadOptions& ro,
                             FilePrefetchBuffer* prefetch_buffer,
                             InternalIterator* meta_iter,
                             const SequenceNumber largest_seqno);
  Status ReadRangeDelBlock(const ReadOptions& ro,
                           FilePrefetchBuffer* prefetch_buffer,
                           InternalIterator* meta_iter,
                           const InternalKeyComparator& internal_comparator,
                           BlockCacheLookupContext* lookup_context);
  // If index and filter blocks do not need to be pinned, `prefetch_all`
  // determines whether they will be read and add to cache.
  Status PrefetchIndexAndFilterBlocks(
      const ReadOptions& ro, FilePrefetchBuffer* prefetch_buffer,
      InternalIterator* meta_iter, BlockBasedTable* new_table,
      bool prefetch_all, const BlockBasedTableOptions& table_options,
      const int level, size_t file_size, size_t max_file_size_for_l0_meta_pin,
      BlockCacheLookupContext* lookup_context);

  static BlockType GetBlockTypeForMetaBlockByName(const Slice& meta_block_name);

  Status VerifyChecksumInMetaBlocks(const ReadOptions& read_options,
                                    InternalIteratorBase<Slice>* index_iter);
  Status VerifyChecksumInBlocks(const ReadOptions& read_options,
                                InternalIteratorBase<IndexValue>* index_iter);

  // Create the filter from the filter block.
  std::unique_ptr<FilterBlockReader> CreateFilterBlockReader(
      const ReadOptions& ro, FilePrefetchBuffer* prefetch_buffer,
      bool use_cache, bool prefetch, bool pin,
      BlockCacheLookupContext* lookup_context);

  // Size of all data blocks, maybe approximate
  uint64_t GetApproximateDataSize();

  // Given an iterator return its offset in data block section of file.
  uint64_t ApproximateDataOffsetOf(
      const InternalIteratorBase<IndexValue>& index_iter,
      uint64_t data_size) const;

  // Helper functions for DumpTable()
  Status DumpIndexBlock(std::ostream& out_stream);
  Status DumpDataBlocks(std::ostream& out_stream);
  void DumpKeyValue(const Slice& key, const Slice& value,
                    std::ostream& out_stream);

  // Returns false if prefix_extractor exists and is compatible with that used
  // in building the table file, otherwise true.
  bool PrefixExtractorChanged(const SliceTransform* prefix_extractor) const;

  bool TimestampMayMatch(const ReadOptions& read_options) const;

  // A cumulative data block file read in MultiGet lower than this size will
  // use a stack buffer
  static constexpr size_t kMultiGetReadStackBufSize = 8192;

  friend class PartitionedFilterBlockReader;
  friend class PartitionedFilterBlockTest;
  friend class DBBasicTest_MultiGetIOBufferOverrun_Test;
};

// Maintaining state of a two-level iteration on a partitioned index structure.
class BlockBasedTable::PartitionedIndexIteratorState
    : public TwoLevelIteratorState {
 public:
  PartitionedIndexIteratorState(
      const BlockBasedTable* table,
      UnorderedMap<uint64_t, CachableEntry<Block>>* block_map);
  InternalIteratorBase<IndexValue>* NewSecondaryIterator(
      const BlockHandle& index_value) override;

 private:
  // Don't own table_
  const BlockBasedTable* table_;
  UnorderedMap<uint64_t, CachableEntry<Block>>* block_map_;
};

// Stores all the properties associated with a BlockBasedTable.
// These are immutable.
struct BlockBasedTable::Rep {
  Rep(const ImmutableOptions& _ioptions, const EnvOptions& _env_options,
      const BlockBasedTableOptions& _table_opt,
      const InternalKeyComparator& _internal_comparator, bool skip_filters,
      uint64_t _file_size, int _level, const bool _immortal_table,
      const bool _user_defined_timestamps_persisted = true)
      : ioptions(_ioptions),
        env_options(_env_options),
        table_options(_table_opt),
        filter_policy(skip_filters ? nullptr : _table_opt.filter_policy.get()),
        internal_comparator(_internal_comparator),
        filter_type(FilterType::kNoFilter),
        index_type(BlockBasedTableOptions::IndexType::kBinarySearch),
        whole_key_filtering(_table_opt.whole_key_filtering),
        prefix_filtering(true),
        global_seqno(kDisableGlobalSequenceNumber),
        file_size(_file_size),
        level(_level),
        immortal_table(_immortal_table),
        user_defined_timestamps_persisted(_user_defined_timestamps_persisted) {}
  ~Rep() { status.PermitUncheckedError(); }
  const ImmutableOptions& ioptions;
  const EnvOptions& env_options;
  const BlockBasedTableOptions table_options;
  const FilterPolicy* const filter_policy;
  const InternalKeyComparator& internal_comparator;
  Status status;
  std::unique_ptr<RandomAccessFileReader> file;
  OffsetableCacheKey base_cache_key;
  PersistentCacheOptions persistent_cache_options;

  // Footer contains the fixed table information
  Footer footer;

  std::unique_ptr<IndexReader> index_reader;
  std::unique_ptr<FilterBlockReader> filter;
  std::unique_ptr<UncompressionDictReader> uncompression_dict_reader;

  enum class FilterType {
    kNoFilter,
    kFullFilter,
    kPartitionedFilter,
  };
  FilterType filter_type;
  BlockHandle filter_handle;
  BlockHandle compression_dict_handle;

  std::shared_ptr<const TableProperties> table_properties;
  SeqnoToTimeMapping seqno_to_time_mapping;
  BlockHandle index_handle;
  BlockBasedTableOptions::IndexType index_type;
  bool whole_key_filtering;
  bool prefix_filtering;
  std::shared_ptr<const SliceTransform> table_prefix_extractor;

  std::shared_ptr<FragmentedRangeTombstoneList> fragmented_range_dels;

  // Context for block cache CreateCallback
  BlockCreateContext create_context;

  // If global_seqno is used, all Keys in this file will have the same
  // seqno with value `global_seqno`.
  //
  // A value of kDisableGlobalSequenceNumber means that this feature is disabled
  // and every key have it's own seqno.
  SequenceNumber global_seqno;

  // Size of the table file on disk
  uint64_t file_size;

  // the level when the table is opened, could potentially change when trivial
  // move is involved
  int level;

  // the timestamp range of table
  // Points into memory owned by TableProperties. This would need to change if
  // TableProperties become subject to cache eviction.
  Slice min_timestamp;
  Slice max_timestamp;

  // If false, blocks in this file are definitely all uncompressed. Knowing this
  // before reading individual blocks enables certain optimizations.
  bool blocks_maybe_compressed = true;

  // These describe how index is encoded.
  bool index_has_first_key = false;
  bool index_key_includes_seq = true;
  bool index_value_is_full = true;

  // Whether block checksums in metadata blocks were verified on open.
  // This is only to mostly maintain current dubious behavior of VerifyChecksum
  // with respect to index blocks, but only when the checksum was previously
  // verified.
  bool verify_checksum_set_on_open = false;

  const bool immortal_table;
  // Whether the user key contains user-defined timestamps. If this is false and
  // the running user comparator has a non-zero timestamp size, a min timestamp
  // of this size will be padded to each user key while parsing blocks whenever
  // it applies.  This includes the keys in data block, index block for data
  // block, top-level index for index partitions (if index type is
  // `kTwoLevelIndexSearch`), top-level index for filter partitions (if using
  // partitioned filters), the `first_internal_key` in `IndexValue`, the
  // `end_key` for range deletion entries.
  const bool user_defined_timestamps_persisted;

  // Set to >0 when the file is known to be obsolete and should have its block
  // cache entries evicted on close. NOTE: when the file becomes obsolete,
  // there could be multiple table cache references that all mark this file as
  // obsolete. An atomic resolves the race quite reasonably. Even in the rare
  // case of such a race, they will most likely be storing the same value.
  RelaxedAtomic<uint32_t> uncache_aggressiveness{0};

  std::unique_ptr<CacheReservationManager::CacheReservationHandle>
      table_reader_cache_res_handle = nullptr;

  SequenceNumber get_global_seqno(BlockType block_type) const {
    return (block_type == BlockType::kFilterPartitionIndex ||
            block_type == BlockType::kCompressionDictionary)
               ? kDisableGlobalSequenceNumber
               : global_seqno;
  }

  uint64_t cf_id_for_tracing() const {
    return table_properties
               ? table_properties->column_family_id
               : ROCKSDB_NAMESPACE::TablePropertiesCollectorFactory::Context::
                     kUnknownColumnFamily;
  }

  Slice cf_name_for_tracing() const {
    return table_properties ? table_properties->column_family_name
                            : BlockCacheTraceHelper::kUnknownColumnFamilyName;
  }

  uint32_t level_for_tracing() const { return level >= 0 ? level : UINT32_MAX; }

  uint64_t sst_number_for_tracing() const {
    return file ? TableFileNameToNumber(file->file_name()) : UINT64_MAX;
  }
  void CreateFilePrefetchBuffer(
      const ReadaheadParams& readahead_params,
      std::unique_ptr<FilePrefetchBuffer>* fpb,
      const std::function<void(bool, uint64_t&, uint64_t&)>& readaheadsize_cb,
      FilePrefetchBufferUsage usage) const {
    fpb->reset(new FilePrefetchBuffer(
        readahead_params, !ioptions.allow_mmap_reads /* enable */,
        false /* track_min_offset */, ioptions.fs.get(), ioptions.clock,
        ioptions.stats, readaheadsize_cb, usage));
  }

  void CreateFilePrefetchBufferIfNotExists(
      const ReadaheadParams& readahead_params,
      std::unique_ptr<FilePrefetchBuffer>* fpb,
      const std::function<void(bool, uint64_t&, uint64_t&)>& readaheadsize_cb,
      FilePrefetchBufferUsage usage = FilePrefetchBufferUsage::kUnknown) const {
    if (!(*fpb)) {
      CreateFilePrefetchBuffer(readahead_params, fpb, readaheadsize_cb, usage);
    }
  }

  std::size_t ApproximateMemoryUsage() const {
    std::size_t usage = 0;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    usage += malloc_usable_size(const_cast<BlockBasedTable::Rep*>(this));
#else
    usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    return usage;
  }
};

// This is an adapter class for `WritableFile` to be used for `std::ostream`.
// The adapter wraps a `WritableFile`, which can be passed to a `std::ostream`
// constructor for storing streaming data.
// Note:
//  * This adapter doesn't provide any buffering, each write is forwarded to
//    `WritableFile->Append()` directly.
//  * For a failed write, the user needs to check the status by `ostream.good()`
class WritableFileStringStreamAdapter : public std::stringbuf {
 public:
  explicit WritableFileStringStreamAdapter(WritableFile* writable_file)
      : file_(writable_file) {}

  // Override overflow() to handle `sputc()`. There are cases that will not go
  // through `xsputn()` e.g. `std::endl` or an unsigned long long is written by
  // `os.put()` directly and will call `sputc()` By internal implementation:
  //    int_type __CLR_OR_THIS_CALL sputc(_Elem _Ch) {  // put a character
  //        return 0 < _Pnavail() ? _Traits::to_int_type(*_Pninc() = _Ch) :
  //        overflow(_Traits::to_int_type(_Ch));
  //    }
  // As we explicitly disabled buffering (_Pnavail() is always 0), every write,
  // not captured by xsputn(), becomes an overflow here.
  int overflow(int ch = EOF) override {
    if (ch != EOF) {
      Status s = file_->Append(Slice((char*)&ch, 1));
      if (s.ok()) {
        return ch;
      }
    }
    return EOF;
  }

  std::streamsize xsputn(char const* p, std::streamsize n) override {
    Status s = file_->Append(Slice(p, n));
    if (!s.ok()) {
      return 0;
    }
    return n;
  }

 private:
  WritableFile* file_;
};

}  // namespace ROCKSDB_NAMESPACE
