// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#pragma once

#include <atomic>
#include <memory>
#include <thread>

#include "async/random_read_context.h"
#include "rocksdb/async/callables.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "table/block_based_table_reader.h"
#include "table/format.h"

namespace rocksdb {

struct EnvOptions;
class  Block;
class  BlockBasedTable;
struct BlockBasedTableOptions;
struct ImmutableCFOptions;
class  InternalIterator;
class  RandomAccessFileReader;
struct ReadOptions;
struct TableProperties;
class  TableReader;

namespace async {

// Abstract to a separate class for reuse
class MaybeLoadDataBlockToCacheHelper {
 public:

  using
  BlockContCallback = ReadBlockContentsContext::ReadBlockContCallback;

  MaybeLoadDataBlockToCacheHelper(bool is_index, BlockBasedTable::Rep* rep) :
    is_index_(is_index),
    sw_(rep->ioptions.env, rep->ioptions.statistics, READ_BLOCK_GET_MICROS, true) {}

  MaybeLoadDataBlockToCacheHelper(
    const MaybeLoadDataBlockToCacheHelper&) = delete;
  MaybeLoadDataBlockToCacheHelper& operator=(
    const MaybeLoadDataBlockToCacheHelper&) = delete;

  // Check if the block can be fetched from cache
  // if cache is enabled withih the table reader
  // The class is no-op if neither uncompressed or compressed
  // caches are present within the table
  // Returns NoFound if not found
  Status GetBlockFromCache(
    BlockBasedTable::Rep* rep,
    const ReadOptions& ro,
    const BlockHandle& handle,
    const Slice& compression_dict,
    BlockBasedTable::CachableEntry<Block>* entry);

  // Reading a raw block
  // This performs both sync and async depending on the presents
  // of the callback.
  // The caller must invoke PutBlockToCache either directly
  // after sync completion or via a specified callback
  Status RequestCachebableBlock(const BlockContCallback& cb,
                                BlockBasedTable::Rep* rep,
                                const ReadOptions & ro,
                                const BlockHandle& block_handle,
                                BlockContents* result,
                                bool do_uncompress);

  Status OnBlockReadComplete(const Status&, BlockBasedTable::Rep* rep,
                             const ReadOptions& ro,
                             BlockContents&& block_cont,
                             const Slice& compression_dict,
                             BlockBasedTable::CachableEntry<Block>* entry);

 private:

  Status PutBlockToCache(BlockBasedTable::Rep* rep,
                         const ReadOptions& ro,
                         BlockContents&& block_cont,
                         const Slice& compression_dict,
                         BlockBasedTable::CachableEntry<Block>* entry);

  bool                                   is_index_;
  StopWatch                              sw_;

  char cache_key_[BlockBasedTable::kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  char compressed_cache_key_[BlockBasedTable::kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  Slice                                  key_;
  Slice                                  ckey_;
};

// This class attempts to load in parallel 3 things
// - Properties if present
// - Compression Dictionary if present
// - Range of tombstones if present
// All of the above are separate block reads
// The last one completing calls the parent context callback
// This class guarantees to invoke the callback and destroy
// itself whether sync or async
class TableReadMetaBlocksContext : public MaybeLoadDataBlockToCacheHelper  {
 public:
  // Client callback when all is done
  using
  ReadMetaBlocksCallback = async::Callable<Status>;

  // Flags
  enum MetaKinds : uint32_t {
    mNone,  // nothing to read or all read
    mProperties = 0x1, // Properties
    mCompDict = 0x2, // Compression dictionary
    mRangDel = 0x4 // range del block
  };

  TableReadMetaBlocksContext(BlockBasedTable* table, bool is_index) :
    MaybeLoadDataBlockToCacheHelper(is_index, table->rep_),
    table_(table),
    ro_nochecksum_(),
    ro_default_(),
    op_count_(0) {
    ro_nochecksum_.verify_checksums = false;
  }

  void SetCB(const ReadMetaBlocksCallback& cb) {
    cb_ = cb;
  }

  void AddProperties(const BlockHandle& handle) {
    prop_handle_ = handle;
  }

  void AddCompDict( const BlockHandle& handle) {
    com_dict_handle_ = handle;
  }

  void SetCount(uint32_t count) {
    op_count_.store(count, std::memory_order_relaxed);
  }

  // returns true when the last caller and must destroy
  bool DecCount() {
    return op_count_.fetch_sub(1U, std::memory_order_acq_rel) == 1;
  }

  // Initiate async or sync properties read
  Status ReadProperties();

  Status ReadCompDict();

  Status ReadRangeDel();

 private:

  // ReadBlockContents
  Status OnPropertiesReadComplete(const Status&);

  Status OnCompDicReadComplete(const Status&);

  // Raw block read here
  Status OnRangeDelReadComplete(const Status&);

  Status OnComplete(const Status&);

  ReadMetaBlocksCallback  cb_;
  BlockBasedTable*        table_;
  // No checksum for Properties and CompressionDict
  ReadOptions             ro_nochecksum_;
  // Read range_del_block
  ReadOptions             ro_default_;
  // Number of operations to complete
  std::atomic_uint64_t    op_count_;

  // Properties data
  BlockHandle              prop_handle_;
  BlockContents            properties_block_;
  // Compression Dictionary data
  BlockHandle              com_dict_handle_;
  // Must be dynamically allocated
  std::unique_ptr<BlockContents>   com_dict_block_;
  // Range Del
  BlockContents            range_del_block_;
};

// ReadFilterHelper class
class ReadFilterHelper {
 public:

  // This class does not supply an intermediate
  // callback for efficiency. We specify a callback
  // for the underlying class.
  using
  ReadFilterCallback = ReadBlockContentsContext::ReadBlockContCallback;

  ReadFilterHelper(const ReadFilterHelper&) = delete;
  ReadFilterHelper& operator=(const ReadFilterHelper&) = delete;

  ReadFilterHelper(BlockBasedTable* table,
                   bool is_a_filter_partition) :
    table_(table),
    is_a_filter_partition_ (is_a_filter_partition),
    block_reader_(nullptr)
  {}

  ~ReadFilterHelper() {
    delete block_reader_;
  }

  Status Read(const ReadFilterCallback& client_cb,
              const BlockHandle& filter_handle);

  // The client class must call this method either
  // directly or via a supplied callback but only
  // if the actual read take place
  Status OnFilterReadComplete(const Status&);

  BlockBasedTable* GetTable() {
    return table_;
  }

  bool IsFilterPartition() const {
    return is_a_filter_partition_;
  }

  // This is the onwly way to get result
  // as the callback invoked is in the host class
  // to reduce the number of invocations by ptr

  // May return nullptr if creation failed
  // The caller assumes the ownership
  FilterBlockReader* GetReader() {
    FilterBlockReader* result = nullptr;
    std::swap(result, block_reader_);
    return result;
  }

 private:

  BlockBasedTable*         table_;
  bool                     is_a_filter_partition_;
  // The end result
  FilterBlockReader*       block_reader_;

  // We read here to move into the reader block
  BlockContents            block_;
};


// This class attempts to get filter from cache
// but request a filter read using ReadFilterHelper
class GetFilterHelper {
 public:

  using
  GetFilterCallback = ReadFilterHelper::ReadFilterCallback;

  GetFilterHelper(const GetFilterHelper&) = delete;
  GetFilterHelper& operator=(const GetFilterHelper&) = delete;

  explicit
  GetFilterHelper(BlockBasedTable* table, bool no_io = false) :
    GetFilterHelper(table, table->rep_->filter_handle,
                    true /* is_a_filter_partition = true */,
                    no_io) {
  }

  GetFilterHelper(BlockBasedTable* table,
                  const BlockHandle& filter_blk_handle,
                  bool is_a_filter_partition,
                  bool no_io) :
    rf_helper_(table, is_a_filter_partition),
    filter_blk_handle_(filter_blk_handle),
    no_io_(no_io),
    PERF_TIMER_INIT(read_filter_block_nanos),
    was_read_(false),
    cache_handle_(nullptr) {
  }

  Status GetFilter(const GetFilterCallback& client_cb);

  // The client must call this either via callback
  // or directly on completion of Get
  Status OnGetFilterComplete(const Status&);

  // Return result
  BlockBasedTable::CachableEntry<FilterBlockReader>& GetEntry() {
    return entry_;
  }

 private:

  ReadFilterHelper   rf_helper_;
  BlockHandle        filter_blk_handle_;
  bool               no_io_;
  PERF_TIMER_DECL(read_filter_block_nanos);
  bool               was_read_;
  char cache_key_[BlockBasedTable::kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  Slice key_;
  Cache::Handle*     cache_handle_;
  BlockBasedTable::CachableEntry<FilterBlockReader> entry_;
};

// This class helps to load and create either of the 3 types
// of the Index readers. We incur 1 read for kTwoLevelIndexSearch
// or kBinarySearch indexes. For kHashSearch type we will need
// to load two more optional blocks if either of them or both
// are found. We will do so in parallel.
// However, if the index_block and its iterator are not readily
// available we will need to read the block for kHashSearch
class CreateIndexReaderContext {
 public:

  using
  IndexReader = BlockBasedTable::IndexReader;

  using
  CreateIndexCallback = async::Callable<Status, const Status&, IndexReader*>;

  CreateIndexReaderContext(const CreateIndexReaderContext&) = delete;
  CreateIndexReaderContext& operator=(const CreateIndexReaderContext&) = delete;

  // TODO: Add implementation on how to create reader w/o preloaded_meta_index_iter
  // However, it only needed for Hash based index. (Hint: we will need to async
  // read the index block again)
  // Create sync
  static Status CreateReader(BlockBasedTable* table,
                             const ReadOptions& readoptions,
                             InternalIterator* preloaded_meta_index_iter,
                             IndexReader** index_reader,
                             int level);

  // Request async creation
  static Status RequestCreateReader(const CreateIndexCallback& client_cb,
                                    BlockBasedTable* table,
                                    const ReadOptions& readoptions,
                                    InternalIterator* preloaded_meta_index_iter,
                                    int level);

  ~CreateIndexReaderContext() {
    delete index_reader_;
  }

  // Fetch the result
  IndexReader* GetIndexReader() {
    IndexReader* result= nullptr;
    std::swap(result, index_reader_);
    return result;
  }

 private:

  CreateIndexReaderContext(const CreateIndexCallback& client_cb,
                           BlockBasedTable* table,
                           const ReadOptions* readoptions,
                           InternalIterator* preloaded_meta_index_iter,
                           int level) :
    cb_(client_cb),
    table_(table),
    readoptions_(readoptions),
    preloaded_meta_index_iter_(preloaded_meta_index_iter),
    level_(level),
    index_type_on_file_(BlockBasedTableOptions::kBinarySearch),
    index_reader_(nullptr),
    failed_(false),
    pref_block_reads_(0) {
  }

  Status CreateIndexReader();

  // Once we read the index block we attempt to
  // instantiate the the actual index reader
  // However, for kHashSearch we attempt to load the meta
  // block if the iterator passed to use is nullptr
  // During table openining the iterator is usually there
  // but it may not be so otherwise
  Status OnIndexBlockReadComplete(const Status&);

  // This is when we succeeded loading the metablock
  // and can now search for prefix_meta_handle and
  // prefix_handle
  Status OnMetaBlockReadComplete(const Status&);

  Status CreateHashIndexReader();

  // Tryng reading prefix and prefix_meta blocks
  Status ReadPrefixIndex(const BlockHandle& prefix_handle,
                         const BlockHandle& prefix_meta_handle);

  // This is called when the index reader creation
  // is complete
  Status OnPrefixIndexComplete(const Status&);

  // Encapsulates logic of invoking callback and
  // self-cleanup
  Status OnComplete(const Status& status);

  // For use by reading prefix blocks in parallel
  // Returns true when it is time to invoke the
  // uuser supplied callback if async
  bool DecCount() {
    assert(pref_block_reads_.load(std::memory_order_relaxed) > 0U);
    return pref_block_reads_.fetch_sub(1U, std::memory_order_acq_rel)
           == 1;
  }

  CreateIndexCallback     cb_;
  // table we are building
  BlockBasedTable*        table_;
  const ReadOptions*      readoptions_;
  // Iterator to the meta block loaded earlier
  InternalIterator*       preloaded_meta_index_iter_;
  int                     level_;
  BlockBasedTableOptions::IndexType index_type_on_file_;
  // The result of this class
  IndexReader*            index_reader_;

  // Optional, in case no preloaded_meta_index_iter_
  // was provided
  BlockContents                     meta_cont_;
  std::unique_ptr<Block>            meta_block_;
  std::unique_ptr<InternalIterator> meta_iter_;

  // Reading prefix and prefix_meta blocks to
  // create BlockPrefixIndex if both blocks present
  // both block reads must succeed, however, the
  // failure is not terminal for opening
  // the table
  std::atomic_uint64_t    pref_block_reads_;
  std::atomic_bool        failed_; // Set if any of the reads failed

  // Read the index block
  BlockContents           index_block_cont_;
  std::unique_ptr<Block>  index_block_;
  BlockContents           prefixes_cont_;
  BlockContents           prefixes_meta_cont_;
};

// This class attempts to create an iterator
// based on the IndexReader. It first checks the
// cache if not found it creates a new Index reader
// after reading the index block if needed
class NewIndexIteratorContext {
 public:

  using
  IndexReader = BlockBasedTable::IndexReader;

  using
  IndexIterCallback = async::Callable<Status, const Status&,
  InternalIterator*>;

  NewIndexIteratorContext(const NewIndexIteratorContext&) = delete;
  NewIndexIteratorContext& operator=(const NewIndexIteratorContext&) = delete;

  static Status Create(BlockBasedTable* table,
                       const ReadOptions& read_options,
                       InternalIterator*  preloaded_meta_index_iter,
                       BlockIter* input_iter,
                       BlockBasedTable::CachableEntry<IndexReader>* index_entry,
                       InternalIterator** index_iterator);

  static Status RequestCreate(const IndexIterCallback&,
                              BlockBasedTable* table,
                              const ReadOptions& read_options,
                              InternalIterator*  preloaded_meta_index_iter,
                              BlockIter* input_iter,
                              BlockBasedTable::CachableEntry<IndexReader>* index_entry,
                              InternalIterator** index_iterator);

  ~NewIndexIteratorContext() {
    delete result_;
  }

  InternalIterator* GetResult() {
    InternalIterator* result = nullptr;
    std::swap(result, result_);
    return result;
  }

 private:

  // TODO: Add implementation to create index w/o preloaded_index_iterator
  NewIndexIteratorContext(BlockBasedTable* table,
                          const ReadOptions& read_options,
                          InternalIterator*  preloaded_meta_index_iter,
                          BlockIter* input_iter,
                          BlockBasedTable::CachableEntry<IndexReader>*
                          index_entry)
    : table_(table),
      ro_(&read_options),
      preloaded_meta_index_iter_(preloaded_meta_index_iter),
      input_iter_(input_iter),
      index_entry_(index_entry),
      PERF_TIMER_INIT(read_index_block_nanos),
      result_(nullptr),
      cache_handle_(nullptr) {
  }

  // Returns error, OK() or NotFound()
  // On Notfound schedule an async read
  Status GetFromCache();

  Status RequestIndexRead(const IndexIterCallback& client_cb);

  // This uses IndexReader that was either fetched from cache or
  // just created to make an index iterator
  Status ReaderToIterator(const Status&, IndexReader*);

  Status OnCreateComplete(const Status&, IndexReader*);

  Status OnComplete(const Status&);


  IndexIterCallback  cb_;
  BlockBasedTable*   table_;
  const ReadOptions* ro_;
  // Iterator to the meta block loaded earlier
  InternalIterator*  preloaded_meta_index_iter_;
  BlockIter*         input_iter_;
  BlockBasedTable::CachableEntry<IndexReader>* index_entry_;
  PERF_TIMER_DECL(read_index_block_nanos);

  // End result, this is either passed out
  // to the caller supplied cache or registered
  // as cleanable so we never destroy it
  // or never create
  InternalIterator*  result_;

  char cache_key_[BlockBasedTable::kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  Slice             key_;

  Cache::Handle*    cache_handle_;
};

// This class facilitate opening a new
// table and multiple disk IO in a async manner
class TableOpenRequestContext {
 public:

  using
  IndexReader = BlockBasedTable::IndexReader;

  using
  TableOpenCallback = async::Callable<Status, const Status&, TableReader*>;

  TableOpenRequestContext(const TableOpenCallback& client_cb,
                          const ImmutableCFOptions& ioptions,
                          const EnvOptions& env_options,
                          const BlockBasedTableOptions& table_options,
                          const InternalKeyComparator& internal_comparator,
                          std::unique_ptr<RandomAccessFileReader>&& file,
                          uint64_t file_size,
                          const bool prefetch_index_and_filter_in_cache,
                          const bool skip_filters, int level);

  static
  Status Open(const ImmutableCFOptions& ioptions,
              const EnvOptions& env_options,
              const BlockBasedTableOptions& table_options,
              const InternalKeyComparator& internal_comparator,
              std::unique_ptr<RandomAccessFileReader>&& file,
              uint64_t file_size,
              std::unique_ptr<TableReader>* table_reader,
              const bool prefetch_index_and_filter_in_cache,
              const bool skip_filters, const int level);

  static
  Status RequestOpen(const TableOpenCallback& client_cb,
                     const ImmutableCFOptions& ioptions,
                     const EnvOptions& env_options,
                     const BlockBasedTableOptions& table_options,
                     const InternalKeyComparator& internal_comparator,
                     std::unique_ptr<RandomAccessFileReader>&& file,
                     uint64_t file_size,
                     const bool prefetch_index_and_filter_in_cache,
                     const bool skip_filters, const int level);

  std::unique_ptr<TableReader> GetTableReader() {
    std::unique_ptr<TableReader> result(std::move(new_table_));
    return result;
  }

 private:

  // Capture the footer block
  Status OnFooterReadComplete(const Status&);

  // Capture the meta block and schedule reads of properties,
  // compression dictionary and range delete blocks if any of them are present
  Status OnMetaBlockReadComplete(const Status&);

  // Callback on reading prop, comp_dict and
  // range_del in parallel if any of them present
  Status OnMetasReadComplete();

  // Create index reader
  Status OnCreateIndexReader(const Status&, IndexReader*
                             index_reader);

  // Callback for the NewIndexIterator so we can cache the filter
  Status OnNewIndexIterator(const Status&, InternalIterator* index_iterator);

  // When filter is read this is invoked
  Status OnGetFilter(const Status&);

  Status OnReadFilter(const Status&);

  // This is a final callback
  Status OnComplete(const Status&);

  // Comes from a constructor
  TableOpenCallback                cb_;
  ReadOptions                      readoptions_; // Default
  bool    prefetch_index_and_filter_in_cache_;
  int                              level_;
  Slice                            decomp_dict_;

  // Table being built
  std::unique_ptr<BlockBasedTable> new_table_;

  // Populated as Open proceeds. This is
  // auxiliary data non of which is a final result
  Footer                           footer_;
  // Meta block read which is used everywhere
  // with its iterator
  BlockContents                     meta_cont_;
  std::unique_ptr<Block>            meta_block_;
  std::unique_ptr<InternalIterator> meta_iter_;

  // Optional
  std::unique_ptr<GetFilterHelper>  get_filter_helper_;
  std::unique_ptr<ReadFilterHelper> read_filter_helper_;
};

} // namepsace async
} // namespace rocksdb
