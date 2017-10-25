// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#pragma once

#include <atomic>
#include <memory>
#include <thread>

#include "async/async_status_capture.h"
#include "rocksdb/async/callables.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "table/block_based_table_reader.h"
#include "table/format.h"
#include "util/arena.h"
#include "util/random_read_context.h"

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
// This class is only practical to use if either
// of the two table caches are enabled. Compressed or
// uncompressed. If neither is available which should be
// checked by IsCacheAvaiable() method then one can skip the rest.
// If either caches are available then
// -- GetBlockFromCache() if Success we are done
//  - If not Found from cache then calls ShouldRead() to see if read is
//    warranted
// - otherwise RequestCachebableBlock(). If empty callback is provided
//   then the read is sync.
// - When the call returns call OnBlockReadComplete() regardless of the status
//   returned by Read request. The return on this function is the status of the
//  whole operation
class MaybeLoadDataBlockToCacheHelper {
 public:

  using
  BlockContCallback = ReadBlockContentsContext::ReadBlockContCallback;

  MaybeLoadDataBlockToCacheHelper(bool is_index, BlockBasedTable::Rep* rep) :
    is_index_(is_index),
    sw_(rep->ioptions.env, rep->ioptions.statistics, READ_BLOCK_GET_MICROS,
        true /* don't start */)
  {}

  ~MaybeLoadDataBlockToCacheHelper() {
    sw_.disarm();
  }

  MaybeLoadDataBlockToCacheHelper(
    const MaybeLoadDataBlockToCacheHelper&) = delete;
  MaybeLoadDataBlockToCacheHelper& operator=(
    const MaybeLoadDataBlockToCacheHelper&) = delete;

  // if neither caches are enabled then nothing to do
  static
  bool IsCacheEnabled(const BlockBasedTable::Rep* rep) {
    return rep->table_options.block_cache ||
           rep->table_options.block_cache_compressed;
  }

  // Returns true if reading from disk
  // is undesirable
  static
  bool IsNoIo(const ReadOptions& ro) {
    return (ro.read_tier == kBlockCacheTier);
  }

  // Assumes the block was not found in cache
  // and we need to know if reading is needed
  static
  bool ShouldRead(const ReadOptions& ro) {
    return !IsNoIo(ro) && ro.fill_cache;
  }

  // Check if the block can be fetched from cache
  // if cache is enabled within the table reader
  // The class should not be used if neither uncompressed or compressed
  // caches are present within the table IsCacheEnbled() == false
  // Returns NoFound if not found
  Status GetBlockFromCache(
    BlockBasedTable::Rep* rep,
    const ReadOptions& ro,
    const BlockHandle& handle,
    const Slice& compression_dict,
    BlockBasedTable::CachableEntry<Block>* entry) {
    assert(IsCacheEnabled(rep));

    Cache* block_cache = rep->table_options.block_cache.get();
    Cache* block_cache_compressed =
      rep->table_options.block_cache_compressed.get();
    // create key for block cache
    if (block_cache != nullptr) {
      key_ = BlockBasedTable::GetCacheKey(rep->cache_key_prefix,
                                          rep->cache_key_prefix_size,
                                          handle, cache_key_);
    }
    if (block_cache_compressed != nullptr) {
      ckey_ = BlockBasedTable::GetCacheKey(rep->compressed_cache_key_prefix,
                                           rep->compressed_cache_key_prefix_size, handle,
                                           compressed_cache_key_);
    }
    Status s = BlockBasedTable::GetDataBlockFromCache(
                 key_, ckey_, block_cache, block_cache_compressed, rep->ioptions, ro,
                 entry, rep->table_options.format_version, compression_dict,
                 rep->table_options.read_amp_bytes_per_bit, is_index_);
    if (entry->value == nullptr) {
      s = Status::NotFound();
    }
    return s;
  }

  // Reading a raw block
  // This performs both sync and async depending on the presents
  // of the callback.
  // The caller must invoke OnBlockReadComplete either directly
  // after sync completion or via a specified callback
  Status RequestCachebableBlock(const BlockContCallback& cb,
                                BlockBasedTable::Rep* rep,
                                const ReadOptions & ro,
                                const BlockHandle& block_handle,
                                BlockContents* result,
                                bool do_uncompress) {
    assert(IsCacheEnabled(rep));
    assert(ShouldRead(ro));
    Status s;
    sw_.start();
    if (cb) {
      s = ReadBlockContentsContext::RequestContentstRead(cb, rep->file.get(),
          rep->footer, ro, block_handle, result, rep->ioptions, do_uncompress,
          Slice(), rep->persistent_cache_options);

      if (s.IsIOPending()) {
        return s;
      }
    } else {
      s = ReadBlockContentsContext::ReadContents(rep->file.get(),
          rep->footer, ro, block_handle, result, rep->ioptions, do_uncompress,
          Slice(), rep->persistent_cache_options);
    }
    return s;
  }

  Status OnBlockReadComplete(const Status& status, BlockBasedTable::Rep* rep,
                             const ReadOptions& ro,
                             BlockContents&& block_cont,
                             const Slice& compression_dict,
                             BlockBasedTable::CachableEntry<Block>* entry) {
    sw_.elapsed();
    if (status.ok()) {
      return PutBlockToCache(rep, ro, std::move(block_cont), compression_dict,
                             entry);
    }
    return status;
  }

  bool IsIndex() const {
    return is_index_;
  }

  // Returns nullptr if block is not available
  Block* GetUncompressedBlock() {
    return uncompressed_block_.release();
  }

 private:

  Status PutBlockToCache(BlockBasedTable::Rep* rep,
                         const ReadOptions& ro,
                         BlockContents&& block_cont,
                         const Slice& compression_dict,
                         BlockBasedTable::CachableEntry<Block>* entry);

  bool                                   is_index_;
  StopWatch                              sw_;

  char cache_key_[BlockBasedTable::kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  char compressed_cache_key_[BlockBasedTable::kMaxCacheKeyPrefixSize +
                                                                     kMaxVarint64Length];
  Slice                                  key_;
  Slice                                  ckey_;
  // This becomes available in case we read the block, decompress it
  // and then fail to insert into cache. NewDataBlockIterator() then can
  // still re-use the block so no more reading is required.
  // However, if decompression fails then the block read the block
  // is not available
  std::unique_ptr<Block>                 uncompressed_block_;
};

// This class attempts to load in parallel 3 things
// - Properties if present
// - Compression Dictionary if present
// - Range of tombstones if present
// All of the above are separate block reads
// The context is destroyed automatically by the last
// invocation of OnComplete(). If the last invocation
// is synchronous the call back is not invoked and the
// call is considered to complete sync. Thus, the client code
// must ensure continuation. On sync completion the status
// is still IOPending with subcode is kOnComplete
class TableReadMetaBlocksContext {
 public:
  // Client callback when all is done
  using
  ReadMetaBlocksCallback = async::Callable<Status, const Status&>;

  // Flags
  enum MetaKinds : uint32_t {
    mNone,  // nothing to read or all read
    mProperties = 0x1, // Properties
    mCompDict = 0x2, // Compression dictionary
    mRangDel = 0x4 // range del block
  };

  TableReadMetaBlocksContext(BlockBasedTable* table, bool is_index) :
    table_(table),
    cache_helper_(is_index, table->rep_),
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
  Status ReadProperties() {
    Status s;
    Slice decomp_dict;
    // If this is an async environment
    if (cb_) {
      CallableFactory<TableReadMetaBlocksContext, Status, const Status&> fac(this);
      auto cb = fac.GetCallable<&TableReadMetaBlocksContext::OnPropertiesReadComplete>();
      s = ReadBlockContentsContext::RequestContentstRead(cb,
          table_->rep_->file.get(),
          table_->rep_->footer, ro_nochecksum_, prop_handle_,
          &properties_block_, table_->rep_->ioptions, false /* decompress */,
          decomp_dict, table_->rep_->persistent_cache_options);
      if (s.IsIOPending()) {
        return s;
      }
    } else {
      s = ReadBlockContentsContext::ReadContents(table_->rep_->file.get(),
          table_->rep_->footer, ro_nochecksum_, prop_handle_,
          &properties_block_, table_->rep_->ioptions, false /* decompress */,
          decomp_dict, table_->rep_->persistent_cache_options);
    }
    return OnPropertiesReadComplete(s);
  }

  Status ReadCompDict() {

    Status s;
    Slice decomp_dict;
    com_dict_block_.reset(new BlockContents());
    // If this is an async environment
    if (cb_) {

      CallableFactory<TableReadMetaBlocksContext, Status, const Status&> fac(this);
      auto cb = fac.GetCallable<&TableReadMetaBlocksContext::OnCompDicReadComplete>();
      s = ReadBlockContentsContext::RequestContentstRead(cb,
          table_->rep_->file.get(),
          table_->rep_->footer, ro_nochecksum_, com_dict_handle_,
          com_dict_block_.get(), table_->rep_->ioptions, false /* decompress */,
          decomp_dict, table_->rep_->persistent_cache_options);

      if (s.IsIOPending()) {
        return s;
      }
    } else {
      s = ReadBlockContentsContext::ReadContents(table_->rep_->file.get(),
          table_->rep_->footer, ro_nochecksum_, com_dict_handle_,
          com_dict_block_.get(), table_->rep_->ioptions, false /* decompress */,
          decomp_dict, table_->rep_->persistent_cache_options);
    }
    return OnCompDicReadComplete(s);
  }

  Status ReadRangeDel();

 private:

  // ReadBlockContents
  Status OnPropertiesReadComplete(const Status&);

  Status OnCompDicReadComplete(const Status& s) {
    if (!s.ok()) {
      ROCKS_LOG_WARN(
        table_->rep_->ioptions.info_log,
        "Encountered error while reading data from compression dictionary "
        "block %s",
        s.ToString().c_str());
    } else {
      table_->rep_->compression_dict_block = std::move(com_dict_block_);
    }
    // Async status propagates
    return OnComplete(s);
  }

  // Raw block read here
  Status OnRangeDelReadComplete(const Status&);

  Status OnComplete(const Status& status) {
    bool lastOnComplete = DecCount();
    if (cb_ && status.async()) {
      if (lastOnComplete) {
        cb_.Invoke(status);
      }
    }
    // Both sync and async completions delete
    // this context but only sync status
    // if propagated back to the caller
    // async invocation status is ignored
    if (lastOnComplete) {
      delete this;
      return Status::IOPending(Status::kOnComplete);
    }
    return status;
  }

  ReadMetaBlocksCallback  cb_;
  BlockBasedTable*        table_;
  MaybeLoadDataBlockToCacheHelper  cache_helper_;

  // No checksum for Properties and CompressionDict
  ReadOptions             ro_nochecksum_;
  // Read range_del_block
  ReadOptions             ro_default_;
  // Number of operations to complete
  std::atomic<uint64_t>    op_count_;


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

  ReadFilterHelper(const BlockBasedTable* table,
                   bool is_a_filter_partition) :
    table_(table),
    is_a_filter_partition_ (is_a_filter_partition),
    block_reader_(nullptr)
  {}

  ~ReadFilterHelper() {
    delete block_reader_;
  }

  Status Read(const ReadFilterCallback& client_cb,
              const BlockHandle& filter_handle) {
    Status s;
    auto rep = table_->rep_;
    // TODO: We might want to unify with ReadBlockFromFile() if we start
    // requiring checksum verification in Table::Open.
    if (rep->filter_type == BlockBasedTable::Rep::FilterType::kNoFilter) {
      return Status::NotSupported();
    }
    // Async mode
    if (client_cb) {
      s = ReadBlockContentsContext::RequestContentstRead(client_cb,
          rep->file.get(),
          rep->footer, ReadOptions(),
          filter_handle,
          &block_,
          rep->ioptions,
          false /* decompress */, Slice() /*compression dict*/,
          rep->persistent_cache_options);
    } else {
      s = ReadBlockContentsContext::ReadContents(
            rep->file.get(),
            rep->footer, ReadOptions(),
            filter_handle,
            &block_,
            rep->ioptions,
            false /* decompress */, Slice() /*compression dict*/,
            rep->persistent_cache_options);
    }
    return s;
  }

  // The client class must call this method either
  // directly or via a supplied callback but only
  // if the actual read take place
  Status OnFilterReadComplete(const Status&);

  const BlockBasedTable* GetTable() const {
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

  const BlockBasedTable*   table_;
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
  GetFilterHelper(const BlockBasedTable* table, bool no_io = false) :
    GetFilterHelper(table, table->rep_->filter_handle,
                    false /* is_a_filter_partition = !true */,
                    no_io) {
  }

  GetFilterHelper(const BlockBasedTable* table,
                  const BlockHandle& filter_blk_handle,
                  bool is_a_filter_partition,
                  bool no_io) :
    rf_helper_(table, is_a_filter_partition),
    filter_blk_handle_(filter_blk_handle),
    no_io_(no_io),
    PERF_METER_INIT(read_filter_block_nanos),
    was_read_(false),
    cache_handle_(nullptr) {
  }

  // This interface invokes ReadFilter helper if
  // the desired filter is not found in cache
  // The presence of the non-empty callback will determine
  // if the read would execute sync or async.
  // If no-io is true then GetFilter returns Incomplete in which case
  // no futher action is necessary
  // This is a helper so the client is responsible
  // for invoking OnGetFilterComplete(). It can be done
  // directly in case of the sync execution or indirectly
  // via a supplied callback for async.
  // In case the read was invoked async it will return IOPending
  Status GetFilter(const GetFilterCallback& client_cb);

  // The client must call this either via callback
  // or directly on completion of Get
  Status OnGetFilterComplete(const Status&);

  // Return result
  BlockBasedTable::CachableEntry<FilterBlockReader>& GetEntry() {
    return entry_;
  }

  const BlockBasedTable* GetTable() const {
    return rf_helper_.GetTable();
  }

  BlockBasedTable* GetTable() {
    return const_cast<BlockBasedTable*>(rf_helper_.GetTable());
  }

 private:

  ReadFilterHelper   rf_helper_;
  BlockHandle        filter_blk_handle_;
  bool               no_io_;
  PERF_METER_DECL(read_filter_block_nanos);
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
class CreateIndexReaderContext : private AsyncStatusCapture {
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
                             int level) {
    assert(index_reader != nullptr);
    *index_reader = nullptr;
    CreateIndexReaderContext ctx(CreateIndexCallback(), table, &readoptions,
                                 preloaded_meta_index_iter, level);
    Status s = ctx.CreateIndexReader();
    if (s.ok()) {
      *index_reader = ctx.GetIndexReader();
    }
    return s;
  }

  // Request async creation
  static Status RequestCreateReader(const CreateIndexCallback& client_cb,
                                    BlockBasedTable* table,
                                    const ReadOptions& readoptions,
                                    InternalIterator* preloaded_meta_index_iter,
                                    IndexReader** index_reader,
                                    int level) {
    assert(index_reader != nullptr);
    *index_reader = nullptr;
    // Context is gauaranteed to be destroyed
    // by OnComplete since the client_cb is supplied
    assert(client_cb);
    std::unique_ptr<CreateIndexReaderContext> ctx(new CreateIndexReaderContext(
          client_cb,
          table, &readoptions,
          preloaded_meta_index_iter, level));
    Status s = ctx->CreateIndexReader();
    if (s.IsIOPending()) {
      ctx.release();
      return s;
    }
    if (s.ok()) {
      *index_reader = ctx->GetIndexReader();
    }
    return s;
  }

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
  Status OnComplete(const Status& status) {
    if (cb_ && async()) {
      // we want to make sure that the cb downstream
      // receives our async status
      auto index_reader = GetIndexReader();
      if (status.async()) {
        cb_.Invoke(status, index_reader);
      } else {
        Status s(status);
        s.async(true);
        cb_.Invoke(s, index_reader);
      }
      delete this;
      return status;
    }
    return status;
  }

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
  std::atomic<bool>        failed_; // Set if any of the reads failed
  std::atomic<uint64_t >   pref_block_reads_;

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
class NewIndexIteratorContext : private AsyncStatusCapture {
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
                       InternalIterator** index_iterator) {

    assert(index_iterator != nullptr);
    *index_iterator = nullptr;
    NewIndexIteratorContext ctx(table, read_options, preloaded_meta_index_iter,
                                input_iter, index_entry);

    Status s = ctx.GetFromCache();
    if (s.IsNotFound()) {
      s = ctx.RequestIndexRead(IndexIterCallback());
    }
    if (s.ok()) {
      *index_iterator = ctx.GetResult();
    }
    return s;
  }

  static Status RequestCreate(const IndexIterCallback& client_cb,
                              BlockBasedTable* table,
                              const ReadOptions& read_options,
                              InternalIterator*  preloaded_meta_index_iter,
                              BlockIter* input_iter,
                              BlockBasedTable::CachableEntry<IndexReader>* index_entry,
                              InternalIterator** index_iterator) {

    assert(index_iterator != nullptr);
    *index_iterator = nullptr;

    std::unique_ptr<NewIndexIteratorContext> ctx(new NewIndexIteratorContext(table,
        read_options,
        preloaded_meta_index_iter, input_iter, index_entry));
    Status s = ctx->GetFromCache();
    if (s.IsNotFound()) {
      s = ctx->RequestIndexRead(client_cb);
    }
    if (s.IsIOPending()) {
      ctx.release();
      return s;
    }
    if (s.ok()) {
      *index_iterator = ctx->GetResult();
      return s;
    }
    return s;
  }

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
      PERF_METER_INIT(read_index_block_nanos),
      result_(nullptr),
      cache_handle_(nullptr) {
  }

  // Returns error, OK() or NotFound()
  // On Notfound schedule an async read
  Status GetFromCache();

  Status RequestIndexRead(const IndexIterCallback& client_cb);

  // This uses IndexReader that was either fetched from cache or
  // just created to make an index iterator
  Status ReaderToIterator(const Status& status, IndexReader* index_reader) {
    if (status.ok()) {
      Cache* block_cache = table_->rep_->table_options.block_cache.get();
      assert(cache_handle_);
      result_ = index_reader->NewIterator(
                  input_iter_, ro_->total_order_seek);
      // the caller would like to take ownership of the index block
      // don't call RegisterCleanup() in this case, the caller will take care of it
      if (index_entry_ != nullptr) {
        *index_entry_ = { index_reader, cache_handle_ };
      } else {
        result_->RegisterCleanup(&BlockBasedTable::ReleaseCachedEntry, block_cache,
                                 cache_handle_);
      }
    }
    return status;
  }

  Status OnCreateComplete(const Status&, IndexReader*);

  Status OnComplete(const Status& s) {
    if (cb_ && async()) {
      assert(s.async());
      auto result = GetResult();
      cb_.Invoke(s, result);
      delete this;
      return s;
    }
    return s;
  }

  IndexIterCallback  cb_;
  BlockBasedTable*   table_;
  const ReadOptions* ro_;
  // Iterator to the meta block loaded earlier
  InternalIterator*  preloaded_meta_index_iter_;
  BlockIter*         input_iter_;
  BlockBasedTable::CachableEntry<IndexReader>* index_entry_;
  PERF_METER_DECL(read_index_block_nanos);

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
class TableOpenRequestContext : private AsyncStatusCapture {
 public:

  using
  IndexReader = BlockBasedTable::IndexReader;

  using
  TableOpenCallback = async::Callable<Status, const Status&,
  std::unique_ptr<TableReader>&&>;

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
                     std::unique_ptr<TableReader>* table_reader,
                     const bool prefetch_index_and_filter_in_cache,
                     const bool skip_filters, const int level);

  std::unique_ptr<TableReader> GetTableReader() {
    std::unique_ptr<TableReader> result(std::move(new_table_));
    return result;
  }

 private:

  TableOpenRequestContext(const TableOpenCallback& client_cb,
                          const ImmutableCFOptions& ioptions,
                          const EnvOptions& env_options,
                          const BlockBasedTableOptions& table_options,
                          const InternalKeyComparator& internal_comparator,
                          std::unique_ptr<RandomAccessFileReader>&& file,
                          uint64_t file_size,
                          const bool prefetch_index_and_filter_in_cache,
                          const bool skip_filters, int level);

  // Capture the footer block
  Status OnFooterReadComplete(const Status&);

  // Capture the meta block and schedule reads of properties,
  // compression dictionary and range delete blocks if any of them are present
  Status OnMetaBlockReadComplete(const Status&);

  // Callback on reading prop, comp_dict and
  // range_del in parallel if any of them present
  Status OnMetasReadComplete(const Status&);

  // Create index reader
  Status OnCreateIndexReader(const Status&, IndexReader*
                             index_reader);

  // Callback for the NewIndexIterator so we can cache the filter
  Status OnNewIndexIterator(const Status&, InternalIterator* index_iterator);

  // When filter is read this is invoked
  Status OnGetFilter(const Status&);

  Status OnReadFilter(const Status& status) {
    async(status);
    Status s = read_filter_helper_->OnFilterReadComplete(status);
    if (s.ok()) {
      BlockBasedTable::Rep* rep = new_table_->rep_;
      // May return nullptr
      rep->filter.reset(read_filter_helper_->GetReader());
      if (rep->filter) {
        rep->filter->SetLevel(level_);
      }
    }
    return OnComplete(s);
  }

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

// This is a helper class that implements
// NewDataBlockIterator functionality in both sync
// and async manner
// If the input iterator pointer is not null then
// we update that iterator either with status or
// create a new state within it
// In any case the result is stored within instance
// that is pointed to by input_iter_
// The caller must determine if the result returned
// matches the ptr that was pointed to and act accordingly
class NewDataBlockIteratorHelper {
 public:

  using
  ReadDataBlockCallback = ReadBlockContentsContext::ReadBlockContCallback;

  NewDataBlockIteratorHelper(BlockBasedTable::Rep* rep, const ReadOptions& ro,
                             bool is_index = false) :
    rep_(rep),
    ro_(&ro),
    mb_helper_(is_index, rep),
    PERF_METER_INIT(new_table_block_iter_nanos),
    input_iter_(nullptr),
    action_(aNone) {
  }

  // This Attempts to do the following:
  // If cache is enabled, it will query DataBlock from cache
  // if io is allowed and fill cache option is set it then will
  // read the block and put it into cache
  // otherwise it will simply read the block
  // and create an iterator on top of it
  // If the callback passed in is empty the reads are performed
  // synchronously, otherwise, reads are dispatched in async manner
  //
  // The function returns IOPending() when reads are dispatched
  // async.
  //
  // After Create() returns you need to call OnCreateComplete()
  // before calling GetResult(). On Async invocation it must be called
  // from your callback
  //
  Status Create(const ReadDataBlockCallback&,
                const BlockHandle&,
                BlockIter* input_iter);

  Status OnCreateComplete(const Status& s);

  // Call this to obtain the pointer
  // that is either newed or allocated on the heap.
  // If the pointer doesn't match the one you passed
  // then the result is on the heap and you are responsible
  // for releasing it
  InternalIterator* GetResult() {
    if (new_iterator_) {
      return new_iterator_.release();
    }
    return input_iter_;
  }

  static InternalIterator* StatusToIterator(BlockIter* input_iter,
      const Status& status) {
    if (input_iter) {
      input_iter->SetStatus(status);
      return input_iter;
    } else {
      return NewErrorInternalIterator(status);
    }
  }

  BlockBasedTable::Rep* GetTableRep() {
    return rep_;
  }

  const BlockBasedTable::Rep* GetTableRep() const {
    return rep_;
  }

  const ReadOptions* GetReadOptions() const {
    return ro_;
  }

 private:

  // This enum specifies the actions
  // that were performed in Create() so
  // OnCreateComplete() can act accordingly
  enum Action {
    aNone = 0,
    aCache = 1,
    aCachableRead = 2,
    aDirectRead = 3
  };

  void StatusToIterator(const Status& status) {
    auto iter = StatusToIterator(input_iter_, status);
    if (input_iter_ != iter) {
      new_iterator_.reset(iter);
    }
  }

  // Reset for repeated use
  void Reset() {
    input_iter_ = nullptr;
    action_ = aNone;
    block_cont_ = std::move(BlockContents());
    entry_ = { nullptr, nullptr };
    new_iterator_.reset();
  }

  BlockBasedTable::Rep*                 rep_;
  const ReadOptions*                    ro_;
  MaybeLoadDataBlockToCacheHelper       mb_helper_;
  PERF_METER_DECL(new_table_block_iter_nanos);
  // If this is not nullptr then the result will be assigned
  // to the instance that is pointed to. Otherwise,
  // the result will be allocated on the heap
  BlockIter*                            input_iter_;
  Action                                action_;
  // Block becomes available either from MaybeLoad
  // or the actual BlockRead
  BlockContents                          block_cont_;
  BlockBasedTable::CachableEntry<Block>  entry_;
  std::unique_ptr<InternalIterator>      new_iterator_;
};

class NewDataBlockIteratorContext : private AsyncStatusCapture {
 public:

  using
  Callback = Callable<Status, const Status&, InternalIterator*>;

  NewDataBlockIteratorContext(const NewDataBlockIteratorContext&) = delete;
  NewDataBlockIteratorContext& operator=(const NewDataBlockIteratorContext&) =
    delete;

  static Status Create(BlockBasedTable::Rep* rep, const ReadOptions& ro,
                       const BlockHandle& block_handle,
                       InternalIterator** internal_iterator,
                       BlockIter* input_iter = nullptr,
                       bool is_index = false) {
    assert(internal_iterator);
    *internal_iterator = nullptr;

    Callback empty_cb;
    NewDataBlockIteratorContext context(empty_cb, rep, ro, is_index);
    NewDataBlockIteratorHelper::ReadDataBlockCallback empty_readblock_cb;
    Status s = context.biter_helper_.Create(empty_readblock_cb, block_handle,
                                            input_iter);
    s = context.OnBlockReadComplete(s);
    if (s.ok()) {
      *internal_iterator = context.GetResult();
    }
    return s;
  }

  static Status RequestCreate(const Callback& cb, BlockBasedTable::Rep* rep,
                              const ReadOptions& ro,
                              const BlockHandle& block_handle,
                              InternalIterator** internal_iterator,
                              BlockIter* input_iter = nullptr,
                              bool is_index = false) {

    assert(cb);
    assert(internal_iterator);
    *internal_iterator = nullptr;

    std::unique_ptr<NewDataBlockIteratorContext> context(new
        NewDataBlockIteratorContext(cb, rep, ro, is_index));
    CallableFactory<NewDataBlockIteratorContext, Status, const Status&> f(
      context.get());
    auto readblock_cb =
      f.GetCallable<&NewDataBlockIteratorContext::OnBlockReadComplete>();
    Status s = context->biter_helper_.Create(readblock_cb, block_handle,
               input_iter);
    if (s.IsIOPending()) {
      context.release();
    } else {
      s = context->OnBlockReadComplete(s);
      if (s.ok()) {
        *internal_iterator = context->GetResult();
      }
    }
    return s;
  }

  InternalIterator* GetResult() {
    return biter_helper_.GetResult();
  }

 private:

  NewDataBlockIteratorContext(const Callback& cb, BlockBasedTable::Rep* rep,
                              const ReadOptions& ro,
                              bool is_index) : cb_(cb), biter_helper_(rep, ro, is_index)
  {}

  Status OnBlockReadComplete(const Status& status) {
    async(status);
    Status s = biter_helper_.OnCreateComplete(status);
    s.async(async());
    return OnComplete(s);
  }

  Status OnComplete(const Status& status) {
    if (cb_ && async()) {
      auto result = biter_helper_.GetResult();
      cb_.Invoke(status, result);
      delete this;
      return status;
    }
    return status;
  }

  Callback                   cb_;
  NewDataBlockIteratorHelper biter_helper_;
};

class NewRangeTombstoneIterContext : private AsyncStatusCapture {
 public:

  using
  Callback = async::Callable<Status, const Status&, InternalIterator*>;

  NewRangeTombstoneIterContext(const NewRangeTombstoneIterContext&) = delete;
  NewRangeTombstoneIterContext& operator=(const NewRangeTombstoneIterContext&) =
    delete;

  // Check if the range_del_block exists. If not
  // no need to instantiate the context
  static bool IsPresent(const BlockBasedTable::Rep* rep) {
    return !rep->range_del_handle.IsNull();
  }

  // returns OK
  // On Error returns error
  // The result is output in iterator and it can be nullptr
  // if no RangeDel present in the table
  static Status CreateIterator(BlockBasedTable::Rep* rep,
                               const ReadOptions& read_options,
                               InternalIterator** iterator) {

    assert(iterator != nullptr);
    *iterator = nullptr;

    Status s;
    if (!IsPresent(rep)) {
      return s;
    }
    s = GetFromCache(rep, iterator);
    if (s.ok()) {
      return s;
    }
    Callback empty_cb;
    NewRangeTombstoneIterContext ctx(empty_cb, rep, read_options);
    s = ctx.RequestRead();
    assert(!s.IsIOPending());
    s = ctx.OnReadBlockComplete(s);
    if (s.ok()) {
      *iterator = ctx.GetResult();
    }
    return s;
  }

  // This is an async version of the API.
  // It may return OK() on success which means that the API
  // has completed synchronously either due to the cache
  // OR because the IO completed sync in which case
  // the result is stored in the *iterator
  // otherwise returns either IOPendning or an error
  static Status RequestCreateIterator(const Callback& cb,
                                      BlockBasedTable::Rep* rep,
                                      const ReadOptions& read_options,
                                      InternalIterator** iterator) {
    assert(iterator != nullptr);
    *iterator = nullptr;
    Status s;
    if (!IsPresent(rep)) {
      return s;
    }
    s = GetFromCache(rep, iterator);
    if (s.ok()) {
      return s;
    }
    std::unique_ptr<NewRangeTombstoneIterContext> ctx(new
        NewRangeTombstoneIterContext(cb, rep, read_options));
    s = ctx->RequestRead();
    if (s.IsIOPending()) {
      ctx.release();
      return s;
    }
    s = ctx->OnReadBlockComplete(s);
    if (s.ok()) {
      *iterator = ctx->GetResult();
    }
    return s;
  }

 private:

  InternalIterator* GetResult() {
    return db_iter_helper_.GetResult();
  }

  const bool is_index_false = false;

  NewRangeTombstoneIterContext(const Callback& cb,
                               BlockBasedTable::Rep* rep,
                               const ReadOptions& ro) :
    cb_(cb),
    db_iter_helper_(rep, ro, is_index_false) {
  }

  static Status GetFromCache(BlockBasedTable::Rep* rep,
                             InternalIterator** iterator);

  // Create the iterator sync or async
  Status RequestRead() {
    Status s;
    BlockBasedTable::Rep* rep = db_iter_helper_.GetTableRep();

    // should call IsPresent() before attempting to create
    assert(!rep->range_del_handle.IsNull());

    BlockIter* const null_input_iter = nullptr;
    NewDataBlockIteratorHelper::ReadDataBlockCallback read_block_cb;

    if (cb_) {
      async::CallableFactory<NewRangeTombstoneIterContext, Status, const Status&> f(
        this);
      read_block_cb =
        f.GetCallable<&NewRangeTombstoneIterContext::OnReadBlockComplete>();
    }
    s = db_iter_helper_.Create(read_block_cb, rep->range_del_handle,
                               null_input_iter);
    return s;
  }

  // Need to be passed to the db_iter_helper for
  // async call
  Status OnReadBlockComplete(const Status& status) {
    async(status);
    Status s = db_iter_helper_.OnCreateComplete(status);
    return OnComplete(s);
  }

  Status OnComplete(const Status& status) {
    if (cb_ && async()) {
      // Make sure async status is preserved
      Status s(status);
      s.async(true);
      cb_.Invoke(s, db_iter_helper_.GetResult());
      delete this;
      return status;
    }
    return status;
  }

  Callback                   cb_;
  NewDataBlockIteratorHelper db_iter_helper_;
};

// This is a sync/async implementation of BlockBasedTable::Get()
class BlockBasedGetContext : private AsyncStatusCapture {
 public:

  using
  Callback = async::Callable<Status, const Status&>;

  BlockBasedGetContext(const BlockBasedGetContext&) = delete;
  BlockBasedGetContext& operator=(const BlockBasedGetContext&) = delete;

  // Sync Get()
  static Status Get(BlockBasedTable* table, const ReadOptions& read_options,
                    const Slice& key, GetContext* get_context, bool skip_filters) {
    const Callback empty_cb;
    BlockBasedGetContext ctx(empty_cb, table, read_options, key, get_context,
                             skip_filters);
    return ctx.GetImpl();
  }


  // Async Get() which may complete sync just as any other interface
  static Status RequestGet(const Callback& cb, BlockBasedTable* table,
                           const ReadOptions& read_options, const Slice& key,
                           GetContext* get_context, bool skip_filters) {

    std::unique_ptr<BlockBasedGetContext> ctx(new BlockBasedGetContext(cb, table,
        read_options, key, get_context,
        skip_filters));
    Status s = ctx->GetImpl();
    if (s.IsIOPending()) {
      ctx.release();
    }
    return s;
  }

 private:

  BlockBasedGetContext(const Callback& cb, BlockBasedTable* table,
                       const ReadOptions& read_options, const Slice& key,
                       GetContext* get_context, bool skip_filters) :
    cb_(cb), key_(key),
    get_context_(get_context),
    skip_filters_(skip_filters),
    gf_helper_(table, read_options.read_tier == kBlockCacheTier),
    biter_helper_(table->rep_, read_options)
  {}

  const BlockBasedTable::Rep* Rep() const {
    return biter_helper_.GetTableRep();
  }

  const ReadOptions* GetReadOptions() const {
    return biter_helper_.GetReadOptions();
  }

  bool IsNoIO() const {
    return biter_helper_.GetReadOptions()->read_tier == kBlockCacheTier;
  }

  BlockBasedTable::CachableEntry<FilterBlockReader>& GetFilterEntry() {
    return gf_helper_.GetEntry();
  }

  // Returns the pointer to the index iterator
  // it may be a heap allocated new instance or
  // a member instance depending on what the NewIndexIterator
  // chose to do
  InternalIterator* GetIndexIter() {
    if (iiter_unique_ptr_) return iiter_unique_ptr_.get();
    return &index_iter_;
  }

  // We need this to keep re-using the same member
  // instance of the block iterator and not to incur
  // memory re-allocation. Otherwise, we hit an assert
  // on repeated initialization of the iterator
  void RecreateBlockIterator() {
    block_iter_.~BlockIter();
    new (&block_iter_) BlockIter();
  }

  // The actual Get() entry point
  Status GetImpl() {
    Status s;
    if (!skip_filters_) {

      Callable<Status, const Status&> on_get_filter_cb;
      if (cb_) {
        CallableFactory<BlockBasedGetContext, Status, const Status&> f(this);
        on_get_filter_cb = f.GetCallable<&BlockBasedGetContext::OnGetFilter>();
      }
      s = gf_helper_.GetFilter(on_get_filter_cb);
      if (s.IsIOPending()) {
        return s;
      }
      s = OnGetFilter(s);
    } else {
      s = CreateIndexIterator();
    }
    return s;
  }

  // Function is called when we complete obtaining a filter
  Status OnGetFilter(const Status& status) {
    async(status);
    Status s = gf_helper_.OnGetFilterComplete(status);
    return CreateIndexIterator();
  }

  // Creates Index iterator
  Status CreateIndexIterator();
  // Callback that is invoked when Index iterator creation is finished.
  Status OnIndexIteratorCreate(const Status&, InternalIterator*);

  // Dereferences current index iterator
  // decodes next block handle value. If filtered out
  // returns NotFound() and there is not a need to call OnNewDataBlockIterator()
  // On async completion returns IOPending and other statuses on sync
  // One must invoke OnNewDataBlockIterator() explicitely on sync completion
  Status CreateDataBlockIterator() {
    Status s;
    auto iiter = GetIndexIter();
    assert(iiter != nullptr);
    assert(iiter->Valid());

    FilterBlockReader* filter = GetFilterEntry().value;
    Slice handle_value = iiter->value();

    BlockHandle handle;
    bool not_exist_in_filter =
      filter != nullptr && filter->IsBlockBased() == true &&
      handle.DecodeFrom(&handle_value).ok() &&
      !filter->KeyMayMatch(ExtractUserKey(key_), handle.offset(), IsNoIO());

    if (not_exist_in_filter) {
      // Not found
      // TODO: think about interaction with Merge. If a user key cannot
      // cross one data block, we should be fine.
      RecordTick(Rep()->ioptions.statistics, BLOOM_FILTER_USEFUL);
      s = Status::NotFound();
    } else {
      NewDataBlockIteratorHelper::ReadDataBlockCallback on_data_block__cb;
      if (cb_) {
        CallableFactory<BlockBasedGetContext, Status, const Status&> f(this);
        on_data_block__cb =
          f.GetCallable<&BlockBasedGetContext::OnNewDataBlockIterator>();
      }

      handle_value = iiter->value();
      s = handle.DecodeFrom(&handle_value);
      if (s.ok()) {
        RecreateBlockIterator();
        s = biter_helper_.Create(on_data_block__cb, handle, &block_iter_);
      }
    }
    return s;
  }

  // Performs the actual iteration. It will either
  // advance index iterator and loop or
  // create a new one async and serve itself as
  // a callback
  Status OnNewDataBlockIterator(const Status&);

  // Final completion
  Status OnComplete(const Status& status) {
    auto rep = Rep();
    // if rep_->filter_entry is not set, we should call Release(); otherwise
    // don't call, in this case we have a local copy in rep_->filter_entry,
    // it's pinned to the cache and will be released in the destructor
    if (!rep->filter_entry.IsSet()) {
      GetFilterEntry().Release(rep->table_options.block_cache.get());
    }
    if (cb_ && async()) {
      Status s(status);
      s.async(true);
      cb_.Invoke(s);
      delete this;
      return s;
    }
    return status;
  }

  Callback              cb_;
  const Slice           key_;
  GetContext*           get_context_;
  const bool            skip_filters_;

  GetFilterHelper             gf_helper_;
  NewDataBlockIteratorHelper  biter_helper_;

  // We strive to re-init this iterator but it may
  // decide to allocate on the heap
  BlockIter             index_iter_;
  std::unique_ptr<InternalIterator>  iiter_unique_ptr_;
  // We re-init block iter on every block
  BlockIter             block_iter_;
};

// This class creates a new iterator on top of the
// blockbased table
class BlockBasedNewIteratorContext : private AsyncStatusCapture {
 public:
  using
  Callback = Callable<Status, const Status&, InternalIterator*>;

  BlockBasedNewIteratorContext(const BlockBasedNewIteratorContext&) = delete;
  BlockBasedNewIteratorContext& operator=(const BlockBasedNewIteratorContext&) =
    delete;

  ~BlockBasedNewIteratorContext() {
    delete result_;
  }

  static Status Create(BlockBasedTable* table, const ReadOptions& read_options,
                       Arena* arena, const InternalKeyComparator* icomp, bool skip_filters,
                       InternalIterator** iterator) {
    assert(iterator != nullptr);
    *iterator = nullptr;
    const Callback empty_cb;
    BlockBasedNewIteratorContext context(empty_cb, table, read_options,
                                         skip_filters, arena,
                                         icomp);
    Status s = context.NewIterator();
    if (s.ok()) {
      *iterator = context.GetResult();
    }
    return s;
  }

  static Status RequestCreate(const Callback& cb, BlockBasedTable* table,
                              const ReadOptions& read_options, Arena* arena,
                              const InternalKeyComparator* icomp,
                              bool skip_filters, InternalIterator** iterator) {
    assert(cb);
    assert(iterator != nullptr);
    *iterator = nullptr;
    std::unique_ptr<BlockBasedNewIteratorContext> context(new
        BlockBasedNewIteratorContext(cb, table, read_options, skip_filters, arena,
                                     icomp));
    Status s = context->NewIterator();
    if (s.ok()) {
      *iterator = context->GetResult();
    } else if (s.IsIOPending()) {
      context.release();
    }
    return s;
  }

  // Get for sync completion

 private:

  BlockBasedNewIteratorContext(const Callback& cb, BlockBasedTable* table,
                               const ReadOptions& read_options, bool skip_filters, Arena* arena,
                               const InternalKeyComparator* icomp) :
    cb_(cb), table_(table), ro_(&read_options), skip_filters_(skip_filters),
    arena_(arena), icomp_(icomp), result_(nullptr) {
  }

  InternalIterator* GetResult() {
    InternalIterator* result = nullptr;
    std::swap(result_, result);
    return result;
  }

  Status NewIterator() {
    Status s;
    InternalIterator* index_iterator = nullptr;
    if (cb_) {
      CallableFactory<BlockBasedNewIteratorContext, Status, const Status&, InternalIterator*>
      f(this);
      auto on_index_iter_cb =
        f.GetCallable<&BlockBasedNewIteratorContext::OnNewIndexIterator>();

      s = NewIndexIteratorContext::RequestCreate(on_index_iter_cb, table_, *ro_,
          nullptr /* preloaded_meta_iter */, nullptr /* input_iter */,
          nullptr /* index_entry */, &index_iterator);

      if (s.IsIOPending()) {
        return s;
      }
    } else {
      s = NewIndexIteratorContext::Create(table_, *ro_,
                                          nullptr /* preloaded_meta_iter */, nullptr /* input_iter */,
                                          nullptr /* index_entry */,
                                          &index_iterator);
    }
    return OnNewIndexIterator(s, index_iterator);
  }

  Status OnNewIndexIterator(const Status& status,
                            InternalIterator* index_iterator) {
    async(status);
    if (status.ok()) {
      assert(index_iterator != nullptr);
      if (cb_) {
        result_ = NewTwoLevelBlockAsyncIterator(
                    new BlockBasedTable::BlockEntryIteratorState(table_, *ro_, icomp_,
                        skip_filters_,
                        false /* is_index */, nullptr /*block_cache_cleaner */, arena_),
                    index_iterator, arena_);
      } else {
        result_ = NewTwoLevelBlockAsyncIterator(
                    new BlockBasedTable::BlockEntryIteratorState(table_, *ro_, icomp_,
                        skip_filters_,
                        false /* is_index */, nullptr /*block_cache_cleaner */, arena_),
                    index_iterator, arena_);
      }
    }
    return OnComplete(status);
  }

  Status OnComplete(const Status& status) {
    if (cb_ && async()) {
      Status s(status);
      s.async(async());
      auto iterator = GetResult();
      cb_.Invoke(s, iterator);
      delete this;
      return s;
    }
    return status;
  }

  Callback            cb_;
  BlockBasedTable*    table_;
  const               ReadOptions*  ro_;
  bool                skip_filters_;
  Arena*              arena_;
  const InternalKeyComparator* icomp_;

  InternalIterator*  result_;
};


} // namepsace async
} // namespace rocksdb
