//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "table/block_based_table_request.h"

#include "table/block.h"
#include "table/block_based_filter_block.h"
#include "table/meta_blocks.h"
#include "table/get_context.h"
#include "table/partitioned_filter_block.h"
#include "util/random_read_context.h"
#include "util/string_util.h"
#include "util/sync_point.h"

namespace rocksdb {

extern const uint64_t kBlockBasedTableMagicNumber;

namespace async {
/////////////////////////////////////////////////////////////////////////////////////////
// MaybeLoadDataBlockToCacheHelper

Status MaybeLoadDataBlockToCacheHelper::GetBlockFromCache(BlockBasedTable::Rep* rep,
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
    key_ = BlockBasedTable::GetCacheKey(rep->cache_key_prefix, rep->cache_key_prefix_size,
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

  if(entry->value == nullptr) {
    s = Status::NotFound();
  }

  return s;
}

Status MaybeLoadDataBlockToCacheHelper::RequestCachebableBlock(
  const BlockContCallback& cb,
  BlockBasedTable::Rep* rep,
  const ReadOptions& ro,
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

  ROCKS_LOG_DEBUG(rep->ioptions.info_log, "Read returning with: %s",
                  s.ToString().c_str());

  return s;
}

Status MaybeLoadDataBlockToCacheHelper::OnBlockReadComplete(const Status& status,
    BlockBasedTable::Rep * rep,
    const ReadOptions& ro,
    BlockContents&& block_cont,
    const Slice & compression_dict,
    BlockBasedTable::CachableEntry<Block>* entry) {

  sw_.elapsed();

  if (status.ok()) {
    return PutBlockToCache(rep, ro, std::move(block_cont), compression_dict, entry);
  }

  return status;
}

Status MaybeLoadDataBlockToCacheHelper::PutBlockToCache(BlockBasedTable::Rep* rep,
    const ReadOptions& ro,
    BlockContents&& block_cont,
    const Slice& compression_dict,
    BlockBasedTable::CachableEntry<Block>* entry) {

  assert(IsCacheEnabled(rep));

  Cache* block_cache = rep->table_options.block_cache.get();
  Cache* block_cache_compressed =
    rep->table_options.block_cache_compressed.get();

  Status s;

  std::unique_ptr<Block> read_block(new Block(std::move(block_cont),
                              rep->global_seqno,
                              rep->table_options.read_amp_bytes_per_bit,
                              rep->ioptions.statistics));

  // PutDataBlockToCache() deletes the block in case of failure
  s = BlockBasedTable::PutDataBlockToCache(
        key_, ckey_, block_cache, block_cache_compressed, ro, rep->ioptions,
        entry, read_block, uncompressed_block_, rep->table_options.format_version,
        compression_dict, rep->table_options.read_amp_bytes_per_bit,
        is_index_,
        is_index_ &&
        rep->table_options
        .cache_index_and_filter_blocks_with_high_priority
        ? Cache::Priority::HIGH
        : Cache::Priority::LOW);

  return s;
}

/////////////////////////////////////////////////////////////////////////////////////
/// TableReadMetaBlocksContext

Status TableReadMetaBlocksContext::ReadProperties() {

  Status s;
  Slice decomp_dict;

  ROCKS_LOG_DEBUG(
    table_->rep_->ioptions.info_log,
    "TableReadMetaBlocksContext starting properties read");

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

  ROCKS_LOG_DEBUG(table_->rep_->ioptions.info_log,
                  "ReadProperties returning with: %s",
                  s.ToString().c_str());


  return OnPropertiesReadComplete(s);
}

Status TableReadMetaBlocksContext::OnPropertiesReadComplete(const Status& status) {

  bool async = status.async();

  Status s;

  if (status.ok()) {

    Block properties_block(std::move(properties_block_),
                           kDisableGlobalSequenceNumber);

    BlockIter iter;
    properties_block.NewIterator(BytewiseComparator(), &iter);

    std::unique_ptr<TableProperties> new_table_properties(new TableProperties());

    // All pre-defined properties of type uint64_t
    std::unordered_map<std::string, uint64_t*> predefined_uint64_properties = {
      { TablePropertiesNames::kDataSize, &new_table_properties->data_size },
      { TablePropertiesNames::kIndexSize, &new_table_properties->index_size },
      { TablePropertiesNames::kFilterSize, &new_table_properties->filter_size },
      { TablePropertiesNames::kRawKeySize, &new_table_properties->raw_key_size },
      {
        TablePropertiesNames::kRawValueSize,
        &new_table_properties->raw_value_size
      },
      {
        TablePropertiesNames::kNumDataBlocks,
        &new_table_properties->num_data_blocks
      },
      { TablePropertiesNames::kNumEntries, &new_table_properties->num_entries },
      {
        TablePropertiesNames::kFormatVersion,
        &new_table_properties->format_version
      },
      {
        TablePropertiesNames::kFixedKeyLen,
        &new_table_properties->fixed_key_len
      },
      {
        TablePropertiesNames::kColumnFamilyId,
        &new_table_properties->column_family_id
      },
    };

    std::string last_key;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
      s = iter.status();
      if (!s.ok()) {
        break;
      }

      auto key = iter.key().ToString();
      // properties block is strictly sorted with no duplicate key.
      assert(last_key.empty() ||
             BytewiseComparator()->Compare(key, last_key) > 0);
      last_key = key;

      auto raw_val = iter.value();
      auto pos = predefined_uint64_properties.find(key);

      new_table_properties->properties_offsets.insert(
      { key, prop_handle_.offset() + iter.ValueOffset() });

      if (pos != predefined_uint64_properties.end()) {
        // handle predefined rocksdb properties
        uint64_t val;
        if (!GetVarint64(&raw_val, &val)) {
          // skip malformed value
          auto error_msg =
            "Detect malformed value in properties meta-block:"
            "\tkey: " + key + "\tval: " + raw_val.ToString();
          ROCKS_LOG_ERROR(table_->rep_->ioptions.info_log, "%s", error_msg.c_str());
          continue;
        }
        *(pos->second) = val;
      } else if (key == TablePropertiesNames::kFilterPolicy) {
        new_table_properties->filter_policy_name = raw_val.ToString();
      } else if (key == TablePropertiesNames::kColumnFamilyName) {
        new_table_properties->column_family_name = raw_val.ToString();
      } else if (key == TablePropertiesNames::kComparator) {
        new_table_properties->comparator_name = raw_val.ToString();
      } else if (key == TablePropertiesNames::kMergeOperator) {
        new_table_properties->merge_operator_name = raw_val.ToString();
      } else if (key == TablePropertiesNames::kPrefixExtractorName) {
        new_table_properties->prefix_extractor_name = raw_val.ToString();
      } else if (key == TablePropertiesNames::kPropertyCollectors) {
        new_table_properties->property_collectors_names = raw_val.ToString();
      } else if (key == TablePropertiesNames::kCompression) {
        new_table_properties->compression_name = raw_val.ToString();
      } else {
        // handle user-collected properties
        new_table_properties->user_collected_properties.insert(
        { key, raw_val.ToString() });
      }
    }
    if (s.ok()) {
      table_->rep_->table_properties = std::move(new_table_properties);
    }
  } else {
    s = status;
  }

  if (!s.ok()) {
    ROCKS_LOG_WARN(table_->rep_->ioptions.info_log,
                   "Encountered error while reading data from properties "
                   "block %s",
                   s.ToString().c_str());
  }

  s.async(async);
  return OnComplete(s);
}

Status TableReadMetaBlocksContext::ReadCompDict() {

  Status s;
  Slice decomp_dict;

  ROCKS_LOG_DEBUG(
    table_->rep_->ioptions.info_log,
    "TableReadMetaBlocksContext starting CompDict read");


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

Status TableReadMetaBlocksContext::OnCompDicReadComplete(const Status& s) {

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

Status TableReadMetaBlocksContext::ReadRangeDel() {

  Status s;

  ROCKS_LOG_DEBUG(
    table_->rep_->ioptions.info_log,
    "TableReadMetaBlocksContext starting RangeDel read");

  using Rep = BlockBasedTable::Rep;

  Rep* rep = table_->rep_;

  // Cache is not enabled nothing to do
  if (!cache_helper_.IsCacheEnabled(rep)) {
    return OnComplete(s);
  }

  s = cache_helper_.GetBlockFromCache(rep,
                                      ro_default_,
                                      rep->range_del_handle,
                                      Slice(),
                                      &rep->range_del_entry); // compression_dict

  // We got this from cache nothing to do
  if(s.ok() && rep->range_del_entry.value != nullptr) {
    return OnComplete(s);
  }

  if (!cache_helper_.ShouldRead(ro_default_)) {
    return OnComplete(Status::OK());
  }

  // We uncompress if compressed cache is nullptr
  const bool do_uncompress = (nullptr ==
                              rep->table_options.block_cache_compressed);

  MaybeLoadDataBlockToCacheHelper::BlockContCallback on_rangedel_cb;

  if (cb_) {
    CallableFactory<TableReadMetaBlocksContext, Status, const Status&> fac(this);
    on_rangedel_cb =
      fac.GetCallable<&TableReadMetaBlocksContext::OnRangeDelReadComplete>();
  }

  s = cache_helper_.RequestCachebableBlock(on_rangedel_cb, rep,
      ro_default_, rep->range_del_handle, &range_del_block_, do_uncompress);

  if (s.IsIOPending()) {
    return s;
  }

  return OnRangeDelReadComplete(s);
}

Status TableReadMetaBlocksContext::OnRangeDelReadComplete(
  const Status& status) {

  bool async = status.async();
  Status s(status);

  if(status.ok()) {
    s = cache_helper_.OnBlockReadComplete(status, table_->rep_, ro_default_,
                                          std::move(range_del_block_),
                                          Slice(), &table_->rep_->range_del_entry);
  }

  if (!s.ok() && !s.IsNotFound()) {
    ROCKS_LOG_WARN(
      table_->rep_->ioptions.info_log,
      "Encountered error while reading data from range del block %s",
      s.ToString().c_str());
  }

  s.async(async);
  return OnComplete(s);
}

Status TableReadMetaBlocksContext::OnComplete(const Status& s) {

  bool lastOnComplete = DecCount();

  if (cb_ && s.async()) {

    ROCKS_LOG_DEBUG(
      table_->rep_->ioptions.info_log,
      "TableReadMetaBlocksContext async completion: %s",
      s.ToString().c_str());

    if (lastOnComplete) {
      cb_.Invoke(s);
    }

  } else {

    ROCKS_LOG_DEBUG(
      table_->rep_->ioptions.info_log,
      "TableReadMetaBlocksContext sync completion: %s",
      s.ToString().c_str());
  }

  // Both sync and async completions delete
  // this context but only sync status
  // if propagated back to the caller
  // async invocation status is ignored
  if (lastOnComplete) {
    delete this;
    return Status::IOPending(Status::kOnComplete);
  }

  return s;
}

////////////////////////////////////////////////////////////////////////////////////////////////
// ReadFilterHelper
Status ReadFilterHelper::Read(const ReadFilterCallback& client_cb,
                              const BlockHandle & filter_handle) {

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

Status ReadFilterHelper::OnFilterReadComplete(const Status& status) {

  ROCKS_LOG_DEBUG(
    table_->rep_->ioptions.info_log,
    "OnFilterReadComplete completion: %s",
    status.ToString().c_str());

  if (status.IsNotSupported()) {
    return Status::OK();
  }

  if (!status.ok()) {
    return status;
  }

  using Rep = BlockBasedTable::Rep;

  auto rep = table_->rep_;

  assert(rep->filter_policy);

  auto filter_type = rep->filter_type;
  if (rep->filter_type == Rep::FilterType::kPartitionedFilter &&
      is_a_filter_partition_) {
    filter_type = Rep::FilterType::kFullFilter;
  }

  switch (filter_type) {
  case Rep::FilterType::kPartitionedFilter: {
    block_reader_ = new PartitionedFilterBlockReader(
      rep->prefix_filtering ? rep->ioptions.prefix_extractor : nullptr,
      rep->whole_key_filtering, std::move(block_), nullptr,
      rep->ioptions.statistics, rep->internal_comparator, table_);
  }
  break;

  case Rep::FilterType::kBlockFilter:
    block_reader_ = new BlockBasedFilterBlockReader(
      rep->prefix_filtering ? rep->ioptions.prefix_extractor : nullptr,
      rep->table_options, rep->whole_key_filtering, std::move(block_),
      rep->ioptions.statistics);
    break;

  case Rep::FilterType::kFullFilter: {
    auto filter_bits_reader =
      rep->filter_policy->GetFilterBitsReader(block_.data);
    assert(filter_bits_reader != nullptr);
    block_reader_ = new FullFilterBlockReader(
      rep->prefix_filtering ? rep->ioptions.prefix_extractor : nullptr,
      rep->whole_key_filtering, std::move(block_), filter_bits_reader,
      rep->ioptions.statistics);
  }
  break;

  default:
    // filter_type is either kNoFilter (exited the function at the first if),
    // or it must be covered in this switch block
    assert(false);
    return Status::NotSupported("Unsupported filter_type");
  }

  return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
// GetFilterHelper
Status GetFilterHelper::GetFilter(const GetFilterCallback& client_cb) {

  Status s;

  BlockBasedTable::Rep* rep = rf_helper_.GetTable()->rep_;
  bool is_a_filter_partition = rf_helper_.IsFilterPartition();

  // If cache_index_and_filter_blocks is false, filter should be pre-populated.
  // We will return rep_->filter anyway. rep_->filter can be nullptr if filter
  // read fails at Open() time. We don't want to reload again since it will
  // most probably fail again.
  if (!is_a_filter_partition &&
      !rep->table_options.cache_index_and_filter_blocks) {
    entry_  = { rep->filter.get(), nullptr /* cache handle */ };
    return s;
  }

  Cache* block_cache = rep->table_options.block_cache.get();
  if (rep->filter_policy == nullptr /* do not use filter */ ||
      block_cache == nullptr /* no block cache at all */) {
    entry_ = { nullptr /* filter */, nullptr /* cache handle */ };
    return s;
  }

  if (!is_a_filter_partition && rep->filter_entry.IsSet()) {
    entry_ = rep->filter_entry;
    return s;
  }

  // Now we have to read the block
  PERF_TIMER_START(read_filter_block_nanos);

  key_ = BlockBasedTable::GetCacheKey(rep->cache_key_prefix, rep->cache_key_prefix_size,
                                      filter_blk_handle_, cache_key_);

  Statistics* statistics = rep->ioptions.statistics;
  cache_handle_ =
    BlockBasedTable::GetEntryFromCache(block_cache, key_, BLOCK_CACHE_FILTER_MISS,
                                       BLOCK_CACHE_FILTER_HIT, statistics);

  FilterBlockReader* filter = nullptr;
  if (cache_handle_ != nullptr) {
    filter = reinterpret_cast<FilterBlockReader*>(
               block_cache->Value(cache_handle_));
    entry_ = { filter, cache_handle_ };
    return s;
  }

  if (no_io_) {
    // Do not invoke any io.
    entry_ = BlockBasedTable::CachableEntry<FilterBlockReader>();
    PERF_TIMER_STOP(read_filter_block_nanos);
    return Status::Incomplete();
  }

  was_read_ = true;
  return rf_helper_.Read(client_cb, filter_blk_handle_);
}

Status GetFilterHelper::OnGetFilterComplete(const Status& status) {

  ROCKS_LOG_DEBUG(
    rf_helper_.GetTable()->rep_->ioptions.info_log,
    "OnGetFilterComplete completion: %s",
    status.ToString().c_str());

  Status s(status);
  if(was_read_) {
    s = rf_helper_.OnFilterReadComplete(status);
  }

  PERF_TIMER_STOP(read_filter_block_nanos);

  if (!s.ok()) {
    return s;
  }

  auto filter = rf_helper_.GetReader();

  if (filter != nullptr) {

    BlockBasedTable::Rep* rep = rf_helper_.GetTable()->rep_;
    Cache* block_cache = rep->table_options.block_cache.get();
    Statistics* statistics = rep->ioptions.statistics;

    assert(filter->size() > 0);
    s = block_cache->Insert(
          key_, filter, filter->size(), &BlockBasedTable::DeleteCachedFilterEntry, &cache_handle_,
          rep->table_options.cache_index_and_filter_blocks_with_high_priority
          ? Cache::Priority::HIGH
          : Cache::Priority::LOW);

    if (s.ok()) {
      RecordTick(statistics, BLOCK_CACHE_ADD);
      RecordTick(statistics, BLOCK_CACHE_FILTER_ADD);
      RecordTick(statistics, BLOCK_CACHE_FILTER_BYTES_INSERT, filter->size());
      RecordTick(statistics, BLOCK_CACHE_BYTES_WRITE, filter->size());

      entry_ = { filter, cache_handle_ };
    } else {
      RecordTick(statistics, BLOCK_CACHE_ADD_FAILURES);
      delete filter;
      entry_ = BlockBasedTable::CachableEntry<FilterBlockReader>();
    }
  }

  return s;
}

///////////////////////////////////////////////////////////////////////////////////////////
// CreateIndexReaderContext
//
Status CreateIndexReaderContext::CreateReader(BlockBasedTable * table,
    const ReadOptions& readoptions,
    InternalIterator * preloaded_meta_index_iter,
    IndexReader** index_reader, int level) {

  assert(index_reader != nullptr);

  CreateIndexReaderContext ctx(CreateIndexCallback(), table, &readoptions,
                               preloaded_meta_index_iter, level);

  Status s = ctx.CreateIndexReader();

  if (s.ok()) {
    *index_reader = ctx.GetIndexReader();
  }

  return s;
}

Status CreateIndexReaderContext::RequestCreateReader(const CreateIndexCallback& client_cb,
    BlockBasedTable * table,
    const ReadOptions & readoptions,
    InternalIterator * preloaded_meta_index_iter,
    IndexReader** index_reader,
    int level) {

  // Context is gauaranteed to be destroyed
  // by OnComplete since the client_cb is supplied
  assert(client_cb);
  std::unique_ptr<CreateIndexReaderContext> ctx(new CreateIndexReaderContext(client_cb,
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

Status CreateIndexReaderContext::CreateIndexReader() {

  Status s;

  auto rep = table_->rep_;

  // Some old version of block-based tables don't have index type present in
  // table properties. If that's the case we can safely use the kBinarySearch.
  auto index_type_on_file = BlockBasedTableOptions::kBinarySearch;
  if (rep->table_properties) {
    auto& props = rep->table_properties->user_collected_properties;
    auto pos = props.find(BlockBasedTablePropertyNames::kIndexType);
    if (pos != props.end()) {
      index_type_on_file = static_cast<BlockBasedTableOptions::IndexType>(
                             DecodeFixed32(pos->second.c_str()));
    }
  }

  auto file = rep->file.get();
  auto comparator = &rep->internal_comparator;
  const Footer& footer = rep->footer;
  if (index_type_on_file == BlockBasedTableOptions::kHashSearch &&
      rep->ioptions.prefix_extractor == nullptr) {
    ROCKS_LOG_WARN(rep->ioptions.info_log,
                   "BlockBasedTableOptions::kHashSearch requires "
                   "options.prefix_extractor to be set."
                   " Fall back to binary search index.");
    index_type_on_file = BlockBasedTableOptions::kBinarySearch;
  }

  switch (index_type_on_file) {
  case BlockBasedTableOptions::kTwoLevelIndexSearch:
  case BlockBasedTableOptions::kBinarySearch:
  case BlockBasedTableOptions::kHashSearch :
    // These are valid
    break;

  default: {
    std::string error_message =
      "Unrecognized index type: " + ToString(index_type_on_file);
    return Status::InvalidArgument(error_message.c_str());
  }
  }

  index_type_on_file_ = index_type_on_file;

  // Fire up reading index block
  if (cb_) {

    CallableFactory<CreateIndexReaderContext,Status, const Status&> fac(this);
    auto on_index_block_cb = fac.GetCallable<&CreateIndexReaderContext::OnIndexBlockReadComplete>();

    s = ReadBlockContentsContext::RequestContentstRead(on_index_block_cb, rep->file.get(),
        rep->footer, *readoptions_, rep->footer.index_handle(), &index_block_cont_, rep->ioptions,
        true /* (do_uncompress */,
        Slice(), // Compression dictionary
        rep->persistent_cache_options);

    if (s.IsIOPending()) {
      return s;
    }

  } else {

    s = ReadBlockContentsContext::ReadContents(rep->file.get(),
        rep->footer, *readoptions_, rep->footer.index_handle(), &index_block_cont_, rep->ioptions,
        true /* (do_uncompress */,
        Slice(), // Compression dictionary
        rep->persistent_cache_options);
  }

  return OnIndexBlockReadComplete(s);
}

Status CreateIndexReaderContext::OnIndexBlockReadComplete(const Status& status) {

  async(status);

  ROCKS_LOG_DEBUG(
    table_->rep_->ioptions.info_log,
    "OnIndexBlockReadComplete completion: %s",
    status.ToString().c_str());


  if (!status.ok()) {
    return OnComplete(status);
  }

  Status s;

  // Next step we dispatch more reads for Hash type and
  // finish processing for other two
  index_block_.reset(new Block(std::move(index_block_cont_),
                               kDisableGlobalSequenceNumber,
                               0 /* read_amp_bytes_per_bit */,
                               table_->rep_->ioptions.statistics));

  switch (index_type_on_file_) {

  case BlockBasedTableOptions::kTwoLevelIndexSearch:
    PartitionIndexReader::Create(table_,
                                 &table_->rep_->internal_comparator,
                                 std::move(index_block_),
                                 table_->rep_->ioptions.statistics,
                                 level_,
                                 &index_reader_);
    break;

  case BlockBasedTableOptions::kBinarySearch:

    BinarySearchIndexReader::Create(
      &table_->rep_->internal_comparator,
      std::move(index_block_),
      table_->rep_->ioptions.statistics,
      &index_reader_);
    break;

  case BlockBasedTableOptions::kHashSearch: {

    if (preloaded_meta_index_iter_ == nullptr) {
      // Request meta block read then
      if (cb_) {
        // Get the callback
        async::CallableFactory<CreateIndexReaderContext, Status, const Status&>
        factory(this);
        auto meta_cb =
          factory.GetCallable<&CreateIndexReaderContext::OnMetaBlockReadComplete>();

        s = ReadBlockContentsContext::RequestContentstRead(meta_cb,
            table_->rep_->file.get(),
            table_->rep_->footer, *readoptions_, table_->rep_->footer.metaindex_handle(),
            &meta_cont_, table_->rep_->ioptions, true /* decompress */,
            Slice(), table_->rep_->persistent_cache_options);

        // If async or error return
        if (s.IsIOPending()) {
          return s;
        }
      } else {
        s = ReadBlockContentsContext::ReadContents(table_->rep_->file.get(),
            table_->rep_->footer, *readoptions_, table_->rep_->footer.metaindex_handle(),
            &meta_cont_, table_->rep_->ioptions, true /* decompress */,
            Slice(), table_->rep_->persistent_cache_options);
      }

      return OnMetaBlockReadComplete(s);

    } else {
      return CreateHashIndexReader();
    }
  }
  break;
  // Invalid case
  default: {
    std::string error_message =
      "Unrecognized index type: " + ToString(index_type_on_file_);
    s = Status::InvalidArgument(error_message.c_str());
  }
  }

  // Finish on sync completion
  return OnComplete(s);
}

Status CreateIndexReaderContext::OnMetaBlockReadComplete(const Status& status) {

  async(status);

  ROCKS_LOG_DEBUG(
    table_->rep_->ioptions.info_log,
    "CreateIndexReaderContext::OnMetaBlockReadComplete completion: %s",
    status.ToString().c_str());

  // This function is called after we read a metablock in case
  // the iterator to it was not supplied.
  assert(index_block_);
  assert(index_reader_ == nullptr);

  // If we failed to read the metablock
  // then we fall-back to binary search index
  // using the same index block
  if (!status.ok()) {

    ROCKS_LOG_DEBUG(
      table_->rep_->ioptions.info_log,
      "CreateIndexReaderContext::OnMetaBlockReadComplete completion: %s",
      status.ToString().c_str());

    BinarySearchIndexReader::Create(&table_->rep_->internal_comparator,
                                    std::move(index_block_),
                                    table_->rep_->ioptions.statistics,
                                    &index_reader_);
    return OnComplete(Status::OK());
  }

  meta_block_.reset(new Block(std::move(meta_cont_),
    kDisableGlobalSequenceNumber, 0 /* read_amp_bytes_per_bit */,
    table_->rep_->ioptions.statistics));

  meta_iter_.reset(meta_block_->NewIterator(BytewiseComparator()));
  preloaded_meta_index_iter_ = meta_iter_.get();

  return CreateHashIndexReader();
}

Status CreateIndexReaderContext::CreateHashIndexReader() {

  assert(index_reader_ == nullptr);
  assert(preloaded_meta_index_iter_);

  HashIndexReader::Create(&table_->rep_->internal_comparator,
    std::move(index_block_),
    table_->rep_->ioptions.statistics,
    &index_reader_);

  BlockHandle prefixes_handle;
  // Seek prefix blocks
  // Get prefixes block
  Status s = FindMetaBlock(preloaded_meta_index_iter_, kHashIndexPrefixesBlock,
    &prefixes_handle);

  // We need both blocks to be successful if one is not found
  // or errors out we do not continue. However, this is not a
  // terminal error
  BlockHandle prefixes_meta_handle;
  if (s.ok()) {
    s = FindMetaBlock(preloaded_meta_index_iter_, kHashIndexPrefixesMetadataBlock,
      &prefixes_meta_handle);
  }

  // Fire up reading blocks in parallel
  if (s.ok()) {
    // This will invoke callbacks both
    // sync and async
    return ReadPrefixIndex(prefixes_handle, prefixes_meta_handle);
  } else {
    s = Status::OK();
  }

  return OnComplete(s);
}

Status CreateIndexReaderContext::ReadPrefixIndex(const BlockHandle& prefixes_handle,
    const BlockHandle& prefixes_meta_handle) {
  Status s;

  BlockBasedTable::Rep* rep = table_->rep_;

  // Two operations to try meaning we need
  // to invoke the callback two times no matter what
  pref_block_reads_.store(2U, std::memory_order_relaxed);

  if (cb_) {

    CallableFactory<CreateIndexReaderContext, Status, const Status&> fac(this);
    auto index_cb = fac.GetCallable<&CreateIndexReaderContext::OnPrefixIndexComplete>();

    s = ReadBlockContentsContext::RequestContentstRead(index_cb,
        rep->file.get(),
        rep->footer, *readoptions_, prefixes_handle,
        &prefixes_cont_, rep->ioptions,
        true /* (do_uncompress */,
        Slice(), // Compression dictionary
        rep->persistent_cache_options);

    bool first_pending = s.IsIOPending();

    if (!first_pending) {
      // Invoke callback manually as the above would not invoke it
      OnPrefixIndexComplete(s);
    }

    // Is it worth trying the second one?
    if (s.ok() || first_pending) {
      s = ReadBlockContentsContext::RequestContentstRead(index_cb,
          rep->file.get(),
          rep->footer, *readoptions_, prefixes_meta_handle,
          &prefixes_meta_cont_, rep->ioptions,
          true /* (do_uncompress */,
          Slice(), // Compression dictionary
          rep->persistent_cache_options);

      if (!s.IsIOPending()) {
        s = OnPrefixIndexComplete(s);
      }

      if (first_pending || s.IsIOPending()) {
        return Status::IOPending();
      }

    } else {
      // First one failed complete the second
      s = OnPrefixIndexComplete(s);
    }

    return s;

  } else {
    s = ReadBlockContentsContext::ReadContents(rep->file.get(),
        rep->footer, *readoptions_, prefixes_handle,
        &prefixes_cont_, rep->ioptions,
        true /* (do_uncompress */,
        Slice(), // Compression dictionary
        rep->persistent_cache_options);

    s = OnPrefixIndexComplete(s);

    if(s.ok()) {
      s = ReadBlockContentsContext::ReadContents(rep->file.get(),
          rep->footer, *readoptions_, prefixes_meta_handle,
          &prefixes_meta_cont_, rep->ioptions,
          true /* (do_uncompress */,
          Slice(), // Compression dictionary
          rep->persistent_cache_options);
    }

    s = OnPrefixIndexComplete(s);
  }

  return s;
}

Status CreateIndexReaderContext::OnPrefixIndexComplete(const Status& s) {

  async(s);

  ROCKS_LOG_DEBUG(
    table_->rep_->ioptions.info_log,
    "OnPrefixIndexComplete completion: %s",
    s.ToString().c_str());

  // Always report OK at this stage
  Status s_ok;

  if (!s.ok()) {
    failed_.store(true, std::memory_order_relaxed);
  }

  if (DecCount()) {
    // We are the last block to complete loading
    if (!failed_.load(std::memory_order_relaxed)) {
      BlockPrefixIndex* prefix_index = nullptr;
      Status st = BlockPrefixIndex::Create(table_->rep_->internal_prefix_transform.get(),
                                           prefixes_cont_.data,
                                           prefixes_meta_cont_.data,
                                           &prefix_index);

      if (st.ok()) {
        reinterpret_cast<HashIndexReader*>(index_reader_)->
        SetBlockPrefixIndex(prefix_index);
      }
    }
    s_ok.async(s.async());
    return OnComplete(s_ok);
  }

  return s_ok;
}

Status CreateIndexReaderContext::OnComplete(const Status& status) {

  if (cb_ && async()) {

    ROCKS_LOG_DEBUG(
      table_->rep_->ioptions.info_log,
      "CreateIndexReaderContext async completion: %s",
      status.ToString().c_str());

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

  ROCKS_LOG_DEBUG(
    table_->rep_->ioptions.info_log,
    "CreateIndexReaderContext async completion: %s",
    status.ToString().c_str());

  return status;
}

////////////////////////////////////////////////////////////////
/// NewIndexIteratorContext
//
Status NewIndexIteratorContext::Create(BlockBasedTable* table,
                                       const ReadOptions& read_options,
                                       InternalIterator * preloaded_meta_index_iter,
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

Status NewIndexIteratorContext::RequestCreate(const IndexIterCallback& client_cb,
    BlockBasedTable* table,
    const ReadOptions& read_options,
    InternalIterator* preloaded_meta_index_iter,
    BlockIter * input_iter,
    BlockBasedTable::CachableEntry<IndexReader>* index_entry,
   InternalIterator** index_iterator) {

  assert(index_iterator != nullptr);
  *index_iterator = nullptr;

  std::unique_ptr<NewIndexIteratorContext> ctx(new NewIndexIteratorContext(table, read_options,
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


Status NewIndexIteratorContext::GetFromCache() {

  BlockBasedTable::Rep* rep = table_->rep_;

  Status s;

  // index reader has already been pre-populated.
  if (rep->index_reader) {
    result_ = rep->index_reader->NewIterator(
                input_iter_, ro_->total_order_seek);
    return s;
  }

  // we have a pinned index block
  if (rep->index_entry.IsSet()) {
    result_ = rep->index_entry.value->NewIterator(input_iter_,
              ro_->total_order_seek);
    return s;
  }

  PERF_TIMER_START(read_index_block_nanos);

  const bool no_io = ro_->read_tier == kBlockCacheTier;
  Cache* block_cache = rep->table_options.block_cache.get();

  key_ =
    BlockBasedTable::GetCacheKeyFromOffset(rep->cache_key_prefix,
        rep->cache_key_prefix_size, rep->dummy_index_reader_offset, cache_key_);

  Statistics* statistics = rep->ioptions.statistics;
  cache_handle_ =
    BlockBasedTable::GetEntryFromCache(block_cache, key_, BLOCK_CACHE_INDEX_MISS,
                                       BLOCK_CACHE_INDEX_HIT, statistics);

  // XXXX: Does no_io applies to any io
  // or just sync, after all the request will have to wait for io
  // sync or async
  if (cache_handle_ == nullptr && no_io) {
    s = Status::Incomplete("no blocking io");
    if (input_iter_ != nullptr) {
      input_iter_->SetStatus(s);
      result_ = input_iter_;
    } else {
      result_ = NewErrorInternalIterator(s);
    }

    PERF_TIMER_STOP(read_index_block_nanos);
    return s;
  }

  if (cache_handle_ != nullptr) {
    IndexReader* index_reader = reinterpret_cast<IndexReader*>(block_cache->Value(cache_handle_));
    s = ReaderToIterator(s, index_reader);
    PERF_TIMER_STOP(read_index_block_nanos);
    return s;
  }

  return Status::NotFound();
}

Status NewIndexIteratorContext::RequestIndexRead(const IndexIterCallback& client_cb) {

  TEST_SYNC_POINT("BlockBasedTable::NewIndexIterator::thread2:2");
  Status s;

  IndexReader* index_reader = nullptr;

  cb_ = client_cb;

  if (cb_) {
    async::CallableFactory<NewIndexIteratorContext, Status, const Status&,
          IndexReader*> f(this);
    auto on_create_cb =
      f.GetCallable<&NewIndexIteratorContext::OnCreateComplete>();

    s = CreateIndexReaderContext::RequestCreateReader(on_create_cb, table_, *ro_,
        preloaded_meta_index_iter_, &index_reader, -1);

    if (s.IsIOPending()) {
      return s;
    }

  } else {

    s = CreateIndexReaderContext::CreateReader(table_, *ro_,
        preloaded_meta_index_iter_, &index_reader, -1);
  }

  assert(!s.IsIOPending());

  return OnCreateComplete(s, index_reader);
}

Status NewIndexIteratorContext::OnCreateComplete(const Status& status, IndexReader* index_reader) {

  async(status);

  ROCKS_LOG_DEBUG(
    table_->rep_->ioptions.info_log,
    "NewIndexIteratorContext creation completion: %s",
    status.ToString().c_str());

  TEST_SYNC_POINT("BlockBasedTable::NewIndexIterator::thread1:1");
  TEST_SYNC_POINT("BlockBasedTable::NewIndexIterator::thread2:3");
  TEST_SYNC_POINT("BlockBasedTable::NewIndexIterator::thread1:4");

  Status s;

  Cache* block_cache = table_->rep_->table_options.block_cache.get();
  Statistics* statistics = table_->rep_->ioptions.statistics;

  if (status.ok()) {
    assert(index_reader != nullptr);
    s = block_cache->Insert(
          key_, index_reader, index_reader->usable_size(),
          &BlockBasedTable::DeleteCachedIndexEntry, &cache_handle_,
          table_->rep_->table_options.cache_index_and_filter_blocks_with_high_priority
          ? Cache::Priority::HIGH
          : Cache::Priority::LOW);
  }

  if (s.ok()) {
    size_t usable_size = index_reader->usable_size();
    RecordTick(statistics, BLOCK_CACHE_ADD);
    RecordTick(statistics, BLOCK_CACHE_INDEX_ADD);
    RecordTick(statistics, BLOCK_CACHE_INDEX_BYTES_INSERT, usable_size);
    RecordTick(statistics, BLOCK_CACHE_BYTES_WRITE, usable_size);

    s = ReaderToIterator(s, index_reader);

  } else {

    delete index_reader;

    RecordTick(statistics, BLOCK_CACHE_ADD_FAILURES);

    if (input_iter_ != nullptr) {
      input_iter_->SetStatus(s);
      result_ = input_iter_;
    } else {
      result_ = NewErrorInternalIterator(s);
    }
  }

  PERF_TIMER_STOP(read_index_block_nanos);

  s.async(async());
  return OnComplete(s);
}

Status NewIndexIteratorContext::ReaderToIterator(const Status& status, IndexReader* index_reader) {

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
      result_->RegisterCleanup(&BlockBasedTable::ReleaseCachedEntry, block_cache, cache_handle_);
    }
  }

  PERF_TIMER_STOP(read_index_block_nanos);

  return status;
}

Status NewIndexIteratorContext::OnComplete(const Status& s) {

  if (cb_ && async()) {

    ROCKS_LOG_DEBUG(
      table_->rep_->ioptions.info_log,
      "NewIndexIteratorContext async completion: %s",
      s.ToString().c_str());

    assert(s.async());
    auto result = GetResult();
    cb_.Invoke(s, result);

    delete this;
    return s;
  }

  ROCKS_LOG_DEBUG(
    table_->rep_->ioptions.info_log,
    "NewIndexIteratorContext sync completion: %s",
    s.ToString().c_str());

  return s;
}

//////////////////////////////////////////////////////////////////////////////////////////////
// TableOpenRequestContext
//
TableOpenRequestContext::TableOpenRequestContext(const TableOpenCallback& client_cb,
    const ImmutableCFOptions& ioptions,
    const EnvOptions& env_options,
    const BlockBasedTableOptions& table_options,
    const InternalKeyComparator& internal_comparator,
    std::unique_ptr<RandomAccessFileReader>&& file,
    uint64_t file_size,
    const bool prefetch_index_and_filter_in_cache,
    const bool skip_filters, int level) :
  cb_(client_cb),
  readoptions_(),
  prefetch_index_and_filter_in_cache_(prefetch_index_and_filter_in_cache),
  level_(level) {

  // Create table in advance even though we read the footer a bit later
  // So we avoid storing too much space within the context
  BlockBasedTable::Rep* rep = new BlockBasedTable::Rep(ioptions,
      env_options, table_options, internal_comparator, skip_filters);

  rep->file = std::move(file);
  // rep->footer = footer_; This is done after we read it
  rep->index_type = table_options.index_type;
  rep->hash_index_allow_collision = table_options.hash_index_allow_collision;
  // We need to wrap data with internal_prefix_transform to make sure it can
  // handle prefix correctly.
  rep->internal_prefix_transform.reset(
    new InternalKeySliceTransform(rep->ioptions.prefix_extractor));
  BlockBasedTable::SetupCacheKeyPrefix(rep, file_size);
  new_table_.reset(new BlockBasedTable(rep));

  // page cache options
  rep->persistent_cache_options =
    PersistentCacheOptions(rep->table_options.persistent_cache,
      std::string(rep->persistent_cache_key_prefix,
        rep->persistent_cache_key_prefix_size),
      rep->ioptions.statistics);

  // XXX:: dmitrism
  // This is not implemented on windows but this will block on Linux populating
  // page cache
  // This has two implications: 1) this call will block so needs to be async
  // 2) The rest of the calls will likely not block

  // Before read footer, readahead backwards to prefetch data
  rep->file->Prefetch((file_size < 512 * 1024 ? 0 : file_size - 512 * 1024),
      512 * 1024 /* 512 KB prefetching */);
}

Status TableOpenRequestContext::Open(const ImmutableCFOptions& ioptions,
                                     const EnvOptions& env_options,
                                     const BlockBasedTableOptions& table_options,
                                     const InternalKeyComparator& internal_comparator,
                                     std::unique_ptr<RandomAccessFileReader>&& file,
                                     uint64_t file_size,
                                     std::unique_ptr<TableReader>* table_reader,
                                     const bool prefetch_index_and_filter_in_cache,
                                     const bool skip_filters, const int level) {

  assert(table_reader);
  table_reader->reset();

  auto file_ptr = file.get();

  TableOpenCallback empty_cb;
  TableOpenRequestContext context(empty_cb, ioptions, env_options, table_options,
                                  internal_comparator, std::move(file), file_size,
                                  prefetch_index_and_filter_in_cache,
                                  skip_filters,
                                  level);

  Status s = ReadFooterContext::ReadFooter(file_ptr, file_size,
             &context.footer_,
             kBlockBasedTableMagicNumber);

  assert(!s.IsIOPending());

  // This will start the chain for loading
  // the Table reader and sync completion
  s = context.OnFooterReadComplete(s);

  if (s.ok()) {
    *table_reader = std::move(context.GetTableReader());
  }

  return s;
}

Status TableOpenRequestContext::RequestOpen(const TableOpenCallback& client_cb,
    const ImmutableCFOptions& ioptions,
    const EnvOptions& env_options,
    const BlockBasedTableOptions& table_options,
    const InternalKeyComparator & internal_comparator,
    std::unique_ptr<RandomAccessFileReader>&& file,
    uint64_t file_size,
    std::unique_ptr<TableReader>* table_reader,
    const bool prefetch_index_and_filter_in_cache,
    const bool skip_filters,
    const int level) {

  assert(table_reader);
  table_reader->reset();

  auto file_ptr = file.get();

  std::unique_ptr<TableOpenRequestContext> context(new TableOpenRequestContext(client_cb,
      ioptions, env_options, table_options,
      internal_comparator, std::move(file), file_size,
      prefetch_index_and_filter_in_cache,
      skip_filters,
      level));

  CallableFactory<TableOpenRequestContext, Status, const Status&> fac(context.get());
  auto footer_cb =
    fac.GetCallable<&TableOpenRequestContext::OnFooterReadComplete>();

  Status s = ReadFooterContext::RequestFooterRead(footer_cb, file_ptr, file_size,
             &context->footer_,
             kBlockBasedTableMagicNumber);

  if (s.IsIOPending()) {
    context.release();
    return s;
  }

  s = context->OnFooterReadComplete(s);

  if (s.IsIOPending()) {
    context.release();
  } else if (s.ok()) {
    *table_reader = std::move(context->GetTableReader());
  }

  return s;
}

Status TableOpenRequestContext::OnFooterReadComplete(const Status& status) {

  async(status);

  Status s;
  BlockBasedTable::Rep* rep = new_table_->rep_;

  ROCKS_LOG_DEBUG(
    rep->ioptions.info_log,
    "OnFooterReadComplete: %s",
    status.ToString().c_str());

  if (status.ok()) {

    if (!BlockBasedTableSupportedVersion(footer_.version())) {
      s = Status::Corruption(
            "Unknown Footer version. Maybe this file was created with newer "
            "version of RocksDB?");

      return OnComplete(s);
    }
  } else {
    return OnComplete(status);
  }

  new_table_->rep_->footer = footer_;

  if (cb_) {
    // Get the callback
    async::CallableFactory<TableOpenRequestContext, Status, const Status&>
    factory(this);
    auto meta_cb = factory.GetCallable<&TableOpenRequestContext::OnMetaBlockReadComplete>();

    s = ReadBlockContentsContext::RequestContentstRead(meta_cb,
        rep->file.get(),
        rep->footer, readoptions_, rep->footer.metaindex_handle(),
        &meta_cont_, rep->ioptions, true /* decompress */,
        decomp_dict_, rep->persistent_cache_options);

    // If async or error return
    if (s.IsIOPending()) {
      return s;
    }
  } else {
    s = ReadBlockContentsContext::ReadContents(rep->file.get(),
        rep->footer, readoptions_, rep->footer.metaindex_handle(),
        &meta_cont_, rep->ioptions, true /* decompress */,
        decomp_dict_, rep->persistent_cache_options);
  }

  // Follow-up the content of the read
  return OnMetaBlockReadComplete(s);
}

Status TableOpenRequestContext::OnMetaBlockReadComplete(const Status& s) {

  async(s);

  using Rep = BlockBasedTable::Rep;
  Rep* rep = new_table_->rep_;

  ROCKS_LOG_DEBUG(
    rep->ioptions.info_log,
    "OnMetaBlockReadComplete: %s",
    s.ToString().c_str());

  if (!s.ok()) {
    return OnComplete(s);
  }

  Status status;

  meta_block_.reset(new Block(std::move(meta_cont_),
                              kDisableGlobalSequenceNumber, 0 /* read_amp_bytes_per_bit */,
                              rep->ioptions.statistics));

  meta_iter_.reset(meta_block_->NewIterator(BytewiseComparator()));

  // Find filter handle and filter type
  if (rep->filter_policy) {
    for (auto filter_type : {
           Rep::FilterType::kFullFilter, Rep::FilterType::kPartitionedFilter,
           Rep::FilterType::kBlockFilter
         }) {
      std::string prefix;
      switch (filter_type) {
      case Rep::FilterType::kFullFilter:
        prefix = BlockBasedTable::kFullFilterBlockPrefix;
        break;
      case Rep::FilterType::kPartitionedFilter:
        prefix = BlockBasedTable::kPartitionedFilterBlockPrefix;
        break;
      case Rep::FilterType::kBlockFilter:
        prefix = BlockBasedTable::kFilterBlockPrefix;
        break;
      default:
        assert(0);
      }
      std::string filter_block_key = prefix;
      filter_block_key.append(rep->filter_policy->Name());
      if (FindMetaBlock(meta_iter_.get(), filter_block_key, &rep->filter_handle)
          .ok()) {
        rep->filter_type = filter_type;
        break;
      }
    }
  }

  // Collect flags as to which of these
  // we have found and going to load
  uint32_t  metas = 0;
  uint32_t  metas_count = 0;

  const bool is_index_false = false;
  TableReadMetaBlocksContext* meta_context(new TableReadMetaBlocksContext(
        new_table_.get(), is_index_false));

  bool found_properties_block = false; // XXX: original is true
  BlockHandle prop_block_handle;
  status = SeekToPropertiesBlock(meta_iter_.get(), &found_properties_block,
                                 &prop_block_handle);

  if (!status.ok()) {
    ROCKS_LOG_WARN(rep->ioptions.info_log,
                   "Error when seeking to properties block from file: %s",
                   status.ToString().c_str());
  }  else if (found_properties_block && !prop_block_handle.IsNull()) {
    meta_context->AddProperties(prop_block_handle);
    metas |= TableReadMetaBlocksContext::mProperties;
    ++metas_count;
  } else {
    ROCKS_LOG_ERROR(rep->ioptions.info_log,
                    "Cannot find Properties block from file.");
  }

  bool found_compression_dict = false;
  BlockHandle compression_handle;
  status = SeekToCompressionDictBlock(meta_iter_.get(), &found_compression_dict,
                                      &compression_handle);

  if (!status.ok()) {
    ROCKS_LOG_WARN(
      rep->ioptions.info_log,
      "Error when seeking to compression dictionary block from file: %s",
      status.ToString().c_str());
  } else if (found_compression_dict && !compression_handle.IsNull()) {
    meta_context->AddCompDict(compression_handle);
    metas |= TableReadMetaBlocksContext::mCompDict;
    ++metas_count;
  }

  // Read the range del meta block
  bool found_range_del_block = false;
  status = SeekToRangeDelBlock(meta_iter_.get(), &found_range_del_block,
                               &rep->range_del_handle);

  if (!status.ok()) {
    ROCKS_LOG_WARN(
      rep->ioptions.info_log,
      "Error when seeking to range delete tombstones block from file: %s",
      status.ToString().c_str());
  } else {
    if (found_range_del_block && !rep->range_del_handle.IsNull()) {
      metas |= TableReadMetaBlocksContext::mRangDel;
      ++metas_count;
    }
  }

  // Fire up reading prop, comp_dict and range_del in parallel
  if (metas_count > 0) {
    // Must set count to avoid premature destruction
    meta_context->SetCount(metas_count);

    // If we are async set callback

    if (cb_) {
      CallableFactory<TableOpenRequestContext, Status, const Status&> fac(this);
      auto meta_cb = fac.GetCallable<&TableOpenRequestContext::OnMetasReadComplete>();
      meta_context->SetCB(meta_cb);
    }

    // Whoever is the last one, sync or async, will destroy
    // the
    size_t io_pending = 0;
    // Indicates if a sync operation was the last one
    // so we need to invoke the callback ourselves
    size_t complete = 0;

    if((metas & TableReadMetaBlocksContext::mProperties) != 0) {
      status = meta_context->ReadProperties();
      io_pending += status.IsIOPending();
      complete += (status.subcode() == Status::kOnComplete);
    }

    if ((metas & TableReadMetaBlocksContext::mCompDict) != 0) {
      status = meta_context->ReadCompDict();
      io_pending += status.IsIOPending();
      complete += (status.subcode() == Status::kOnComplete);
    }

    if ((metas & TableReadMetaBlocksContext::mRangDel) != 0) {
      status = meta_context->ReadRangeDel();
      io_pending += status.IsIOPending();
      complete += (status.subcode() == Status::kOnComplete);
    }

    // If async operation is expected to be the last
    if (io_pending > 0 && complete == 0) {
      return Status::IOPending();
    }
  }

  return OnMetasReadComplete(status);
}

Status TableOpenRequestContext::OnMetasReadComplete(const Status& status) {

  async(status);

  BlockBasedTable::Rep* rep = new_table_->rep_;

  ROCKS_LOG_DEBUG(
    rep->ioptions.info_log,
    "OnMetasReadComplete");

  Status s;

  // Determine whether whole key filtering is supported.
  if (rep->table_properties) {
    rep->whole_key_filtering &=
      BlockBasedTable::IsFeatureSupported(*(rep->table_properties),
                                          BlockBasedTablePropertyNames::kWholeKeyFiltering,
                                          rep->ioptions.info_log);
    rep->prefix_filtering &= BlockBasedTable::IsFeatureSupported(
                               *(rep->table_properties),
                               BlockBasedTablePropertyNames::kPrefixFiltering, rep->ioptions.info_log);

    rep->global_seqno = BlockBasedTable::GetGlobalSequenceNumber(*(rep->table_properties),
                        rep->ioptions.info_log);
  }

  // pre-fetching of blocks is turned on
  // Will use block cache for index/filter blocks access
  // Always prefetch index and filter for level 0
  if (rep->table_options.cache_index_and_filter_blocks) {
    if (prefetch_index_and_filter_in_cache_ || level_ == 0) {
      assert(rep->table_options.block_cache != nullptr);
      // Hack: Call NewIndexIterator() to implicitly add index to the
      // block_cache

      // if pin_l0_filter_and_index_blocks_in_cache is true and this is
      // a level0 file, then we will pass in this pointer to rep->index
      // to NewIndexIterator(), which will save the index block in there
      // else it's a nullptr and nothing special happens
      BlockBasedTable::CachableEntry<BlockBasedTable::IndexReader>* index_entry = nullptr;
      if (rep->table_options.pin_l0_filter_and_index_blocks_in_cache &&
          level_ == 0) {
        index_entry = &rep->index_entry;
      }

      // This creates a chain of OnNewIndexIterator -> OnGetFilter ->
      // OnComplete()
      InternalIterator* index_iterator = nullptr;
      if (cb_) {

        async::CallableFactory<TableOpenRequestContext, Status, const Status&,
              InternalIterator*> f(this);
        auto on_index_iter_create =
          f.GetCallable<&TableOpenRequestContext::OnNewIndexIterator>();

        s = NewIndexIteratorContext::RequestCreate(on_index_iter_create, new_table_.get(),
            readoptions_, meta_iter_.get(), nullptr, index_entry, &index_iterator);

        // index_iterator was or will be reported via callback
        if (s.IsIOPending()) {
          return s;
        }

      } else {
        s = NewIndexIteratorContext::Create(new_table_.get(), readoptions_, meta_iter_.get(),
                                            nullptr, index_entry, &index_iterator);
      }

      // Really serves only to bring things into cache
      return OnNewIndexIterator(s, index_iterator);
    }

  } else {
    // If we don't use block cache for index/filter blocks access, we'll
    // pre-load these blocks, which will kept in member variables in Rep
    // and with a same life-time as this table object.

    // This runs a chain of ->OnCreateIndexReader()->OnReaderFilter()->
    // OnComplete()
    IndexReader* index_reader = nullptr;
    if (cb_) {

      CallableFactory<TableOpenRequestContext, Status, const Status&,
                      IndexReader*> f(this);

      auto on_create_index_reader =
        f.GetCallable<&TableOpenRequestContext::OnCreateIndexReader>();

      s = CreateIndexReaderContext::RequestCreateReader(on_create_index_reader,
          new_table_.get(), readoptions_, meta_iter_.get(), &index_reader, level_);

      if (s.IsIOPending()) {
        return s;
      }

    } else {
      s = CreateIndexReaderContext::CreateReader(new_table_.get(), readoptions_, meta_iter_.get(),
          &index_reader, level_);
    }
    return OnCreateIndexReader(s, index_reader);
  }

  return OnComplete(s);
}

Status TableOpenRequestContext::OnNewIndexIterator(const Status& status,
    InternalIterator* index_iterator) {

  async(status);

  ROCKS_LOG_DEBUG(
    new_table_->rep_->ioptions.info_log,
    "OnNewIndexIterator: %s",
    status.ToString().c_str());

  Status s;

  std::unique_ptr<InternalIterator> iter_guard(index_iterator);

  if (!status.ok()) {
    s = status;
  } else if (!index_iterator->status().ok()) {
    s = index_iterator->status();
  }

  if (s.ok()) {

    // Hack: Call GetFilter() to implicitly add filter to the block_cache

    // This will cache the filter and read it if necessary
    // no_io is false by default
    get_filter_helper_.reset(new GetFilterHelper(new_table_.get()));

    // In this case both may return success in theory due to the cache hit
    // in practice we are still filling the cache and no_io is false in this case
    if(cb_) {
      CallableFactory<TableOpenRequestContext, Status, const Status&> f(this);
      auto on_get_filter_cb = f.GetCallable<&TableOpenRequestContext::OnGetFilter>();

      s = get_filter_helper_->GetFilter(on_get_filter_cb);

      if (s.IsIOPending()) {
        return s;
      }

    } else {
      s = get_filter_helper_->GetFilter(GetFilterHelper::GetFilterCallback());
    }

    // Get Filter may return NotSupported() in case there is no
    // filter
    return OnGetFilter(s);
  }

  return OnComplete(s);
}

Status TableOpenRequestContext::OnCreateIndexReader(const Status& status,
    BlockBasedTable::IndexReader * index_reader) {

  async(status);

  ROCKS_LOG_DEBUG(
    new_table_->rep_->ioptions.info_log,
    "OnCreateIndexReader: %s",
    status.ToString().c_str());

  Status s;

  if (status.ok()) {
    assert(index_reader != nullptr);

    BlockBasedTable::Rep* rep = new_table_->rep_;

    rep->index_reader.reset(index_reader);

    // Set filter block
    if (rep->filter_policy) {
      const bool is_a_filter_partition = true;

      read_filter_helper_.reset(new ReadFilterHelper(new_table_.get(),
                                !is_a_filter_partition));

      if(cb_) {

        CallableFactory<TableOpenRequestContext, Status, const Status&> f(this);
        auto on_read_filter_cb =
          f.GetCallable<&TableOpenRequestContext::OnReadFilter>();

        s = read_filter_helper_->Read(on_read_filter_cb, rep->filter_handle);

        if (s.IsIOPending()) {
          return s;
        }

      } else {
        auto empty_cb = ReadFilterHelper::ReadFilterCallback();
        s = read_filter_helper_->Read(empty_cb, rep->filter_handle);
      }

      return OnReadFilter(s);
    }
  } else {
    delete index_reader;
    s = status;
  }

  return OnComplete(s);
}

Status TableOpenRequestContext::OnGetFilter(const Status& status) {

  async(status);

  Status s = get_filter_helper_->OnGetFilterComplete(status);

  ROCKS_LOG_DEBUG(
    new_table_->rep_->ioptions.info_log,
    "OnGetFilter: %s",
    s.ToString().c_str());

  if (s.ok()) {

    BlockBasedTable::Rep* rep = new_table_->rep_;
    // if pin_l0_filter_and_index_blocks_in_cache is true, and this is
    // a level0 file, then save it in rep_->filter_entry; it will be
    // released in the destructor only, hence it will be pinned in the
    // cache while this reader is alive
    auto& filter_entry = get_filter_helper_->GetEntry();
    if (rep->table_options.pin_l0_filter_and_index_blocks_in_cache &&
        level_ == 0) {
      rep->filter_entry = filter_entry;
      rep->filter_entry.value->SetLevel(level_);
    } else {
      filter_entry.Release(rep->table_options.block_cache.get());
    }
  }

  return OnComplete(s);
}

Status TableOpenRequestContext::OnReadFilter(const Status& status) {

  async(status);

  Status s = read_filter_helper_->OnFilterReadComplete(status);

  ROCKS_LOG_DEBUG(
    new_table_->rep_->ioptions.info_log,
    "OnReadFilter: %s",
    s.ToString().c_str());

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

Status TableOpenRequestContext::OnComplete(const Status& status) {

  if (cb_ && async()) {

    ROCKS_LOG_DEBUG(
      new_table_->rep_->ioptions.info_log,
      "TableOpenRequestContext async completion: %s",
      status.ToString().c_str());

    // Make sure async status is passed
    if (status.async()) {
      cb_.Invoke(status, std::move(new_table_));
    } else {
      Status s(status);
      s.async(true);
      cb_.Invoke(s, std::move(new_table_));
    }

    delete this;
    return status;
  }

  ROCKS_LOG_DEBUG(
    new_table_->rep_->ioptions.info_log,
    "TableOpenRequestContext sync completion: %s",
    status.ToString().c_str());

  return status;
}

///////////////////////////////////////////////////////////////////////
/// NewBlockIteratorHelper
Status NewDataBlockIteratorHelper::Create(const ReadDataBlockCallback& cb,
    const BlockHandle& handle, BlockIter* input_iter) {

  Status s;

  // Important for repeated invocations
  Reset();

  input_iter_ = input_iter;

  Slice compression_dict;
  if (rep_->compression_dict_block) {
    compression_dict = rep_->compression_dict_block->data;
  }

  PERF_TIMER_START(new_table_block_iter_nanos);

  if (mb_helper_.IsCacheEnabled(rep_)) {

    s = mb_helper_.GetBlockFromCache(rep_, *ro_, handle, compression_dict,
                                     &entry_);

    if (s.ok() && entry_.value != nullptr) {
      action_ = aCache;
      return s;
    }

    /// Not Found
    if (mb_helper_.ShouldRead(*ro_)) {
      // The result must be cached
      action_ = aCachableRead;

      const bool do_uncompress = (nullptr ==
        rep_->table_options.block_cache_compressed);
      return mb_helper_.RequestCachebableBlock(cb, rep_, *ro_, handle, &block_cont_,
             do_uncompress);
    }
  }

  // When we get there it means that either of the three things below
  // -- Cache is not not enabled OR
  //  - The item is not in the cache and either reads are disabled OR
  //    fill_cache is false
  if (mb_helper_.IsNoIo(*ro_)) {
    s = Status::Incomplete("no blocking io");
  } else {

    action_ = aDirectRead;
    const bool do_uncompress_true = true;

    if (cb) {
      s = ReadBlockContentsContext::RequestContentstRead(cb, rep_->file.get(),
          rep_->footer, *ro_,
          handle, &block_cont_, rep_->ioptions, do_uncompress_true, compression_dict,
          rep_->persistent_cache_options);
    } else {
      s = ReadBlockContentsContext::ReadContents(rep_->file.get(), rep_->footer,
          *ro_,
          handle, &block_cont_, rep_->ioptions, do_uncompress_true, compression_dict,
          rep_->persistent_cache_options);
    }
  }

  return s;
}

Status NewDataBlockIteratorHelper::OnCreateComplete(const Status& status) {

  Status s;

  if (status.ok()) {

    if (action_ == aCache) {
      assert(entry_.value != nullptr);
    } else if (action_ == aCachableRead) {

      Slice compression_dict;
      if (rep_->compression_dict_block) {
        compression_dict = rep_->compression_dict_block->data;
      }

      s = mb_helper_.OnBlockReadComplete(status, rep_, *ro_, std::move(block_cont_),
                                         compression_dict,
                                         &entry_);
    } else if(action_ == aDirectRead) {

      entry_.value = new Block(std::move(block_cont_),
                               rep_->global_seqno,
                               rep_->table_options.read_amp_bytes_per_bit,
                               rep_->ioptions.statistics);

    } else {
      assert(false);
    }

  } else {
    s = status;
  }

  if (s.ok()) {

    assert(entry_.value != nullptr);

    auto iter = entry_.value->NewIterator(&rep_->internal_comparator, input_iter_,
                                          true,
                                          rep_->ioptions.statistics);

    if (input_iter_ == nullptr) {
      new_iterator_.reset(iter);
    }

    if (entry_.cache_handle != nullptr) {
      Cache* block_cache = rep_->table_options.block_cache.get();
      iter->RegisterCleanup(&BlockBasedTable::ReleaseCachedEntry, block_cache,
                            entry_.cache_handle);
    } else {
      iter->RegisterCleanup(&DeleteHeldResource<Block>, entry_.value, nullptr);
    }

  } else {
    assert(entry_.value == nullptr);
    StatusToIterator(s);
    // Status is reported via iterator
    s = Status::OK();
  }

  PERF_TIMER_STOP(new_table_block_iter_nanos);

  return s;
}

////////////////////////////////////////////////////////////////////////////
//// NewDataBlockIteratorContext
/// 

Status NewDataBlockIteratorContext::Create(BlockBasedTable::Rep* rep, 
  const ReadOptions& ro, const BlockHandle& block_handle,
  InternalIterator** internal_iterator,
  BlockIter* input_iter, bool is_index) {

  assert(internal_iterator);
  *internal_iterator = nullptr;

  Callback empty_cb;
  NewDataBlockIteratorContext context(empty_cb, rep, ro, is_index);

  NewDataBlockIteratorHelper::ReadDataBlockCallback empty_readblock_cb;
  Status s = context.biter_helper_.Create(empty_readblock_cb, block_handle, input_iter);
  s = context.OnBlockReadComplete(s);
  if (s.ok()) {
    *internal_iterator = context.GetResult();
  }
  return s;
}

Status NewDataBlockIteratorContext::RequestCreate(const Callback& cb,
    BlockBasedTable::Rep* rep, const ReadOptions& ro,
    const BlockHandle& block_handle, InternalIterator** internal_iterator,
    BlockIter* input_iter, bool is_index) {

  assert(cb);
  assert(internal_iterator);
  *internal_iterator = nullptr;

  std::unique_ptr<NewDataBlockIteratorContext> context (new 
    NewDataBlockIteratorContext(cb, rep, ro, is_index));

  CallableFactory<NewDataBlockIteratorContext, Status, const Status&> f(context.get());
  auto readblock_cb = f.GetCallable<&NewDataBlockIteratorContext::OnBlockReadComplete>();

  Status s = context->biter_helper_.Create(readblock_cb, block_handle, input_iter);

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

inline
Status NewDataBlockIteratorContext::OnBlockReadComplete(const Status& status) {
  async(status);

  Status s = biter_helper_.OnCreateComplete(status);

  s.async(async());
  return OnComplete(s);
}

Status NewDataBlockIteratorContext::OnComplete(const Status& status) {

  if (cb_ && async()) {
    ROCKS_LOG_DEBUG(
      biter_helper_.GetTableRep()->ioptions.info_log,
      "NewDataBlockIteratorContext async completion: %s",
      status.ToString().c_str());

    auto result = biter_helper_.GetResult();
    cb_.Invoke(status, result);
    delete this;
    return status;
  }

  ROCKS_LOG_DEBUG(
    biter_helper_.GetTableRep()->ioptions.info_log,
    "NewDataBlockIteratorContext sync completion: %s",
    status.ToString().c_str());
  return status;
}

////////////////////////////////////////////////////////////////////////////////
// NewRangeTombstoneIterContext
//
Status NewRangeTombstoneIterContext::CreateIterator(BlockBasedTable::Rep* rep,
    const ReadOptions & read_options, InternalIterator** iterator) {

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

Status NewRangeTombstoneIterContext::RequestCreateIterator(const Callback& cb,
    BlockBasedTable::Rep* rep, const ReadOptions& read_options,
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

Status NewRangeTombstoneIterContext::GetFromCache(BlockBasedTable::Rep* rep,
    InternalIterator** iterator) {

  assert(iterator != nullptr);
  *iterator = nullptr;

  // should call IsPresent() before attempting to create
  assert(!rep->range_del_handle.IsNull());

  if (rep->range_del_entry.cache_handle != nullptr) {
    // We have a handle to an uncompressed block cache entry that's held for
    // this table's lifetime. Increment its refcount before returning an
    // iterator based on it since the returned iterator may outlive this table
    // reader.
    assert(rep->range_del_entry.value != nullptr);
    Cache* block_cache = rep->table_options.block_cache.get();
    assert(block_cache != nullptr);
    if (block_cache->Ref(rep->range_del_entry.cache_handle)) {
      *iterator = rep->range_del_entry.value->NewIterator(
                    &rep->internal_comparator, nullptr /* iter */,
                    true /* total_order_seek */, rep->ioptions.statistics);
      (*iterator)->RegisterCleanup(&BlockBasedTable::ReleaseCachedEntry, block_cache,
                                   rep->range_del_entry.cache_handle);
      return Status::OK();
    }
  }

  return Status::NotFound();
}

Status NewRangeTombstoneIterContext::RequestRead() {

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

Status NewRangeTombstoneIterContext::OnReadBlockComplete(const Status& status) {
  async(status);
  Status s = db_iter_helper_.OnCreateComplete(status);
  return OnComplete(s);
}

Status NewRangeTombstoneIterContext::OnComplete(const Status& status) {

  BlockBasedTable::Rep* rep = db_iter_helper_.GetTableRep();

  if (cb_ && async()) {

    ROCKS_LOG_DEBUG(
      rep->ioptions.info_log,
      "TableOpenRequestContext async completion: %s",
      status.ToString().c_str());

    // Make sure async status is preserved
    Status s(status);
    s.async(true);
    cb_.Invoke(s, db_iter_helper_.GetResult());

    delete this;
    return status;
  }

  ROCKS_LOG_DEBUG(
    rep->ioptions.info_log,
    "TableOpenRequestContext sync completion: %s",
    status.ToString().c_str());

  return status;
}

//////////////////////////////////////////////////////////////////////
/// BlockBasedGetContext

Status BlockBasedGetContext::Get(BlockBasedTable* table,
                                 const ReadOptions& read_options,
                                 const Slice & key, GetContext * get_context,
                                 bool skip_filters) {

  const Callback empty_cb;
  BlockBasedGetContext ctx(empty_cb, table, read_options, key, get_context,
                           skip_filters);
  return ctx.GetImpl();
}

Status BlockBasedGetContext::RequestGet(const Callback& cb,
                                        BlockBasedTable * table,
                                        const ReadOptions& read_options, const Slice & key,
                                        GetContext * get_context, bool skip_filters) {

  std::unique_ptr<BlockBasedGetContext> ctx(new BlockBasedGetContext(cb, table,
      read_options, key, get_context,
      skip_filters));

  Status s = ctx->GetImpl();

  if (s.IsIOPending()) {
    ctx.release();
  }

  return s;
}

Status BlockBasedGetContext::GetImpl() {

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

Status BlockBasedGetContext::OnGetFilter(const Status& status) {
  async(status);

  auto rep = Rep();

  ROCKS_LOG_DEBUG(
    rep->ioptions.info_log,
    "OnGetFilter invoked with: %s",
    status.ToString().c_str());

  Status s = gf_helper_.OnGetFilterComplete(status);

  ROCKS_LOG_DEBUG(
    rep->ioptions.info_log,
    "OnGetFilterComplete returned: %s",
    s.ToString().c_str());


  return CreateIndexIterator();
}

Status BlockBasedGetContext::CreateIndexIterator() {

  Status s;
  FilterBlockReader* filter = GetFilterEntry().value;

  // XXX: This should not incur IO on a block based table
  if (!gf_helper_.GetTable()->FullFilterKeyMayMatch(*GetReadOptions(), filter,
      key_, IsNoIO())) {
    RecordTick(Rep()->ioptions.statistics, BLOOM_FILTER_USEFUL);

    s.async(async());
    return OnComplete(s);
  }

  // Create index iterator
  InternalIterator* index_iterator = nullptr;
  auto table = gf_helper_.GetTable();

  if (cb_) {
    CallableFactory<BlockBasedGetContext, Status, const Status&, InternalIterator*>
    f(this);
    auto on_index_iterator_cb =
      f.GetCallable<&BlockBasedGetContext::OnIndexIteratorCreate>();
    s = NewIndexIteratorContext::RequestCreate(on_index_iterator_cb, table,
        *GetReadOptions(), nullptr /* preloaded meta iterator*/, &index_iter_,
        nullptr /* index_entry */,
        &index_iterator);

    if (s.IsIOPending()) {
      return s;
    }

  } else {
    s = NewIndexIteratorContext::Create(table,
                                        *GetReadOptions(), nullptr /* preloaded meta iterator*/, &index_iter_,
                                        nullptr /* index_entry */,
                                        &index_iterator);
  }

  return OnIndexIteratorCreate(s, index_iterator);
}

Status BlockBasedGetContext::OnIndexIteratorCreate(const Status& status,
    InternalIterator* index_iterator) {
  async(status);

  Status s;

  if (status.ok()) {

    if (index_iterator != nullptr && index_iterator != &index_iter_) {
      iiter_unique_ptr_.reset(index_iterator);
    }

    auto iiter = GetIndexIter();
    // XXX: At this point Seek/Next()
    // must be always sync although at other levels
    // it can be both
    iiter->Seek(key_);
    if (iiter->Valid()) {

      s = CreateDataBlockIterator();

      if (s.IsIOPending()) {
        return s;
      }

      // NotFound -> filtered out
      if (!s.IsNotFound()) {
        return OnNewDataBlockIterator(s);
      } else {
        // Return ok on NotFound and let
        // get_context express its state
        s = Status::OK();
      }

    } else {
      s = iiter->status();
    }

  } else {
    s = status;
  }

  s.async(async());
  return OnComplete(s);
}

Status BlockBasedGetContext::CreateDataBlockIterator() {
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

Status BlockBasedGetContext::OnNewDataBlockIterator(const Status& status) {
  async(status);

  Status s(status);
  bool done = false;
  auto iiter = GetIndexIter();

  while (!done) {

    // New Data Block iterator created
    s = biter_helper_.OnCreateComplete(s);

    if (s.ok()) {
      auto biter = biter_helper_.GetResult();
      // Expecting to point to our member instance
      // so no need to deallocate
      assert(biter == &block_iter_);

      if (IsNoIO() && biter->status().IsIncomplete()) {
        // couldn't get block from block_cache
        // Update Saver.state to Found because we are only looking for whether
        // we can guarantee the key is not there when "no_io" is set

        /// XXX: It looks like we return OK in this case or index iterator status
        get_context_->MarkKeyMayExist();
        break;
      }

      if (!biter->status().ok()) {
        s = biter->status();
        break;
      }

      // Call the *saver function on each entry/block until it returns false
      for (biter->Seek(key_); biter->Valid(); biter->Next()) {
        ParsedInternalKey parsed_key;
        if (!ParseInternalKey(biter->key(), &parsed_key)) {
          s = Status::Corruption(Slice());
        }

        if (!get_context_->SaveValue(parsed_key, biter->value(), &block_iter_)) {
          done = true;
          break;
        }
      }

      s = biter->status();

      if (done) {
        break;
      }

      iiter->Next();

      if (!iiter->Valid()) {
        break;
      }

      s = CreateDataBlockIterator();

      if (s.IsIOPending()) {
        return s;
      }

      if (s.IsNotFound()) {
        s = Status::OK();
        break;
      }

      if (!s.ok()) {
        break;
      }

      // New data block iterator was created sync, continue
      // iteration
    } else {
      break;
    }
  }


  // Check index iterator status if OK
  if (s.ok()) {
    s = iiter->status();
  }

  s.async(async());
  return OnComplete(s);
}

Status BlockBasedGetContext::OnComplete(const Status& status) {

  auto rep = Rep();
  // if rep_->filter_entry is not set, we should call Release(); otherwise
  // don't call, in this case we have a local copy in rep_->filter_entry,
  // it's pinned to the cache and will be released in the destructor
  if (!rep->filter_entry.IsSet()) {
    GetFilterEntry().Release(rep->table_options.block_cache.get());
  }

  if (cb_ && async()) {

    Status s(status);

    ROCKS_LOG_DEBUG(
      rep->ioptions.info_log,
      "TableOpenRequestContext async completion: %s",
      s.ToString().c_str());

    s.async(true);
    cb_.Invoke(s);

    delete this;
    return s;
  }

  ROCKS_LOG_DEBUG(
    rep->ioptions.info_log,
    "TableOpenRequestContext sync completion: %s",
    status.ToString().c_str());

  return status;
}

//////////////////////////////////////////////////////////
// BlockBasedNewIteratorContext
//

Status BlockBasedNewIteratorContext::Create(BlockBasedTable* table,
    const ReadOptions& read_options, Arena* arena, bool skip_filters,
    InternalIterator** iterator) {
  assert(iterator != nullptr);
  *iterator = nullptr;
  const Callback empty_cb;
  BlockBasedNewIteratorContext context(empty_cb, table, read_options, skip_filters, arena);
  Status s = context.NewIterator();
  if (s.ok()) {
    *iterator = context.GetResult();
  }
  return s;
}

Status BlockBasedNewIteratorContext::RequestCreate(const Callback& cb,
    BlockBasedTable* table, const ReadOptions& read_options, Arena* arena,
    bool skip_filters, InternalIterator** iterator) {

  assert(cb);
  assert(iterator != nullptr);
  *iterator = nullptr;
  std::unique_ptr<BlockBasedNewIteratorContext> context(new
      BlockBasedNewIteratorContext(cb, table, read_options, skip_filters, arena));
  Status s = context->NewIterator();
  if (s.ok()) {
    *iterator = context->GetResult();
  } else if (s.IsIOPending()) {
    context.release();
  }
  return s;
}

Status BlockBasedNewIteratorContext::NewIterator() {

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

Status BlockBasedNewIteratorContext::OnNewIndexIterator(const Status& status,
    InternalIterator* index_iterator) {
  async(status);

  if (status.ok()) {
    assert(index_iterator != nullptr);
    result_ = NewTwoLevelIterator(
      new BlockBasedTable::BlockEntryIteratorState(table_, *ro_, skip_filters_),
      index_iterator, arena_);
  }

  return OnComplete(status);
}

Status BlockBasedNewIteratorContext::OnComplete(const Status& status) {
  if (cb_ && async()) {

    ROCKS_LOG_DEBUG(
      table_->rep_->ioptions.info_log,
      "BlockBasedNewIteratorContext async completion: %s",
      status.ToString().c_str());

    Status s(status);
    s.async(async());

    auto iterator = GetResult();
    cb_.Invoke(s, iterator);
    delete this;
    return s;
  }

  ROCKS_LOG_DEBUG(
    table_->rep_->ioptions.info_log,
    "BlockBasedNewIteratorContext sync completion: %s",
    status.ToString().c_str());

  return status;
}

} // namespace async
} // namespace rocksdb