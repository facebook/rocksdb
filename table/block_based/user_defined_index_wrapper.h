//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/user_defined_index.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_type.h"
#include "table/block_based/cachable_entry.h"
#include "table/block_based/index_builder.h"

namespace ROCKSDB_NAMESPACE {

// UserDefinedIndexWrapper wraps around the existing index types in block based
// table, and supports plugging in an additional user defined index. The wrapper
// class forwards calls to both the wrapped internal index, and a user defined
// index builder.
class UserDefinedIndexBuilderWrapper : public IndexBuilder {
 public:
  UserDefinedIndexBuilderWrapper(
      const std::string& name,
      std::unique_ptr<IndexBuilder> internal_index_builder,
      std::unique_ptr<UserDefinedIndexBuilder> user_defined_index_builder,
      const InternalKeyComparator* comparator, size_t ts_sz,
      bool persist_user_defined_timestamps)
      : IndexBuilder(comparator, ts_sz, persist_user_defined_timestamps),
        name_(name),
        internal_index_builder_(std::move(internal_index_builder)),
        user_defined_index_builder_(std::move(user_defined_index_builder)) {}

  ~UserDefinedIndexBuilderWrapper() override = default;

  Slice AddIndexEntry(const Slice& last_key_in_current_block,
                      const Slice* first_key_in_next_block,
                      const BlockHandle& block_handle,
                      std::string* separator_scratch,
                      bool skip_delta_encoding) override {
    UserDefinedIndexBuilder::BlockHandle handle;
    handle.offset = block_handle.offset();
    handle.size = block_handle.size();
    // Forward the call to both index builders
    ParsedInternalKey pkey_last;
    ParsedInternalKey pkey_first;
    // There's no way to return an error here, so we remember the statsu and
    // return it in Finish()
    if (status_.ok()) {
      status_ = ParseInternalKey(last_key_in_current_block, &pkey_last,
                                 /*lof_err_key*/ false);
    }
    if (status_.ok() && first_key_in_next_block) {
      status_ = ParseInternalKey(*first_key_in_next_block, &pkey_first,
                                 /*lof_err_key*/ false);
    }
    if (status_.ok()) {
      user_defined_index_builder_->AddIndexEntry(
          pkey_last.user_key,
          first_key_in_next_block ? &pkey_first.user_key : nullptr, handle,
          separator_scratch);
    }
    return internal_index_builder_->AddIndexEntry(
        last_key_in_current_block, first_key_in_next_block, block_handle,
        separator_scratch, skip_delta_encoding);
  }

  // Not supported with parallel compression
  std::unique_ptr<PreparedIndexEntry> CreatePreparedIndexEntry() override {
    return nullptr;
  }
  void PrepareIndexEntry(const Slice& last_key_in_current_block,
                         const Slice* first_key_in_next_block,
                         PreparedIndexEntry* out) override {
    (void)last_key_in_current_block;
    (void)first_key_in_next_block;
    (void)out;
    assert(false);
  }
  void FinishIndexEntry(const BlockHandle& block_handle,
                        PreparedIndexEntry* entry,
                        bool skip_delta_encoding) override {
    (void)block_handle;
    (void)entry;
    (void)skip_delta_encoding;
    assert(false);
  }

  void OnKeyAdded(const Slice& key,
                  const std::optional<Slice>& value) override {
    ParsedInternalKey pkey;
    if (status_.ok()) {
      if (!value.has_value()) {
        status_ = Status::InvalidArgument(
            "user_defined_index_factory not supported with parallel "
            "compression");
      } else {
        status_ = ParseInternalKey(key, &pkey, /*lof_err_key*/ false);
        if (status_.ok() && pkey.type != ValueType::kTypeValue) {
          status_ = Status::InvalidArgument(
              "user_defined_index_factory only supported with Puts");
        }
      }
    }
    if (!status_.ok()) {
      return;
    }

    // Forward the call to both index builders
    internal_index_builder_->OnKeyAdded(key, value);

    // Pass the user key to the UDI. We don't expect multiple entries with
    // different sequence numbers for the same key in the file. RocksDB may
    // enforce it in the future by allowing UDIs only for read only
    // bulkloaded use cases, and only allow ingestion of files with
    // sequence number 0.
    user_defined_index_builder_->OnKeyAdded(
        pkey.user_key, UserDefinedIndexBuilder::ValueType::kValue,
        value.value());
  }

  Status Finish(IndexBlocks* index_blocks,
                const BlockHandle& last_partition_block_handle) override {
    if (!status_.ok() && !status_.IsIncomplete()) {
      return status_;
    }

    if (!udi_finished_) {
      // Finish the user defined index builder
      Slice user_index_contents;
      status_ = user_defined_index_builder_->Finish(&user_index_contents);
      if (!status_.ok()) {
        return status_;
      }

      // Add the user defined index to the meta blocks
      std::string block_name = kUserDefinedIndexPrefix + name_;
      index_blocks->meta_blocks.insert(
          {block_name, {BlockType::kUserDefinedIndex, user_index_contents}});
      udi_finished_ = true;
    }

    // Finish the internal index builder
    status_ = internal_index_builder_->Finish(index_blocks,
                                              last_partition_block_handle);
    if (!status_.ok()) {
      return status_;
    }

    index_size_ = internal_index_builder_->IndexSize();
    return status_;
  }

  size_t IndexSize() const override { return index_size_; }

  uint64_t EstimateCurrentIndexSize() const override { return 0; }

  bool separator_is_key_plus_seq() override {
    return internal_index_builder_->separator_is_key_plus_seq();
  }

 private:
  const std::string name_;
  std::unique_ptr<IndexBuilder> internal_index_builder_;
  std::unique_ptr<UserDefinedIndexBuilder> user_defined_index_builder_;
  Status status_;
  bool udi_finished_ = false;
};

class UserDefinedIndexIteratorWrapper
    : public InternalIteratorBase<IndexValue> {
 public:
  explicit UserDefinedIndexIteratorWrapper(
      std::unique_ptr<UserDefinedIndexIterator>&& udi_iter)
      : udi_iter_(std::move(udi_iter)), valid_(false) {}

  bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    status_ = Status::NotSupported("SeekToFirst not supported");
  }

  void SeekToLast() override {
    status_ = Status::NotSupported("SeekToLast not supported");
  }

  void Seek(const Slice& target) override {
    ParsedInternalKey pkey;
    status_ = ParseInternalKey(target, &pkey, /*log_err_key=*/false);
    if (status_.ok()) {
      status_ = udi_iter_->SeekAndGetResult(pkey.user_key, &result_);
    }
    if (status_.ok()) {
      valid_ = result_.bound_check_result == IterBoundCheck::kInbound;
      if (valid_) {
        ikey_.Set(result_.key, 0, ValueType::kTypeValue);
      }
    } else {
      valid_ = false;
    }
  }

  void Next() override {
    status_ = udi_iter_->NextAndGetResult(&result_);
    if (status_.ok()) {
      valid_ = result_.bound_check_result == IterBoundCheck::kInbound;
      if (valid_) {
        ikey_.Set(result_.key, 0, ValueType::kTypeValue);
      }
    } else {
      valid_ = false;
    }
  }

  bool NextAndGetResult(IterateResult* result) override {
    status_ = udi_iter_->NextAndGetResult(&result_);
    if (status_.ok()) {
      valid_ = result_.bound_check_result == IterBoundCheck::kInbound;
      if (valid_) {
        ikey_.Set(result_.key, 0, ValueType::kTypeValue);
      }
      if (status_.ok()) {
        *result = result_;
      }
    } else {
      valid_ = false;
    }
    return valid_;
  }

  void SeekForPrev(const Slice& /*target*/) override {
    status_ = Status::NotSupported("SeekForPrev not supported");
  }

  void Prev() override { status_ = Status::NotSupported("Prev not supported"); }

  Slice key() const override { return Slice(*ikey_.const_rep()); }

  IndexValue value() const override {
    auto handle = udi_iter_->value();
    IndexValue val(BlockHandle(handle.offset, handle.size), Slice());
    return val;
  }

  Status status() const override { return status_; }

  void Prepare(const MultiScanArgs* scan_opts) override {
    if (scan_opts) {
      udi_iter_->Prepare(scan_opts->GetScanRanges().data(),
                         scan_opts->GetScanRanges().size());
    }
  }

  IterBoundCheck UpperBoundCheckResult() override {
    return result_.bound_check_result;
  }

 private:
  std::unique_ptr<UserDefinedIndexIterator> udi_iter_;
  IterateResult result_;
  InternalKey ikey_;
  Status status_;
  bool valid_;
};

class UserDefinedIndexReaderWrapper : public BlockBasedTable::IndexReader {
 public:
  UserDefinedIndexReaderWrapper(
      const std::string& name,
      std::unique_ptr<BlockBasedTable::IndexReader>&& reader,
      std::unique_ptr<UserDefinedIndexReader>&& udi_reader)
      : name_(name),
        reader_(std::move(reader)),
        udi_reader_(std::move(udi_reader)) {}

  virtual InternalIteratorBase<IndexValue>* NewIterator(
      const ReadOptions& read_options, bool disable_prefix_seek,
      IndexBlockIter* iter, GetContext* get_context,
      BlockCacheLookupContext* lookup_context) override {
    if (!read_options.table_index_factory) {
      return reader_->NewIterator(read_options, disable_prefix_seek, iter,
                                  get_context, lookup_context);
    }
    if (name_ != read_options.table_index_factory->Name()) {
      return NewErrorInternalIterator<IndexValue>(Status::InvalidArgument(
          "Bad index name" +
          std::string(read_options.table_index_factory->Name()) +
          ". Only supported UDI is " + name_));
    }
    std::unique_ptr<UserDefinedIndexIterator> udi_iter =
        udi_reader_->NewIterator(read_options);
    if (udi_iter) {
      InternalIteratorBase<IndexValue>* wrap_iter =
          new UserDefinedIndexIteratorWrapper(std::move(udi_iter));
      return wrap_iter;
    }
    return NewErrorInternalIterator<IndexValue>(
        Status::NotFound("COuld not create UDI iterator"));
  }

  virtual Status CacheDependencies(
      const ReadOptions& ro, bool pin,
      FilePrefetchBuffer* tail_prefetch_buffer) override {
    return reader_->CacheDependencies(ro, pin, tail_prefetch_buffer);
  }

  size_t ApproximateMemoryUsage() const override {
    return reader_->ApproximateMemoryUsage();
  }

  virtual void EraseFromCacheBeforeDestruction(
      uint32_t uncache_aggressiveness) override {
    reader_->EraseFromCacheBeforeDestruction(uncache_aggressiveness);
  }

 private:
  std::string name_;
  std::unique_ptr<BlockBasedTable::IndexReader> reader_;
  std::unique_ptr<UserDefinedIndexReader> udi_reader_;
};
}  // namespace ROCKSDB_NAMESPACE
