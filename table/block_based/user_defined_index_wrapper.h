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
                      std::string* separator_scratch) override {
    UserDefinedIndexBuilder::BlockHandle handle;
    handle.offset = block_handle.offset();
    handle.size = block_handle.size();
    // Forward the call to both index builders
    user_defined_index_builder_->AddIndexEntry(last_key_in_current_block,
                                               first_key_in_next_block, handle,
                                               separator_scratch);
    return internal_index_builder_->AddIndexEntry(
        last_key_in_current_block, first_key_in_next_block, block_handle,
        separator_scratch);
  }

  void OnKeyAdded(const Slice& key,
                  const std::optional<Slice>& value) override {
    if (status_.ok()) {
      if (!value.has_value()) {
        status_ = Status::InvalidArgument(
            "user_defined_index_factory not supported with parallel "
            "compression");
      } else {
        ParsedInternalKey pkey;
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
    user_defined_index_builder_->OnKeyAdded(
        key, UserDefinedIndexBuilder::ValueType::kValue, value.value());
  }

  Status Finish(IndexBlocks* index_blocks,
                const BlockHandle& last_partition_block_handle) override {
    if (!status_.ok()) {
      return status_;
    }

    // Finish the internal index builder
    status_ = internal_index_builder_->Finish(index_blocks,
                                              last_partition_block_handle);
    if (!status_.ok()) {
      return status_;
    }

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

    index_size_ = internal_index_builder_->IndexSize();
    return status_;
  }

  size_t IndexSize() const override { return index_size_; }

  bool seperator_is_key_plus_seq() override {
    return internal_index_builder_->seperator_is_key_plus_seq();
  }

 private:
  const std::string name_;
  std::unique_ptr<IndexBuilder> internal_index_builder_;
  std::unique_ptr<UserDefinedIndexBuilder> user_defined_index_builder_;
  Status status_;
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
      valid_ = status_.ok() &&
               result_.bound_check_result == IterBoundCheck::kInbound;
    }
  }

  void Next() override {
    status_ = udi_iter_->NextAndGetResult(&result_);
    valid_ =
        status_.ok() && result_.bound_check_result == IterBoundCheck::kInbound;
  }

  bool NextAndGetResult(IterateResult* result) override {
    status_ = udi_iter_->NextAndGetResult(&result_);
    valid_ =
        status_.ok() && result_.bound_check_result == IterBoundCheck::kInbound;
    if (status_.ok()) {
      *result = result_;
    }
    return valid_;
  }

  void SeekForPrev(const Slice& /*target*/) override {
    status_ = Status::NotSupported("SeekForPrev not supported");
  }

  void Prev() override { status_ = Status::NotSupported("Prev not supported"); }

  Slice key() const override { return result_.key; }

  IndexValue value() const override {
    auto handle = udi_iter_->value();
    IndexValue val(BlockHandle(handle.offset, handle.size), Slice());
    return val;
  }

  Status status() const override { return status_; }

  void Prepare(const std::vector<ScanOptions>* scan_opts) override {
    udi_iter_->Prepare(scan_opts->data(), scan_opts->size());
  }

 private:
  std::unique_ptr<UserDefinedIndexIterator> udi_iter_;
  IterateResult result_;
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
    return new UserDefinedIndexIteratorWrapper(std::move(udi_iter));
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
