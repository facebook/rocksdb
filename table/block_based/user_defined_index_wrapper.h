//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "db/seqno_to_time_mapping.h"
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
    UserDefinedIndexBuilder::BlockHandle handle{};
    handle.offset = block_handle.offset();
    handle.size = block_handle.size();
    // Forward the call to both index builders.
    // Parse the internal keys to extract user keys and sequence numbers.
    // There's no way to return an error here, so we remember the status and
    // return it in Finish().
    ParsedInternalKey pkey_last;
    ParsedInternalKey pkey_first;
    if (status_.ok()) {
      status_ = ParseInternalKey(last_key_in_current_block, &pkey_last,
                                 /*log_err_key*/ false);
    }
    if (status_.ok() && first_key_in_next_block) {
      status_ = ParseInternalKey(*first_key_in_next_block, &pkey_first,
                                 /*log_err_key*/ false);
    }
    if (status_.ok()) {
      // Pass both user keys AND sequence numbers to the UDI builder via
      // the IndexEntryContext. The sequence numbers are needed when the
      // same user key spans a data block boundary (e.g., due to snapshots
      // keeping multiple versions). Without sequence numbers, the UDI
      // cannot produce a separator that distinguishes the two blocks,
      // causing incorrect Seek results.
      UserDefinedIndexBuilder::IndexEntryContext ctx;
      ctx.last_key_seq = pkey_last.sequence;
      ctx.first_key_seq = first_key_in_next_block ? pkey_first.sequence : 0;
      user_defined_index_builder_->AddIndexEntry(
          pkey_last.user_key,
          first_key_in_next_block ? &pkey_first.user_key : nullptr, handle,
          separator_scratch, ctx);
    }
    return internal_index_builder_->AddIndexEntry(
        last_key_in_current_block, first_key_in_next_block, block_handle,
        separator_scratch, skip_delta_encoding);
  }

  // Parallel compression splits AddIndexEntry() into PrepareIndexEntry() (emit
  // thread) and FinishIndexEntry() (worker thread). This wrapper does not
  // implement that split yet, so parallel compression is rejected at option
  // validation time (see BlockBasedTableFactory::ValidateOptions and the Rep
  // constructor). These stubs exist only to satisfy the interface.
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
    // Always forward to internal index builder first. It relies on receiving
    // OnKeyAdded for every key to maintain state (e.g.,
    // current_block_first_internal_key_) needed by AddIndexEntry, which is
    // always forwarded regardless of UDI status.
    internal_index_builder_->OnKeyAdded(key, value);

    ParsedInternalKey pkey;
    if (status_.ok()) {
      // Defensive: value should always be present since OnKeyAdded() is called
      // on the main thread in Add() with the original value Slice. No current
      // code path passes std::nullopt here.
      if (!value.has_value()) {
        assert(false);
        status_ = Status::InvalidArgument(
            "OnKeyAdded called without a value; UDI requires the value to "
            "forward to the plugin builder");
      } else {
        status_ = ParseInternalKey(key, &pkey, /*log_err_key*/ false);
      }
    }
    if (!status_.ok()) {
      return;
    }

    // Pass the user key to the UDI with the mapped value type. In SST files
    // produced by flush or compaction, there may be multiple entries for the
    // same user key with different sequence numbers (e.g., when snapshots are
    // active). UDI builders that use OnKeyAdded() should handle this; builders
    // that only use AddIndexEntry() separator keys (e.g., trie) are unaffected.
    Slice udi_value = value.value();
    if (pkey.type == kTypeValuePreferredSeqno) {
      // Strip the packed preferred seqno suffix so the UDI plugin receives
      // only the user value, consistent with the kValue contract.
      udi_value = ParsePackedValueForValue(udi_value);
    }
    user_defined_index_builder_->OnKeyAdded(
        pkey.user_key, MapToUDIValueType(pkey.type), udi_value);
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

  uint64_t CurrentIndexSizeEstimate() const override {
    return internal_index_builder_->CurrentIndexSizeEstimate();
  }

  bool separator_is_key_plus_seq() override {
    return internal_index_builder_->separator_is_key_plus_seq();
  }

 private:
  static UserDefinedIndexBuilder::ValueType MapToUDIValueType(
      ROCKSDB_NAMESPACE::ValueType t) {
    switch (t) {
      case kTypeValue:
      case kTypeValuePreferredSeqno:
        return UserDefinedIndexBuilder::kValue;
      case kTypeDeletion:
      case kTypeSingleDeletion:
      case kTypeDeletionWithTimestamp:
        return UserDefinedIndexBuilder::kDelete;
      case kTypeMerge:
        return UserDefinedIndexBuilder::kMerge;
      case kTypeBlobIndex:
      case kTypeWideColumnEntity:
        return UserDefinedIndexBuilder::kOther;
      default:
        // Any new type that reaches OnKeyAdded() should be explicitly mapped
        // above. Falling through to kOther is a safe default but indicates a
        // missing case that should be added.
        assert(false);
        return UserDefinedIndexBuilder::kOther;
    }
  }

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

  ~UserDefinedIndexIteratorWrapper() override = default;

  bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    status_ = udi_iter_->SeekToFirstAndGetResult(&result_);
    if (status_.ok()) {
      valid_ = result_.bound_check_result == IterBoundCheck::kInbound;
      if (valid_) {
        SetInternalKeyFromUDIResult();
      }
    } else {
      valid_ = false;
    }
  }

  void SeekToLast() override {
    valid_ = false;
    status_ = Status::NotSupported("SeekToLast not supported");
  }

  void Seek(const Slice& target) override {
    ParsedInternalKey pkey;
    status_ = ParseInternalKey(target, &pkey, /*log_err_key=*/false);
    if (status_.ok()) {
      // Pass both user key AND sequence number to the UDI iterator via
      // SeekContext. The sequence number is needed when the same user key
      // spans multiple data blocks with different sequence numbers (e.g.,
      // due to snapshots). Without it, the UDI cannot distinguish which
      // block to return for a given (user_key, seqno) target.
      UserDefinedIndexIterator::SeekContext ctx;
      ctx.target_seq = pkey.sequence;
      status_ = udi_iter_->SeekAndGetResult(pkey.user_key, &result_, ctx);
    }
    if (status_.ok()) {
      valid_ = result_.bound_check_result == IterBoundCheck::kInbound;
      if (valid_) {
        SetInternalKeyFromUDIResult();
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
        SetInternalKeyFromUDIResult();
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
        SetInternalKeyFromUDIResult();
        result->key = key();
      }
      result->bound_check_result = result_.bound_check_result;
      result->value_prepared = result_.value_prepared;
    } else {
      valid_ = false;
    }
    return valid_;
  }

  void SeekForPrev(const Slice& /*target*/) override {
    valid_ = false;
    status_ = Status::NotSupported("SeekForPrev not supported");
  }

  void Prev() override {
    valid_ = false;
    status_ = Status::NotSupported("Prev not supported");
  }

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
  // Convert the UDI result's user key into an internal key for the index
  // iterator contract. UDI separators are user keys, but
  // InternalIteratorBase<IndexValue> must expose internal keys (user key +
  // 8-byte trailer). We use seq=0 / kTypeValue so that the resulting
  // internal key compares as "greater than or equal to" any real data key
  // with the same user key (lower seqno = later in internal key order),
  // which is the correct upper-bound semantics for an index separator.
  void SetInternalKeyFromUDIResult() {
    ikey_.Set(result_.key, 0, ValueType::kTypeValue);
  }

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

  InternalIteratorBase<IndexValue>* NewIterator(
      const ReadOptions& read_options, bool disable_prefix_seek,
      IndexBlockIter* iter, GetContext* get_context,
      BlockCacheLookupContext* lookup_context) override {
    if (!read_options.table_index_factory) {
      return reader_->NewIterator(read_options, disable_prefix_seek, iter,
                                  get_context, lookup_context);
    }
    if (name_ != read_options.table_index_factory->Name()) {
      return NewErrorInternalIterator<IndexValue>(Status::InvalidArgument(
          "Bad index name: " +
          std::string(read_options.table_index_factory->Name()) +
          ". Only supported UDI is " + name_));
    }
    std::unique_ptr<UserDefinedIndexIterator> udi_iter =
        udi_reader_->NewIterator(read_options);
    if (udi_iter) {
      return new UserDefinedIndexIteratorWrapper(std::move(udi_iter));
    }
    return NewErrorInternalIterator<IndexValue>(
        Status::NotFound("Could not create UDI iterator"));
  }

  Status CacheDependencies(const ReadOptions& ro, bool pin,
                           FilePrefetchBuffer* tail_prefetch_buffer) override {
    return reader_->CacheDependencies(ro, pin, tail_prefetch_buffer);
  }

  size_t ApproximateMemoryUsage() const override {
    return reader_->ApproximateMemoryUsage() +
           udi_reader_->ApproximateMemoryUsage();
  }

  void EraseFromCacheBeforeDestruction(
      uint32_t uncache_aggressiveness) override {
    reader_->EraseFromCacheBeforeDestruction(uncache_aggressiveness);
  }

 private:
  std::string name_;
  std::unique_ptr<BlockBasedTable::IndexReader> reader_;
  std::unique_ptr<UserDefinedIndexReader> udi_reader_;
};
}  // namespace ROCKSDB_NAMESPACE
