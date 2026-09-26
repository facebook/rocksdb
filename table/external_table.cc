//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/external_table.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <limits>
#include <unordered_map>

#include "db/dbformat.h"
#include "logging/logging.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/options_type.h"
#include "rocksdb/utilities/types_util.h"
#include "table/block_based/block.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {

Status GetValueType(EntryType entry_type, ValueType* value_type) {
  switch (entry_type) {
    case kEntryPut:
      *value_type = kTypeValue;
      return Status::OK();
    case kEntryDelete:
      *value_type = kTypeDeletion;
      return Status::OK();
    case kEntrySingleDelete:
      *value_type = kTypeSingleDeletion;
      return Status::OK();
    case kEntryMerge:
      *value_type = kTypeMerge;
      return Status::OK();
    case kEntryBlobIndex:
      *value_type = kTypeBlobIndex;
      return Status::OK();
    case kEntryWideColumnEntity:
      *value_type = kTypeWideColumnEntity;
      return Status::OK();
    case kEntryRangeDeletion:
    case kEntryDeleteWithTimestamp:
    case kEntryTimedPut:
      return Status::NotSupported(
          "External table entry type " +
          std::to_string(static_cast<unsigned int>(entry_type)) +
          " is not supported");
    case kEntryOther:
      return Status::Corruption("Invalid external table entry type");
  }
  return Status::Corruption("Invalid external table entry type");
}

// Returns the effective full-mode key after applying any file-wide sequence
// number. effective_entry->user_key aliases stored_internal_key.
Status ParseExternalTableEffectiveEntry(
    const Slice& stored_internal_key,
    const InternalKeyComparator& internal_comparator,
    SequenceNumber global_seqno, ParsedInternalKey* effective_entry) {
  ParsedEntryInfo entry;
  Status status = ParseEntry(stored_internal_key,
                             internal_comparator.user_comparator(), &entry);
  if (!status.ok()) {
    return status;
  }

  ValueType value_type;
  status = GetValueType(entry.type, &value_type);
  if (!status.ok()) {
    return status;
  }

  SequenceNumber sequence = entry.sequence;
  if (global_seqno != kDisableGlobalSequenceNumber) {
    if (sequence != 0) {
      return Status::Corruption(
          "External table entry has a nonzero sequence number in global "
          "sequence number mode");
    }
    sequence = global_seqno;
  }
  *effective_entry = ParsedInternalKey(ExtractUserKey(stored_internal_key),
                                       sequence, value_type);
  return Status::OK();
}

// Leaves lookup_key unchanged unless a global sequence number is active.
// Global-seqno files store sequence zero, so their seek boundary is rewritten;
// value_type selects the forward or reverse boundary. A rewritten lookup_key
// aliases stored_lookup_key.
Status SetExternalTableGlobalSeqnoLookupKey(Slice* lookup_key,
                                            SequenceNumber global_seqno,
                                            ValueType value_type,
                                            IterKey* stored_lookup_key) {
  if (global_seqno == kDisableGlobalSequenceNumber) {
    return Status::OK();
  }

  ParsedInternalKey parsed_lookup_key;
  Status status = ParseInternalKey(*lookup_key, &parsed_lookup_key,
                                   /*log_err_key=*/false);
  if (!status.ok()) {
    return status;
  }
  stored_lookup_key->SetInternalKey(parsed_lookup_key.user_key, 0, value_type);
  *lookup_key = stored_lookup_key->GetInternalKey();
  return Status::OK();
}

// Adapts full-mode reader callbacks to GetContext. Reset() initializes the
// fixed storage used by MultiGet().
class ExternalTableGetContextImpl final : public ExternalTableGetContext {
 public:
  ExternalTableGetContextImpl() = default;

  ExternalTableGetContextImpl(GetContext* get_context,
                              const InternalKeyComparator* comparator,
                              const Slice& lookup_key,
                              SequenceNumber global_seqno) {
    Reset(get_context, comparator, lookup_key, global_seqno);
  }

  void Reset(GetContext* get_context, const InternalKeyComparator* comparator,
             const Slice& lookup_key, SequenceNumber global_seqno) {
    get_context_ = get_context;
    comparator_ = comparator;
    lookup_key_ = lookup_key;
    global_seqno_ = global_seqno;
  }

  Status Save(const Slice& stored_internal_key, PinnableSlice* value,
              bool* continue_reading) override;

 private:
  GetContext* get_context_ = nullptr;
  const InternalKeyComparator* comparator_ = nullptr;
  Slice lookup_key_;
  SequenceNumber global_seqno_ = kDisableGlobalSequenceNumber;
};

Status ExternalTableGetContextImpl::Save(const Slice& stored_internal_key,
                                         PinnableSlice* value,
                                         bool* continue_reading) {
  assert(value != nullptr);
  assert(continue_reading != nullptr);
  *continue_reading = false;

  ParsedInternalKey parsed_entry;
  Status status = ParseExternalTableEffectiveEntry(
      stored_internal_key, *comparator_, global_seqno_, &parsed_entry);
  if (!status.ok()) {
    return status;
  }
  // Applying global_seqno can make this same-user entry newer than the lookup
  // snapshot. Skip it as an effective-key seek would.
  if (global_seqno_ != kDisableGlobalSequenceNumber &&
      comparator_->user_comparator()->EqualWithoutTimestamp(
          parsed_entry.user_key, ExtractUserKey(lookup_key_)) &&
      comparator_->Compare(parsed_entry, lookup_key_) < 0) {
    value->Reset();
    *continue_reading = true;
    return Status::OK();
  }

  bool matched = false;
  Status read_status;
  // GetContext decides whether it needs another older entry, for example to
  // finish resolving a merge chain.
  *continue_reading =
      get_context_->SaveValue(parsed_entry, *value, &matched, &read_status,
                              value->IsPinned() ? value : nullptr);
  value->Reset();
  return read_status;
}

// Keeps translated keys and per-key GetContext adapters alive during a
// full-mode MultiGet, and maps its dense results back to the input range.
class ExternalTableMultiGetContextImpl final
    : public ExternalTableMultiGetContext {
 public:
  ExternalTableMultiGetContextImpl(const MultiGetContext::Range& mget_range,
                                   const InternalKeyComparator& comparator,
                                   SequenceNumber global_seqno) {
    for (auto iter = mget_range.begin(); iter != mget_range.end(); ++iter) {
      Entry& entry = entries_[size_];
      entry.lookup_key = iter->ikey;
      Status status = SetExternalTableGlobalSeqnoLookupKey(
          &entry.lookup_key, global_seqno, kValueTypeForSeek,
          &entry.stored_lookup_key);
      if (!status.ok()) {
        *iter->s = status;
        continue;
      }
      entry.output_status = iter->s;
      entry.get_context.Reset(iter->get_context, &comparator, iter->ikey,
                              global_seqno);
      ++size_;
    }
  }

  size_t Size() const override { return size_; }

  ExternalTableMultiGetRequest GetRequest(size_t index) override {
    Entry& entry = entries_[index];
    return {entry.lookup_key, &entry.get_context};
  }

  void ApplyStatuses(std::vector<Status>* statuses) const {
    assert(statuses->size() == size_);
    for (size_t i = 0; i < size_; ++i) {
      Status& status = (*statuses)[i];
      // NotFound is table-local; let RocksDB continue searching older files.
      *entries_[i].output_status =
          status.IsNotFound() ? Status::OK() : std::move(status);
    }
  }

 private:
  struct Entry {
    Slice lookup_key;
    Status* output_status = nullptr;
    IterKey stored_lookup_key;
    ExternalTableGetContextImpl get_context;
  };

  std::array<Entry, MultiGetContext::MAX_BATCH_SIZE> entries_;
  size_t size_ = 0;
};

// Translates basic user keys and full stored keys into RocksDB's effective
// internal-key order, including global-seqno seek correction.
template <ExternalTableMode Mode>
class ExternalTableIteratorAdapter : public InternalIterator {
 public:
  ExternalTableIteratorAdapter(ExternalTableIteratorBase* iterator,
                               const InternalKeyComparator& internal_comparator,
                               SequenceNumber global_seqno)
      : iterator_(iterator),
        internal_comparator_(internal_comparator),
        global_seqno_(global_seqno),
        valid_(false),
        status_(iterator != nullptr ? iterator->status() : Status::OK()) {}

  // No copying allowed
  ExternalTableIteratorAdapter(const ExternalTableIteratorAdapter&) = delete;
  ExternalTableIteratorAdapter& operator=(const ExternalTableIteratorAdapter&) =
      delete;

  ~ExternalTableIteratorAdapter() override {}

  bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    status_ = Status::OK();
    if (iterator_) {
      iterator_->SeekToFirst();
      UpdateKey();
    }
  }

  void SeekToLast() override {
    status_ = Status::OK();
    if (iterator_) {
      iterator_->SeekToLast();
      UpdateKey();
    }
  }

  void Seek(const Slice& target) override {
    status_ = Status::OK();
    valid_ = false;
    if (iterator_) {
      if constexpr (Mode == ExternalTableMode::kFull) {
        Slice external_target = target;
        IterKey stored_target;
        status_ = SetExternalTableGlobalSeqnoLookupKey(
            &external_target, global_seqno_, kValueTypeForSeek, &stored_target);
        if (status_.ok()) {
          iterator_->Seek(external_target);
        }
      } else {
        ParsedEntryInfo lookup_key;
        status_ = ParseEntry(target, internal_comparator_.user_comparator(),
                             &lookup_key);
        if (status_.ok()) {
          iterator_->Seek(lookup_key.user_key);
        }
      }
      if (status_.ok()) {
        UpdateKey();
        if (Mode == ExternalTableMode::kOnlyZeroSeqnoAndPuts ||
            global_seqno_ != kDisableGlobalSequenceNumber) {
          // Advance until the translated result satisfies InternalIterator's
          // Seek() contract.
          while (valid_ && internal_comparator_.Compare(key(), target) < 0) {
            iterator_->Next();
            UpdateKey();
          }
        }
      }
    }
  }

  void SeekForPrev(const Slice& target) override {
    status_ = Status::OK();
    valid_ = false;
    if (iterator_) {
      if constexpr (Mode == ExternalTableMode::kFull) {
        Slice external_target = target;
        IterKey stored_target;
        status_ = SetExternalTableGlobalSeqnoLookupKey(
            &external_target, global_seqno_, kValueTypeForSeekForPrev,
            &stored_target);
        if (status_.ok()) {
          iterator_->SeekForPrev(external_target);
        }
      } else {
        ParsedEntryInfo lookup_key;
        status_ = ParseEntry(target, internal_comparator_.user_comparator(),
                             &lookup_key);
        if (status_.ok()) {
          iterator_->SeekForPrev(lookup_key.user_key);
        }
      }
      if (status_.ok()) {
        UpdateKey();
        if (Mode == ExternalTableMode::kOnlyZeroSeqnoAndPuts ||
            global_seqno_ != kDisableGlobalSequenceNumber) {
          // Retreat until the translated result satisfies SeekForPrev().
          while (valid_ && internal_comparator_.Compare(key(), target) > 0) {
            iterator_->Prev();
            UpdateKey();
          }
        }
      }
    }
  }

  void Next() override {
    if (iterator_) {
      iterator_->Next();
      UpdateKey();
    }
  }

  bool NextAndGetResult(IterateResult* result) override {
    if (iterator_) {
      valid_ = iterator_->NextAndGetResult(&result_);
      result->value_prepared = result_.value_prepared;
      result->bound_check_result = result_.bound_check_result;
      if (valid_) {
        UpdateKey(result_.key);
        if (valid_) {
          result->key = key();
        }
      } else {
        status_ = iterator_->status();
      }
    } else {
      valid_ = false;
    }
    return valid_;
  }

  bool PrepareValue() override {
    if (iterator_ && !result_.value_prepared) {
      valid_ = iterator_->PrepareValue();
      status_ = iterator_->status();
      if (!status_.ok()) {
        valid_ = false;
      }
      result_.value_prepared = true;
    }
    return valid_;
  }

  IterBoundCheck UpperBoundCheckResult() override {
    if (iterator_) {
      result_.bound_check_result = iterator_->UpperBoundCheckResult();
    }
    return result_.bound_check_result;
  }

  void Prev() override {
    if (iterator_) {
      iterator_->Prev();
      UpdateKey();
    }
  }

  Slice key() const override {
    if (iterator_) {
      return key_.GetInternalKey();
    }
    return Slice();
  }

  Slice value() const override {
    if (iterator_) {
      return iterator_->value();
    }
    return Slice();
  }

  Status status() const override { return status_; }

  void Prepare(const MultiScanArgs* scan_opts) override {
    if (iterator_ && scan_opts) {
      iterator_->Prepare(scan_opts->GetScanRanges().data(), scan_opts->size());
    } else if (iterator_) {
      iterator_->Prepare(nullptr, 0);
    }
  }

 private:
  std::unique_ptr<ExternalTableIteratorBase> iterator_;
  const InternalKeyComparator& internal_comparator_;
  const SequenceNumber global_seqno_;
  IterKey key_;
  bool valid_;
  Status status_;
  IterateResult result_;

  void UpdateKey(OptSlice res = OptSlice()) {
    if (iterator_) {
      valid_ = iterator_->Valid();
      status_ = iterator_->status();
      if (valid_ && status_.ok()) {
        if constexpr (Mode == ExternalTableMode::kOnlyZeroSeqnoAndPuts) {
          key_.SetInternalKey(
              res.has_value() ? res.value() : iterator_->key(),
              global_seqno_ == kDisableGlobalSequenceNumber ? 0 : global_seqno_,
              kTypeValue);
          return;
        } else {
          const Slice stored_key =
              res.has_value() ? res.value() : iterator_->key();
          ParsedInternalKey parsed_entry;
          status_ = ParseExternalTableEffectiveEntry(
              stored_key, internal_comparator_, global_seqno_, &parsed_entry);
          if (!status_.ok()) {
            valid_ = false;
            return;
          }
          if (global_seqno_ == kDisableGlobalSequenceNumber) {
            // The external iterator keeps stored_key valid until it moves,
            // matching InternalIterator's borrowed-key lifetime.
            key_.SetInternalKey(stored_key, /*copy=*/false);
          } else {
            key_.SetInternalKey(parsed_entry);
          }
        }
      } else if (!status_.ok()) {
        valid_ = false;
      }
    }
  }
};

// Prefer a RocksDB properties block, but allow the external implementation to
// provide TableProperties directly.
template <ExternalTableMode Mode>
Status LoadExternalTableProperties(
    const ImmutableOptions& ioptions, ExternalTableReaderBase<Mode>* reader,
    std::shared_ptr<const TableProperties>* table_properties) {
  std::unique_ptr<char[]> property_block;
  uint64_t property_block_size = 0;
  uint64_t property_block_offset = 0;
  Status status = reader->GetPropertiesBlock(
      &property_block, &property_block_size, &property_block_offset);
  std::shared_ptr<TableProperties> loaded_properties;
  if (status.ok()) {
    auto parsed_properties = std::make_unique<TableProperties>();
    BlockContents block_contents(std::move(property_block),
                                 property_block_size);
    Block block(std::move(block_contents));
    status = ParsePropertiesBlock(ioptions, property_block_offset, block,
                                  parsed_properties);
    if (!status.ok()) {
      return status;
    }
    loaded_properties = std::move(parsed_properties);
  } else {
    if (!status.IsNotSupported()) {
      return status;
    }
    loaded_properties =
        std::make_shared<TableProperties>(*reader->GetTableProperties());
  }

  if constexpr (Mode == ExternalTableMode::kOnlyZeroSeqnoAndPuts) {
    if (!status.ok()) {
      loaded_properties->key_largest_seqno = 0;
      loaded_properties->key_smallest_seqno = 0;
    }
  }
  if (loaded_properties->num_range_deletions != 0) {
    // Range tombstones need a separate iterator that external tables do not
    // currently expose.
    return Status::NotSupported(
        "External tables do not support range deletions");
  }

  *table_properties = std::move(loaded_properties);
  return Status::OK();
}

template <ExternalTableMode Mode>
class ExternalTableReaderAdapter : public TableReader {
 public:
  ExternalTableReaderAdapter(
      const InternalKeyComparator& internal_comparator,
      std::unique_ptr<ExternalTableReaderBase<Mode>>&& reader,
      std::shared_ptr<const TableProperties> table_properties,
      SequenceNumber global_seqno)
      : internal_comparator_(internal_comparator),
        reader_(std::move(reader)),
        table_properties_(std::move(table_properties)),
        global_seqno_(global_seqno) {}

  ~ExternalTableReaderAdapter() override {}

  // No copying allowed
  ExternalTableReaderAdapter(const ExternalTableReaderAdapter&) = delete;
  ExternalTableReaderAdapter& operator=(const ExternalTableReaderAdapter&) =
      delete;

  InternalIterator* NewIterator(
      const ReadOptions& read_options, const SliceTransform* prefix_extractor,
      Arena* arena, bool /* skip_filters */, TableReaderCaller /* caller */,
      size_t /* compaction_readahead_size */ = 0,
      bool /* allow_unprepared_value */ = false) override {
    auto iterator = reader_->NewIterator(read_options, prefix_extractor);
    if (arena == nullptr) {
      return new ExternalTableIteratorAdapter<Mode>(
          iterator, internal_comparator_, global_seqno_);
    } else {
      auto* mem =
          arena->AllocateAligned(sizeof(ExternalTableIteratorAdapter<Mode>));
      return new (mem) ExternalTableIteratorAdapter<Mode>(
          iterator, internal_comparator_, global_seqno_);
    }
  }

  uint64_t ApproximateOffsetOf(const ReadOptions&, const Slice&,
                               TableReaderCaller) override {
    return 0;
  }

  uint64_t ApproximateSize(const ReadOptions&, const Slice&, const Slice&,
                           TableReaderCaller) override {
    return 0;
  }

  void SetupForCompaction() override {}

  std::shared_ptr<const TableProperties> GetTableProperties() const override {
    return table_properties_;
  }

  size_t ApproximateMemoryUsage() const override { return 0; }

  Status Get(const ReadOptions& read_options, const Slice& key,
             GetContext* get_context, const SliceTransform* prefix_extractor,
             bool /*skip_filters*/ = false) override {
    // TableReader reports a miss as OK without modifying GetContext.
    if constexpr (Mode == ExternalTableMode::kOnlyZeroSeqnoAndPuts) {
      ParsedEntryInfo lookup_key;
      Status status =
          ParseEntry(key, internal_comparator_.user_comparator(), &lookup_key);
      if (!status.ok()) {
        return status;
      }
      // A file-wide sequence newer than the snapshot makes the whole table
      // invisible to this lookup.
      if (global_seqno_ != kDisableGlobalSequenceNumber &&
          global_seqno_ > lookup_key.sequence) {
        return Status::OK();
      }

      PinnableSlice value;
      status = reader_->Get(read_options, lookup_key.user_key, prefix_extractor,
                            &value);
      if (status.IsNotFound()) {
        return Status::OK();
      }
      if (!status.ok()) {
        return status;
      }
      if (global_seqno_ == kDisableGlobalSequenceNumber || global_seqno_ == 0) {
        get_context->SaveValue(value, /*seq=*/0,
                               value.IsPinned() ? &value : nullptr);
        return Status::OK();
      }
      const ParsedInternalKey parsed_key(lookup_key.user_key, global_seqno_,
                                         kTypeValue);
      bool matched = false;
      Status read_status;
      get_context->SaveValue(parsed_key, value, &matched, &read_status,
                             value.IsPinned() ? &value : nullptr);
      return read_status;
    } else {
      Slice external_lookup_key = key;
      IterKey stored_lookup_key;
      Status status = SetExternalTableGlobalSeqnoLookupKey(
          &external_lookup_key, global_seqno_, kValueTypeForSeek,
          &stored_lookup_key);
      if (!status.ok()) {
        return status;
      }
      ExternalTableGetContextImpl context(get_context, &internal_comparator_,
                                          key, global_seqno_);
      status = reader_->Get(read_options, external_lookup_key, prefix_extractor,
                            &context);
      if (status.IsNotFound()) {
        return Status::OK();
      }
      return status;
    }
  }

  void MultiGet(const ReadOptions& read_options,
                const MultiGetContext::Range* mget_range,
                const SliceTransform* prefix_extractor,
                bool /*skip_filters*/ = false) override {
    if constexpr (Mode == ExternalTableMode::kOnlyZeroSeqnoAndPuts) {
      const size_t num_keys = mget_range->KeysLeft();
      std::vector<Slice> lookup_keys;
      lookup_keys.reserve(num_keys);
      // KeysLeft() is bounded by MultiGetContext::MAX_BATCH_SIZE.
      std::array<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> key_contexts{};

      size_t index = 0;
      for (auto iter = mget_range->begin(); iter != mget_range->end(); ++iter) {
        ParsedEntryInfo lookup_key;
        Status status = ParseEntry(
            iter->ikey, internal_comparator_.user_comparator(), &lookup_key);
        if (!status.ok()) {
          *iter->s = status;
          continue;
        }
        if (global_seqno_ != kDisableGlobalSequenceNumber &&
            global_seqno_ > lookup_key.sequence) {
          *iter->s = Status::OK();
          continue;
        }
        lookup_keys.push_back(lookup_key.user_key);
        key_contexts[index] = &*iter;
        ++index;
      }
      if (lookup_keys.empty()) {
        return;
      }

      std::vector<PinnableSlice> values(lookup_keys.size());
      std::vector<Status> statuses(lookup_keys.size());
      reader_->MultiGet(read_options, lookup_keys, prefix_extractor, &values,
                        &statuses);
      for (size_t i = 0; i < statuses.size(); ++i) {
        if (statuses[i].IsNotFound()) {
          *key_contexts[i]->s = Status::OK();
        } else if (!statuses[i].ok()) {
          *key_contexts[i]->s = statuses[i];
        } else {
          if (global_seqno_ == kDisableGlobalSequenceNumber ||
              global_seqno_ == 0) {
            key_contexts[i]->get_context->SaveValue(
                values[i], /*seq=*/0,
                values[i].IsPinned() ? &values[i] : nullptr);
            *key_contexts[i]->s = Status::OK();
          } else {
            const ParsedInternalKey parsed_key(lookup_keys[i], global_seqno_,
                                               kTypeValue);
            bool matched = false;
            Status read_status;
            key_contexts[i]->get_context->SaveValue(
                parsed_key, values[i], &matched, &read_status,
                values[i].IsPinned() ? &values[i] : nullptr);
            *key_contexts[i]->s = std::move(read_status);
          }
        }
      }
    } else {
      ExternalTableMultiGetContextImpl context(
          *mget_range, internal_comparator_, global_seqno_);
      if (context.Size() == 0) {
        return;
      }

      std::vector<Status> statuses(context.Size());
      reader_->MultiGet(read_options, prefix_extractor, &context, &statuses);
      context.ApplyStatuses(&statuses);
    }
  }

  Status VerifyChecksum(const ReadOptions& read_options,
                        TableReaderCaller /*caller*/,
                        bool /*meta_blocks_only*/ = false) override {
    return reader_->VerifyChecksum(read_options);
  }

 private:
  const InternalKeyComparator& internal_comparator_;
  std::unique_ptr<ExternalTableReaderBase<Mode>> reader_;
  std::shared_ptr<const TableProperties> table_properties_;
  const SequenceNumber global_seqno_;
};

// Converts RocksDB's internal Add() stream to the representation selected by
// Mode and maintains the table properties expected by the rest of RocksDB.
template <ExternalTableMode Mode>
class ExternalTableBuilderAdapter : public TableBuilder {
 public:
  explicit ExternalTableBuilderAdapter(
      const TableBuilderOptions& topts,
      std::unique_ptr<ExternalTableBuilderBase>&& builder)
      : builder_(std::move(builder)), ioptions_(topts.ioptions) {
    properties_.num_data_blocks = 1;
    properties_.index_size = 0;
    properties_.filter_size = 0;
    properties_.format_version = 0;
    properties_.key_largest_seqno = 0;
    properties_.key_smallest_seqno = std::numeric_limits<uint64_t>::max();
    properties_.column_family_id = topts.column_family_id;
    properties_.column_family_name = topts.column_family_name;
    properties_.db_id = topts.db_id;
    properties_.db_session_id = topts.db_session_id;
    properties_.db_host_id = topts.ioptions.db_host_id;
    if (!ReifyDbHostIdProperty(topts.ioptions.env, &properties_.db_host_id)
             .ok()) {
      ROCKS_LOG_INFO(topts.ioptions.logger,
                     "db_host_id property will not be set");
    }
    properties_.orig_file_number = topts.cur_file_num;
    properties_.comparator_name = topts.ioptions.user_comparator != nullptr
                                      ? topts.ioptions.user_comparator->Name()
                                      : "nullptr";
    properties_.prefix_extractor_name =
        topts.moptions.prefix_extractor != nullptr
            ? topts.moptions.prefix_extractor->AsString()
            : "nullptr";

    for (auto& factory : *topts.internal_tbl_prop_coll_factories) {
      assert(factory);
      std::unique_ptr<InternalTblPropColl> collector{
          factory->CreateInternalTblPropColl(topts.column_family_id,
                                             topts.level_at_creation,
                                             topts.ioptions.num_levels)};
      if (collector) {
        table_properties_collectors_.emplace_back(std::move(collector));
      }
    }
  }

  void Add(const Slice& key, const Slice& value) override {
    if (!status_.ok()) {
      return;
    }

    ParsedEntryInfo entry;
    status_ = ParseEntry(key, ioptions_.user_comparator, &entry);
    if (!status_.ok()) {
      return;
    }
    if (!entry.timestamp.empty()) {
      status_ = Status::NotSupported(
          "External table does not support user-defined timestamps");
      return;
    }
    if constexpr (Mode == ExternalTableMode::kOnlyZeroSeqnoAndPuts) {
      if (entry.sequence != 0 || entry.type != kEntryPut) {
        status_ = Status::NotSupported(
            "External table factory only supports sequence-zero Put entries");
        return;
      }
    }

    ValueType value_type;
    status_ = GetValueType(entry.type, &value_type);
    if (!status_.ok()) {
      return;
    }
    if constexpr (Mode == ExternalTableMode::kFull) {
      builder_->Add(key, value);
    } else {
      builder_->Add(entry.user_key, value);
    }
    status_ = builder_->status();
    if (!status_.ok()) {
      return;
    }

    // Only entries accepted by the external builder contribute to properties
    // and property collectors.
    properties_.key_largest_seqno =
        std::max(properties_.key_largest_seqno, entry.sequence);
    properties_.key_smallest_seqno =
        std::min(properties_.key_smallest_seqno, entry.sequence);
    properties_.num_entries++;
    properties_.raw_key_size += key.size();
    properties_.raw_value_size += value.size();
    if (value_type == kTypeDeletion || value_type == kTypeSingleDeletion) {
      properties_.num_deletions++;
    } else if (value_type == kTypeMerge) {
      properties_.num_merge_operands++;
    }
    NotifyCollectTableCollectorsOnAdd(key, value, /*file_size=*/0,
                                      table_properties_collectors_,
                                      ioptions_.logger);
  }

  Status status() const override {
    if (status_.ok()) {
      return builder_->status();
    } else {
      return status_;
    }
  }

  IOStatus io_status() const override { return status_to_io_status(status()); }

  Status Finish() override {
    status_ = status();
    if (!status_.ok()) {
      builder_->Abandon();
      return status_;
    }

    // Approximate the data size
    properties_.data_size =
        properties_.raw_key_size + properties_.raw_value_size;

    PropertyBlockBuilder property_block_builder;
    property_block_builder.AddTableProperty(properties_);
    UserCollectedProperties more_user_collected_properties;
    NotifyCollectTableCollectorsOnFinish(
        table_properties_collectors_, ioptions_.logger, &property_block_builder,
        more_user_collected_properties, properties_.readable_properties);
    properties_.user_collected_properties.insert(
        more_user_collected_properties.begin(),
        more_user_collected_properties.end());

    Slice prop_block = property_block_builder.Finish();
    // The external implementation may supply TableProperties directly when it
    // does not persist RocksDB's properties block.
    status_ = builder_->PutPropertiesBlock(prop_block);
    properties_block_persisted_ = status_.ok();
    if (status_.ok() || status_.IsNotSupported()) {
      status_ = builder_->Finish();
    }

    return status_;
  }

  void Abandon() override { builder_->Abandon(); }

  uint64_t FileSize() const override { return builder_->FileSize(); }

  uint64_t NumEntries() const override { return properties_.num_entries; }

  TableProperties GetTableProperties() const override {
    if (properties_block_persisted_) {
      return properties_;
    }
    // Overlay entry properties tracked here when no RocksDB properties block
    // was persisted.
    TableProperties properties = builder_->GetTableProperties();
    properties.key_largest_seqno = properties_.key_largest_seqno;
    properties.key_smallest_seqno = properties_.key_smallest_seqno;
    properties.num_deletions = properties_.num_deletions;
    properties.num_merge_operands = properties_.num_merge_operands;
    for (const auto& property : properties_.user_collected_properties) {
      properties.user_collected_properties.insert_or_assign(property.first,
                                                            property.second);
    }
    return properties;
  }

  std::string GetFileChecksum() const override {
    return builder_->GetFileChecksum();
  }

  const char* GetFileChecksumFuncName() const override {
    return builder_->GetFileChecksumFuncName();
  }

 private:
  Status status_;
  std::unique_ptr<ExternalTableBuilderBase> builder_;
  const ImmutableOptions& ioptions_;
  TableProperties properties_;
  std::vector<std::unique_ptr<InternalTblPropColl>>
      table_properties_collectors_;
  bool properties_block_persisted_ = false;
};

struct ExternalTableFactoryAdapterOptions {
  static const char* kName() { return "ExternalTableFactoryAdapterOptions"; }

  std::string config;
};

static const std::unordered_map<std::string, OptionTypeInfo>&
GetExternalTableFactoryAdapterOptionsTypeInfo() {
  static const std::unordered_map<std::string, OptionTypeInfo> type_info = {
      {"external_table_config",
       OptionTypeInfo(offsetof(ExternalTableFactoryAdapterOptions, config),
                      OptionType::kString)
           .SetSerializeFunc([](const ConfigOptions&, const std::string&,
                                const void* addr, std::string* value) {
             const std::string* config = static_cast<const std::string*>(addr);
             *value = "{" + EscapeOptionString(*config) + "}";
             return Status::OK();
           })}};
  return type_info;
}

// Adapts a mode-specific external factory without runtime capability checks.
template <ExternalTableMode Mode>
class ExternalTableFactoryAdapter : public TableFactory {
 public:
  explicit ExternalTableFactoryAdapter(
      std::shared_ptr<ExternalTableFactoryBase<Mode>> inner)
      : inner_(std::move(inner)) {
    RegisterOptions(&options_,
                    &GetExternalTableFactoryAdapterOptionsTypeInfo());
  }

  const char* Name() const override { return inner_->Name(); }

  Status PrepareOptions(const ConfigOptions& config_options) override {
    Status status = TableFactory::PrepareOptions(config_options);
    if (status.ok()) {
      status = inner_->Configure(options_.config);
    }
    return status;
  }

  using TableFactory::NewTableReader;
  Status NewTableReader(
      const ReadOptions& ro, const TableReaderOptions& topts,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      bool /* prefetch_index_and_filter_in_cache */) const override {
    if (topts.ioptions.user_comparator != nullptr &&
        topts.ioptions.user_comparator->timestamp_size() != 0) {
      return Status::NotSupported(
          "External table does not support user-defined timestamps");
    }
    std::unique_ptr<ExternalTableReaderBase<Mode>> reader;
    FileOptions fopts(topts.env_options);
    fopts.io_options.io_activity = ro.io_activity;
    // Files opened by external readers lack MANIFEST checksum metadata.
    fopts.file_checksum_func_name = kNoFileChecksumFuncName;
    ExternalTableOptions ext_topts(
        topts.prefix_extractor, topts.ioptions.user_comparator,
        &topts.internal_comparator, topts.ioptions.fs, fopts, &topts);
    const std::string file_name = file->file_name();
    auto status = inner_->NewTableReader(ro, file_name, ext_topts,
                                         std::move(file), file_size, &reader);
    if (!status.ok()) {
      return status;
    }

    std::shared_ptr<const TableProperties> table_properties;
    status = LoadExternalTableProperties<Mode>(topts.ioptions, reader.get(),
                                               &table_properties);
    if (!status.ok()) {
      return status;
    }

    // TableCache passes file_meta.fd.largest_seqno from the current Version
    // through TableReaderOptions. For an ingested file, that MANIFEST-backed
    // value is the DB-assigned global seqno. Resolve it once for readers and
    // iterators.
    SequenceNumber global_seqno;
    status = GetGlobalSequenceNumber(*table_properties, topts.largest_seqno,
                                     &global_seqno);
    if (!status.ok()) {
      return status;
    }

    table_reader->reset(new ExternalTableReaderAdapter<Mode>(
        topts.internal_comparator, std::move(reader),
        std::move(table_properties), global_seqno));
    return Status::OK();
  }

  using TableFactory::NewTableBuilder;
  TableBuilder* NewTableBuilder(const TableBuilderOptions& topts,
                                WritableFileWriter* file) const override {
    std::unique_ptr<ExternalTableBuilderBase> builder;
    ExternalTableBuilderOptions ext_topts(
        topts.read_options, topts.write_options,
        topts.moptions.prefix_extractor, topts.ioptions.user_comparator,
        topts.column_family_name, topts.reason, topts.ioptions.fs, &topts);
    builder.reset(inner_->NewTableBuilder(ext_topts, file->file_name(), file));
    if (builder) {
      return new ExternalTableBuilderAdapter<Mode>(topts, std::move(builder));
    }
    return nullptr;
  }

  std::unique_ptr<TableFactory> Clone() const override { return nullptr; }

 private:
  std::shared_ptr<ExternalTableFactoryBase<Mode>> inner_;
  ExternalTableFactoryAdapterOptions options_;
};

template <ExternalTableMode Mode>
std::unique_ptr<TableFactory> NewExternalTableFactoryImpl(
    std::shared_ptr<ExternalTableFactoryBase<Mode>> inner_factory) {
  return std::make_unique<ExternalTableFactoryAdapter<Mode>>(
      std::move(inner_factory));
}

}  // namespace

std::unique_ptr<TableFactory> NewExternalTableFactory(
    std::shared_ptr<ExternalTableFactory> inner_factory) {
  return NewExternalTableFactoryImpl(std::move(inner_factory));
}

std::unique_ptr<TableFactory> NewExternalTableFactory(
    std::shared_ptr<FullExternalTableFactory> inner_factory) {
  return NewExternalTableFactoryImpl(std::move(inner_factory));
}

}  // namespace ROCKSDB_NAMESPACE
