//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/merge_helper.h"
#include "db/wide/wide_column_serialization.h"
#include "db/wide/wide_columns_helper.h"
#include "options/cf_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
BaseDeltaIterator::BaseDeltaIterator(ColumnFamilyHandle* column_family,
                                     Iterator* base_iterator,
                                     WBWIIteratorImpl* delta_iterator,
                                     const Comparator* comparator)
    : forward_(true),
      current_at_base_(true),
      equal_keys_(false),
      status_(Status::OK()),
      column_family_(column_family),
      base_iterator_(base_iterator),
      delta_iterator_(delta_iterator),
      comparator_(comparator) {
  assert(base_iterator_);
  assert(delta_iterator_);
  assert(comparator_);
}

bool BaseDeltaIterator::Valid() const {
  return status_.ok() ? (current_at_base_ ? BaseValid() : DeltaValid()) : false;
}

void BaseDeltaIterator::SeekToFirst() {
  forward_ = true;
  base_iterator_->SeekToFirst();
  delta_iterator_->SeekToFirst();
  UpdateCurrent();
}

void BaseDeltaIterator::SeekToLast() {
  forward_ = false;
  base_iterator_->SeekToLast();
  delta_iterator_->SeekToLast();
  UpdateCurrent();
}

void BaseDeltaIterator::Seek(const Slice& k) {
  forward_ = true;
  base_iterator_->Seek(k);
  delta_iterator_->Seek(k);
  UpdateCurrent();
}

void BaseDeltaIterator::SeekForPrev(const Slice& k) {
  forward_ = false;
  base_iterator_->SeekForPrev(k);
  delta_iterator_->SeekForPrev(k);
  UpdateCurrent();
}

void BaseDeltaIterator::Next() {
  if (!Valid()) {
    status_ = Status::NotSupported("Next() on invalid iterator");
    return;
  }

  if (!forward_) {
    // Need to change direction
    // if our direction was backward and we're not equal, we have two states:
    // * both iterators are valid: we're already in a good state (current
    // shows to smaller)
    // * only one iterator is valid: we need to advance that iterator
    forward_ = true;
    equal_keys_ = false;
    if (!BaseValid()) {
      assert(DeltaValid());
      base_iterator_->SeekToFirst();
    } else if (!DeltaValid()) {
      delta_iterator_->SeekToFirst();
    } else if (current_at_base_) {
      // Change delta from larger than base to smaller
      AdvanceDelta();
    } else {
      // Change base from larger than delta to smaller
      AdvanceBase();
    }
    if (DeltaValid() && BaseValid()) {
      if (0 == comparator_->CompareWithoutTimestamp(
                   delta_iterator_->Entry().key, /*a_has_ts=*/false,
                   base_iterator_->key(), /*b_has_ts=*/false)) {
        equal_keys_ = true;
      }
    }
  }
  Advance();
}

void BaseDeltaIterator::Prev() {
  if (!Valid()) {
    status_ = Status::NotSupported("Prev() on invalid iterator");
    return;
  }

  if (forward_) {
    // Need to change direction
    // if our direction was backward and we're not equal, we have two states:
    // * both iterators are valid: we're already in a good state (current
    // shows to smaller)
    // * only one iterator is valid: we need to advance that iterator
    forward_ = false;
    equal_keys_ = false;
    if (!BaseValid()) {
      assert(DeltaValid());
      base_iterator_->SeekToLast();
    } else if (!DeltaValid()) {
      delta_iterator_->SeekToLast();
    } else if (current_at_base_) {
      // Change delta from less advanced than base to more advanced
      AdvanceDelta();
    } else {
      // Change base from less advanced than delta to more advanced
      AdvanceBase();
    }
    if (DeltaValid() && BaseValid()) {
      if (0 == comparator_->CompareWithoutTimestamp(
                   delta_iterator_->Entry().key, /*a_has_ts=*/false,
                   base_iterator_->key(), /*b_has_ts=*/false)) {
        equal_keys_ = true;
      }
    }
  }

  Advance();
}

Slice BaseDeltaIterator::key() const {
  return current_at_base_ ? base_iterator_->key()
                          : delta_iterator_->Entry().key;
}

Slice BaseDeltaIterator::timestamp() const {
  return current_at_base_ ? base_iterator_->timestamp() : Slice();
}

Status BaseDeltaIterator::status() const {
  if (!status_.ok()) {
    return status_;
  }
  if (!base_iterator_->status().ok()) {
    return base_iterator_->status();
  }
  return delta_iterator_->status();
}

void BaseDeltaIterator::Invalidate(Status s) { status_ = s; }

void BaseDeltaIterator::AssertInvariants() {
#ifndef NDEBUG
  bool not_ok = false;
  if (!base_iterator_->status().ok()) {
    assert(!base_iterator_->Valid());
    not_ok = true;
  }
  if (!delta_iterator_->status().ok()) {
    assert(!delta_iterator_->Valid());
    not_ok = true;
  }
  if (not_ok) {
    assert(!Valid());
    assert(!status().ok());
    return;
  }

  if (!Valid()) {
    return;
  }
  if (!BaseValid()) {
    assert(!current_at_base_ && delta_iterator_->Valid());
    return;
  }
  if (!DeltaValid()) {
    assert(current_at_base_ && base_iterator_->Valid());
    return;
  }
  // we don't support those yet
  assert(delta_iterator_->Entry().type != kMergeRecord &&
         delta_iterator_->Entry().type != kLogDataRecord);
  int compare = comparator_->CompareWithoutTimestamp(
      delta_iterator_->Entry().key, /*a_has_ts=*/false, base_iterator_->key(),
      /*b_has_ts=*/false);
  if (forward_) {
    // current_at_base -> compare < 0
    assert(!current_at_base_ || compare < 0);
    // !current_at_base -> compare <= 0
    assert(current_at_base_ && compare >= 0);
  } else {
    // current_at_base -> compare > 0
    assert(!current_at_base_ || compare > 0);
    // !current_at_base -> compare <= 0
    assert(current_at_base_ && compare <= 0);
  }
  // equal_keys_ <=> compare == 0
  assert((equal_keys_ || compare != 0) && (!equal_keys_ || compare == 0));
#endif
}

void BaseDeltaIterator::Advance() {
  if (equal_keys_) {
    assert(BaseValid() && DeltaValid());
    AdvanceBase();
    AdvanceDelta();
  } else {
    if (current_at_base_) {
      assert(BaseValid());
      AdvanceBase();
    } else {
      assert(DeltaValid());
      AdvanceDelta();
    }
  }
  UpdateCurrent();
}

void BaseDeltaIterator::AdvanceDelta() {
  if (forward_) {
    delta_iterator_->NextKey();
  } else {
    delta_iterator_->PrevKey();
  }
}
void BaseDeltaIterator::AdvanceBase() {
  if (forward_) {
    base_iterator_->Next();
  } else {
    base_iterator_->Prev();
  }
}

bool BaseDeltaIterator::BaseValid() const { return base_iterator_->Valid(); }
bool BaseDeltaIterator::DeltaValid() const { return delta_iterator_->Valid(); }

void BaseDeltaIterator::ResetValueAndColumns() {
  value_.clear();
  columns_.clear();
}

void BaseDeltaIterator::SetValueAndColumnsFromBase() {
  assert(current_at_base_);
  assert(BaseValid());
  assert(value_.empty());
  assert(columns_.empty());

  value_ = base_iterator_->value();
  columns_ = base_iterator_->columns();
}

void BaseDeltaIterator::SetValueAndColumnsFromDelta() {
  assert(!current_at_base_);
  assert(DeltaValid());
  assert(value_.empty());
  assert(columns_.empty());

  WriteEntry delta_entry = delta_iterator_->Entry();

  if (merge_context_.GetNumOperands() == 0) {
    if (delta_entry.type == kPutRecord) {
      value_ = delta_entry.value;
      columns_.emplace_back(kDefaultWideColumnName, value_);
    } else if (delta_entry.type == kPutEntityRecord) {
      Slice value_copy(delta_entry.value);

      status_ = WideColumnSerialization::Deserialize(value_copy, columns_);
      if (!status_.ok()) {
        return;
      }

      if (WideColumnsHelper::HasDefaultColumn(columns_)) {
        value_ = WideColumnsHelper::GetDefaultColumn(columns_);
      }
    }

    return;
  }

  ValueType result_type = kTypeValue;

  if (delta_entry.type == kDeleteRecord ||
      delta_entry.type == kSingleDeleteRecord) {
    status_ = WriteBatchWithIndexInternal::MergeKeyWithNoBaseValue(
        column_family_, delta_entry.key, merge_context_, &merge_result_,
        /* result_operand */ nullptr, &result_type);
  } else if (delta_entry.type == kPutRecord) {
    status_ = WriteBatchWithIndexInternal::MergeKeyWithBaseValue(
        column_family_, delta_entry.key, MergeHelper::kPlainBaseValue,
        delta_entry.value, merge_context_, &merge_result_,
        /* result_operand */ nullptr, &result_type);
  } else if (delta_entry.type == kPutEntityRecord) {
    status_ = WriteBatchWithIndexInternal::MergeKeyWithBaseValue(
        column_family_, delta_entry.key, MergeHelper::kWideBaseValue,
        delta_entry.value, merge_context_, &merge_result_,
        /* result_operand */ nullptr, &result_type);
  } else if (delta_entry.type == kMergeRecord) {
    if (equal_keys_) {
      if (WideColumnsHelper::HasDefaultColumnOnly(base_iterator_->columns())) {
        status_ = WriteBatchWithIndexInternal::MergeKeyWithBaseValue(
            column_family_, delta_entry.key, MergeHelper::kPlainBaseValue,
            base_iterator_->value(), merge_context_, &merge_result_,
            /* result_operand */ nullptr, &result_type);
      } else {
        status_ = WriteBatchWithIndexInternal::MergeKeyWithBaseValue(
            column_family_, delta_entry.key, MergeHelper::kWideBaseValue,
            base_iterator_->columns(), merge_context_, &merge_result_,
            /* result_operand */ nullptr, &result_type);
      }
    } else {
      status_ = WriteBatchWithIndexInternal::MergeKeyWithNoBaseValue(
          column_family_, delta_entry.key, merge_context_, &merge_result_,
          /* result_operand */ nullptr, &result_type);
    }
  } else {
    status_ = Status::NotSupported("Unsupported entry type for merge");
  }

  if (!status_.ok()) {
    return;
  }

  if (result_type == kTypeWideColumnEntity) {
    Slice entity(merge_result_);

    status_ = WideColumnSerialization::Deserialize(entity, columns_);
    if (!status_.ok()) {
      return;
    }

    if (WideColumnsHelper::HasDefaultColumn(columns_)) {
      value_ = WideColumnsHelper::GetDefaultColumn(columns_);
    }

    return;
  }

  assert(result_type == kTypeValue);

  value_ = merge_result_;
  columns_.emplace_back(kDefaultWideColumnName, value_);
}

void BaseDeltaIterator::UpdateCurrent() {
// Suppress false positive clang analyzer warnings.
#ifndef __clang_analyzer__
  status_ = Status::OK();
  ResetValueAndColumns();

  while (true) {
    auto delta_result = WBWIIteratorImpl::kNotFound;
    WriteEntry delta_entry;
    if (DeltaValid()) {
      assert(delta_iterator_->status().ok());
      delta_result = delta_iterator_->FindLatestUpdate(&merge_context_);
      delta_entry = delta_iterator_->Entry();
    } else if (!delta_iterator_->status().ok()) {
      // Expose the error status and stop.
      current_at_base_ = false;
      return;
    }
    equal_keys_ = false;
    if (!BaseValid()) {
      if (!base_iterator_->status().ok()) {
        // Expose the error status and stop.
        current_at_base_ = true;
        return;
      }

      // Base has finished.
      if (!DeltaValid()) {
        // Finished
        return;
      }
      if (delta_result == WBWIIteratorImpl::kDeleted &&
          merge_context_.GetNumOperands() == 0) {
        AdvanceDelta();
      } else {
        current_at_base_ = false;
        SetValueAndColumnsFromDelta();
        return;
      }
    } else if (!DeltaValid()) {
      // Delta has finished.
      current_at_base_ = true;
      SetValueAndColumnsFromBase();
      return;
    } else {
      int compare =
          (forward_ ? 1 : -1) * comparator_->CompareWithoutTimestamp(
                                    delta_entry.key, /*a_has_ts=*/false,
                                    base_iterator_->key(), /*b_has_ts=*/false);
      if (compare <= 0) {  // delta bigger or equal
        if (compare == 0) {
          equal_keys_ = true;
        }
        if (delta_result != WBWIIteratorImpl::kDeleted ||
            merge_context_.GetNumOperands() > 0) {
          current_at_base_ = false;
          SetValueAndColumnsFromDelta();
          return;
        }
        // Delta is less advanced and is delete.
        AdvanceDelta();
        if (equal_keys_) {
          AdvanceBase();
        }
      } else {
        current_at_base_ = true;
        SetValueAndColumnsFromBase();
        return;
      }
    }
  }

  AssertInvariants();
#endif  // __clang_analyzer__
}

void WBWIIteratorImpl::AdvanceKey(bool forward) {
  if (Valid()) {
    Slice key = Entry().key;
    do {
      if (forward) {
        Next();
      } else {
        Prev();
      }
    } while (MatchesKey(column_family_id_, key));
  }
}

void WBWIIteratorImpl::NextKey() { AdvanceKey(true); }

void WBWIIteratorImpl::PrevKey() {
  AdvanceKey(false);  // Move to the tail of the previous key
  if (Valid()) {
    AdvanceKey(false);  // Move back another key.  Now we are at the start of
                        // the previous key
    if (Valid()) {      // Still a valid
      Next();           // Move forward one onto this key
    } else {
      SeekToFirst();  // Not valid, move to the start
    }
  }
}

WBWIIteratorImpl::Result WBWIIteratorImpl::FindLatestUpdate(
    MergeContext* merge_context) {
  if (Valid()) {
    Slice key = Entry().key;
    return FindLatestUpdate(key, merge_context);
  } else {
    merge_context->Clear();  // Clear any entries in the MergeContext
    return WBWIIteratorImpl::kNotFound;
  }
}

WBWIIteratorImpl::Result WBWIIteratorImpl::FindLatestUpdate(
    const Slice& key, MergeContext* merge_context) {
  Result result = WBWIIteratorImpl::kNotFound;
  merge_context->Clear();  // Clear any entries in the MergeContext
  // TODO(agiardullo): consider adding support for reverse iteration
  if (!Valid()) {
    return result;
  } else if (comparator_->CompareKey(column_family_id_, Entry().key, key) !=
             0) {
    return result;
  } else {
    // We want to iterate in the reverse order that the writes were added to the
    // batch.  Since we don't have a reverse iterator, we must seek past the
    // end. We do this by seeking to the next key, and then back one step
    NextKey();
    if (Valid()) {
      Prev();
    } else {
      SeekToLast();
    }

    // We are at the end of the iterator for this key.  Search backwards for the
    // last Put or Delete, accumulating merges along the way.
    while (Valid()) {
      const WriteEntry entry = Entry();
      if (comparator_->CompareKey(column_family_id_, entry.key, key) != 0) {
        break;  // Unexpected error or we've reached a different next key
      }

      switch (entry.type) {
        case kPutRecord:
          return WBWIIteratorImpl::kFound;
        case kDeleteRecord:
          return WBWIIteratorImpl::kDeleted;
        case kSingleDeleteRecord:
          return WBWIIteratorImpl::kDeleted;
        case kMergeRecord:
          result = WBWIIteratorImpl::kMergeInProgress;
          merge_context->PushOperand(entry.value);
          break;
        case kLogDataRecord:
          break;  // ignore
        case kXIDRecord:
          break;  // ignore
        case kPutEntityRecord:
          return WBWIIteratorImpl::kFound;
        default:
          return WBWIIteratorImpl::kError;
      }  // end switch statement
      Prev();
    }  // End while Valid()
    // At this point, we have been through the whole list and found no Puts or
    // Deletes. The iterator points to the previous key.  Move the iterator back
    // onto this one.
    if (Valid()) {
      Next();
    } else {
      SeekToFirst();
    }
  }
  return result;
}

Status ReadableWriteBatch::GetEntryFromDataOffset(size_t data_offset,
                                                  WriteType* type, Slice* key,
                                                  Slice* value, Slice* blob,
                                                  Slice* xid) const {
  if (type == nullptr || key == nullptr || value == nullptr ||
      blob == nullptr || xid == nullptr) {
    return Status::InvalidArgument("Output parameters cannot be null");
  }

  if (data_offset == GetDataSize()) {
    // reached end of batch.
    return Status::NotFound();
  }

  if (data_offset > GetDataSize()) {
    return Status::InvalidArgument("data offset exceed write batch size");
  }
  Slice input = Slice(rep_.data() + data_offset, rep_.size() - data_offset);
  char tag;
  uint32_t column_family = 0;  // default
  uint64_t unix_write_time = 0;
  Status s = ReadRecordFromWriteBatch(&input, &tag, &column_family, key, value,
                                      blob, xid, &unix_write_time);
  if (!s.ok()) {
    return s;
  }

  switch (tag) {
    case kTypeColumnFamilyValue:
    case kTypeValue:
      *type = kPutRecord;
      break;
    case kTypeColumnFamilyDeletion:
    case kTypeDeletion:
      *type = kDeleteRecord;
      break;
    case kTypeColumnFamilySingleDeletion:
    case kTypeSingleDeletion:
      *type = kSingleDeleteRecord;
      break;
    case kTypeColumnFamilyRangeDeletion:
    case kTypeRangeDeletion:
      *type = kDeleteRangeRecord;
      break;
    case kTypeColumnFamilyMerge:
    case kTypeMerge:
      *type = kMergeRecord;
      break;
    case kTypeLogData:
      *type = kLogDataRecord;
      break;
    case kTypeNoop:
    case kTypeBeginPrepareXID:
    case kTypeBeginPersistedPrepareXID:
    case kTypeBeginUnprepareXID:
    case kTypeEndPrepareXID:
    case kTypeCommitXID:
    case kTypeRollbackXID:
      *type = kXIDRecord;
      break;
    case kTypeColumnFamilyWideColumnEntity:
    case kTypeWideColumnEntity: {
      *type = kPutEntityRecord;
      break;
    }
    case kTypeColumnFamilyValuePreferredSeqno:
    case kTypeValuePreferredSeqno:
      // TimedPut is not supported in Transaction APIs.
      return Status::Corruption("unexpected WriteBatch tag ",
                                std::to_string(static_cast<unsigned int>(tag)));
    default:
      return Status::Corruption("unknown WriteBatch tag ",
                                std::to_string(static_cast<unsigned int>(tag)));
  }
  return Status::OK();
}

// If both of `entry1` and `entry2` point to real entry in write batch, we
// compare the entries as following:
// 1. first compare the column family, the one with larger CF will be larger;
// 2. Inside the same CF, we first decode the entry to find the key of the entry
//    and the entry with larger key will be larger;
// 3. If two entries are of the same CF and key, the one with larger offset
//    will be larger.
// Some times either `entry1` or `entry2` is dummy entry, which is actually
// a search key. In this case, in step 2, we don't go ahead and decode the
// entry but use the value in WriteBatchIndexEntry::search_key.
// One special case is WriteBatchIndexEntry::key_size is kFlagMinInCf.
// This indicate that we are going to seek to the first of the column family.
// Once we see this, this entry will be smaller than all the real entries of
// the column family.
int WriteBatchEntryComparator::operator()(
    const WriteBatchIndexEntry* entry1,
    const WriteBatchIndexEntry* entry2) const {
  if (entry1->column_family > entry2->column_family) {
    return 1;
  } else if (entry1->column_family < entry2->column_family) {
    return -1;
  }

  // Deal with special case of seeking to the beginning of a column family
  if (entry1->is_min_in_cf()) {
    return -1;
  } else if (entry2->is_min_in_cf()) {
    return 1;
  }

  Slice key1, key2;
  if (entry1->search_key == nullptr) {
    key1 = Slice(write_batch_->Data().data() + entry1->key_offset,
                 entry1->key_size);
  } else {
    key1 = *(entry1->search_key);
  }
  if (entry2->search_key == nullptr) {
    key2 = Slice(write_batch_->Data().data() + entry2->key_offset,
                 entry2->key_size);
  } else {
    key2 = *(entry2->search_key);
  }

  int cmp = CompareKey(entry1->column_family, key1, key2);
  if (cmp != 0) {
    return cmp;
  } else if (entry1->offset > entry2->offset) {
    return 1;
  } else if (entry1->offset < entry2->offset) {
    return -1;
  }
  return 0;
}

int WriteBatchEntryComparator::CompareKey(uint32_t column_family,
                                          const Slice& key1,
                                          const Slice& key2) const {
  if (column_family < cf_comparators_.size() &&
      cf_comparators_[column_family] != nullptr) {
    return cf_comparators_[column_family]->CompareWithoutTimestamp(
        key1, /*a_has_ts=*/false, key2, /*b_has_ts=*/false);
  } else {
    return default_comparator_->CompareWithoutTimestamp(
        key1, /*a_has_ts=*/false, key2, /*b_has_ts=*/false);
  }
}

const Comparator* WriteBatchEntryComparator::GetComparator(
    const ColumnFamilyHandle* column_family) const {
  return column_family ? column_family->GetComparator() : default_comparator_;
}

const Comparator* WriteBatchEntryComparator::GetComparator(
    uint32_t column_family) const {
  if (column_family < cf_comparators_.size() &&
      cf_comparators_[column_family]) {
    return cf_comparators_[column_family];
  }
  return default_comparator_;
}

WriteEntry WBWIIteratorImpl::Entry() const {
  WriteEntry ret;
  Slice blob, xid;
  const WriteBatchIndexEntry* iter_entry = skip_list_iter_.key();
  // this is guaranteed with Valid()
  assert(iter_entry != nullptr &&
         iter_entry->column_family == column_family_id_);
  auto s = write_batch_->GetEntryFromDataOffset(
      iter_entry->offset, &ret.type, &ret.key, &ret.value, &blob, &xid);
  assert(s.ok());
  assert(ret.type == kPutRecord || ret.type == kPutEntityRecord ||
         ret.type == kDeleteRecord || ret.type == kSingleDeleteRecord ||
         ret.type == kDeleteRangeRecord || ret.type == kMergeRecord);
  // Make sure entry.key does not include user-defined timestamp.
  const Comparator* const ucmp = comparator_->GetComparator(column_family_id_);
  size_t ts_sz = ucmp->timestamp_size();
  if (ts_sz > 0) {
    ret.key = StripTimestampFromUserKey(ret.key, ts_sz);
  }
  return ret;
}

bool WBWIIteratorImpl::MatchesKey(uint32_t cf_id, const Slice& key) {
  if (Valid()) {
    return comparator_->CompareKey(cf_id, key, Entry().key) == 0;
  } else {
    return false;
  }
}

Status WriteBatchWithIndexInternal::CheckAndGetImmutableOptions(
    ColumnFamilyHandle* column_family, const ImmutableOptions** ioptions) {
  assert(ioptions);
  assert(!*ioptions);

  if (!column_family) {
    return Status::InvalidArgument("Must provide a column family");
  }

  const auto& iopts = GetImmutableOptions(column_family);

  const auto* merge_operator = iopts.merge_operator.get();
  if (!merge_operator) {
    return Status::InvalidArgument(
        "Merge operator must be set for column family");
  }

  *ioptions = &iopts;

  return Status::OK();
}

template <typename Traits>
WBWIIteratorImpl::Result WriteBatchWithIndexInternal::GetFromBatchImpl(
    WriteBatchWithIndex* batch, ColumnFamilyHandle* column_family,
    const Slice& key, MergeContext* context,
    typename Traits::OutputType* output, Status* s) {
  assert(batch);
  assert(context);
  assert(output);
  assert(s);

  std::unique_ptr<WBWIIteratorImpl> iter(
      static_cast_with_check<WBWIIteratorImpl>(
          batch->NewIterator(column_family)));

  iter->Seek(key);
  auto result = iter->FindLatestUpdate(key, context);

  if (result == WBWIIteratorImpl::kError) {
    Traits::ClearOutput(output);
    *s = Status::Corruption("Unexpected entry in WriteBatchWithIndex:",
                            std::to_string(iter->Entry().type));
    return result;
  }

  if (result == WBWIIteratorImpl::kNotFound) {
    Traits::ClearOutput(output);
    *s = Status::OK();
    return result;
  }

  auto resolve_merge_outputs = [](auto out) {
    std::string* output_value = nullptr;
    PinnableWideColumns* output_entity = nullptr;

    if constexpr (std::is_same_v<typename Traits::OutputType, std::string>) {
      output_value = out;
    } else {
      static_assert(
          std::is_same_v<typename Traits::OutputType, PinnableWideColumns>,
          "unexpected type");
      output_entity = out;
    }

    return std::pair<std::string*, PinnableWideColumns*>(output_value,
                                                         output_entity);
  };

  if (result == WBWIIteratorImpl::Result::kFound) {  // Put/PutEntity
    WriteEntry entry = iter->Entry();

    if (context->GetNumOperands() > 0) {
      auto [output_value, output_entity] = resolve_merge_outputs(output);

      if (entry.type == kPutRecord) {
        *s = MergeKeyWithBaseValue(column_family, key,
                                   MergeHelper::kPlainBaseValue, entry.value,
                                   *context, output_value, output_entity);
      } else {
        assert(entry.type == kPutEntityRecord);

        *s = MergeKeyWithBaseValue(column_family, key,
                                   MergeHelper::kWideBaseValue, entry.value,
                                   *context, output_value, output_entity);
      }
    } else {
      if (entry.type == kPutRecord) {
        *s = Traits::SetPlainValue(entry.value, output);
      } else {
        assert(entry.type == kPutEntityRecord);
        *s = Traits::SetWideColumnValue(entry.value, output);
      }
    }

    if (!s->ok()) {
      Traits::ClearOutput(output);
      result = WBWIIteratorImpl::Result::kError;
    }

    return result;
  }

  if (result == WBWIIteratorImpl::kDeleted) {
    if (context->GetNumOperands() > 0) {
      auto [output_value, output_entity] = resolve_merge_outputs(output);

      *s = MergeKeyWithNoBaseValue(column_family, key, *context, output_value,
                                   output_entity);
      if (s->ok()) {
        result = WBWIIteratorImpl::Result::kFound;
      } else {
        Traits::ClearOutput(output);
        result = WBWIIteratorImpl::Result::kError;
      }
    }

    return result;
  }

  assert(result == WBWIIteratorImpl::Result::kMergeInProgress);

  Traits::ClearOutput(output);
  *s = Status::OK();
  return result;
}

WBWIIteratorImpl::Result WriteBatchWithIndexInternal::GetFromBatch(
    WriteBatchWithIndex* batch, ColumnFamilyHandle* column_family,
    const Slice& key, MergeContext* context, std::string* value, Status* s) {
  struct Traits {
    using OutputType = std::string;

    static void ClearOutput(OutputType* output) {
      assert(output);
      output->clear();
    }

    static Status SetPlainValue(const Slice& value, OutputType* output) {
      assert(output);
      output->assign(value.data(), value.size());

      return Status::OK();
    }

    static Status SetWideColumnValue(const Slice& entity, OutputType* output) {
      assert(output);

      Slice entity_copy = entity;
      Slice value_of_default;
      const Status s = WideColumnSerialization::GetValueOfDefaultColumn(
          entity_copy, value_of_default);
      if (!s.ok()) {
        ClearOutput(output);
        return s;
      }

      output->assign(value_of_default.data(), value_of_default.size());
      return Status::OK();
    }
  };

  return GetFromBatchImpl<Traits>(batch, column_family, key, context, value, s);
}

WBWIIteratorImpl::Result WriteBatchWithIndexInternal::GetEntityFromBatch(
    WriteBatchWithIndex* batch, ColumnFamilyHandle* column_family,
    const Slice& key, MergeContext* context, PinnableWideColumns* columns,
    Status* s) {
  struct Traits {
    using OutputType = PinnableWideColumns;

    static void ClearOutput(OutputType* output) {
      assert(output);
      output->Reset();
    }

    static Status SetPlainValue(const Slice& value, OutputType* output) {
      assert(output);
      output->SetPlainValue(value);

      return Status::OK();
    }

    static Status SetWideColumnValue(const Slice& entity, OutputType* output) {
      assert(output);

      const Status s = output->SetWideColumnValue(entity);
      if (!s.ok()) {
        ClearOutput(output);
        return s;
      }

      return Status::OK();
    }
  };

  return GetFromBatchImpl<Traits>(batch, column_family, key, context, columns,
                                  s);
}

}  // namespace ROCKSDB_NAMESPACE
