//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "memtable/wbwi_memtable.h"

#include "db/memtable.h"

namespace ROCKSDB_NAMESPACE {

const std::unordered_map<WriteType, ValueType>
    WBWIMemTableIterator::WriteTypeToValueTypeMap = {
        {kPutRecord, kTypeValue},
        {kMergeRecord, kTypeMerge},
        {kDeleteRecord, kTypeDeletion},
        {kSingleDeleteRecord, kTypeSingleDeletion},
        {kDeleteRangeRecord, kTypeRangeDeletion},
        {kPutEntityRecord, kTypeWideColumnEntity},
        // Only the above record types are added to WBWI.
        // kLogDataRecord, kXIDRecord, kUnknownRecord
};

InternalIterator* WBWIMemTable::NewIterator(
    const ReadOptions&, UnownedPtr<const SeqnoToTimeMapping>, Arena* arena,
    const SliceTransform* /* prefix_extractor */, bool for_flush) {
  // Ingested WBWIMemTable should have an assigned seqno
  assert(assigned_seqno_.upper_bound != kMaxSequenceNumber);
  assert(assigned_seqno_.lower_bound != kMaxSequenceNumber);
  assert(arena);
  auto mem = arena->AllocateAligned(sizeof(WBWIMemTableIterator));
  return new (mem) WBWIMemTableIterator(
      std::unique_ptr<WBWIIterator>(wbwi_->NewIterator(cf_id_)),
      assigned_seqno_, comparator_, for_flush);
}

inline InternalIterator* WBWIMemTable::NewIterator() const {
  assert(assigned_seqno_.upper_bound != kMaxSequenceNumber);
  assert(assigned_seqno_.lower_bound != kMaxSequenceNumber);
  return new WBWIMemTableIterator(
      std::unique_ptr<WBWIIterator>(wbwi_->NewIterator(cf_id_)),
      assigned_seqno_, comparator_, /*for_flush=*/false);
}

bool WBWIMemTable::Get(const LookupKey& key, std::string* value,
                       PinnableWideColumns* columns, std::string* timestamp,
                       Status* s, MergeContext* merge_context,
                       SequenceNumber* max_covering_tombstone_seq,
                       SequenceNumber* out_seq, const ReadOptions&,
                       bool immutable_memtable, ReadCallback* callback,
                       bool* is_blob_index, bool do_merge) {
  (void)immutable_memtable;
  (void)timestamp;
  (void)columns;
  assert(immutable_memtable);
  assert(!timestamp);  // TODO: support UDT
  assert(!columns);    // TODO: support WideColumn
  assert(assigned_seqno_.upper_bound != kMaxSequenceNumber);
  assert(assigned_seqno_.lower_bound != kMaxSequenceNumber);
  // WBWI does not support DeleteRange yet.
  assert(!wbwi_->GetWriteBatch()->HasDeleteRange());

  [[maybe_unused]] SequenceNumber read_seq =
      GetInternalKeySeqno(key.internal_key());
  std::unique_ptr<InternalIterator> iter{NewIterator()};
  iter->Seek(key.internal_key());
  const Slice lookup_user_key = key.user_key();

  while (iter->Valid() && comparator_->EqualWithoutTimestamp(
                              ExtractUserKey(iter->key()), lookup_user_key)) {
    uint64_t tag = ExtractInternalKeyFooter(iter->key());
    ValueType type;
    SequenceNumber seq;
    UnPackSequenceAndType(tag, &seq, &type);
    // Unsupported operations.
    assert(type != kTypeBlobIndex);
    assert(type != kTypeWideColumnEntity);
    assert(type != kTypeValuePreferredSeqno);
    assert(type != kTypeDeletionWithTimestamp);
    assert(type != kTypeMerge);
    if (!callback || callback->IsVisible(seq)) {
      if (*out_seq == kMaxSequenceNumber) {
        *out_seq = std::max(seq, *max_covering_tombstone_seq);
      }
      if (*max_covering_tombstone_seq > seq) {
        type = kTypeRangeDeletion;
      }
      switch (type) {
        case kTypeValue: {
          HandleTypeValue(lookup_user_key, iter->value(), iter->IsValuePinned(),
                          do_merge, s->IsMergeInProgress(), merge_context,
                          moptions_.merge_operator, clock_,
                          moptions_.statistics, moptions_.info_log, s, value,
                          columns, is_blob_index);
          assert(seq <= read_seq);
          return /*found_final_value=*/true;
        }
        case kTypeDeletion:
        case kTypeSingleDeletion:
        case kTypeRangeDeletion: {
          HandleTypeDeletion(lookup_user_key, s->IsMergeInProgress(),
                             merge_context, moptions_.merge_operator, clock_,
                             moptions_.statistics, moptions_.info_log, s, value,
                             columns);
          assert(seq <= read_seq);
          return /*found_final_value=*/true;
        }
        default: {
          std::string msg("Unrecognized or unsupported value type: " +
                          std::to_string(static_cast<int>(type)) + ". ");
          msg.append("User key: " +
                     ExtractUserKey(iter->key()).ToString(/*hex=*/true) + ". ");
          msg.append("seq: " + std::to_string(seq) + ".");
          *s = Status::Corruption(msg.c_str());
          return /*found_final_value=*/true;
        }
      }
    }
    // Current key not visible or we read a merge key
    assert(s->IsMergeInProgress() || (callback && !callback->IsVisible(seq)));
    iter->Next();
  }
  if (!iter->status().ok() &&
      (s->ok() || s->IsMergeInProgress() || s->IsNotFound())) {
    *s = iter->status();
    // stop further look up
    return true;
  }
  return /*found_final_value=*/false;
}

void WBWIMemTable::MultiGet(const ReadOptions& read_options,
                            MultiGetRange* range, ReadCallback* callback,
                            bool immutable_memtable) {
  (void)immutable_memtable;
  // Should only be used as immutable memtable.
  assert(immutable_memtable);
  // TODO: reuse the InternalIterator created in Get().
  for (auto iter = range->begin(); iter != range->end(); ++iter) {
    SequenceNumber dummy_seq = 0;
    bool found_final_value =
        Get(*iter->lkey, iter->value ? iter->value->GetSelf() : nullptr,
            iter->columns, iter->timestamp, iter->s, &(iter->merge_context),
            &(iter->max_covering_tombstone_seq), &dummy_seq, read_options, true,
            callback, nullptr, true);
    if (found_final_value) {
      if (iter->s->ok() || iter->s->IsNotFound()) {
        if (iter->value) {
          iter->value->PinSelf();
          range->AddValueSize(iter->value->size());
        } else {
          assert(iter->columns);
          range->AddValueSize(iter->columns->serialized_size());
        }
      }
      range->MarkKeyDone(iter);
      if (range->GetValueSize() > read_options.value_size_soft_limit) {
        // Set all remaining keys in range to Abort
        for (auto range_iter = range->begin(); range_iter != range->end();
             ++range_iter) {
          range->MarkKeyDone(range_iter);
          *(range_iter->s) = Status::Aborted();
        }
        break;
      }
    }
  }
}
}  // namespace ROCKSDB_NAMESPACE
