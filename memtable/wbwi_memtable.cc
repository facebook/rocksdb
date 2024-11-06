//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "wbwi_memtable.h"

#include "db/merge_helper.h"

namespace ROCKSDB_NAMESPACE {

bool WBWIMemTable::Get(const LookupKey& key, std::string* value,
                       PinnableWideColumns* columns, std::string* timestamp,
                       Status* s, MergeContext* merge_context,
                       SequenceNumber* max_covering_tombstone_seq,
                       SequenceNumber* out_seq, const ReadOptions&,
                       bool immutable_memtable, ReadCallback* callback,
                       bool* is_blob_index, bool do_merge) {
  assert(immutable_memtable);
  assert(!timestamp);  // TODO: support UDT
  assert(!columns);    // TODO: support WideColumn
  assert(global_seqno_ != kMaxSequenceNumber);
  // WBWI does not support DeleteRange yet.
  assert(!wbwi_->GetWriteBatch()->HasDeleteRange());

  SequenceNumber read_seq = GetInternalKeySeqno(key.internal_key());
  bool found_final_result = false;
  std::unique_ptr<InternalIterator> iter{NewIterator()};
  iter->Seek(key.internal_key());
  std::vector<WriteEntry> entries;
  const Slice lookup_user_key = key.user_key();

  // Note: the read logic here should be the same or very similar to read path
  // in memtable.
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
          found_final_result = true;
          *s = Status::OK();

          if (!do_merge) {
            merge_context->PushOperand(
                iter->value(), /*operand_pinned=*/iter->IsValuePinned());
          } else if (s->IsMergeInProgress()) {
            assert(do_merge);
            if (value || columns) {
              *s = MergeHelper::TimedFullMerge(
                  moptions_.merge_operator, lookup_user_key,
                  MergeHelper::kPlainBaseValue, iter->value(),
                  merge_context->GetOperands(), moptions_.info_log,
                  moptions_.statistics, clock_,
                  /* update_num_ops_stats */ true,
                  /* op_failure_scope */ nullptr, value, columns);
            }
          } else if (value) {
            value->assign(iter->value().data(), iter->value().size());
          } else if (columns) {
            columns->SetPlainValue(iter->value());
          }

          if (is_blob_index) {
            *is_blob_index = false;
          }

          assert(seq <= read_seq);
          return found_final_result;
        }
        case kTypeDeletion:
        case kTypeSingleDeletion:
        case kTypeRangeDeletion: {
          found_final_result = true;
          if (s->IsMergeInProgress()) {
            if (value || columns) {
              // `op_failure_scope` (an output parameter) is not provided (set
              // to nullptr) since a failure must be propagated regardless of
              // its value.
              *s = MergeHelper::TimedFullMerge(
                  moptions_.merge_operator, lookup_user_key,
                  MergeHelper::kPlainBaseValue, iter->value(),
                  merge_context->GetOperands(), moptions_.info_log,
                  moptions_.statistics, clock_,
                  /* update_num_ops_stats */ true,
                  /* op_failure_scope */ nullptr, value, columns);
            } else {
              // We have found a final value (a base deletion) and have newer
              // merge operands that we do not intend to merge. Nothing remains
              // to be done so assign status to OK.
              *s = Status::OK();
            }
          } else {
            *s = Status::NotFound();
          }
          assert(seq <= read_seq);
          return found_final_result;
        }
        default: {
          std::string msg("Unrecognized or unsupported value type: " +
                          std::to_string(static_cast<int>(type)) + ". ");
          msg.append("User key: " +
                     ExtractUserKey(iter->key()).ToString(/*hex=*/true) + ". ");
          msg.append("seq: " + std::to_string(seq) + ".");
          found_final_result = true;
          *s = Status::Corruption(msg.c_str());
          return found_final_result;
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
  return found_final_result;
}

void WBWIMemTable::MultiGet(const ReadOptions& read_options,
                            MultiGetRange* range, ReadCallback* callback,
                            bool immutable_memtable) {
  // Should only be used as immutable memtable.
  assert(immutable_memtable);
  for (auto iter = range->begin(); iter != range->end(); ++iter) {
    SequenceNumber dummy_seq;
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