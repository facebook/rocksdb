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
  assert(s->ok() || s->IsMergeInProgress());
  (void)immutable_memtable;
  (void)timestamp;
  (void)columns;
  assert(immutable_memtable);
  assert(!timestamp);  // TODO: support UDT
  assert(assigned_seqno_.upper_bound != kMaxSequenceNumber);
  assert(assigned_seqno_.lower_bound != kMaxSequenceNumber);
  // WBWI does not support DeleteRange yet.
  assert(!wbwi_->GetWriteBatch()->HasDeleteRange());
  assert(merge_context);

  *out_seq = kMaxSequenceNumber;
  [[maybe_unused]] SequenceNumber read_seq =
      GetInternalKeySeqno(key.internal_key());
  // This is memtable is a single write batch, no snapshot can be taken within
  // assigned seqnos for this memtable.
  assert(read_seq >= assigned_seqno_.upper_bound ||
         read_seq < assigned_seqno_.lower_bound);
  std::unique_ptr<InternalIterator> iter{NewIterator()};
  iter->Seek(key.internal_key());
  const Slice lookup_user_key = key.user_key();
  bool merge_in_progress = s->IsMergeInProgress();

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
                          do_merge, merge_in_progress, merge_context,
                          moptions_.merge_operator, clock_,
                          moptions_.statistics, moptions_.info_log, s, value,
                          columns, is_blob_index);
          assert(seq <= read_seq);
          return /*found_final_value=*/true;
        }
        case kTypeDeletion:
        case kTypeSingleDeletion:
        case kTypeRangeDeletion: {
          HandleTypeDeletion(lookup_user_key, merge_in_progress, merge_context,
                             moptions_.merge_operator, clock_,
                             moptions_.statistics, moptions_.info_log, s, value,
                             columns);
          assert(seq <= read_seq);
          return /*found_final_value=*/true;
        }
        case kTypeMerge: {
          merge_in_progress = true;
          if (ReadOnlyMemTable::HandleTypeMerge(
                  lookup_user_key, iter->value(), iter->IsValuePinned(),
                  do_merge, merge_context, moptions_.merge_operator, clock_,
                  moptions_.statistics, moptions_.info_log, s, value,
                  columns)) {
            return true;
          }
          break;
        }
        default: {
          std::string msg(
              "Unrecognized or unsupported value type for "
              "WBWI-based memtable: " +
              std::to_string(static_cast<int>(type)) + ". ");
          msg.append("User key: " +
                     ExtractUserKey(iter->key()).ToString(/*hex=*/true) + ". ");
          msg.append("seq: " + std::to_string(seq) + ".");
          *s = Status::Corruption(msg.c_str());
          return /*found_final_value=*/true;
        }
      }
    }
    // Current key is a merge key or not visible
    assert(merge_in_progress || (callback && !callback->IsVisible(seq)));
    iter->Next();
  }
  if (!iter->status().ok() &&
      (s->ok() || s->IsMergeInProgress() || s->IsNotFound())) {
    *s = iter->status();
    // stop further look up
    return true;
  }
  if (merge_in_progress) {
    assert(s->ok() || s->IsMergeInProgress());
    *s = Status::MergeInProgress();
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

size_t WBWIMemTable::MultiPrefixExists(
    const ReadOptions& /*read_options*/, size_t num_prefixes,
    const Slice* prefixes, bool* prefix_exists,
    std::vector<std::unique_ptr<std::unordered_set<std::string>>>&
        tombstoned_keys,
    bool sorted_input) {
  // WBWIMemTable uses an InternalIterator-based approach since it wraps a
  // WriteBatchWithIndex rather than having the skiplist structure of MemTable.
  //
  // Note: WBWIMemTable does not support user-defined timestamps, so no
  // timestamp stripping is needed here.
  std::unique_ptr<InternalIterator> iter(NewIterator());

  // Track if iterator is positioned for forward scan optimization.
  bool iter_positioned = false;

  // Count how many prefixes we find (for O(1) early termination tracking)
  size_t num_found = 0;

  for (size_t i = 0; i < num_prefixes; ++i) {
    if (prefix_exists[i]) {
      continue;  // Already found in a higher level
    }

    const Slice& prefix = prefixes[i];
    if (prefix.empty()) {
      continue;
    }

    // Forward scan optimization for sorted input:
    // If the iterator is already positioned, check if we can skip the seek.
    bool need_seek = true;
    if (sorted_input && iter_positioned && iter->Valid()) {
      Slice current_key = ExtractUserKey(iter->key());
      if (current_key.compare(prefix) >= 0) {
        if (current_key.size() >= prefix.size() &&
            memcmp(current_key.data(), prefix.data(), prefix.size()) == 0) {
          // Iterator is already at a key with our prefix - no seek needed
          need_seek = false;
        } else {
          // Current key is past our prefix - no keys with this prefix exist
          iter_positioned = true;
          continue;
        }
      }
    }

    if (need_seek) {
      // Seek to the first key >= prefix
      LookupKey lkey(prefix, kMaxSequenceNumber);
      iter->Seek(lkey.internal_key());
    }
    iter_positioned = true;

    // Lazy tombstone access - only dereference when needed
    std::unique_ptr<std::unordered_set<std::string>>& tombstones_ptr =
        tombstoned_keys[i];

    while (iter->Valid() && iter->status().ok()) {
      Slice internal_key = iter->key();
      Slice user_key = ExtractUserKey(internal_key);

      // Check if this key still has the target prefix
      if (user_key.size() < prefix.size() ||
          memcmp(user_key.data(), prefix.data(), prefix.size()) != 0) {
        break;  // Moved past keys with this prefix
      }

      // Check if this key was already marked as deleted. This handles:
      // 1. Within this memtable: A DELETE with a higher sequence number shadows
      //    a PUT with a lower sequence number.
      // 2. Across levels: A DELETE in a newer level shadows a PUT in this
      //    level. The tombstoned_keys vector accumulates deleted keys as we
      //    traverse from newest to oldest.
      // Note: ToString() only called when tombstones_ptr is non-null, avoiding
      // allocation when there are no tombstones.
      if (tombstones_ptr && tombstones_ptr->count(user_key.ToString()) > 0) {
        iter->Next();
        continue;
      }

      ValueType type = ExtractValueType(internal_key);
      if (type == kTypeValue || type == kTypeBlobIndex ||
          type == kTypeWideColumnEntity || type == kTypeMerge ||
          type == kTypeValuePreferredSeqno) {
        prefix_exists[i] = true;
        ++num_found;
        // For sorted input, advance past this prefix's keys
        if (sorted_input) {
          while (iter->Valid()) {
            Slice next_key = ExtractUserKey(iter->key());
            if (next_key.size() < prefix.size() ||
                memcmp(next_key.data(), prefix.data(), prefix.size()) != 0) {
              break;
            }
            iter->Next();
          }
        }
        break;
      }

      // Point deletion - record for cross-level and within-level shadowing.
      // Lazy allocation: only create the set when we actually see a tombstone.
      if (!tombstones_ptr) {
        tombstones_ptr = std::make_unique<std::unordered_set<std::string>>();
      }
      tombstones_ptr->emplace(user_key.data(), user_key.size());
      iter->Next();
    }
  }
  return num_found;
}
}  // namespace ROCKSDB_NAMESPACE
