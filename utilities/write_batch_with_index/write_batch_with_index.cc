//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/write_batch_with_index.h"

#include <memory>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "memory/arena.h"
#include "memtable/skiplist.h"
#include "options/db_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "util/cast_util.h"
#include "util/string_util.h"
#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"

namespace ROCKSDB_NAMESPACE {
struct WriteBatchWithIndex::Rep {
  explicit Rep(const Comparator* index_comparator, size_t reserved_bytes = 0,
               size_t max_bytes = 0, bool _overwrite_key = false)
      : write_batch(reserved_bytes, max_bytes),
        comparator(index_comparator, &write_batch),
        skip_list(comparator, &arena),
        overwrite_key(_overwrite_key),
        last_entry_offset(0),
        last_sub_batch_offset(0),
        sub_batch_cnt(1) {}
  ReadableWriteBatch write_batch;
  WriteBatchEntryComparator comparator;
  Arena arena;
  WriteBatchEntrySkipList skip_list;
  bool overwrite_key;
  size_t last_entry_offset;
  // The starting offset of the last sub-batch. A sub-batch starts right before
  // inserting a key that is a duplicate of a key in the last sub-batch. Zero,
  // the default, means that no duplicate key is detected so far.
  size_t last_sub_batch_offset;
  // Total number of sub-batches in the write batch. Default is 1.
  size_t sub_batch_cnt;

  // Remember current offset of internal write batch, which is used as
  // the starting offset of the next record.
  void SetLastEntryOffset() { last_entry_offset = write_batch.GetDataSize(); }

  // In overwrite mode, find the existing entry for the same key and update it
  // to point to the current entry.
  // Return true if the key is found and updated.
  bool UpdateExistingEntry(ColumnFamilyHandle* column_family, const Slice& key,
                           WriteType type);
  bool UpdateExistingEntryWithCfId(uint32_t column_family_id, const Slice& key,
                                   WriteType type);

  // Add the recent entry to the update.
  // In overwrite mode, if key already exists in the index, update it.
  void AddOrUpdateIndex(ColumnFamilyHandle* column_family, const Slice& key,
                        WriteType type);
  void AddOrUpdateIndex(const Slice& key, WriteType type);

  // Allocate an index entry pointing to the last entry in the write batch and
  // put it to skip list.
  void AddNewEntry(uint32_t column_family_id);

  // Clear all updates buffered in this batch.
  void Clear();
  void ClearIndex();

  // Rebuild index by reading all records from the batch.
  // Returns non-ok status on corruption.
  Status ReBuildIndex();
};

bool WriteBatchWithIndex::Rep::UpdateExistingEntry(
    ColumnFamilyHandle* column_family, const Slice& key, WriteType type) {
  uint32_t cf_id = GetColumnFamilyID(column_family);
  return UpdateExistingEntryWithCfId(cf_id, key, type);
}

bool WriteBatchWithIndex::Rep::UpdateExistingEntryWithCfId(
    uint32_t column_family_id, const Slice& key, WriteType type) {
  if (!overwrite_key) {
    return false;
  }

  WBWIIteratorImpl iter(column_family_id, &skip_list, &write_batch,
                        &comparator);
  iter.Seek(key);
  if (!iter.Valid()) {
    return false;
  } else if (!iter.MatchesKey(column_family_id, key)) {
    return false;
  } else {
    // Move to the end of this key (NextKey-Prev)
    iter.NextKey();  // Move to the next key
    if (iter.Valid()) {
      iter.Prev();  // Move back one entry
    } else {
      iter.SeekToLast();
    }
  }
  WriteBatchIndexEntry* non_const_entry =
      const_cast<WriteBatchIndexEntry*>(iter.GetRawEntry());
  if (LIKELY(last_sub_batch_offset <= non_const_entry->offset)) {
    last_sub_batch_offset = last_entry_offset;
    sub_batch_cnt++;
  }
  if (type == kMergeRecord) {
    return false;
  } else {
    non_const_entry->offset = last_entry_offset;
    return true;
  }
}

void WriteBatchWithIndex::Rep::AddOrUpdateIndex(
    ColumnFamilyHandle* column_family, const Slice& key, WriteType type) {
  if (!UpdateExistingEntry(column_family, key, type)) {
    uint32_t cf_id = GetColumnFamilyID(column_family);
    const auto* cf_cmp = GetColumnFamilyUserComparator(column_family);
    if (cf_cmp != nullptr) {
      comparator.SetComparatorForCF(cf_id, cf_cmp);
    }
    AddNewEntry(cf_id);
  }
}

void WriteBatchWithIndex::Rep::AddOrUpdateIndex(const Slice& key,
                                                WriteType type) {
  if (!UpdateExistingEntryWithCfId(0, key, type)) {
    AddNewEntry(0);
  }
}

void WriteBatchWithIndex::Rep::AddNewEntry(uint32_t column_family_id) {
  const std::string& wb_data = write_batch.Data();
  Slice entry_ptr = Slice(wb_data.data() + last_entry_offset,
                          wb_data.size() - last_entry_offset);
  // Extract key
  Slice key;
  bool success __attribute__((__unused__));
  success =
      ReadKeyFromWriteBatchEntry(&entry_ptr, &key, column_family_id != 0);
  assert(success);

  auto* mem = arena.Allocate(sizeof(WriteBatchIndexEntry));
  auto* index_entry =
      new (mem) WriteBatchIndexEntry(last_entry_offset, column_family_id,
                                      key.data() - wb_data.data(), key.size());
  skip_list.Insert(index_entry);
}

void WriteBatchWithIndex::Rep::Clear() {
  write_batch.Clear();
  ClearIndex();
}

void WriteBatchWithIndex::Rep::ClearIndex() {
  skip_list.~WriteBatchEntrySkipList();
  arena.~Arena();
  new (&arena) Arena();
  new (&skip_list) WriteBatchEntrySkipList(comparator, &arena);
  last_entry_offset = 0;
  last_sub_batch_offset = 0;
  sub_batch_cnt = 1;
}

Status WriteBatchWithIndex::Rep::ReBuildIndex() {
  Status s;

  ClearIndex();

  if (write_batch.Count() == 0) {
    // Nothing to re-index
    return s;
  }

  size_t offset = WriteBatchInternal::GetFirstOffset(&write_batch);

  Slice input(write_batch.Data());
  input.remove_prefix(offset);

  // Loop through all entries in Rep and add each one to the index
  uint32_t found = 0;
  while (s.ok() && !input.empty()) {
    Slice key, value, blob, xid;
    uint32_t column_family_id = 0;  // default
    char tag = 0;

    // set offset of current entry for call to AddNewEntry()
    last_entry_offset = input.data() - write_batch.Data().data();

    s = ReadRecordFromWriteBatch(&input, &tag, &column_family_id, &key,
                                  &value, &blob, &xid);
    if (!s.ok()) {
      break;
    }

    switch (tag) {
      case kTypeColumnFamilyValue:
      case kTypeValue:
        found++;
        if (!UpdateExistingEntryWithCfId(column_family_id, key, kPutRecord)) {
          AddNewEntry(column_family_id);
        }
        break;
      case kTypeColumnFamilyDeletion:
      case kTypeDeletion:
        found++;
        if (!UpdateExistingEntryWithCfId(column_family_id, key,
                                         kDeleteRecord)) {
          AddNewEntry(column_family_id);
        }
        break;
      case kTypeColumnFamilySingleDeletion:
      case kTypeSingleDeletion:
        found++;
        if (!UpdateExistingEntryWithCfId(column_family_id, key,
                                         kSingleDeleteRecord)) {
          AddNewEntry(column_family_id);
        }
        break;
      case kTypeColumnFamilyMerge:
      case kTypeMerge:
        found++;
        if (!UpdateExistingEntryWithCfId(column_family_id, key, kMergeRecord)) {
          AddNewEntry(column_family_id);
        }
        break;
      case kTypeLogData:
      case kTypeBeginPrepareXID:
      case kTypeBeginPersistedPrepareXID:
      case kTypeBeginUnprepareXID:
      case kTypeEndPrepareXID:
      case kTypeCommitXID:
      case kTypeRollbackXID:
      case kTypeNoop:
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag in ReBuildIndex",
                                  ToString(static_cast<unsigned int>(tag)));
    }
  }

  if (s.ok() && found != write_batch.Count()) {
    s = Status::Corruption("WriteBatch has wrong count");
  }

  return s;
}

WriteBatchWithIndex::WriteBatchWithIndex(
    const Comparator* default_index_comparator, size_t reserved_bytes,
    bool overwrite_key, size_t max_bytes)
    : rep(new Rep(default_index_comparator, reserved_bytes, max_bytes,
                  overwrite_key)) {}

WriteBatchWithIndex::~WriteBatchWithIndex() {}

WriteBatchWithIndex::WriteBatchWithIndex(WriteBatchWithIndex&&) = default;

WriteBatchWithIndex& WriteBatchWithIndex::operator=(WriteBatchWithIndex&&) =
    default;

WriteBatch* WriteBatchWithIndex::GetWriteBatch() { return &rep->write_batch; }

size_t WriteBatchWithIndex::SubBatchCnt() { return rep->sub_batch_cnt; }

WBWIIterator* WriteBatchWithIndex::NewIterator() {
  return new WBWIIteratorImpl(0, &(rep->skip_list), &rep->write_batch,
                              &(rep->comparator));
}

WBWIIterator* WriteBatchWithIndex::NewIterator(
    ColumnFamilyHandle* column_family) {
  return new WBWIIteratorImpl(GetColumnFamilyID(column_family),
                              &(rep->skip_list), &rep->write_batch,
                              &(rep->comparator));
}

Iterator* WriteBatchWithIndex::NewIteratorWithBase(
    ColumnFamilyHandle* column_family, Iterator* base_iterator,
    const ReadOptions* read_options) {
  auto wbwiii =
      new WBWIIteratorImpl(GetColumnFamilyID(column_family), &(rep->skip_list),
                           &rep->write_batch, &rep->comparator);
  return new BaseDeltaIterator(column_family, base_iterator, wbwiii,
                               GetColumnFamilyUserComparator(column_family),
                               read_options);
}

Iterator* WriteBatchWithIndex::NewIteratorWithBase(Iterator* base_iterator) {
  // default column family's comparator
  auto wbwiii = new WBWIIteratorImpl(0, &(rep->skip_list), &rep->write_batch,
                                     &rep->comparator);
  return new BaseDeltaIterator(nullptr, base_iterator, wbwiii,
                               rep->comparator.default_comparator());
}

Status WriteBatchWithIndex::Put(ColumnFamilyHandle* column_family,
                                const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.Put(column_family, key, value);
  if (s.ok()) {
    rep->AddOrUpdateIndex(column_family, key, kPutRecord);
  }
  return s;
}

Status WriteBatchWithIndex::Put(const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.Put(key, value);
  if (s.ok()) {
    rep->AddOrUpdateIndex(key, kPutRecord);
  }
  return s;
}

Status WriteBatchWithIndex::Delete(ColumnFamilyHandle* column_family,
                                   const Slice& key) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.Delete(column_family, key);
  if (s.ok()) {
    rep->AddOrUpdateIndex(column_family, key, kDeleteRecord);
  }
  return s;
}

Status WriteBatchWithIndex::Delete(const Slice& key) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.Delete(key);
  if (s.ok()) {
    rep->AddOrUpdateIndex(key, kDeleteRecord);
  }
  return s;
}

Status WriteBatchWithIndex::SingleDelete(ColumnFamilyHandle* column_family,
                                         const Slice& key) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.SingleDelete(column_family, key);
  if (s.ok()) {
    rep->AddOrUpdateIndex(column_family, key, kSingleDeleteRecord);
  }
  return s;
}

Status WriteBatchWithIndex::SingleDelete(const Slice& key) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.SingleDelete(key);
  if (s.ok()) {
    rep->AddOrUpdateIndex(key, kSingleDeleteRecord);
  }
  return s;
}

Status WriteBatchWithIndex::Merge(ColumnFamilyHandle* column_family,
                                  const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.Merge(column_family, key, value);
  if (s.ok()) {
    rep->AddOrUpdateIndex(column_family, key, kMergeRecord);
  }
  return s;
}

Status WriteBatchWithIndex::Merge(const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.Merge(key, value);
  if (s.ok()) {
    rep->AddOrUpdateIndex(key, kMergeRecord);
  }
  return s;
}

Status WriteBatchWithIndex::PutLogData(const Slice& blob) {
  return rep->write_batch.PutLogData(blob);
}

void WriteBatchWithIndex::Clear() { rep->Clear(); }

Status WriteBatchWithIndex::GetFromBatch(ColumnFamilyHandle* column_family,
                                         const DBOptions& options,
                                         const Slice& key, std::string* value) {
  Status s;
  WriteBatchWithIndexInternal wbwii(&options, column_family);
  auto result = wbwii.GetFromBatch(this, key, value, &s);

  switch (result) {
    case WBWIIteratorImpl::kFound:
    case WBWIIteratorImpl::kError:
      // use returned status
      break;
    case WBWIIteratorImpl::kDeleted:
    case WBWIIteratorImpl::kNotFound:
      s = Status::NotFound();
      break;
    case WBWIIteratorImpl::kMergeInProgress:
      s = Status::MergeInProgress();
      break;
    default:
      assert(false);
  }

  return s;
}

Status WriteBatchWithIndex::GetFromBatchAndDB(DB* db,
                                              const ReadOptions& read_options,
                                              const Slice& key,
                                              std::string* value) {
  assert(value != nullptr);
  PinnableSlice pinnable_val(value);
  assert(!pinnable_val.IsPinned());
  auto s = GetFromBatchAndDB(db, read_options, db->DefaultColumnFamily(), key,
                             &pinnable_val);
  if (s.ok() && pinnable_val.IsPinned()) {
    value->assign(pinnable_val.data(), pinnable_val.size());
  }  // else value is already assigned
  return s;
}

Status WriteBatchWithIndex::GetFromBatchAndDB(DB* db,
                                              const ReadOptions& read_options,
                                              const Slice& key,
                                              PinnableSlice* pinnable_val) {
  return GetFromBatchAndDB(db, read_options, db->DefaultColumnFamily(), key,
                           pinnable_val);
}

Status WriteBatchWithIndex::GetFromBatchAndDB(DB* db,
                                              const ReadOptions& read_options,
                                              ColumnFamilyHandle* column_family,
                                              const Slice& key,
                                              std::string* value) {
  assert(value != nullptr);
  PinnableSlice pinnable_val(value);
  assert(!pinnable_val.IsPinned());
  auto s =
      GetFromBatchAndDB(db, read_options, column_family, key, &pinnable_val);
  if (s.ok() && pinnable_val.IsPinned()) {
    value->assign(pinnable_val.data(), pinnable_val.size());
  }  // else value is already assigned
  return s;
}

Status WriteBatchWithIndex::GetFromBatchAndDB(DB* db,
                                              const ReadOptions& read_options,
                                              ColumnFamilyHandle* column_family,
                                              const Slice& key,
                                              PinnableSlice* pinnable_val) {
  return GetFromBatchAndDB(db, read_options, column_family, key, pinnable_val,
                           nullptr);
}

Status WriteBatchWithIndex::GetFromBatchAndDB(
    DB* db, const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    const Slice& key, PinnableSlice* pinnable_val, ReadCallback* callback) {
  Status s;
  WriteBatchWithIndexInternal wbwii(db, column_family);

  // Since the lifetime of the WriteBatch is the same as that of the transaction
  // we cannot pin it as otherwise the returned value will not be available
  // after the transaction finishes.
  std::string& batch_value = *pinnable_val->GetSelf();
  auto result = wbwii.GetFromBatch(this, key, &batch_value, &s);

  if (result == WBWIIteratorImpl::kFound) {
    pinnable_val->PinSelf();
    return s;
  } else if (!s.ok() || result == WBWIIteratorImpl::kError) {
    return s;
  } else if (result == WBWIIteratorImpl::kDeleted) {
    return Status::NotFound();
  }
  assert(result == WBWIIteratorImpl::kMergeInProgress ||
         result == WBWIIteratorImpl::kNotFound);

  // Did not find key in batch OR could not resolve Merges.  Try DB.
  if (!callback) {
    s = db->Get(read_options, column_family, key, pinnable_val);
  } else {
    DBImpl::GetImplOptions get_impl_options;
    get_impl_options.column_family = column_family;
    get_impl_options.value = pinnable_val;
    get_impl_options.callback = callback;
    s = static_cast_with_check<DBImpl>(db->GetRootDB())
            ->GetImpl(read_options, key, get_impl_options);
  }

  if (s.ok() || s.IsNotFound()) {  // DB Get Succeeded
    if (result == WBWIIteratorImpl::kMergeInProgress) {
      // Merge result from DB with merges in Batch
      std::string merge_result;
      if (s.ok()) {
        s = wbwii.MergeKey(key, pinnable_val, &merge_result);
      } else {  // Key not present in db (s.IsNotFound())
        s = wbwii.MergeKey(key, nullptr, &merge_result);
      }
      if (s.ok()) {
        pinnable_val->Reset();
        *pinnable_val->GetSelf() = std::move(merge_result);
        pinnable_val->PinSelf();
      }
    }
  }

  return s;
}

void WriteBatchWithIndex::MultiGetFromBatchAndDB(
    DB* db, const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    const size_t num_keys, const Slice* keys, PinnableSlice* values,
    Status* statuses, bool sorted_input) {
  MultiGetFromBatchAndDB(db, read_options, column_family, num_keys, keys,
                         values, statuses, sorted_input, nullptr);
}

void WriteBatchWithIndex::MultiGetFromBatchAndDB(
    DB* db, const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    const size_t num_keys, const Slice* keys, PinnableSlice* values,
    Status* statuses, bool sorted_input, ReadCallback* callback) {
  WriteBatchWithIndexInternal wbwii(db, column_family);

  autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_context;
  autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
  // To hold merges from the write batch
  autovector<std::pair<WBWIIteratorImpl::Result, MergeContext>,
             MultiGetContext::MAX_BATCH_SIZE>
      merges;
  // Since the lifetime of the WriteBatch is the same as that of the transaction
  // we cannot pin it as otherwise the returned value will not be available
  // after the transaction finishes.
  for (size_t i = 0; i < num_keys; ++i) {
    MergeContext merge_context;
    std::string batch_value;
    Status* s = &statuses[i];
    PinnableSlice* pinnable_val = &values[i];
    pinnable_val->Reset();
    auto result =
        wbwii.GetFromBatch(this, keys[i], &merge_context, &batch_value, s);

    if (result == WBWIIteratorImpl::kFound) {
      *pinnable_val->GetSelf() = std::move(batch_value);
      pinnable_val->PinSelf();
      continue;
    }
    if (result == WBWIIteratorImpl::kDeleted) {
      *s = Status::NotFound();
      continue;
    }
    if (result == WBWIIteratorImpl::kError) {
      continue;
    }
    assert(result == WBWIIteratorImpl::kMergeInProgress ||
           result == WBWIIteratorImpl::kNotFound);
    key_context.emplace_back(column_family, keys[i], &values[i],
                             /*timestamp*/ nullptr, &statuses[i]);
    merges.emplace_back(result, std::move(merge_context));
  }

  for (KeyContext& key : key_context) {
    sorted_keys.emplace_back(&key);
  }

  // Did not find key in batch OR could not resolve Merges.  Try DB.
  static_cast_with_check<DBImpl>(db->GetRootDB())
      ->PrepareMultiGetKeys(key_context.size(), sorted_input, &sorted_keys);
  static_cast_with_check<DBImpl>(db->GetRootDB())
      ->MultiGetWithCallback(read_options, column_family, callback,
                             &sorted_keys);

  for (auto iter = key_context.begin(); iter != key_context.end(); ++iter) {
    KeyContext& key = *iter;
    if (key.s->ok() || key.s->IsNotFound()) {  // DB Get Succeeded
      size_t index = iter - key_context.begin();
      std::pair<WBWIIteratorImpl::Result, MergeContext>& merge_result =
          merges[index];
      if (merge_result.first == WBWIIteratorImpl::kMergeInProgress) {
        std::string merged_value;
        // Merge result from DB with merges in Batch
        if (key.s->ok()) {
          *key.s = wbwii.MergeKey(*key.key, iter->value, merge_result.second,
                                  &merged_value);
        } else {  // Key not present in db (s.IsNotFound())
          *key.s = wbwii.MergeKey(*key.key, nullptr, merge_result.second,
                                  &merged_value);
        }
        if (key.s->ok()) {
          key.value->Reset();
          *key.value->GetSelf() = std::move(merged_value);
          key.value->PinSelf();
        }
      }
    }
  }
}

void WriteBatchWithIndex::SetSavePoint() { rep->write_batch.SetSavePoint(); }

Status WriteBatchWithIndex::RollbackToSavePoint() {
  Status s = rep->write_batch.RollbackToSavePoint();

  if (s.ok()) {
    rep->sub_batch_cnt = 1;
    rep->last_sub_batch_offset = 0;
    s = rep->ReBuildIndex();
  }

  return s;
}

Status WriteBatchWithIndex::PopSavePoint() {
  return rep->write_batch.PopSavePoint();
}

void WriteBatchWithIndex::SetMaxBytes(size_t max_bytes) {
  rep->write_batch.SetMaxBytes(max_bytes);
}

size_t WriteBatchWithIndex::GetDataSize() const {
  return rep->write_batch.GetDataSize();
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
