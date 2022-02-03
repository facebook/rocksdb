// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/transaction_base.h"

#include <cinttypes>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "util/cast_util.h"
#include "util/string_util.h"
#include "utilities/transactions/lock/lock_tracker.h"

namespace ROCKSDB_NAMESPACE {

TransactionBaseImpl::TransactionBaseImpl(
    DB* db, const WriteOptions& write_options,
    const LockTrackerFactory& lock_tracker_factory)
    : db_(db),
      dbimpl_(static_cast_with_check<DBImpl>(db)),
      write_options_(write_options),
      cmp_(GetColumnFamilyUserComparator(db->DefaultColumnFamily())),
      lock_tracker_factory_(lock_tracker_factory),
      start_time_(dbimpl_->GetSystemClock()->NowMicros()),
      write_batch_(cmp_, 0, true, 0),
      tracked_locks_(lock_tracker_factory_.Create()),
      indexing_enabled_(true) {
  assert(dynamic_cast<DBImpl*>(db_) != nullptr);
  log_number_ = 0;
  if (dbimpl_->allow_2pc()) {
    InitWriteBatch();
  }
}

TransactionBaseImpl::~TransactionBaseImpl() {
  // Release snapshot if snapshot is set
  SetSnapshotInternal(nullptr);
}

void TransactionBaseImpl::Clear() {
  save_points_.reset(nullptr);
  write_batch_.Clear();
  commit_time_batch_.Clear();
  tracked_locks_->Clear();
  num_puts_ = 0;
  num_deletes_ = 0;
  num_merges_ = 0;

  if (dbimpl_->allow_2pc()) {
    InitWriteBatch();
  }
}

void TransactionBaseImpl::Reinitialize(DB* db,
                                       const WriteOptions& write_options) {
  Clear();
  ClearSnapshot();
  id_ = 0;
  db_ = db;
  name_.clear();
  log_number_ = 0;
  write_options_ = write_options;
  start_time_ = dbimpl_->GetSystemClock()->NowMicros();
  indexing_enabled_ = true;
  cmp_ = GetColumnFamilyUserComparator(db_->DefaultColumnFamily());
}

void TransactionBaseImpl::SetSnapshot() {
  const Snapshot* snapshot = dbimpl_->GetSnapshotForWriteConflictBoundary();
  SetSnapshotInternal(snapshot);
}

void TransactionBaseImpl::SetSnapshotInternal(const Snapshot* snapshot) {
  // Set a custom deleter for the snapshot_ SharedPtr as the snapshot needs to
  // be released, not deleted when it is no longer referenced.
  snapshot_.reset(snapshot, std::bind(&TransactionBaseImpl::ReleaseSnapshot,
                                      this, std::placeholders::_1, db_));
  snapshot_needed_ = false;
  snapshot_notifier_ = nullptr;
}

void TransactionBaseImpl::SetSnapshotOnNextOperation(
    std::shared_ptr<TransactionNotifier> notifier) {
  snapshot_needed_ = true;
  snapshot_notifier_ = notifier;
}

void TransactionBaseImpl::SetSnapshotIfNeeded() {
  if (snapshot_needed_) {
    std::shared_ptr<TransactionNotifier> notifier = snapshot_notifier_;
    SetSnapshot();
    if (notifier != nullptr) {
      notifier->SnapshotCreated(GetSnapshot());
    }
  }
}

Status TransactionBaseImpl::TryLock(ColumnFamilyHandle* column_family,
                                    const SliceParts& key, bool read_only,
                                    bool exclusive, const bool do_validate,
                                    const bool assume_tracked) {
  size_t key_size = 0;
  for (int i = 0; i < key.num_parts; ++i) {
    key_size += key.parts[i].size();
  }

  std::string str;
  str.reserve(key_size);

  for (int i = 0; i < key.num_parts; ++i) {
    str.append(key.parts[i].data(), key.parts[i].size());
  }

  return TryLock(column_family, str, read_only, exclusive, do_validate,
                 assume_tracked);
}

void TransactionBaseImpl::SetSavePoint() {
  if (save_points_ == nullptr) {
    save_points_.reset(new std::stack<TransactionBaseImpl::SavePoint, autovector<TransactionBaseImpl::SavePoint>>());
  }
  save_points_->emplace(snapshot_, snapshot_needed_, snapshot_notifier_,
                        num_puts_, num_deletes_, num_merges_,
                        lock_tracker_factory_);
  write_batch_.SetSavePoint();
}

Status TransactionBaseImpl::RollbackToSavePoint() {
  if (save_points_ != nullptr && save_points_->size() > 0) {
    // Restore saved SavePoint
    TransactionBaseImpl::SavePoint& save_point = save_points_->top();
    snapshot_ = save_point.snapshot_;
    snapshot_needed_ = save_point.snapshot_needed_;
    snapshot_notifier_ = save_point.snapshot_notifier_;
    num_puts_ = save_point.num_puts_;
    num_deletes_ = save_point.num_deletes_;
    num_merges_ = save_point.num_merges_;

    // Rollback batch
    Status s = write_batch_.RollbackToSavePoint();
    assert(s.ok());

    // Rollback any keys that were tracked since the last savepoint
    tracked_locks_->Subtract(*save_point.new_locks_);

    save_points_->pop();

    return s;
  } else {
    assert(write_batch_.RollbackToSavePoint().IsNotFound());
    return Status::NotFound();
  }
}

Status TransactionBaseImpl::PopSavePoint() {
  if (save_points_ == nullptr ||
      save_points_->empty()) {
    // No SavePoint yet.
    assert(write_batch_.PopSavePoint().IsNotFound());
    return Status::NotFound();
  }

  assert(!save_points_->empty());
  // If there is another savepoint A below the current savepoint B, then A needs
  // to inherit tracked_keys in B so that if we rollback to savepoint A, we
  // remember to unlock keys in B. If there is no other savepoint below, then we
  // can safely discard savepoint info.
  if (save_points_->size() == 1) {
    save_points_->pop();
  } else {
    TransactionBaseImpl::SavePoint top(lock_tracker_factory_);
    std::swap(top, save_points_->top());
    save_points_->pop();

    save_points_->top().new_locks_->Merge(*top.new_locks_);
  }

  return write_batch_.PopSavePoint();
}

Status TransactionBaseImpl::Get(const ReadOptions& read_options,
                                ColumnFamilyHandle* column_family,
                                const Slice& key, std::string* value) {
  assert(value != nullptr);
  PinnableSlice pinnable_val(value);
  assert(!pinnable_val.IsPinned());
  auto s = Get(read_options, column_family, key, &pinnable_val);
  if (s.ok() && pinnable_val.IsPinned()) {
    value->assign(pinnable_val.data(), pinnable_val.size());
  }  // else value is already assigned
  return s;
}

Status TransactionBaseImpl::Get(const ReadOptions& read_options,
                                ColumnFamilyHandle* column_family,
                                const Slice& key, PinnableSlice* pinnable_val) {
  return write_batch_.GetFromBatchAndDB(db_, read_options, column_family, key,
                                        pinnable_val);
}

Status TransactionBaseImpl::GetForUpdate(const ReadOptions& read_options,
                                         ColumnFamilyHandle* column_family,
                                         const Slice& key, std::string* value,
                                         bool exclusive,
                                         const bool do_validate) {
  if (!do_validate && read_options.snapshot != nullptr) {
    return Status::InvalidArgument(
        "If do_validate is false then GetForUpdate with snapshot is not "
        "defined.");
  }
  Status s =
      TryLock(column_family, key, true /* read_only */, exclusive, do_validate);

  if (s.ok() && value != nullptr) {
    assert(value != nullptr);
    PinnableSlice pinnable_val(value);
    assert(!pinnable_val.IsPinned());
    s = Get(read_options, column_family, key, &pinnable_val);
    if (s.ok() && pinnable_val.IsPinned()) {
      value->assign(pinnable_val.data(), pinnable_val.size());
    }  // else value is already assigned
  }
  return s;
}

Status TransactionBaseImpl::GetForUpdate(const ReadOptions& read_options,
                                         ColumnFamilyHandle* column_family,
                                         const Slice& key,
                                         PinnableSlice* pinnable_val,
                                         bool exclusive,
                                         const bool do_validate) {
  if (!do_validate && read_options.snapshot != nullptr) {
    return Status::InvalidArgument(
        "If do_validate is false then GetForUpdate with snapshot is not "
        "defined.");
  }
  Status s =
      TryLock(column_family, key, true /* read_only */, exclusive, do_validate);

  if (s.ok() && pinnable_val != nullptr) {
    s = Get(read_options, column_family, key, pinnable_val);
  }
  return s;
}

std::vector<Status> TransactionBaseImpl::MultiGet(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  size_t num_keys = keys.size();
  values->resize(num_keys);

  std::vector<Status> stat_list(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    stat_list[i] = Get(read_options, column_family[i], keys[i], &(*values)[i]);
  }

  return stat_list;
}

void TransactionBaseImpl::MultiGet(const ReadOptions& read_options,
                                   ColumnFamilyHandle* column_family,
                                   const size_t num_keys, const Slice* keys,
                                   PinnableSlice* values, Status* statuses,
                                   const bool sorted_input) {
  write_batch_.MultiGetFromBatchAndDB(db_, read_options, column_family,
                                      num_keys, keys, values, statuses,
                                      sorted_input);
}

std::vector<Status> TransactionBaseImpl::MultiGetForUpdate(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  // Regardless of whether the MultiGet succeeded, track these keys.
  size_t num_keys = keys.size();
  values->resize(num_keys);

  // Lock all keys
  for (size_t i = 0; i < num_keys; ++i) {
    Status s = TryLock(column_family[i], keys[i], true /* read_only */,
                       true /* exclusive */);
    if (!s.ok()) {
      // Fail entire multiget if we cannot lock all keys
      return std::vector<Status>(num_keys, s);
    }
  }

  // TODO(agiardullo): optimize multiget?
  std::vector<Status> stat_list(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    stat_list[i] = Get(read_options, column_family[i], keys[i], &(*values)[i]);
  }

  return stat_list;
}

Iterator* TransactionBaseImpl::GetIterator(const ReadOptions& read_options) {
  Iterator* db_iter = db_->NewIterator(read_options);
  assert(db_iter);

  return write_batch_.NewIteratorWithBase(db_iter);
}

Iterator* TransactionBaseImpl::GetIterator(const ReadOptions& read_options,
                                           ColumnFamilyHandle* column_family) {
  Iterator* db_iter = db_->NewIterator(read_options, column_family);
  assert(db_iter);

  return write_batch_.NewIteratorWithBase(column_family, db_iter,
                                          &read_options);
}

Status TransactionBaseImpl::Put(ColumnFamilyHandle* column_family,
                                const Slice& key, const Slice& value,
                                const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, do_validate, assume_tracked);

  if (s.ok()) {
    s = GetBatchForWrite()->Put(column_family, key, value);
    if (s.ok()) {
      num_puts_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::Put(ColumnFamilyHandle* column_family,
                                const SliceParts& key, const SliceParts& value,
                                const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, do_validate, assume_tracked);

  if (s.ok()) {
    s = GetBatchForWrite()->Put(column_family, key, value);
    if (s.ok()) {
      num_puts_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::Merge(ColumnFamilyHandle* column_family,
                                  const Slice& key, const Slice& value,
                                  const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, do_validate, assume_tracked);

  if (s.ok()) {
    s = GetBatchForWrite()->Merge(column_family, key, value);
    if (s.ok()) {
      num_merges_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::Delete(ColumnFamilyHandle* column_family,
                                   const Slice& key,
                                   const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, do_validate, assume_tracked);

  if (s.ok()) {
    s = GetBatchForWrite()->Delete(column_family, key);
    if (s.ok()) {
      num_deletes_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::Delete(ColumnFamilyHandle* column_family,
                                   const SliceParts& key,
                                   const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, do_validate, assume_tracked);

  if (s.ok()) {
    s = GetBatchForWrite()->Delete(column_family, key);
    if (s.ok()) {
      num_deletes_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::SingleDelete(ColumnFamilyHandle* column_family,
                                         const Slice& key,
                                         const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, do_validate, assume_tracked);

  if (s.ok()) {
    s = GetBatchForWrite()->SingleDelete(column_family, key);
    if (s.ok()) {
      num_deletes_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::SingleDelete(ColumnFamilyHandle* column_family,
                                         const SliceParts& key,
                                         const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, do_validate, assume_tracked);

  if (s.ok()) {
    s = GetBatchForWrite()->SingleDelete(column_family, key);
    if (s.ok()) {
      num_deletes_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::PutUntracked(ColumnFamilyHandle* column_family,
                                         const Slice& key, const Slice& value) {
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, false /* do_validate */);

  if (s.ok()) {
    s = GetBatchForWrite()->Put(column_family, key, value);
    if (s.ok()) {
      num_puts_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::PutUntracked(ColumnFamilyHandle* column_family,
                                         const SliceParts& key,
                                         const SliceParts& value) {
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, false /* do_validate */);

  if (s.ok()) {
    s = GetBatchForWrite()->Put(column_family, key, value);
    if (s.ok()) {
      num_puts_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::MergeUntracked(ColumnFamilyHandle* column_family,
                                           const Slice& key,
                                           const Slice& value) {
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, false /* do_validate */);

  if (s.ok()) {
    s = GetBatchForWrite()->Merge(column_family, key, value);
    if (s.ok()) {
      num_merges_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::DeleteUntracked(ColumnFamilyHandle* column_family,
                                            const Slice& key) {
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, false /* do_validate */);

  if (s.ok()) {
    s = GetBatchForWrite()->Delete(column_family, key);
    if (s.ok()) {
      num_deletes_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::DeleteUntracked(ColumnFamilyHandle* column_family,
                                            const SliceParts& key) {
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, false /* do_validate */);

  if (s.ok()) {
    s = GetBatchForWrite()->Delete(column_family, key);
    if (s.ok()) {
      num_deletes_++;
    }
  }

  return s;
}

Status TransactionBaseImpl::SingleDeleteUntracked(
    ColumnFamilyHandle* column_family, const Slice& key) {
  Status s = TryLock(column_family, key, false /* read_only */,
                     true /* exclusive */, false /* do_validate */);

  if (s.ok()) {
    s = GetBatchForWrite()->SingleDelete(column_family, key);
    if (s.ok()) {
      num_deletes_++;
    }
  }

  return s;
}

void TransactionBaseImpl::PutLogData(const Slice& blob) {
  auto s = write_batch_.PutLogData(blob);
  (void)s;
  assert(s.ok());
}

WriteBatchWithIndex* TransactionBaseImpl::GetWriteBatch() {
  return &write_batch_;
}

uint64_t TransactionBaseImpl::GetElapsedTime() const {
  return (dbimpl_->GetSystemClock()->NowMicros() - start_time_) / 1000;
}

uint64_t TransactionBaseImpl::GetNumPuts() const { return num_puts_; }

uint64_t TransactionBaseImpl::GetNumDeletes() const { return num_deletes_; }

uint64_t TransactionBaseImpl::GetNumMerges() const { return num_merges_; }

uint64_t TransactionBaseImpl::GetNumKeys() const {
  return tracked_locks_->GetNumPointLocks();
}

void TransactionBaseImpl::TrackKey(uint32_t cfh_id, const std::string& key,
                                   SequenceNumber seq, bool read_only,
                                   bool exclusive) {
  PointLockRequest r;
  r.column_family_id = cfh_id;
  r.key = key;
  r.seq = seq;
  r.read_only = read_only;
  r.exclusive = exclusive;

  // Update map of all tracked keys for this transaction
  tracked_locks_->Track(r);

  if (save_points_ != nullptr && !save_points_->empty()) {
    // Update map of tracked keys in this SavePoint
    save_points_->top().new_locks_->Track(r);
  }
}

// Gets the write batch that should be used for Put/Merge/Deletes.
//
// Returns either a WriteBatch or WriteBatchWithIndex depending on whether
// DisableIndexing() has been called.
WriteBatchBase* TransactionBaseImpl::GetBatchForWrite() {
  if (indexing_enabled_) {
    // Use WriteBatchWithIndex
    return &write_batch_;
  } else {
    // Don't use WriteBatchWithIndex. Return base WriteBatch.
    return write_batch_.GetWriteBatch();
  }
}

void TransactionBaseImpl::ReleaseSnapshot(const Snapshot* snapshot, DB* db) {
  if (snapshot != nullptr) {
    ROCKS_LOG_DETAILS(dbimpl_->immutable_db_options().info_log,
                      "ReleaseSnapshot %" PRIu64 " Set",
                      snapshot->GetSequenceNumber());
    db->ReleaseSnapshot(snapshot);
  }
}

void TransactionBaseImpl::UndoGetForUpdate(ColumnFamilyHandle* column_family,
                                           const Slice& key) {
  PointLockRequest r;
  r.column_family_id = GetColumnFamilyID(column_family);
  r.key = key.ToString();
  r.read_only = true;

  bool can_untrack = false;
  if (save_points_ != nullptr && !save_points_->empty()) {
    // If there is no GetForUpdate of the key in this save point,
    // then cannot untrack from the global lock tracker.
    UntrackStatus s = save_points_->top().new_locks_->Untrack(r);
    can_untrack = (s != UntrackStatus::NOT_TRACKED);
  } else {
    // No save point, so can untrack from the global lock tracker.
    can_untrack = true;
  }

  if (can_untrack) {
    // If erased from the global tracker, then can unlock the key.
    UntrackStatus s = tracked_locks_->Untrack(r);
    bool can_unlock = (s == UntrackStatus::REMOVED);
    if (can_unlock) {
      UnlockGetForUpdate(column_family, key);
    }
  }
}

Status TransactionBaseImpl::RebuildFromWriteBatch(WriteBatch* src_batch) {
  struct IndexedWriteBatchBuilder : public WriteBatch::Handler {
    Transaction* txn_;
    DBImpl* db_;
    IndexedWriteBatchBuilder(Transaction* txn, DBImpl* db)
        : txn_(txn), db_(db) {
      assert(dynamic_cast<TransactionBaseImpl*>(txn_) != nullptr);
    }

    Status PutCF(uint32_t cf, const Slice& key, const Slice& val) override {
      return txn_->Put(db_->GetColumnFamilyHandle(cf), key, val);
    }

    Status DeleteCF(uint32_t cf, const Slice& key) override {
      return txn_->Delete(db_->GetColumnFamilyHandle(cf), key);
    }

    Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
      return txn_->SingleDelete(db_->GetColumnFamilyHandle(cf), key);
    }

    Status MergeCF(uint32_t cf, const Slice& key, const Slice& val) override {
      return txn_->Merge(db_->GetColumnFamilyHandle(cf), key, val);
    }

    // this is used for reconstructing prepared transactions upon
    // recovery. there should not be any meta markers in the batches
    // we are processing.
    Status MarkBeginPrepare(bool) override { return Status::InvalidArgument(); }

    Status MarkEndPrepare(const Slice&) override {
      return Status::InvalidArgument();
    }

    Status MarkCommit(const Slice&) override {
      return Status::InvalidArgument();
    }

    Status MarkRollback(const Slice&) override {
      return Status::InvalidArgument();
    }
  };

  IndexedWriteBatchBuilder copycat(this, dbimpl_);
  return src_batch->Iterate(&copycat);
}

WriteBatch* TransactionBaseImpl::GetCommitTimeWriteBatch() {
  return &commit_time_batch_;
}
}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
