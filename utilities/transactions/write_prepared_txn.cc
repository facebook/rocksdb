//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/write_prepared_txn.h"

#include <map>

#include "db/column_family.h"
#include "db/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/transaction_db.h"
#include "utilities/transactions/pessimistic_transaction.h"
#include "utilities/transactions/write_prepared_txn_db.h"

namespace rocksdb {

struct WriteOptions;

WritePreparedTxn::WritePreparedTxn(WritePreparedTxnDB* txn_db,
                                   const WriteOptions& write_options,
                                   const TransactionOptions& txn_options)
    : PessimisticTransaction(txn_db, write_options, txn_options),
      wpt_db_(txn_db) {
  GetWriteBatch()->DisableDuplicateMergeKeys();
}

Status WritePreparedTxn::Get(const ReadOptions& read_options,
                             ColumnFamilyHandle* column_family,
                             const Slice& key, PinnableSlice* pinnable_val) {
  auto snapshot = read_options.snapshot;
  auto snap_seq =
      snapshot != nullptr ? snapshot->GetSequenceNumber() : kMaxSequenceNumber;

  WritePreparedTxnReadCallback callback(wpt_db_, snap_seq);
  return write_batch_.GetFromBatchAndDB(db_, read_options, column_family, key,
                                        pinnable_val, &callback);
}

Iterator* WritePreparedTxn::GetIterator(const ReadOptions& options) {
  // Make sure to get iterator from WritePrepareTxnDB, not the root db.
  Iterator* db_iter = wpt_db_->NewIterator(options);
  assert(db_iter);

  return write_batch_.NewIteratorWithBase(db_iter);
}

Iterator* WritePreparedTxn::GetIterator(const ReadOptions& options,
                                        ColumnFamilyHandle* column_family) {
  // Make sure to get iterator from WritePrepareTxnDB, not the root db.
  Iterator* db_iter = wpt_db_->NewIterator(options, column_family);
  assert(db_iter);

  return write_batch_.NewIteratorWithBase(db_iter);
}

Status WritePreparedTxn::PrepareInternal() {
  WriteOptions write_options = write_options_;
  write_options.disableWAL = false;
  const bool WRITE_AFTER_COMMIT = true;
  WriteBatchInternal::MarkEndPrepare(GetWriteBatch()->GetWriteBatch(), name_,
                                     !WRITE_AFTER_COMMIT);
  const bool DISABLE_MEMTABLE = true;
  uint64_t seq_used = kMaxSequenceNumber;
  bool collapsed = GetWriteBatch()->Collapse();
  if (collapsed) {
    ROCKS_LOG_WARN(db_impl_->immutable_db_options().info_log,
                   "Collapse overhead due to duplicate keys");
  }
  Status s =
      db_impl_->WriteImpl(write_options, GetWriteBatch()->GetWriteBatch(),
                          /*callback*/ nullptr, &log_number_, /*log ref*/ 0,
                          !DISABLE_MEMTABLE, &seq_used);
  assert(seq_used != kMaxSequenceNumber);
  auto prepare_seq = seq_used;
  SetId(prepare_seq);
  wpt_db_->AddPrepared(prepare_seq);
  return s;
}

Status WritePreparedTxn::CommitWithoutPrepareInternal() {
  bool collapsed = GetWriteBatch()->Collapse();
  if (collapsed) {
    ROCKS_LOG_WARN(db_impl_->immutable_db_options().info_log,
                   "Collapse overhead due to duplicate keys");
  }
  return CommitBatchInternal(GetWriteBatch()->GetWriteBatch());
}

Status WritePreparedTxn::CommitBatchInternal(WriteBatch* batch) {
  // TODO(myabandeh): handle the duplicate keys in the batch
  // In the absence of Prepare markers, use Noop as a batch separator
  WriteBatchInternal::InsertNoop(batch);
  const bool DISABLE_MEMTABLE = true;
  const uint64_t no_log_ref = 0;
  uint64_t seq_used = kMaxSequenceNumber;
  auto s = db_impl_->WriteImpl(write_options_, batch, nullptr, nullptr,
                               no_log_ref, !DISABLE_MEMTABLE, &seq_used);
  assert(seq_used != kMaxSequenceNumber);
  uint64_t& prepare_seq = seq_used;
  // Commit the batch by writing an empty batch to the queue that will release
  // the commit sequence number to readers.
  WritePreparedCommitEntryPreReleaseCallback update_commit_map(
      wpt_db_, db_impl_, prepare_seq);
  WriteBatch empty_batch;
  empty_batch.PutLogData(Slice());
  // In the absence of Prepare markers, use Noop as a batch separator
  WriteBatchInternal::InsertNoop(&empty_batch);
  s = db_impl_->WriteImpl(write_options_, &empty_batch, nullptr, nullptr,
                          no_log_ref, DISABLE_MEMTABLE, &seq_used,
                          &update_commit_map);
  assert(seq_used != kMaxSequenceNumber);
  return s;
}

Status WritePreparedTxn::CommitInternal() {
  // We take the commit-time batch and append the Commit marker.
  // The Memtable will ignore the Commit marker in non-recovery mode
  WriteBatch* working_batch = GetCommitTimeWriteBatch();
  const bool empty = working_batch->Count() == 0;
  WriteBatchInternal::MarkCommit(working_batch, name_);

  const bool for_recovery = use_only_the_last_commit_time_batch_for_recovery_;
  if (!empty && for_recovery) {
    // When not writing to memtable, we can still cache the latest write batch.
    // The cached batch will be written to memtable in WriteRecoverableState
    // during FlushMemTable
    WriteBatchInternal::SetAsLastestPersistentState(working_batch);
  }

  // TODO(myabandeh): Reject a commit request if AddCommitted cannot encode
  // commit_seq. This happens if prep_seq <<< commit_seq.
  auto prepare_seq = GetId();
  const bool includes_data = !empty && !for_recovery;
  WritePreparedCommitEntryPreReleaseCallback update_commit_map(
      wpt_db_, db_impl_, prepare_seq, includes_data);
  const bool disable_memtable = !includes_data;
  uint64_t seq_used = kMaxSequenceNumber;
  // Since the prepared batch is directly written to memtable, there is already
  // a connection between the memtable and its WAL, so there is no need to
  // redundantly reference the log that contains the prepared data.
  const uint64_t zero_log_number = 0ull;
  auto s = db_impl_->WriteImpl(write_options_, working_batch, nullptr, nullptr,
                               zero_log_number, disable_memtable, &seq_used,
                               &update_commit_map);
  assert(seq_used != kMaxSequenceNumber);
  return s;
}

Status WritePreparedTxn::RollbackInternal() {
  WriteBatch rollback_batch;
  assert(GetId() != kMaxSequenceNumber);
  assert(GetId() > 0);
  // In WritePrepared, the txn is is the same as prepare seq
  auto last_visible_txn = GetId() - 1;
  struct RollbackWriteBatchBuilder : public WriteBatch::Handler {
    DBImpl* db_;
    ReadOptions roptions;
    WritePreparedTxnReadCallback callback;
    WriteBatch* rollback_batch_;
    RollbackWriteBatchBuilder(DBImpl* db, WritePreparedTxnDB* wpt_db,
                              SequenceNumber snap_seq, WriteBatch* dst_batch)
        : db_(db), callback(wpt_db, snap_seq), rollback_batch_(dst_batch) {}

    Status Rollback(uint32_t cf, const Slice& key) {
      PinnableSlice pinnable_val;
      bool not_used;
      auto cf_handle = db_->GetColumnFamilyHandle(cf);
      auto s = db_->GetImpl(roptions, cf_handle, key, &pinnable_val, &not_used,
                            &callback);
      assert(s.ok() || s.IsNotFound());
      if (s.ok()) {
        s = rollback_batch_->Put(cf_handle, key, pinnable_val);
        assert(s.ok());
      } else if (s.IsNotFound()) {
        // There has been no readable value before txn. By adding a delete we
        // make sure that there will be none afterwards either.
        s = rollback_batch_->Delete(cf_handle, key);
        assert(s.ok());
      } else {
        // Unexpected status. Return it to the user.
      }
      return s;
    }

    Status PutCF(uint32_t cf, const Slice& key, const Slice& val) override {
      return Rollback(cf, key);
    }

    Status DeleteCF(uint32_t cf, const Slice& key) override {
      return Rollback(cf, key);
    }

    Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
      return Rollback(cf, key);
    }

    Status MergeCF(uint32_t cf, const Slice& key, const Slice& val) override {
      return Rollback(cf, key);
    }

    Status MarkNoop(bool) override { return Status::OK(); }
    Status MarkBeginPrepare() override { return Status::OK(); }
    Status MarkEndPrepare(const Slice&) override { return Status::OK(); }
    Status MarkCommit(const Slice&) override { return Status::OK(); }
    Status MarkRollback(const Slice&) override {
      return Status::InvalidArgument();
    }

   protected:
    virtual bool WriteAfterCommit() const override { return false; }
  } rollback_handler(db_impl_, wpt_db_, last_visible_txn, &rollback_batch);
  auto s = GetWriteBatch()->GetWriteBatch()->Iterate(&rollback_handler);
  assert(s.ok());
  if (!s.ok()) {
    return s;
  }
  // The Rollback marker will be used as a batch separator
  WriteBatchInternal::MarkRollback(&rollback_batch, name_);
  const bool DISABLE_MEMTABLE = true;
  const uint64_t no_log_ref = 0;
  uint64_t seq_used = kMaxSequenceNumber;
  s = db_impl_->WriteImpl(write_options_, &rollback_batch, nullptr, nullptr,
                          no_log_ref, !DISABLE_MEMTABLE, &seq_used);
  assert(seq_used != kMaxSequenceNumber);
  if (!s.ok()) {
    return s;
  }
  uint64_t& prepare_seq = seq_used;
  // Commit the batch by writing an empty batch to the queue that will release
  // the commit sequence number to readers.
  WritePreparedCommitEntryPreReleaseCallback update_commit_map(
      wpt_db_, db_impl_, prepare_seq);
  WriteBatch empty_batch;
  empty_batch.PutLogData(Slice());
  // In the absence of Prepare markers, use Noop as a batch separator
  WriteBatchInternal::InsertNoop(&empty_batch);
  s = db_impl_->WriteImpl(write_options_, &empty_batch, nullptr, nullptr,
                          no_log_ref, DISABLE_MEMTABLE, &seq_used,
                          &update_commit_map);
  assert(seq_used != kMaxSequenceNumber);
  // Mark the txn as rolled back
  uint64_t& rollback_seq = seq_used;
  wpt_db_->RollbackPrepared(GetId(), rollback_seq);

  return s;
}

Status WritePreparedTxn::ValidateSnapshot(ColumnFamilyHandle* column_family,
                                          const Slice& key,
                                          SequenceNumber* tracked_at_seq) {
  assert(snapshot_);

  SequenceNumber snap_seq = snapshot_->GetSequenceNumber();
  // tracked_at_seq is either max or the last snapshot with which this key was
  // trackeed so there is no need to apply the IsInSnapshot to this comparison
  // here as tracked_at_seq is not a prepare seq.
  if (*tracked_at_seq <= snap_seq) {
    // If the key has been previous validated at a sequence number earlier
    // than the curent snapshot's sequence number, we already know it has not
    // been modified.
    return Status::OK();
  }

  *tracked_at_seq = snap_seq;

  ColumnFamilyHandle* cfh =
      column_family ? column_family : db_impl_->DefaultColumnFamily();

  WritePreparedTxnReadCallback snap_checker(wpt_db_, snap_seq);
  return TransactionUtil::CheckKeyForConflicts(db_impl_, cfh, key.ToString(),
                                               snap_seq, false /* cache_only */,
                                               &snap_checker);
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
