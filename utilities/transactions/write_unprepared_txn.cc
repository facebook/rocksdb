//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/write_unprepared_txn.h"
#include "db/db_impl/db_impl.h"
#include "util/cast_util.h"
#include "utilities/transactions/write_unprepared_txn_db.h"

namespace rocksdb {

bool WriteUnpreparedTxnReadCallback::IsVisibleFullCheck(SequenceNumber seq) {
  // Since unprep_seqs maps prep_seq => prepare_batch_cnt, to check if seq is
  // in unprep_seqs, we have to check if seq is equal to prep_seq or any of
  // the prepare_batch_cnt seq nums after it.
  //
  // TODO(lth): Can be optimized with std::lower_bound if unprep_seqs is
  // large.
  for (const auto& it : unprep_seqs_) {
    if (it.first <= seq && seq < it.first + it.second) {
      return true;
    }
  }

  bool snap_released = false;
  auto ret =
      db_->IsInSnapshot(seq, wup_snapshot_, min_uncommitted_, &snap_released);
  assert(!snap_released || backed_by_snapshot_ == kUnbackedByDBSnapshot);
  snap_released_ |= snap_released;
  return ret;
}

WriteUnpreparedTxn::WriteUnpreparedTxn(WriteUnpreparedTxnDB* txn_db,
                                       const WriteOptions& write_options,
                                       const TransactionOptions& txn_options)
    : WritePreparedTxn(txn_db, write_options, txn_options),
      wupt_db_(txn_db),
      last_log_number_(0),
      recovered_txn_(false),
      largest_validated_seq_(0) {
  if (txn_options.write_batch_flush_threshold < 0) {
    write_batch_flush_threshold_ =
        txn_db_impl_->GetTxnDBOptions().default_write_batch_flush_threshold;
  } else {
    write_batch_flush_threshold_ = txn_options.write_batch_flush_threshold;
  }
}

WriteUnpreparedTxn::~WriteUnpreparedTxn() {
  if (!unprep_seqs_.empty()) {
    assert(log_number_ > 0);
    assert(GetId() > 0);
    assert(!name_.empty());

    // We should rollback regardless of GetState, but some unit tests that
    // test crash recovery run the destructor assuming that rollback does not
    // happen, so that rollback during recovery can be exercised.
    if (GetState() == STARTED || GetState() == LOCKS_STOLEN) {
      auto s = RollbackInternal();
      assert(s.ok());
      if (!s.ok()) {
        ROCKS_LOG_FATAL(
            wupt_db_->info_log_,
            "Rollback of WriteUnprepared transaction failed in destructor: %s",
            s.ToString().c_str());
      }
      dbimpl_->logs_with_prep_tracker()->MarkLogAsHavingPrepSectionFlushed(
          log_number_);
    }
  }

  // Call tracked_keys_.clear() so that ~PessimisticTransaction does not
  // try to unlock keys for recovered transactions.
  if (recovered_txn_) {
    tracked_keys_.clear();
  }
}

void WriteUnpreparedTxn::Initialize(const TransactionOptions& txn_options) {
  PessimisticTransaction::Initialize(txn_options);
  if (txn_options.write_batch_flush_threshold < 0) {
    write_batch_flush_threshold_ =
        txn_db_impl_->GetTxnDBOptions().default_write_batch_flush_threshold;
  } else {
    write_batch_flush_threshold_ = txn_options.write_batch_flush_threshold;
  }

  unprep_seqs_.clear();
  flushed_save_points_.reset(nullptr);
  unflushed_save_points_.reset(nullptr);
  recovered_txn_ = false;
  largest_validated_seq_ = 0;
  assert(active_iterators_.empty());
  active_iterators_.clear();
}

Status WriteUnpreparedTxn::HandleWrite(std::function<Status()> do_write) {
  Status s;
  if (active_iterators_.empty()) {
    s = MaybeFlushWriteBatchToDB();
    if (!s.ok()) {
      return s;
    }
  }
  s = do_write();
  if (s.ok()) {
    if (snapshot_) {
      largest_validated_seq_ =
          std::max(largest_validated_seq_, snapshot_->GetSequenceNumber());
    } else {
      // TODO(lth): We should use the same number as tracked_at_seq in TryLock,
      // because what is actually being tracked is the sequence number at which
      // this key was locked at.
      largest_validated_seq_ = db_impl_->GetLastPublishedSequence();
    }
  }
  return s;
}

Status WriteUnpreparedTxn::Put(ColumnFamilyHandle* column_family,
                               const Slice& key, const Slice& value,
                               const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::Put(column_family, key, value, assume_tracked);
  });
}

Status WriteUnpreparedTxn::Put(ColumnFamilyHandle* column_family,
                               const SliceParts& key, const SliceParts& value,
                               const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::Put(column_family, key, value, assume_tracked);
  });
}

Status WriteUnpreparedTxn::Merge(ColumnFamilyHandle* column_family,
                                 const Slice& key, const Slice& value,
                                 const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::Merge(column_family, key, value,
                                      assume_tracked);
  });
}

Status WriteUnpreparedTxn::Delete(ColumnFamilyHandle* column_family,
                                  const Slice& key, const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::Delete(column_family, key, assume_tracked);
  });
}

Status WriteUnpreparedTxn::Delete(ColumnFamilyHandle* column_family,
                                  const SliceParts& key,
                                  const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::Delete(column_family, key, assume_tracked);
  });
}

Status WriteUnpreparedTxn::SingleDelete(ColumnFamilyHandle* column_family,
                                        const Slice& key,
                                        const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::SingleDelete(column_family, key,
                                             assume_tracked);
  });
}

Status WriteUnpreparedTxn::SingleDelete(ColumnFamilyHandle* column_family,
                                        const SliceParts& key,
                                        const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::SingleDelete(column_family, key,
                                             assume_tracked);
  });
}

// WriteUnpreparedTxn::RebuildFromWriteBatch is only called on recovery. For
// WriteUnprepared, the write batches have already been written into the
// database during WAL replay, so all we have to do is just to "retrack" the key
// so that rollbacks are possible.
//
// Calling TryLock instead of TrackKey is also possible, but as an optimization,
// recovered transactions do not hold locks on their keys. This follows the
// implementation in PessimisticTransactionDB::Initialize where we set
// skip_concurrency_control to true.
Status WriteUnpreparedTxn::RebuildFromWriteBatch(WriteBatch* wb) {
  struct TrackKeyHandler : public WriteBatch::Handler {
    WriteUnpreparedTxn* txn_;
    bool rollback_merge_operands_;

    TrackKeyHandler(WriteUnpreparedTxn* txn, bool rollback_merge_operands)
        : txn_(txn), rollback_merge_operands_(rollback_merge_operands) {}

    Status PutCF(uint32_t cf, const Slice& key, const Slice&) override {
      txn_->TrackKey(cf, key.ToString(), kMaxSequenceNumber,
                     false /* read_only */, true /* exclusive */);
      return Status::OK();
    }

    Status DeleteCF(uint32_t cf, const Slice& key) override {
      txn_->TrackKey(cf, key.ToString(), kMaxSequenceNumber,
                     false /* read_only */, true /* exclusive */);
      return Status::OK();
    }

    Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
      txn_->TrackKey(cf, key.ToString(), kMaxSequenceNumber,
                     false /* read_only */, true /* exclusive */);
      return Status::OK();
    }

    Status MergeCF(uint32_t cf, const Slice& key, const Slice&) override {
      if (rollback_merge_operands_) {
        txn_->TrackKey(cf, key.ToString(), kMaxSequenceNumber,
                       false /* read_only */, true /* exclusive */);
      }
      return Status::OK();
    }

    // Recovered batches do not contain 2PC markers.
    Status MarkBeginPrepare(bool) override { return Status::InvalidArgument(); }

    Status MarkEndPrepare(const Slice&) override {
      return Status::InvalidArgument();
    }

    Status MarkNoop(bool) override { return Status::InvalidArgument(); }

    Status MarkCommit(const Slice&) override {
      return Status::InvalidArgument();
    }

    Status MarkRollback(const Slice&) override {
      return Status::InvalidArgument();
    }
  };

  TrackKeyHandler handler(this,
                          wupt_db_->txn_db_options_.rollback_merge_operands);
  return wb->Iterate(&handler);
}

Status WriteUnpreparedTxn::MaybeFlushWriteBatchToDB() {
  const bool kPrepared = true;
  Status s;
  if (write_batch_flush_threshold_ > 0 &&
      write_batch_.GetWriteBatch()->Count() > 0 &&
      write_batch_.GetDataSize() >
          static_cast<size_t>(write_batch_flush_threshold_)) {
    assert(GetState() != PREPARED);
    s = FlushWriteBatchToDB(!kPrepared);
  }
  return s;
}

Status WriteUnpreparedTxn::FlushWriteBatchToDB(bool prepared) {
  // If the current write batch contains savepoints, then some special handling
  // is required so that RollbackToSavepoint can work.
  //
  // RollbackToSavepoint is not supported after Prepare() is called, so only do
  // this for unprepared batches.
  if (!prepared && unflushed_save_points_ != nullptr &&
      !unflushed_save_points_->empty()) {
    return FlushWriteBatchWithSavePointToDB();
  }

  return FlushWriteBatchToDBInternal(prepared);
}

Status WriteUnpreparedTxn::FlushWriteBatchToDBInternal(bool prepared) {
  if (name_.empty()) {
    assert(!prepared);
#ifndef NDEBUG
    static std::atomic_ullong autogen_id{0};
    // To avoid changing all tests to call SetName, just autogenerate one.
    if (wupt_db_->txn_db_options_.autogenerate_name) {
      SetName(std::string("autoxid") + ToString(autogen_id.fetch_add(1)));
    } else
#endif
    {
      return Status::InvalidArgument("Cannot write to DB without SetName.");
    }
  }

  // TODO(lth): Reduce duplicate code with WritePrepared prepare logic.
  WriteOptions write_options = write_options_;
  write_options.disableWAL = false;
  const bool WRITE_AFTER_COMMIT = true;
  const bool first_prepare_batch = log_number_ == 0;
  // MarkEndPrepare will change Noop marker to the appropriate marker.
  WriteBatchInternal::MarkEndPrepare(GetWriteBatch()->GetWriteBatch(), name_,
                                     !WRITE_AFTER_COMMIT, !prepared);
  // For each duplicate key we account for a new sub-batch
  prepare_batch_cnt_ = GetWriteBatch()->SubBatchCnt();
  // AddPrepared better to be called in the pre-release callback otherwise there
  // is a non-zero chance of max advancing prepare_seq and readers assume the
  // data as committed.
  // Also having it in the PreReleaseCallback allows in-order addition of
  // prepared entries to PreparedHeap and hence enables an optimization. Refer
  // to SmallestUnCommittedSeq for more details.
  AddPreparedCallback add_prepared_callback(
      wpt_db_, db_impl_, prepare_batch_cnt_,
      db_impl_->immutable_db_options().two_write_queues, first_prepare_batch);
  const bool DISABLE_MEMTABLE = true;
  uint64_t seq_used = kMaxSequenceNumber;
  // log_number_ should refer to the oldest log containing uncommitted data
  // from the current transaction. This means that if log_number_ is set,
  // WriteImpl should not overwrite that value, so set log_used to nullptr if
  // log_number_ is already set.
  auto s =
      db_impl_->WriteImpl(write_options, GetWriteBatch()->GetWriteBatch(),
                          /*callback*/ nullptr, &last_log_number_, /*log ref*/
                          0, !DISABLE_MEMTABLE, &seq_used, prepare_batch_cnt_,
                          &add_prepared_callback);
  if (log_number_ == 0) {
    log_number_ = last_log_number_;
  }
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  auto prepare_seq = seq_used;

  // Only call SetId if it hasn't been set yet.
  if (GetId() == 0) {
    SetId(prepare_seq);
  }
  // unprep_seqs_ will also contain prepared seqnos since they are treated in
  // the same way in the prepare/commit callbacks. See the comment on the
  // definition of unprep_seqs_.
  unprep_seqs_[prepare_seq] = prepare_batch_cnt_;

  // Reset transaction state.
  if (!prepared) {
    prepare_batch_cnt_ = 0;
    const bool kClear = true;
    TransactionBaseImpl::InitWriteBatch(kClear);
  }

  return s;
}

Status WriteUnpreparedTxn::FlushWriteBatchWithSavePointToDB() {
  assert(unflushed_save_points_ != nullptr &&
         unflushed_save_points_->size() > 0);
  assert(save_points_ != nullptr && save_points_->size() > 0);
  assert(save_points_->size() >= unflushed_save_points_->size());

  // Handler class for creating an unprepared batch from a savepoint.
  struct SavePointBatchHandler : public WriteBatch::Handler {
    WriteBatchWithIndex* wb_;
    const std::map<uint32_t, ColumnFamilyHandle*>& handles_;

    SavePointBatchHandler(
        WriteBatchWithIndex* wb,
        const std::map<uint32_t, ColumnFamilyHandle*>& handles)
        : wb_(wb), handles_(handles) {}

    Status PutCF(uint32_t cf, const Slice& key, const Slice& value) override {
      return wb_->Put(handles_.at(cf), key, value);
    }

    Status DeleteCF(uint32_t cf, const Slice& key) override {
      return wb_->Delete(handles_.at(cf), key);
    }

    Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
      return wb_->SingleDelete(handles_.at(cf), key);
    }

    Status MergeCF(uint32_t cf, const Slice& key, const Slice& value) override {
      return wb_->Merge(handles_.at(cf), key, value);
    }

    // The only expected 2PC marker is the initial Noop marker.
    Status MarkNoop(bool empty_batch) override {
      return empty_batch ? Status::OK() : Status::InvalidArgument();
    }

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

  // The comparator of the default cf is passed in, similar to the
  // initialization of TransactionBaseImpl::write_batch_. This comparator is
  // only used if the write batch encounters an invalid cf id, and falls back to
  // this comparator.
  WriteBatchWithIndex wb(wpt_db_->DefaultColumnFamily()->GetComparator(), 0,
                         true, 0);
  // Swap with write_batch_ so that wb contains the complete write batch. The
  // actual write batch that will be flushed to DB will be built in
  // write_batch_, and will be read by FlushWriteBatchToDBInternal.
  std::swap(wb, write_batch_);
  TransactionBaseImpl::InitWriteBatch();

  size_t prev_boundary = WriteBatchInternal::kHeader;
  const bool kPrepared = true;
  for (size_t i = 0; i < unflushed_save_points_->size() + 1; i++) {
    bool trailing_batch = i == unflushed_save_points_->size();
    SavePointBatchHandler sp_handler(&write_batch_,
                                     *wupt_db_->GetCFHandleMap().get());
    size_t curr_boundary = trailing_batch ? wb.GetWriteBatch()->GetDataSize()
                                          : (*unflushed_save_points_)[i];

    // Construct the partial write batch up to the savepoint.
    //
    // Theoretically, a memcpy between the write batches should be sufficient
    // since the rewriting into the batch should produce the exact same byte
    // representation. Rebuilding the WriteBatchWithIndex index is still
    // necessary though, and would imply doing two passes over the batch though.
    Status s = WriteBatchInternal::Iterate(wb.GetWriteBatch(), &sp_handler,
                                           prev_boundary, curr_boundary);
    if (!s.ok()) {
      return s;
    }

    if (write_batch_.GetWriteBatch()->Count() > 0) {
      // Flush the write batch.
      s = FlushWriteBatchToDBInternal(!kPrepared);
      if (!s.ok()) {
        return s;
      }
    }

    if (!trailing_batch) {
      if (flushed_save_points_ == nullptr) {
        flushed_save_points_.reset(
            new autovector<WriteUnpreparedTxn::SavePoint>());
      }
      flushed_save_points_->emplace_back(
          unprep_seqs_, new ManagedSnapshot(db_impl_, wupt_db_->GetSnapshot()));
    }

    prev_boundary = curr_boundary;
    const bool kClear = true;
    TransactionBaseImpl::InitWriteBatch(kClear);
  }

  unflushed_save_points_->clear();
  return Status::OK();
}

Status WriteUnpreparedTxn::PrepareInternal() {
  const bool kPrepared = true;
  return FlushWriteBatchToDB(kPrepared);
}

Status WriteUnpreparedTxn::CommitWithoutPrepareInternal() {
  if (unprep_seqs_.empty()) {
    assert(log_number_ == 0);
    assert(GetId() == 0);
    return WritePreparedTxn::CommitWithoutPrepareInternal();
  }

  // TODO(lth): We should optimize commit without prepare to not perform
  // a prepare under the hood.
  auto s = PrepareInternal();
  if (!s.ok()) {
    return s;
  }
  return CommitInternal();
}

Status WriteUnpreparedTxn::CommitInternal() {
  // TODO(lth): Reduce duplicate code with WritePrepared commit logic.

  // We take the commit-time batch and append the Commit marker.  The Memtable
  // will ignore the Commit marker in non-recovery mode
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

  const bool includes_data = !empty && !for_recovery;
  size_t commit_batch_cnt = 0;
  if (UNLIKELY(includes_data)) {
    ROCKS_LOG_WARN(db_impl_->immutable_db_options().info_log,
                   "Duplicate key overhead");
    SubBatchCounter counter(*wpt_db_->GetCFComparatorMap());
    auto s = working_batch->Iterate(&counter);
    assert(s.ok());
    commit_batch_cnt = counter.BatchCount();
  }
  const bool disable_memtable = !includes_data;
  const bool do_one_write =
      !db_impl_->immutable_db_options().two_write_queues || disable_memtable;

  WriteUnpreparedCommitEntryPreReleaseCallback update_commit_map(
      wpt_db_, db_impl_, unprep_seqs_, commit_batch_cnt);
  const bool kFirstPrepareBatch = true;
  AddPreparedCallback add_prepared_callback(
      wpt_db_, db_impl_, commit_batch_cnt,
      db_impl_->immutable_db_options().two_write_queues, !kFirstPrepareBatch);
  PreReleaseCallback* pre_release_callback;
  if (do_one_write) {
    pre_release_callback = &update_commit_map;
  } else {
    pre_release_callback = &add_prepared_callback;
  }
  uint64_t seq_used = kMaxSequenceNumber;
  // Since the prepared batch is directly written to memtable, there is
  // already a connection between the memtable and its WAL, so there is no
  // need to redundantly reference the log that contains the prepared data.
  const uint64_t zero_log_number = 0ull;
  size_t batch_cnt = UNLIKELY(commit_batch_cnt) ? commit_batch_cnt : 1;
  auto s = db_impl_->WriteImpl(write_options_, working_batch, nullptr, nullptr,
                               zero_log_number, disable_memtable, &seq_used,
                               batch_cnt, pre_release_callback);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  const SequenceNumber commit_batch_seq = seq_used;
  if (LIKELY(do_one_write || !s.ok())) {
    if (LIKELY(s.ok())) {
      // Note RemovePrepared should be called after WriteImpl that publishsed
      // the seq. Otherwise SmallestUnCommittedSeq optimization breaks.
      for (const auto& seq : unprep_seqs_) {
        wpt_db_->RemovePrepared(seq.first, seq.second);
      }
    }
    if (UNLIKELY(!do_one_write)) {
      wpt_db_->RemovePrepared(commit_batch_seq, commit_batch_cnt);
    }
    unprep_seqs_.clear();
    flushed_save_points_.reset(nullptr);
    unflushed_save_points_.reset(nullptr);
    return s;
  }  // else do the 2nd write to publish seq

  // Populate unprep_seqs_ with commit_batch_seq, since we treat data in the
  // commit write batch as just another "unprepared" batch. This will also
  // update the unprep_seqs_ in the update_commit_map callback.
  unprep_seqs_[commit_batch_seq] = commit_batch_cnt;

  // Note: the 2nd write comes with a performance penality. So if we have too
  // many of commits accompanied with ComitTimeWriteBatch and yet we cannot
  // enable use_only_the_last_commit_time_batch_for_recovery_ optimization,
  // two_write_queues should be disabled to avoid many additional writes here.

  // Update commit map only from the 2nd queue
  WriteBatch empty_batch;
  empty_batch.PutLogData(Slice());
  // In the absence of Prepare markers, use Noop as a batch separator
  WriteBatchInternal::InsertNoop(&empty_batch);
  const bool DISABLE_MEMTABLE = true;
  const size_t ONE_BATCH = 1;
  const uint64_t NO_REF_LOG = 0;
  s = db_impl_->WriteImpl(write_options_, &empty_batch, nullptr, nullptr,
                          NO_REF_LOG, DISABLE_MEMTABLE, &seq_used, ONE_BATCH,
                          &update_commit_map);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  // Note RemovePrepared should be called after WriteImpl that publishsed the
  // seq. Otherwise SmallestUnCommittedSeq optimization breaks.
  for (const auto& seq : unprep_seqs_) {
    wpt_db_->RemovePrepared(seq.first, seq.second);
  }
  unprep_seqs_.clear();
  flushed_save_points_.reset(nullptr);
  unflushed_save_points_.reset(nullptr);
  return s;
}

Status WriteUnpreparedTxn::RollbackInternal() {
  // TODO(lth): Reduce duplicate code with WritePrepared rollback logic.
  WriteBatchWithIndex rollback_batch(
      wpt_db_->DefaultColumnFamily()->GetComparator(), 0, true, 0);
  assert(GetId() != kMaxSequenceNumber);
  assert(GetId() > 0);
  Status s;
  const auto& cf_map = *wupt_db_->GetCFHandleMap();
  auto read_at_seq = kMaxSequenceNumber;
  ReadOptions roptions;
  // to prevent callback's seq to be overrriden inside DBImpk::Get
  roptions.snapshot = wpt_db_->GetMaxSnapshot();
  // Note that we do not use WriteUnpreparedTxnReadCallback because we do not
  // need to read our own writes when reading prior versions of the key for
  // rollback.
  const auto& tracked_keys = GetTrackedKeys();
  WritePreparedTxnReadCallback callback(wpt_db_, read_at_seq);
  for (const auto& cfkey : tracked_keys) {
    const auto cfid = cfkey.first;
    const auto& keys = cfkey.second;
    for (const auto& pair : keys) {
      const auto& key = pair.first;
      const auto& cf_handle = cf_map.at(cfid);
      PinnableSlice pinnable_val;
      bool not_used;
      DBImpl::GetImplOptions get_impl_options;
      get_impl_options.column_family = cf_handle;
      get_impl_options.value = &pinnable_val;
      get_impl_options.value_found = &not_used;
      get_impl_options.callback = &callback;
      s = db_impl_->GetImpl(roptions, key, get_impl_options);

      if (s.ok()) {
        s = rollback_batch.Put(cf_handle, key, pinnable_val);
        assert(s.ok());
      } else if (s.IsNotFound()) {
        s = rollback_batch.Delete(cf_handle, key);
        assert(s.ok());
      } else {
        return s;
      }
    }
  }

  // The Rollback marker will be used as a batch separator
  WriteBatchInternal::MarkRollback(rollback_batch.GetWriteBatch(), name_);
  bool do_one_write = !db_impl_->immutable_db_options().two_write_queues;
  const bool DISABLE_MEMTABLE = true;
  const uint64_t NO_REF_LOG = 0;
  uint64_t seq_used = kMaxSequenceNumber;
  // TODO(lth): We write rollback batch all in a single batch here, but this
  // should be subdivded into multiple batches as well. In phase 2, when key
  // sets are read from WAL, this will happen naturally.
  const size_t ONE_BATCH = 1;
  // We commit the rolled back prepared batches. ALthough this is
  // counter-intuitive, i) it is safe to do so, since the prepared batches are
  // already canceled out by the rollback batch, ii) adding the commit entry to
  // CommitCache will allow us to benefit from the existing mechanism in
  // CommitCache that keeps an entry evicted due to max advance and yet overlaps
  // with a live snapshot around so that the live snapshot properly skips the
  // entry even if its prepare seq is lower than max_evicted_seq_.
  WriteUnpreparedCommitEntryPreReleaseCallback update_commit_map(
      wpt_db_, db_impl_, unprep_seqs_, ONE_BATCH);
  // Note: the rollback batch does not need AddPrepared since it is written to
  // DB in one shot. min_uncommitted still works since it requires capturing
  // data that is written to DB but not yet committed, while the roolback
  // batch commits with PreReleaseCallback.
  s = db_impl_->WriteImpl(write_options_, rollback_batch.GetWriteBatch(),
                          nullptr, nullptr, NO_REF_LOG, !DISABLE_MEMTABLE,
                          &seq_used, rollback_batch.SubBatchCnt(),
                          do_one_write ? &update_commit_map : nullptr);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  if (!s.ok()) {
    return s;
  }
  if (do_one_write) {
    for (const auto& seq : unprep_seqs_) {
      wpt_db_->RemovePrepared(seq.first, seq.second);
    }
    unprep_seqs_.clear();
    flushed_save_points_.reset(nullptr);
    unflushed_save_points_.reset(nullptr);
    return s;
  }  // else do the 2nd write for commit
  uint64_t& prepare_seq = seq_used;
  ROCKS_LOG_DETAILS(db_impl_->immutable_db_options().info_log,
                    "RollbackInternal 2nd write prepare_seq: %" PRIu64,
                    prepare_seq);
  // Commit the batch by writing an empty batch to the queue that will release
  // the commit sequence number to readers.
  WriteUnpreparedRollbackPreReleaseCallback update_commit_map_with_prepare(
      wpt_db_, db_impl_, unprep_seqs_, prepare_seq);
  WriteBatch empty_batch;
  empty_batch.PutLogData(Slice());
  // In the absence of Prepare markers, use Noop as a batch separator
  WriteBatchInternal::InsertNoop(&empty_batch);
  s = db_impl_->WriteImpl(write_options_, &empty_batch, nullptr, nullptr,
                          NO_REF_LOG, DISABLE_MEMTABLE, &seq_used, ONE_BATCH,
                          &update_commit_map_with_prepare);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  // Mark the txn as rolled back
  if (s.ok()) {
    for (const auto& seq : unprep_seqs_) {
      wpt_db_->RemovePrepared(seq.first, seq.second);
    }
  }

  unprep_seqs_.clear();
  flushed_save_points_.reset(nullptr);
  unflushed_save_points_.reset(nullptr);
  return s;
}

void WriteUnpreparedTxn::Clear() {
  if (!recovered_txn_) {
    txn_db_impl_->UnLock(this, &GetTrackedKeys());
  }
  unprep_seqs_.clear();
  flushed_save_points_.reset(nullptr);
  unflushed_save_points_.reset(nullptr);
  recovered_txn_ = false;
  largest_validated_seq_ = 0;
  assert(active_iterators_.empty());
  active_iterators_.clear();
  TransactionBaseImpl::Clear();
}

void WriteUnpreparedTxn::SetSavePoint() {
  assert((unflushed_save_points_ ? unflushed_save_points_->size() : 0) +
             (flushed_save_points_ ? flushed_save_points_->size() : 0) ==
         (save_points_ ? save_points_->size() : 0));
  PessimisticTransaction::SetSavePoint();
  if (unflushed_save_points_ == nullptr) {
    unflushed_save_points_.reset(new autovector<size_t>());
  }
  unflushed_save_points_->push_back(write_batch_.GetDataSize());
}

Status WriteUnpreparedTxn::RollbackToSavePoint() {
  assert((unflushed_save_points_ ? unflushed_save_points_->size() : 0) +
             (flushed_save_points_ ? flushed_save_points_->size() : 0) ==
         (save_points_ ? save_points_->size() : 0));
  if (unflushed_save_points_ != nullptr && unflushed_save_points_->size() > 0) {
    Status s = PessimisticTransaction::RollbackToSavePoint();
    assert(!s.IsNotFound());
    unflushed_save_points_->pop_back();
    return s;
  }

  if (flushed_save_points_ != nullptr && !flushed_save_points_->empty()) {
    return RollbackToSavePointInternal();
  }

  return Status::NotFound();
}

Status WriteUnpreparedTxn::RollbackToSavePointInternal() {
  Status s;

  const bool kClear = true;
  TransactionBaseImpl::InitWriteBatch(kClear);

  assert(flushed_save_points_->size() > 0);
  WriteUnpreparedTxn::SavePoint& top = flushed_save_points_->back();

  assert(save_points_ != nullptr && save_points_->size() > 0);
  const TransactionKeyMap& tracked_keys = save_points_->top().new_keys_;

  // TODO(lth): Reduce duplicate code with RollbackInternal logic.
  ReadOptions roptions;
  roptions.snapshot = top.snapshot_->snapshot();
  SequenceNumber min_uncommitted =
      static_cast_with_check<const SnapshotImpl, const Snapshot>(
          roptions.snapshot)
          ->min_uncommitted_;
  SequenceNumber snap_seq = roptions.snapshot->GetSequenceNumber();
  WriteUnpreparedTxnReadCallback callback(wupt_db_, snap_seq, min_uncommitted,
                                          top.unprep_seqs_,
                                          kBackedByDBSnapshot);
  const auto& cf_map = *wupt_db_->GetCFHandleMap();
  for (const auto& cfkey : tracked_keys) {
    const auto cfid = cfkey.first;
    const auto& keys = cfkey.second;

    for (const auto& pair : keys) {
      const auto& key = pair.first;
      const auto& cf_handle = cf_map.at(cfid);
      PinnableSlice pinnable_val;
      bool not_used;
      DBImpl::GetImplOptions get_impl_options;
      get_impl_options.column_family = cf_handle;
      get_impl_options.value = &pinnable_val;
      get_impl_options.value_found = &not_used;
      get_impl_options.callback = &callback;
      s = db_impl_->GetImpl(roptions, key, get_impl_options);

      if (s.ok()) {
        s = write_batch_.Put(cf_handle, key, pinnable_val);
        assert(s.ok());
      } else if (s.IsNotFound()) {
        s = write_batch_.Delete(cf_handle, key);
        assert(s.ok());
      } else {
        return s;
      }
    }
  }

  const bool kPrepared = true;
  s = FlushWriteBatchToDBInternal(!kPrepared);
  assert(s.ok());
  if (!s.ok()) {
    return s;
  }

  // PessimisticTransaction::RollbackToSavePoint will call also call
  // RollbackToSavepoint on write_batch_. However, write_batch_ is empty and has
  // no savepoints because this savepoint has already been flushed. Work around
  // this by setting a fake savepoint.
  write_batch_.SetSavePoint();
  s = PessimisticTransaction::RollbackToSavePoint();
  assert(s.ok());
  if (!s.ok()) {
    return s;
  }

  flushed_save_points_->pop_back();
  return s;
}

Status WriteUnpreparedTxn::PopSavePoint() {
  assert((unflushed_save_points_ ? unflushed_save_points_->size() : 0) +
             (flushed_save_points_ ? flushed_save_points_->size() : 0) ==
         (save_points_ ? save_points_->size() : 0));
  if (unflushed_save_points_ != nullptr && unflushed_save_points_->size() > 0) {
    Status s = PessimisticTransaction::PopSavePoint();
    assert(!s.IsNotFound());
    unflushed_save_points_->pop_back();
    return s;
  }

  if (flushed_save_points_ != nullptr && !flushed_save_points_->empty()) {
    // PessimisticTransaction::PopSavePoint will call also call PopSavePoint on
    // write_batch_. However, write_batch_ is empty and has no savepoints
    // because this savepoint has already been flushed. Work around this by
    // setting a fake savepoint.
    write_batch_.SetSavePoint();
    Status s = PessimisticTransaction::PopSavePoint();
    assert(!s.IsNotFound());
    flushed_save_points_->pop_back();
    return s;
  }

  return Status::NotFound();
}

void WriteUnpreparedTxn::MultiGet(const ReadOptions& options,
                                  ColumnFamilyHandle* column_family,
                                  const size_t num_keys, const Slice* keys,
                                  PinnableSlice* values, Status* statuses,
                                  const bool sorted_input) {
  SequenceNumber min_uncommitted, snap_seq;
  const SnapshotBackup backed_by_snapshot =
      wupt_db_->AssignMinMaxSeqs(options.snapshot, &min_uncommitted, &snap_seq);
  WriteUnpreparedTxnReadCallback callback(wupt_db_, snap_seq, min_uncommitted,
                                          unprep_seqs_, backed_by_snapshot);
  write_batch_.MultiGetFromBatchAndDB(db_, options, column_family, num_keys,
                                      keys, values, statuses, sorted_input,
                                      &callback);
  if (UNLIKELY(!callback.valid() ||
               !wupt_db_->ValidateSnapshot(snap_seq, backed_by_snapshot))) {
    wupt_db_->WPRecordTick(TXN_GET_TRY_AGAIN);
    for (size_t i = 0; i < num_keys; i++) {
      statuses[i] = Status::TryAgain();
    }
  }
}

Status WriteUnpreparedTxn::Get(const ReadOptions& options,
                               ColumnFamilyHandle* column_family,
                               const Slice& key, PinnableSlice* value) {
  SequenceNumber min_uncommitted, snap_seq;
  const SnapshotBackup backed_by_snapshot =
      wupt_db_->AssignMinMaxSeqs(options.snapshot, &min_uncommitted, &snap_seq);
  WriteUnpreparedTxnReadCallback callback(wupt_db_, snap_seq, min_uncommitted,
                                          unprep_seqs_, backed_by_snapshot);
  auto res = write_batch_.GetFromBatchAndDB(db_, options, column_family, key,
                                            value, &callback);
  if (LIKELY(callback.valid() &&
             wupt_db_->ValidateSnapshot(snap_seq, backed_by_snapshot))) {
    return res;
  } else {
    wupt_db_->WPRecordTick(TXN_GET_TRY_AGAIN);
    return Status::TryAgain();
  }
}

namespace {
static void CleanupWriteUnpreparedWBWIIterator(void* arg1, void* arg2) {
  auto txn = reinterpret_cast<WriteUnpreparedTxn*>(arg1);
  auto iter = reinterpret_cast<Iterator*>(arg2);
  txn->RemoveActiveIterator(iter);
}
}  // anonymous namespace

Iterator* WriteUnpreparedTxn::GetIterator(const ReadOptions& options) {
  return GetIterator(options, wupt_db_->DefaultColumnFamily());
}

Iterator* WriteUnpreparedTxn::GetIterator(const ReadOptions& options,
                                          ColumnFamilyHandle* column_family) {
  // Make sure to get iterator from WriteUnprepareTxnDB, not the root db.
  Iterator* db_iter = wupt_db_->NewIterator(options, column_family, this);
  assert(db_iter);

  auto iter = write_batch_.NewIteratorWithBase(column_family, db_iter);
  active_iterators_.push_back(iter);
  iter->RegisterCleanup(CleanupWriteUnpreparedWBWIIterator, this, iter);
  return iter;
}

Status WriteUnpreparedTxn::ValidateSnapshot(ColumnFamilyHandle* column_family,
                                            const Slice& key,
                                            SequenceNumber* tracked_at_seq) {
  // TODO(lth): Reduce duplicate code with WritePrepared ValidateSnapshot logic.
  assert(snapshot_);

  SequenceNumber min_uncommitted =
      static_cast_with_check<const SnapshotImpl, const Snapshot>(
          snapshot_.get())
          ->min_uncommitted_;
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

  WriteUnpreparedTxnReadCallback snap_checker(
      wupt_db_, snap_seq, min_uncommitted, unprep_seqs_, kBackedByDBSnapshot);
  return TransactionUtil::CheckKeyForConflicts(db_impl_, cfh, key.ToString(),
                                               snap_seq, false /* cache_only */,
                                               &snap_checker, min_uncommitted);
}

const std::map<SequenceNumber, size_t>&
WriteUnpreparedTxn::GetUnpreparedSequenceNumbers() {
  return unprep_seqs_;
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
