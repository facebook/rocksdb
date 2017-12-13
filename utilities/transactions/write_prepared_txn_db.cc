//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/write_prepared_txn_db.h"

#include <inttypes.h>
#include <string>
#include <unordered_set>
#include <vector>

#include "db/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/mutexlock.h"
#include "util/sync_point.h"
#include "utilities/transactions/pessimistic_transaction.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"

namespace rocksdb {

Status WritePreparedTxnDB::Initialize(
    const std::vector<size_t>& compaction_enabled_cf_indices,
    const std::vector<ColumnFamilyHandle*>& handles) {
  auto dbimpl = reinterpret_cast<DBImpl*>(GetRootDB());
  assert(dbimpl != nullptr);
  auto rtxns = dbimpl->recovered_transactions();
  for (auto rtxn : rtxns) {
    AddPrepared(rtxn.second->seq_);
  }
  SequenceNumber prev_max = max_evicted_seq_;
  SequenceNumber last_seq = db_impl_->GetLatestSequenceNumber();
  AdvanceMaxEvictedSeq(prev_max, last_seq);

  db_impl_->SetSnapshotChecker(new WritePreparedSnapshotChecker(this));

  auto s = PessimisticTransactionDB::Initialize(compaction_enabled_cf_indices,
                                                handles);
  return s;
}

Transaction* WritePreparedTxnDB::BeginTransaction(
    const WriteOptions& write_options, const TransactionOptions& txn_options,
    Transaction* old_txn) {
  if (old_txn != nullptr) {
    ReinitializeTransaction(old_txn, write_options, txn_options);
    return old_txn;
  } else {
    return new WritePreparedTxn(this, write_options, txn_options);
  }
}

Status WritePreparedTxnDB::Get(const ReadOptions& options,
                               ColumnFamilyHandle* column_family,
                               const Slice& key, PinnableSlice* value) {
  // We are fine with the latest committed value. This could be done by
  // specifying the snapshot as kMaxSequenceNumber.
  SequenceNumber seq = kMaxSequenceNumber;
  if (options.snapshot != nullptr) {
    seq = options.snapshot->GetSequenceNumber();
  }
  WritePreparedTxnReadCallback callback(this, seq);
  bool* dont_care = nullptr;
  // Note: no need to specify a snapshot for read options as no specific
  // snapshot is requested by the user.
  return db_impl_->GetImpl(options, column_family, key, value, dont_care,
                           &callback);
}

std::vector<Status> WritePreparedTxnDB::MultiGet(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  assert(values);
  size_t num_keys = keys.size();
  values->resize(num_keys);

  std::vector<Status> stat_list(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    std::string* value = values ? &(*values)[i] : nullptr;
    stat_list[i] = this->Get(options, column_family[i], keys[i], value);
  }
  return stat_list;
}

// Struct to hold ownership of snapshot and read callback for iterator cleanup.
struct WritePreparedTxnDB::IteratorState {
  IteratorState(WritePreparedTxnDB* txn_db, SequenceNumber sequence,
                std::shared_ptr<ManagedSnapshot> s)
      : callback(txn_db, sequence), snapshot(s) {}

  WritePreparedTxnReadCallback callback;
  std::shared_ptr<ManagedSnapshot> snapshot;
};

namespace {
static void CleanupWritePreparedTxnDBIterator(void* arg1, void* arg2) {
  delete reinterpret_cast<WritePreparedTxnDB::IteratorState*>(arg1);
}
}  // anonymous namespace

Iterator* WritePreparedTxnDB::NewIterator(const ReadOptions& options,
                                          ColumnFamilyHandle* column_family) {
  std::shared_ptr<ManagedSnapshot> own_snapshot = nullptr;
  SequenceNumber snapshot_seq = kMaxSequenceNumber;
  if (options.snapshot != nullptr) {
    snapshot_seq = options.snapshot->GetSequenceNumber();
  } else {
    auto* snapshot = db_impl_->GetSnapshot();
    // We take a snapshot to make sure that the related data in the commit map
    // are not deleted.
    snapshot_seq = snapshot->GetSequenceNumber();
    own_snapshot = std::make_shared<ManagedSnapshot>(db_impl_, snapshot);
  }
  assert(snapshot_seq != kMaxSequenceNumber);
  auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  auto* state = new IteratorState(this, snapshot_seq, own_snapshot);
  auto* db_iter =
      db_impl_->NewIteratorImpl(options, cfd, snapshot_seq, &state->callback);
  db_iter->RegisterCleanup(CleanupWritePreparedTxnDBIterator, state, nullptr);
  return db_iter;
}

Status WritePreparedTxnDB::NewIterators(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  std::shared_ptr<ManagedSnapshot> own_snapshot = nullptr;
  SequenceNumber snapshot_seq = kMaxSequenceNumber;
  if (options.snapshot != nullptr) {
    snapshot_seq = options.snapshot->GetSequenceNumber();
  } else {
    auto* snapshot = db_impl_->GetSnapshot();
    // We take a snapshot to make sure that the related data in the commit map
    // are not deleted.
    snapshot_seq = snapshot->GetSequenceNumber();
    own_snapshot = std::make_shared<ManagedSnapshot>(db_impl_, snapshot);
  }
  iterators->clear();
  iterators->reserve(column_families.size());
  for (auto* column_family : column_families) {
    auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
    auto* state = new IteratorState(this, snapshot_seq, own_snapshot);
    auto* db_iter =
        db_impl_->NewIteratorImpl(options, cfd, snapshot_seq, &state->callback);
    db_iter->RegisterCleanup(CleanupWritePreparedTxnDBIterator, state, nullptr);
    iterators->push_back(db_iter);
  }
  return Status::OK();
}

void WritePreparedTxnDB::Init(const TransactionDBOptions& /* unused */) {
  // Adcance max_evicted_seq_ no more than 100 times before the cache wraps
  // around.
  INC_STEP_FOR_MAX_EVICTED =
      std::max(SNAPSHOT_CACHE_SIZE / 100, static_cast<size_t>(1));
  snapshot_cache_ = unique_ptr<std::atomic<SequenceNumber>[]>(
      new std::atomic<SequenceNumber>[SNAPSHOT_CACHE_SIZE] {});
  commit_cache_ = unique_ptr<std::atomic<CommitEntry64b>[]>(
      new std::atomic<CommitEntry64b>[COMMIT_CACHE_SIZE] {});
}

#define ROCKSDB_LOG_DETAILS(LGR, FMT, ...) \
  ;  // due to overhead by default skip such lines
// ROCKS_LOG_DEBUG(LGR, FMT, ##__VA_ARGS__)

// Returns true if commit_seq <= snapshot_seq
bool WritePreparedTxnDB::IsInSnapshot(uint64_t prep_seq,
                                      uint64_t snapshot_seq) const {
  // Here we try to infer the return value without looking into prepare list.
  // This would help avoiding synchronization over a shared map.
  // TODO(myabandeh): read your own writes
  // TODO(myabandeh): optimize this. This sequence of checks must be correct but
  // not necessary efficient
  if (prep_seq == 0) {
    // Compaction will output keys to bottom-level with sequence number 0 if
    // it is visible to the earliest snapshot.
    ROCKSDB_LOG_DETAILS(
        info_log_, "IsInSnapshot %" PRIu64 " in %" PRIu64 " returns %" PRId32,
        prep_seq, snapshot_seq, 1);
    return true;
  }
  if (snapshot_seq < prep_seq) {
    // snapshot_seq < prep_seq <= commit_seq => snapshot_seq < commit_seq
    ROCKSDB_LOG_DETAILS(
        info_log_, "IsInSnapshot %" PRIu64 " in %" PRIu64 " returns %" PRId32,
        prep_seq, snapshot_seq, 0);
    return false;
  }
  if (!delayed_prepared_empty_.load(std::memory_order_acquire)) {
    // We should not normally reach here
    ReadLock rl(&prepared_mutex_);
    if (delayed_prepared_.find(prep_seq) != delayed_prepared_.end()) {
      // Then it is not committed yet
      ROCKSDB_LOG_DETAILS(
          info_log_, "IsInSnapshot %" PRIu64 " in %" PRIu64 " returns %" PRId32,
          prep_seq, snapshot_seq, 0);
      return false;
    }
  }
  auto indexed_seq = prep_seq % COMMIT_CACHE_SIZE;
  CommitEntry64b dont_care;
  CommitEntry cached;
  bool exist = GetCommitEntry(indexed_seq, &dont_care, &cached);
  if (exist && prep_seq == cached.prep_seq) {
    // It is committed and also not evicted from commit cache
    ROCKSDB_LOG_DETAILS(
        info_log_, "IsInSnapshot %" PRIu64 " in %" PRIu64 " returns %" PRId32,
        prep_seq, snapshot_seq, cached.commit_seq <= snapshot_seq);
    return cached.commit_seq <= snapshot_seq;
  }
  // else it could be committed but not inserted in the map which could happen
  // after recovery, or it could be committed and evicted by another commit, or
  // never committed.

  // At this point we dont know if it was committed or it is still prepared
  auto max_evicted_seq = max_evicted_seq_.load(std::memory_order_acquire);
  if (max_evicted_seq < prep_seq) {
    // Not evicted from cache and also not present, so must be still prepared
    ROCKSDB_LOG_DETAILS(
        info_log_, "IsInSnapshot %" PRIu64 " in %" PRIu64 " returns %" PRId32,
        prep_seq, snapshot_seq, 0);
    return false;
  }
  // When advancing max_evicted_seq_, we move older entires from prepared to
  // delayed_prepared_. Also we move evicted entries from commit cache to
  // old_commit_map_ if it overlaps with any snapshot. Since prep_seq <=
  // max_evicted_seq_, we have three cases: i) in delayed_prepared_, ii) in
  // old_commit_map_, iii) committed with no conflict with any snapshot (i)
  // delayed_prepared_ is checked above
  if (max_evicted_seq < snapshot_seq) {  // then (ii) cannot be the case
    // only (iii) is the case: committed
    // commit_seq <= max_evicted_seq_ < snapshot_seq => commit_seq <
    // snapshot_seq
    ROCKSDB_LOG_DETAILS(
        info_log_, "IsInSnapshot %" PRIu64 " in %" PRIu64 " returns %" PRId32,
        prep_seq, snapshot_seq, 1);
    return true;
  }
  // else (ii) might be the case: check the commit data saved for this snapshot.
  // If there was no overlapping commit entry, then it is committed with a
  // commit_seq lower than any live snapshot, including snapshot_seq.
  if (old_commit_map_empty_.load(std::memory_order_acquire)) {
    ROCKSDB_LOG_DETAILS(
        info_log_, "IsInSnapshot %" PRIu64 " in %" PRIu64 " returns %" PRId32,
        prep_seq, snapshot_seq, 1);
    return true;
  }
  {
    // We should not normally reach here
    // TODO(myabandeh): check only if snapshot_seq is in the list of snaphots
    ReadLock rl(&old_commit_map_mutex_);
    auto old_commit_entry = old_commit_map_.find(prep_seq);
    if (old_commit_entry == old_commit_map_.end() ||
        old_commit_entry->second <= snapshot_seq) {
      ROCKSDB_LOG_DETAILS(
          info_log_, "IsInSnapshot %" PRIu64 " in %" PRIu64 " returns %" PRId32,
          prep_seq, snapshot_seq, 1);
      return true;
    }
  }
  // (ii) it the case: it is committed but after the snapshot_seq
  ROCKSDB_LOG_DETAILS(
      info_log_, "IsInSnapshot %" PRIu64 " in %" PRIu64 " returns %" PRId32,
      prep_seq, snapshot_seq, 0);
  return false;
}

void WritePreparedTxnDB::AddPrepared(uint64_t seq) {
  ROCKSDB_LOG_DETAILS(info_log_, "Txn %" PRIu64 " Prepareing", seq);
  // TODO(myabandeh): Add a runtime check to ensure the following assert.
  assert(seq > max_evicted_seq_);
  WriteLock wl(&prepared_mutex_);
  prepared_txns_.push(seq);
}

void WritePreparedTxnDB::RollbackPrepared(uint64_t prep_seq,
                                          uint64_t rollback_seq) {
  ROCKSDB_LOG_DETAILS(
      info_log_, "Txn %" PRIu64 " rolling back with rollback seq of " PRIu64 "",
      prep_seq, rollback_seq);
  std::vector<SequenceNumber> snapshots =
      GetSnapshotListFromDB(kMaxSequenceNumber);
  // TODO(myabandeh): currently we are assuming that there is no snapshot taken
  // when a transaciton is rolled back. This is the case the way MySQL does
  // rollback which is after recovery. We should extend it to be able to
  // rollback txns that overlap with exsiting snapshots.
  assert(snapshots.size() == 0);
  if (snapshots.size()) {
    throw std::runtime_error(
        "Rollback reqeust while there are live snapshots.");
  }
  WriteLock wl(&prepared_mutex_);
  prepared_txns_.erase(prep_seq);
  bool was_empty = delayed_prepared_.empty();
  if (!was_empty) {
    delayed_prepared_.erase(prep_seq);
    bool is_empty = delayed_prepared_.empty();
    if (was_empty != is_empty) {
      delayed_prepared_empty_.store(is_empty, std::memory_order_release);
    }
  }
}

void WritePreparedTxnDB::AddCommitted(uint64_t prepare_seq,
                                      uint64_t commit_seq) {
  ROCKSDB_LOG_DETAILS(info_log_, "Txn %" PRIu64 " Committing with %" PRIu64,
                      prepare_seq, commit_seq);
  TEST_SYNC_POINT("WritePreparedTxnDB::AddCommitted:start");
  TEST_SYNC_POINT("WritePreparedTxnDB::AddCommitted:start:pause");
  auto indexed_seq = prepare_seq % COMMIT_CACHE_SIZE;
  CommitEntry64b evicted_64b;
  CommitEntry evicted;
  bool to_be_evicted = GetCommitEntry(indexed_seq, &evicted_64b, &evicted);
  if (to_be_evicted) {
    auto prev_max = max_evicted_seq_.load(std::memory_order_acquire);
    if (prev_max < evicted.commit_seq) {
      // Inc max in larger steps to avoid frequent updates
      auto max_evicted_seq = evicted.commit_seq + INC_STEP_FOR_MAX_EVICTED;
      AdvanceMaxEvictedSeq(prev_max, max_evicted_seq);
    }
    // After each eviction from commit cache, check if the commit entry should
    // be kept around because it overlaps with a live snapshot.
    CheckAgainstSnapshots(evicted);
  }
  bool succ =
      ExchangeCommitEntry(indexed_seq, evicted_64b, {prepare_seq, commit_seq});
  if (!succ) {
    // A very rare event, in which the commit entry is updated before we do.
    // Here we apply a very simple solution of retrying.
    // TODO(myabandeh): do precautions to detect bugs that cause infinite loops
    AddCommitted(prepare_seq, commit_seq);
    return;
  }
  {
    WriteLock wl(&prepared_mutex_);
    prepared_txns_.erase(prepare_seq);
    bool was_empty = delayed_prepared_.empty();
    if (!was_empty) {
      delayed_prepared_.erase(prepare_seq);
      bool is_empty = delayed_prepared_.empty();
      if (was_empty != is_empty) {
        delayed_prepared_empty_.store(is_empty, std::memory_order_release);
      }
    }
  }
  TEST_SYNC_POINT("WritePreparedTxnDB::AddCommitted:end");
  TEST_SYNC_POINT("WritePreparedTxnDB::AddCommitted:end:pause");
}

bool WritePreparedTxnDB::GetCommitEntry(const uint64_t indexed_seq,
                                        CommitEntry64b* entry_64b,
                                        CommitEntry* entry) const {
  *entry_64b = commit_cache_[indexed_seq].load(std::memory_order_acquire);
  bool valid = entry_64b->Parse(indexed_seq, entry, FORMAT);
  return valid;
}

bool WritePreparedTxnDB::AddCommitEntry(const uint64_t indexed_seq,
                                        const CommitEntry& new_entry,
                                        CommitEntry* evicted_entry) {
  CommitEntry64b new_entry_64b(new_entry, FORMAT);
  CommitEntry64b evicted_entry_64b = commit_cache_[indexed_seq].exchange(
      new_entry_64b, std::memory_order_acq_rel);
  bool valid = evicted_entry_64b.Parse(indexed_seq, evicted_entry, FORMAT);
  return valid;
}

bool WritePreparedTxnDB::ExchangeCommitEntry(const uint64_t indexed_seq,
                                             CommitEntry64b& expected_entry_64b,
                                             const CommitEntry& new_entry) {
  auto& atomic_entry = commit_cache_[indexed_seq];
  CommitEntry64b new_entry_64b(new_entry, FORMAT);
  bool succ = atomic_entry.compare_exchange_strong(
      expected_entry_64b, new_entry_64b, std::memory_order_acq_rel,
      std::memory_order_acquire);
  return succ;
}

void WritePreparedTxnDB::AdvanceMaxEvictedSeq(SequenceNumber& prev_max,
                                              SequenceNumber& new_max) {
  // When max_evicted_seq_ advances, move older entries from prepared_txns_
  // to delayed_prepared_. This guarantees that if a seq is lower than max,
  // then it is not in prepared_txns_ ans save an expensive, synchronized
  // lookup from a shared set. delayed_prepared_ is expected to be empty in
  // normal cases.
  {
    WriteLock wl(&prepared_mutex_);
    while (!prepared_txns_.empty() && prepared_txns_.top() <= new_max) {
      auto to_be_popped = prepared_txns_.top();
      delayed_prepared_.insert(to_be_popped);
      prepared_txns_.pop();
      delayed_prepared_empty_.store(false, std::memory_order_release);
    }
  }

  // With each change to max_evicted_seq_ fetch the live snapshots behind it.
  // We use max as the version of snapshots to identify how fresh are the
  // snapshot list. This works because the snapshots are between 0 and
  // max, so the larger the max, the more complete they are.
  SequenceNumber new_snapshots_version = new_max;
  std::vector<SequenceNumber> snapshots;
  bool update_snapshots = false;
  if (new_snapshots_version > snapshots_version_) {
    // This is to avoid updating the snapshots_ if it already updated
    // with a more recent vesion by a concrrent thread
    update_snapshots = true;
    // We only care about snapshots lower then max
    snapshots = GetSnapshotListFromDB(new_max);
  }
  if (update_snapshots) {
    UpdateSnapshots(snapshots, new_snapshots_version);
  }
  while (prev_max < new_max && !max_evicted_seq_.compare_exchange_weak(
                                   prev_max, new_max, std::memory_order_acq_rel,
                                   std::memory_order_relaxed)) {
  };
}

const std::vector<SequenceNumber> WritePreparedTxnDB::GetSnapshotListFromDB(
    SequenceNumber max) {
  InstrumentedMutex(db_impl_->mutex());
  return db_impl_->snapshots().GetAll(nullptr, max);
}

void WritePreparedTxnDB::UpdateSnapshots(
    const std::vector<SequenceNumber>& snapshots,
    const SequenceNumber& version) {
  TEST_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:p:start");
  TEST_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:s:start");
#ifndef NDEBUG
  size_t sync_i = 0;
#endif
  WriteLock wl(&snapshots_mutex_);
  snapshots_version_ = version;
  // We update the list concurrently with the readers.
  // Both new and old lists are sorted and the new list is subset of the
  // previous list plus some new items. Thus if a snapshot repeats in
  // both new and old lists, it will appear upper in the new list. So if
  // we simply insert the new snapshots in order, if an overwritten item
  // is still valid in the new list is either written to the same place in
  // the array or it is written in a higher palce before it gets
  // overwritten by another item. This guarantess a reader that reads the
  // list bottom-up will eventaully see a snapshot that repeats in the
  // update, either before it gets overwritten by the writer or
  // afterwards.
  size_t i = 0;
  auto it = snapshots.begin();
  for (; it != snapshots.end() && i < SNAPSHOT_CACHE_SIZE; it++, i++) {
    snapshot_cache_[i].store(*it, std::memory_order_release);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:p:", ++sync_i);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:s:", sync_i);
  }
#ifndef NDEBUG
  // Release the remaining sync points since they are useless given that the
  // reader would also use lock to access snapshots
  for (++sync_i; sync_i <= 10; ++sync_i) {
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:p:", sync_i);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:s:", sync_i);
  }
#endif
  snapshots_.clear();
  for (; it != snapshots.end(); it++) {
    // Insert them to a vector that is less efficient to access
    // concurrently
    snapshots_.push_back(*it);
  }
  // Update the size at the end. Otherwise a parallel reader might read
  // items that are not set yet.
  snapshots_total_.store(snapshots.size(), std::memory_order_release);
  TEST_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:p:end");
  TEST_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:s:end");
}

void WritePreparedTxnDB::CheckAgainstSnapshots(const CommitEntry& evicted) {
  TEST_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:p:start");
  TEST_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:s:start");
#ifndef NDEBUG
  size_t sync_i = 0;
#endif
  // First check the snapshot cache that is efficient for concurrent access
  auto cnt = snapshots_total_.load(std::memory_order_acquire);
  // The list might get updated concurrently as we are reading from it. The
  // reader should be able to read all the snapshots that are still valid
  // after the update. Since the survived snapshots are written in a higher
  // place before gets overwritten the reader that reads bottom-up will
  // eventully see it.
  const bool next_is_larger = true;
  SequenceNumber snapshot_seq = kMaxSequenceNumber;
  size_t ip1 = std::min(cnt, SNAPSHOT_CACHE_SIZE);
  for (; 0 < ip1; ip1--) {
    snapshot_seq = snapshot_cache_[ip1 - 1].load(std::memory_order_acquire);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:p:",
                        ++sync_i);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:s:", sync_i);
    if (!MaybeUpdateOldCommitMap(evicted.prep_seq, evicted.commit_seq,
                                 snapshot_seq, !next_is_larger)) {
      break;
    }
  }
#ifndef NDEBUG
  // Release the remaining sync points before accquiring the lock
  for (++sync_i; sync_i <= 10; ++sync_i) {
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:p:", sync_i);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:s:", sync_i);
  }
#endif
  TEST_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:p:end");
  TEST_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:s:end");
  if (UNLIKELY(SNAPSHOT_CACHE_SIZE < cnt && ip1 == SNAPSHOT_CACHE_SIZE &&
               snapshot_seq < evicted.prep_seq)) {
    // Then access the less efficient list of snapshots_
    ReadLock rl(&snapshots_mutex_);
    // Items could have moved from the snapshots_ to snapshot_cache_ before
    // accquiring the lock. To make sure that we do not miss a valid snapshot,
    // read snapshot_cache_ again while holding the lock.
    for (size_t i = 0; i < SNAPSHOT_CACHE_SIZE; i++) {
      snapshot_seq = snapshot_cache_[i].load(std::memory_order_acquire);
      if (!MaybeUpdateOldCommitMap(evicted.prep_seq, evicted.commit_seq,
                                   snapshot_seq, next_is_larger)) {
        break;
      }
    }
    for (auto snapshot_seq_2 : snapshots_) {
      if (!MaybeUpdateOldCommitMap(evicted.prep_seq, evicted.commit_seq,
                                   snapshot_seq_2, next_is_larger)) {
        break;
      }
    }
  }
}

bool WritePreparedTxnDB::MaybeUpdateOldCommitMap(
    const uint64_t& prep_seq, const uint64_t& commit_seq,
    const uint64_t& snapshot_seq, const bool next_is_larger = true) {
  // If we do not store an entry in old_commit_map we assume it is committed in
  // all snapshots. if commit_seq <= snapshot_seq, it is considered already in
  // the snapshot so we need not to keep the entry around for this snapshot.
  if (commit_seq <= snapshot_seq) {
    // continue the search if the next snapshot could be smaller than commit_seq
    return !next_is_larger;
  }
  // then snapshot_seq < commit_seq
  if (prep_seq <= snapshot_seq) {  // overlapping range
    WriteLock wl(&old_commit_map_mutex_);
    old_commit_map_empty_.store(false, std::memory_order_release);
    old_commit_map_[prep_seq] = commit_seq;
    // Storing once is enough. No need to check it for other snapshots.
    return false;
  }
  // continue the search if the next snapshot could be larger than prep_seq
  return next_is_larger;
}

WritePreparedTxnDB::~WritePreparedTxnDB() {
  // At this point there could be running compaction/flush holding a
  // SnapshotChecker, which holds a pointer back to WritePreparedTxnDB.
  // Make sure those jobs finished before destructing WritePreparedTxnDB.
  db_impl_->CancelAllBackgroundWork(true /*wait*/);
}

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
