//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/db_iter.h"
#include "db/pre_release_callback.h"
#include "db/read_callback.h"
#include "db/snapshot_checker.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction_db.h"
#include "utilities/transactions/pessimistic_transaction.h"
#include "utilities/transactions/pessimistic_transaction_db.h"
#include "utilities/transactions/transaction_lock_mgr.h"
#include "utilities/transactions/write_prepared_txn.h"

namespace rocksdb {

// A PessimisticTransactionDB that writes data to DB after prepare phase of 2PC.
// In this way some data in the DB might not be committed. The DB provides
// mechanisms to tell such data apart from committed data.
class WritePreparedTxnDB : public PessimisticTransactionDB {
 public:
  explicit WritePreparedTxnDB(
      DB* db, const TransactionDBOptions& txn_db_options,
      size_t snapshot_cache_bits = DEF_SNAPSHOT_CACHE_BITS,
      size_t commit_cache_bits = DEF_COMMIT_CACHE_BITS)
      : PessimisticTransactionDB(db, txn_db_options),
        SNAPSHOT_CACHE_BITS(snapshot_cache_bits),
        SNAPSHOT_CACHE_SIZE(static_cast<size_t>(1ull << SNAPSHOT_CACHE_BITS)),
        COMMIT_CACHE_BITS(commit_cache_bits),
        COMMIT_CACHE_SIZE(static_cast<size_t>(1ull << COMMIT_CACHE_BITS)),
        FORMAT(COMMIT_CACHE_BITS) {
    Init(txn_db_options);
  }

  explicit WritePreparedTxnDB(
      StackableDB* db, const TransactionDBOptions& txn_db_options,
      size_t snapshot_cache_bits = DEF_SNAPSHOT_CACHE_BITS,
      size_t commit_cache_bits = DEF_COMMIT_CACHE_BITS)
      : PessimisticTransactionDB(db, txn_db_options),
        SNAPSHOT_CACHE_BITS(snapshot_cache_bits),
        SNAPSHOT_CACHE_SIZE(static_cast<size_t>(1ull << SNAPSHOT_CACHE_BITS)),
        COMMIT_CACHE_BITS(commit_cache_bits),
        COMMIT_CACHE_SIZE(static_cast<size_t>(1ull << COMMIT_CACHE_BITS)),
        FORMAT(COMMIT_CACHE_BITS) {
    Init(txn_db_options);
  }

  virtual ~WritePreparedTxnDB();

  virtual Status Initialize(
      const std::vector<size_t>& compaction_enabled_cf_indices,
      const std::vector<ColumnFamilyHandle*>& handles) override;

  Transaction* BeginTransaction(const WriteOptions& write_options,
                                const TransactionOptions& txn_options,
                                Transaction* old_txn) override;

  using DB::Get;
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     PinnableSlice* value) override;

  using DB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override;

  using DB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& options,
                                ColumnFamilyHandle* column_family) override;

  using DB::NewIterators;
  virtual Status NewIterators(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families,
      std::vector<Iterator*>* iterators) override;

  // Check whether the transaction that wrote the value with seqeunce number seq
  // is visible to the snapshot with sequence number snapshot_seq
  bool IsInSnapshot(uint64_t seq, uint64_t snapshot_seq) const;
  // Add the trasnaction with prepare sequence seq to the prepared list
  void AddPrepared(uint64_t seq);
  // Rollback a prepared txn identified with prep_seq. rollback_seq is the seq
  // with which the additional data is written to cancel the txn effect. It can
  // be used to idenitfy the snapshots that overlap with the rolled back txn.
  void RollbackPrepared(uint64_t prep_seq, uint64_t rollback_seq);
  // Add the transaction with prepare sequence prepare_seq and commit sequence
  // commit_seq to the commit map
  void AddCommitted(uint64_t prepare_seq, uint64_t commit_seq);

  struct CommitEntry {
    uint64_t prep_seq;
    uint64_t commit_seq;
    CommitEntry() : prep_seq(0), commit_seq(0) {}
    CommitEntry(uint64_t ps, uint64_t cs) : prep_seq(ps), commit_seq(cs) {}
    bool operator==(const CommitEntry& rhs) const {
      return prep_seq == rhs.prep_seq && commit_seq == rhs.commit_seq;
    }
  };

  struct CommitEntry64bFormat {
    explicit CommitEntry64bFormat(size_t index_bits)
        : INDEX_BITS(index_bits),
          PREP_BITS(static_cast<size_t>(64 - PAD_BITS - INDEX_BITS)),
          COMMIT_BITS(static_cast<size_t>(64 - PREP_BITS)),
          COMMIT_FILTER(static_cast<uint64_t>((1ull << COMMIT_BITS) - 1)) {}
    // Number of higher bits of a sequence number that is not used. They are
    // used to encode the value type, ...
    const size_t PAD_BITS = static_cast<size_t>(8);
    // Number of lower bits from prepare seq that can be skipped as they are
    // implied by the index of the entry in the array
    const size_t INDEX_BITS;
    // Number of bits we use to encode the prepare seq
    const size_t PREP_BITS;
    // Number of bits we use to encode the commit seq.
    const size_t COMMIT_BITS;
    // Filter to encode/decode commit seq
    const uint64_t COMMIT_FILTER;
  };

  // Prepare Seq (64 bits) = PAD ... PAD PREP PREP ... PREP INDEX INDEX ...
  // INDEX Detal Seq (64 bits)   = 0 0 0 0 0 0 0 0 0  0 0 0 DELTA DELTA ...
  // DELTA DELTA Encoded Value         = PREP PREP .... PREP PREP DELTA DELTA
  // ... DELTA DELTA PAD: first bits of a seq that is reserved for tagging and
  // hence ignored PREP/INDEX: the used bits in a prepare seq number INDEX: the
  // bits that do not have to be encoded (will be provided externally) DELTA:
  // prep seq - commit seq + 1 Number of DELTA bits should be equal to number of
  // index bits + PADs
  struct CommitEntry64b {
    constexpr CommitEntry64b() noexcept : rep_(0) {}

    CommitEntry64b(const CommitEntry& entry, const CommitEntry64bFormat& format)
        : CommitEntry64b(entry.prep_seq, entry.commit_seq, format) {}

    CommitEntry64b(const uint64_t ps, const uint64_t cs,
                   const CommitEntry64bFormat& format) {
      assert(ps < static_cast<uint64_t>(
                      (1ull << (format.PREP_BITS + format.INDEX_BITS))));
      assert(ps <= cs);
      uint64_t delta = cs - ps + 1;  // make initialized delta always >= 1
      // zero is reserved for uninitialized entries
      assert(0 < delta);
      assert(delta < static_cast<uint64_t>((1ull << format.COMMIT_BITS)));
      rep_ = (ps << format.PAD_BITS) & ~format.COMMIT_FILTER;
      rep_ = rep_ | delta;
    }

    // Return false if the entry is empty
    bool Parse(const uint64_t indexed_seq, CommitEntry* entry,
               const CommitEntry64bFormat& format) {
      uint64_t delta = rep_ & format.COMMIT_FILTER;
      // zero is reserved for uninitialized entries
      assert(delta < static_cast<uint64_t>((1ull << format.COMMIT_BITS)));
      if (delta == 0) {
        return false;  // initialized entry would have non-zero delta
      }

      assert(indexed_seq < static_cast<uint64_t>((1ull << format.INDEX_BITS)));
      uint64_t prep_up = rep_ & ~format.COMMIT_FILTER;
      prep_up >>= format.PAD_BITS;
      const uint64_t& prep_low = indexed_seq;
      entry->prep_seq = prep_up | prep_low;

      entry->commit_seq = entry->prep_seq + delta - 1;
      return true;
    }

   private:
    uint64_t rep_;
  };

  // Struct to hold ownership of snapshot and read callback for cleanup.
  struct IteratorState;

 private:
  friend class WritePreparedTransactionTest_IsInSnapshotTest_Test;
  friend class WritePreparedTransactionTest_CheckAgainstSnapshotsTest_Test;
  friend class WritePreparedTransactionTest_CommitMapTest_Test;
  friend class WritePreparedTransactionTest_SnapshotConcurrentAccessTest_Test;
  friend class WritePreparedTransactionTest;
  friend class PreparedHeap_BasicsTest_Test;
  friend class WritePreparedTxnDBMock;
  friend class WritePreparedTransactionTest_AdvanceMaxEvictedSeqBasicTest_Test;
  friend class WritePreparedTransactionTest_BasicRecoveryTest_Test;
  friend class WritePreparedTransactionTest_IsInSnapshotEmptyMapTest_Test;
  friend class WritePreparedTransactionTest_RollbackTest_Test;

  void Init(const TransactionDBOptions& /* unused */);

  // A heap with the amortized O(1) complexity for erase. It uses one extra heap
  // to keep track of erased entries that are not yet on top of the main heap.
  class PreparedHeap {
    std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<uint64_t>>
        heap_;
    std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<uint64_t>>
        erased_heap_;

   public:
    bool empty() { return heap_.empty(); }
    uint64_t top() { return heap_.top(); }
    void push(uint64_t v) { heap_.push(v); }
    void pop() {
      heap_.pop();
      while (!heap_.empty() && !erased_heap_.empty() &&
             heap_.top() == erased_heap_.top()) {
        heap_.pop();
        erased_heap_.pop();
      }
      while (heap_.empty() && !erased_heap_.empty()) {
        erased_heap_.pop();
      }
    }
    void erase(uint64_t seq) {
      if (!heap_.empty()) {
        if (seq < heap_.top()) {
          // Already popped, ignore it.
        } else if (heap_.top() == seq) {
          pop();
        } else {  // (heap_.top() > seq)
          // Down the heap, remember to pop it later
          erased_heap_.push(seq);
        }
      }
    }
  };

  // Get the commit entry with index indexed_seq from the commit table. It
  // returns true if such entry exists.
  bool GetCommitEntry(const uint64_t indexed_seq, CommitEntry64b* entry_64b,
                      CommitEntry* entry) const;

  // Rewrite the entry with the index indexed_seq in the commit table with the
  // commit entry <prep_seq, commit_seq>. If the rewrite results into eviction,
  // sets the evicted_entry and returns true.
  bool AddCommitEntry(const uint64_t indexed_seq, const CommitEntry& new_entry,
                      CommitEntry* evicted_entry);

  // Rewrite the entry with the index indexed_seq in the commit table with the
  // commit entry new_entry only if the existing entry matches the
  // expected_entry. Returns false otherwise.
  bool ExchangeCommitEntry(const uint64_t indexed_seq,
                           CommitEntry64b& expected_entry,
                           const CommitEntry& new_entry);

  // Increase max_evicted_seq_ from the previous value prev_max to the new
  // value. This also involves taking care of prepared txns that are not
  // committed before new_max, as well as updating the list of live snapshots at
  // the time of updating the max. Thread-safety: this function can be called
  // concurrently. The concurrent invocations of this function is equivalent to
  // a serial invocation in which the last invocation is the one with the
  // largetst new_max value.
  void AdvanceMaxEvictedSeq(SequenceNumber& prev_max, SequenceNumber& new_max);

  virtual const std::vector<SequenceNumber> GetSnapshotListFromDB(
      SequenceNumber max);

  // Update the list of snapshots corresponding to the soon-to-be-updated
  // max_eviceted_seq_. Thread-safety: this function can be called concurrently.
  // The concurrent invocations of this function is equivalent to a serial
  // invocation in which the last invocation is the one with the largetst
  // version value.
  void UpdateSnapshots(const std::vector<SequenceNumber>& snapshots,
                       const SequenceNumber& version);

  // Check an evicted entry against live snapshots to see if it should be kept
  // around or it can be safely discarded (and hence assume committed for all
  // snapshots). Thread-safety: this function can be called concurrently. If it
  // is called concurrently with multiple UpdateSnapshots, the result is the
  // same as checking the intersection of the snapshot list before updates with
  // the snapshot list of all the concurrent updates.
  void CheckAgainstSnapshots(const CommitEntry& evicted);

  // Add a new entry to old_commit_map_ if prep_seq <= snapshot_seq <
  // commit_seq. Return false if checking the next snapshot(s) is not needed.
  // This is the case if the entry already added to old_commit_map_ or none of
  // the next snapshots could satisfy the condition. next_is_larger: the next
  // snapshot will be a larger value
  bool MaybeUpdateOldCommitMap(const uint64_t& prep_seq,
                               const uint64_t& commit_seq,
                               const uint64_t& snapshot_seq,
                               const bool next_is_larger);

  // The list of live snapshots at the last time that max_evicted_seq_ advanced.
  // The list stored into two data structures: in snapshot_cache_ that is
  // efficient for concurrent reads, and in snapshots_ if the data does not fit
  // into snapshot_cache_. The total number of snapshots in the two lists
  std::atomic<size_t> snapshots_total_ = {};
  // The list sorted in ascending order. Thread-safety for writes is provided
  // with snapshots_mutex_ and concurrent reads are safe due to std::atomic for
  // each entry. In x86_64 architecture such reads are compiled to simple read
  // instructions. 128 entries
  static const size_t DEF_SNAPSHOT_CACHE_BITS = static_cast<size_t>(7);
  const size_t SNAPSHOT_CACHE_BITS;
  const size_t SNAPSHOT_CACHE_SIZE;
  unique_ptr<std::atomic<SequenceNumber>[]> snapshot_cache_;
  // 2nd list for storing snapshots. The list sorted in ascending order.
  // Thread-safety is provided with snapshots_mutex_.
  std::vector<SequenceNumber> snapshots_;
  // The version of the latest list of snapshots. This can be used to avoid
  // rewrittiing a list that is concurrently updated with a more recent version.
  SequenceNumber snapshots_version_ = 0;

  // A heap of prepared transactions. Thread-safety is provided with
  // prepared_mutex_.
  PreparedHeap prepared_txns_;
  // 10m entry, 80MB size
  static const size_t DEF_COMMIT_CACHE_BITS = static_cast<size_t>(21);
  const size_t COMMIT_CACHE_BITS;
  const size_t COMMIT_CACHE_SIZE;
  const CommitEntry64bFormat FORMAT;
  // commit_cache_ must be initialized to zero to tell apart an empty index from
  // a filled one. Thread-safety is provided with commit_cache_mutex_.
  unique_ptr<std::atomic<CommitEntry64b>[]> commit_cache_;
  // The largest evicted *commit* sequence number from the commit_cache_
  std::atomic<uint64_t> max_evicted_seq_ = {};
  // Advance max_evicted_seq_ by this value each time it needs an update. The
  // larger the value, the less frequent advances we would have. We do not want
  // it to be too large either as it would cause stalls by doing too much
  // maintenance work under the lock.
  size_t INC_STEP_FOR_MAX_EVICTED = 1;
  // A map of the evicted entries from commit_cache_ that has to be kept around
  // to service the old snapshots. This is expected to be empty normally.
  // Thread-safety is provided with old_commit_map_mutex_.
  std::map<uint64_t, uint64_t> old_commit_map_;
  // A set of long-running prepared transactions that are not finished by the
  // time max_evicted_seq_ advances their sequence number. This is expected to
  // be empty normally. Thread-safety is provided with prepared_mutex_.
  std::set<uint64_t> delayed_prepared_;
  // Update when delayed_prepared_.empty() changes. Expected to be true
  // normally.
  std::atomic<bool> delayed_prepared_empty_ = {true};
  // Update when old_commit_map_.empty() changes. Expected to be true normally.
  std::atomic<bool> old_commit_map_empty_ = {true};
  mutable port::RWMutex prepared_mutex_;
  mutable port::RWMutex old_commit_map_mutex_;
  mutable port::RWMutex commit_cache_mutex_;
  mutable port::RWMutex snapshots_mutex_;
};

class WritePreparedTxnReadCallback : public ReadCallback {
 public:
  WritePreparedTxnReadCallback(WritePreparedTxnDB* db, SequenceNumber snapshot)
      : db_(db), snapshot_(snapshot) {}

  // Will be called to see if the seq number accepted; if not it moves on to the
  // next seq number.
  virtual bool IsCommitted(SequenceNumber seq) override {
    return db_->IsInSnapshot(seq, snapshot_);
  }

 private:
  WritePreparedTxnDB* db_;
  SequenceNumber snapshot_;
};

class WritePreparedCommitEntryPreReleaseCallback : public PreReleaseCallback {
 public:
  // includes_data indicates that the commit also writes non-empty
  // CommitTimeWriteBatch to memtable, which needs to be committed separately.
  WritePreparedCommitEntryPreReleaseCallback(WritePreparedTxnDB* db,
                                             DBImpl* db_impl,
                                             SequenceNumber prep_seq,
                                             bool includes_data = false)
      : db_(db),
        db_impl_(db_impl),
        prep_seq_(prep_seq),
        includes_data_(includes_data) {}

  virtual Status Callback(SequenceNumber commit_seq) {
    db_->AddCommitted(prep_seq_, commit_seq);
    if (includes_data_) {
      // Commit the data that is accompnaied with the commit marker
      // TODO(myabandeh): skip AddPrepared
      db_->AddPrepared(commit_seq);
      db_->AddCommitted(commit_seq, commit_seq);
    }
    // Publish the sequence number. We can do that here assuming the callback is
    // invoked only from one write queue, which would guarantee that the publish
    // sequence numbers will be in order, i.e., once a seq is published all the
    // seq prior to that are also publishable.
    db_impl_->SetLastPublishedSequence(commit_seq);
    return Status::OK();
  }

 private:
  WritePreparedTxnDB* db_;
  DBImpl* db_impl_;
  SequenceNumber prep_seq_;
  bool includes_data_;
};

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
