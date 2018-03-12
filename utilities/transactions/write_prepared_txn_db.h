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
#include "util/set_comparator.h"
#include "util/string_util.h"
#include "utilities/transactions/pessimistic_transaction.h"
#include "utilities/transactions/pessimistic_transaction_db.h"
#include "utilities/transactions/transaction_lock_mgr.h"
#include "utilities/transactions/write_prepared_txn.h"

namespace rocksdb {

#define ROCKS_LOG_DETAILS(LGR, FMT, ...) \
  ;  // due to overhead by default skip such lines
// ROCKS_LOG_DEBUG(LGR, FMT, ##__VA_ARGS__)

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

  // Optimized version of ::Write that receives more optimization request such
  // as skip_concurrency_control.
  using PessimisticTransactionDB::Write;
  Status Write(const WriteOptions& opts, const TransactionDBWriteOptimizations&,
               WriteBatch* updates) override;

  // Write the batch to the underlying DB and mark it as committed. Could be
  // used by both directly from TxnDB or through a transaction.
  Status WriteInternal(const WriteOptions& write_options, WriteBatch* batch,
                       size_t batch_cnt, WritePreparedTxn* txn);

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

  virtual void ReleaseSnapshot(const Snapshot* snapshot) override;

  // Check whether the transaction that wrote the value with sequence number seq
  // is visible to the snapshot with sequence number snapshot_seq
  bool IsInSnapshot(uint64_t seq, uint64_t snapshot_seq) const;
  // Add the transaction with prepare sequence seq to the prepared list
  void AddPrepared(uint64_t seq);
  // Rollback a prepared txn identified with prep_seq. rollback_seq is the seq
  // with which the additional data is written to cancel the txn effect. It can
  // be used to identify the snapshots that overlap with the rolled back txn.
  void RollbackPrepared(uint64_t prep_seq, uint64_t rollback_seq);
  // Add the transaction with prepare sequence prepare_seq and commit sequence
  // commit_seq to the commit map. prepare_skipped is set if the prepare phase
  // is skipped for this commit. loop_cnt is to detect infinite loops.
  void AddCommitted(uint64_t prepare_seq, uint64_t commit_seq,
                    bool prepare_skipped = false, uint8_t loop_cnt = 0);

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
          COMMIT_FILTER(static_cast<uint64_t>((1ull << COMMIT_BITS) - 1)),
          DELTA_UPPERBOUND(static_cast<uint64_t>((1ull << COMMIT_BITS))) {}
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
    // The value of commit_seq - prepare_seq + 1 must be less than this bound
    const uint64_t DELTA_UPPERBOUND;
  };

  // Prepare Seq (64 bits) = PAD ... PAD PREP PREP ... PREP INDEX INDEX ...
  // INDEX Delta Seq (64 bits)   = 0 0 0 0 0 0 0 0 0  0 0 0 DELTA DELTA ...
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
      assert(delta < format.DELTA_UPPERBOUND);
      if (delta >= format.DELTA_UPPERBOUND) {
        throw std::runtime_error(
            "commit_seq >> prepare_seq. The allowed distance is " +
            ToString(format.DELTA_UPPERBOUND) + " commit_seq is " +
            ToString(cs) + " prepare_seq is " + ToString(ps));
      }
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

  std::map<uint32_t, const Comparator*>* GetCFComparatorMap() {
    return cf_map_.load();
  }
  void UpdateCFComparatorMap(
      const std::vector<ColumnFamilyHandle*>& handles) override;
  void UpdateCFComparatorMap(const ColumnFamilyHandle* handle) override;

 protected:
  virtual Status VerifyCFOptions(
      const ColumnFamilyOptions& cf_options) override;

 private:
  friend class WritePreparedTransactionTest_IsInSnapshotTest_Test;
  friend class WritePreparedTransactionTest_CheckAgainstSnapshotsTest_Test;
  friend class WritePreparedTransactionTest_CommitMapTest_Test;
  friend class
      WritePreparedTransactionTest_ConflictDetectionAfterRecoveryTest_Test;
  friend class SnapshotConcurrentAccessTest_SnapshotConcurrentAccessTest_Test;
  friend class WritePreparedTransactionTestBase;
  friend class PreparedHeap_BasicsTest_Test;
  friend class PreparedHeap_EmptyAtTheEnd_Test;
  friend class PreparedHeap_Concurrent_Test;
  friend class WritePreparedTxnDBMock;
  friend class WritePreparedTransactionTest_AdvanceMaxEvictedSeqBasicTest_Test;
  friend class WritePreparedTransactionTest_BasicRecoveryTest_Test;
  friend class WritePreparedTransactionTest_IsInSnapshotEmptyMapTest_Test;
  friend class WritePreparedTransactionTest_OldCommitMapGC_Test;
  friend class WritePreparedTransactionTest_RollbackTest_Test;

  void Init(const TransactionDBOptions& /* unused */);

  // A heap with the amortized O(1) complexity for erase. It uses one extra heap
  // to keep track of erased entries that are not yet on top of the main heap.
  class PreparedHeap {
    std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<uint64_t>>
        heap_;
    std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<uint64_t>>
        erased_heap_;
    // True when testing crash recovery
    bool TEST_CRASH_ = false;
    friend class WritePreparedTxnDB;

   public:
    ~PreparedHeap() {
      if (!TEST_CRASH_) {
        assert(heap_.empty());
        assert(erased_heap_.empty());
      }
    }
    bool empty() { return heap_.empty(); }
    uint64_t top() { return heap_.top(); }
    void push(uint64_t v) { heap_.push(v); }
    void pop() {
      heap_.pop();
      while (!heap_.empty() && !erased_heap_.empty() &&
             // heap_.top() > erased_heap_.top() could happen if we have erased
             // a non-existent entry. Ideally the user should not do that but we
             // should be resilient against it.
             heap_.top() >= erased_heap_.top()) {
        if (heap_.top() == erased_heap_.top()) {
          heap_.pop();
        }
        uint64_t erased __attribute__((__unused__));
        erased = erased_heap_.top();
        erased_heap_.pop();
        // No duplicate prepare sequence numbers
        assert(erased_heap_.empty() || erased_heap_.top() != erased);
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
          assert(heap_.empty() || heap_.top() != seq);
        } else {  // (heap_.top() > seq)
          // Down the heap, remember to pop it later
          erased_heap_.push(seq);
        }
      }
    }
  };

  void TEST_Crash() override { prepared_txns_.TEST_CRASH_ = true; }

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
  // largest new_max value.
  void AdvanceMaxEvictedSeq(const SequenceNumber& prev_max,
                            const SequenceNumber& new_max);

  virtual const std::vector<SequenceNumber> GetSnapshotListFromDB(
      SequenceNumber max);

  // Will be called by the public ReleaseSnapshot method. Does the maintenance
  // internal to WritePreparedTxnDB
  void ReleaseSnapshotInternal(const SequenceNumber snap_seq);

  // Update the list of snapshots corresponding to the soon-to-be-updated
  // max_evicted_seq_. Thread-safety: this function can be called concurrently.
  // The concurrent invocations of this function is equivalent to a serial
  // invocation in which the last invocation is the one with the largest
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
  // This is the case if none of the next snapshots could satisfy the condition.
  // next_is_larger: the next snapshot will be a larger value
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
  // rewriting a list that is concurrently updated with a more recent version.
  SequenceNumber snapshots_version_ = 0;

  // A heap of prepared transactions. Thread-safety is provided with
  // prepared_mutex_.
  PreparedHeap prepared_txns_;
  // 2m entry, 16MB size
  static const size_t DEF_COMMIT_CACHE_BITS = static_cast<size_t>(21);
  const size_t COMMIT_CACHE_BITS;
  const size_t COMMIT_CACHE_SIZE;
  const CommitEntry64bFormat FORMAT;
  // commit_cache_ must be initialized to zero to tell apart an empty index from
  // a filled one. Thread-safety is provided with commit_cache_mutex_.
  unique_ptr<std::atomic<CommitEntry64b>[]> commit_cache_;
  // The largest evicted *commit* sequence number from the commit_cache_. If a
  // seq is smaller than max_evicted_seq_ is might or might not be present in
  // commit_cache_. So commit_cache_ must first be checked before consulting
  // with max_evicted_seq_.
  std::atomic<uint64_t> max_evicted_seq_ = {};
  // Advance max_evicted_seq_ by this value each time it needs an update. The
  // larger the value, the less frequent advances we would have. We do not want
  // it to be too large either as it would cause stalls by doing too much
  // maintenance work under the lock.
  size_t INC_STEP_FOR_MAX_EVICTED = 1;
  // A map from old snapshots (expected to be used by a few read-only txns) to
  // prepared sequence number of the evicted entries from commit_cache_ that
  // overlaps with such snapshot. These are the prepared sequence numbers that
  // the snapshot, to which they are mapped, cannot assume to be committed just
  // because it is no longer in the commit_cache_. The vector must be sorted
  // after each update.
  // Thread-safety is provided with old_commit_map_mutex_.
  std::map<SequenceNumber, std::vector<SequenceNumber>> old_commit_map_;
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
  // A cache of the cf comparators
  std::atomic<std::map<uint32_t, const Comparator*>*> cf_map_;
  // GC of the object above
  std::unique_ptr<std::map<uint32_t, const Comparator*>> cf_map_gc_;
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
                                             size_t prep_batch_cnt,
                                             size_t data_batch_cnt = 0,
                                             bool prep_heap_skipped = false)
      : db_(db),
        db_impl_(db_impl),
        prep_seq_(prep_seq),
        prep_batch_cnt_(prep_batch_cnt),
        data_batch_cnt_(data_batch_cnt),
        prep_heap_skipped_(prep_heap_skipped),
        includes_data_(data_batch_cnt_ > 0) {
    assert((prep_batch_cnt_ > 0) != (prep_seq == kMaxSequenceNumber));  // xor
    assert(prep_batch_cnt_ > 0 || data_batch_cnt_ > 0);
  }

  virtual Status Callback(SequenceNumber commit_seq) override {
    assert(includes_data_ || prep_seq_ != kMaxSequenceNumber);
    const uint64_t last_commit_seq = LIKELY(data_batch_cnt_ <= 1)
                                         ? commit_seq
                                         : commit_seq + data_batch_cnt_ - 1;
    if (prep_seq_ != kMaxSequenceNumber) {
      for (size_t i = 0; i < prep_batch_cnt_; i++) {
        db_->AddCommitted(prep_seq_ + i, last_commit_seq, prep_heap_skipped_);
      }
    }  // else there was no prepare phase
    if (includes_data_) {
      assert(data_batch_cnt_);
      // Commit the data that is accompanied with the commit request
      const bool PREPARE_SKIPPED = true;
      for (size_t i = 0; i < data_batch_cnt_; i++) {
        // For commit seq of each batch use the commit seq of the last batch.
        // This would make debugging easier by having all the batches having
        // the same sequence number.
        db_->AddCommitted(commit_seq + i, last_commit_seq, PREPARE_SKIPPED);
      }
    }
    if (db_impl_->immutable_db_options().two_write_queues) {
      // Publish the sequence number. We can do that here assuming the callback
      // is invoked only from one write queue, which would guarantee that the
      // publish sequence numbers will be in order, i.e., once a seq is
      // published all the seq prior to that are also publishable.
      db_impl_->SetLastPublishedSequence(last_commit_seq);
    }
    // else SequenceNumber that is updated as part of the write already does the
    // publishing
    return Status::OK();
  }

 private:
  WritePreparedTxnDB* db_;
  DBImpl* db_impl_;
  // kMaxSequenceNumber if there was no prepare phase
  SequenceNumber prep_seq_;
  size_t prep_batch_cnt_;
  size_t data_batch_cnt_;
  // An optimization that indicates that there is no need to update the prepare
  // heap since the prepare sequence number was not added to it.
  bool prep_heap_skipped_;
  // Either because it is commit without prepare or it has a
  // CommitTimeWriteBatch
  bool includes_data_;
};

// Count the number of sub-batches inside a batch. A sub-batch does not have
// duplicate keys.
struct SubBatchCounter : public WriteBatch::Handler {
  explicit SubBatchCounter(std::map<uint32_t, const Comparator*>& comparators)
      : comparators_(comparators), batches_(1) {}
  std::map<uint32_t, const Comparator*>& comparators_;
  using CFKeys = std::set<Slice, SetComparator>;
  std::map<uint32_t, CFKeys> keys_;
  size_t batches_;
  size_t BatchCount() { return batches_; }
  void AddKey(const uint32_t cf, const Slice& key);
  void InitWithComp(const uint32_t cf);
  Status MarkNoop(bool) override { return Status::OK(); }
  Status MarkEndPrepare(const Slice&) override { return Status::OK(); }
  Status MarkCommit(const Slice&) override { return Status::OK(); }
  Status PutCF(uint32_t cf, const Slice& key, const Slice&) override {
    AddKey(cf, key);
    return Status::OK();
  }
  Status DeleteCF(uint32_t cf, const Slice& key) override {
    AddKey(cf, key);
    return Status::OK();
  }
  Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
    AddKey(cf, key);
    return Status::OK();
  }
  Status MergeCF(uint32_t cf, const Slice& key, const Slice&) override {
    AddKey(cf, key);
    return Status::OK();
  }
  Status MarkBeginPrepare() override { return Status::OK(); }
  Status MarkRollback(const Slice&) override { return Status::OK(); }
  bool WriteAfterCommit() const override { return false; }
};

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
