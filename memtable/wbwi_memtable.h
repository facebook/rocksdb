//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include "db/memtable.h"
#include "rocksdb/utilities/write_batch_with_index.h"

namespace ROCKSDB_NAMESPACE {
// An implementation of the ReadOnlyMemTable interface based on the content
// of the given write batch with index (WBWI) object. This can be used to ingest
// a transaction (which is based on WBWI) into the DB as an immutable memtable.
//
// REQUIRE overwrite_key to be true for the WBWI
// Since the keys in WBWI do not have sequence number, the memtable needs to be
// assigned a range of sequence numbers through AssignSequenceNumbers(seqno)
// before being available for reads.
// With overwrite_key = true, WBWI keeps track of the most recent update for
// each key, and each such key will be assigned seqno.upper_bound during reads.
// One exception is during flush, consider the following scenario:
// - WBWI has SD(k) then PUT(k, v1)
// - DB has PUT(k, v2) in L1
// - flush WBWI adds PUT(k, v1) into L0
// - live memtable contains SD(k)
// - flush live memtable and compact it with L0 will drop SD(k) and PUT(k, v1)
// - the PUT(k, v2) in L1 incorrectly becomes visible
// So during flush, iterator from this memtable will need emit overwritten
// single deletion. These single deletion entries will be
// assigned seqno.upper_bound - 1.
class WBWIMemTable final : public ReadOnlyMemTable {
 public:
  struct SeqnoRange {
    SequenceNumber lower_bound = kMaxSequenceNumber;
    SequenceNumber upper_bound = kMaxSequenceNumber;
  };
  WBWIMemTable(const std::shared_ptr<WriteBatchWithIndex>& wbwi,
               const Comparator* cmp, uint32_t cf_id,
               const ImmutableOptions* immutable_options,
               const MutableCFOptions* cf_options,
               const WriteBatchWithIndex::CFStat& stat)
      : wbwi_(wbwi),
        comparator_(cmp),
        ikey_comparator_(comparator_),
        moptions_(*immutable_options, *cf_options),
        clock_(immutable_options->clock),
        // We need to include overwritten_sd_count in num_entries_ since flush
        // verifies number of entries processed and that iterator for this
        // memtable will emit overwritten SingleDelete entries during flush, See
        // comment above WBWIMemTableIterator for more detail.
        num_entries_(stat.entry_count + stat.overwritten_sd_count),
        cf_id_(cf_id) {
    assert(wbwi->GetOverwriteKey());
  }

  // No copying allowed
  WBWIMemTable(const WBWIMemTable&) = delete;
  WBWIMemTable& operator=(const WBWIMemTable&) = delete;

  ~WBWIMemTable() override { assert(refs_ == 0); }

  const char* Name() const override { return "WBWIMemTable"; }

  size_t ApproximateMemoryUsage() override {
    // FIXME: we can calculate for each CF or just divide evenly among CFs
    // Used in ReportFlushInputSize(), MemPurgeDecider, flush job event logging,
    // and InternalStats::HandleCurSizeAllMemTables
    return 0;
  }

  size_t MemoryAllocatedBytes() const override {
    // FIXME: similar to ApproximateMemoryUsage().
    //   Used in MemTableList to trim memtable history.
    return 0;
  }

  void UniqueRandomSample(
      const uint64_t& /* target_sample_size */,
      std::unordered_set<const char*>* /* entries */) override {
    // TODO: support mempurge
    assert(false);
  }

  InternalIterator* NewIterator(const ReadOptions&,
                                UnownedPtr<const SeqnoToTimeMapping>,
                                Arena* arena,
                                const SliceTransform* /* prefix_extractor */,
                                bool for_flush) override;

  // Returns an iterator that wraps a MemTableIterator and logically strips the
  // user-defined timestamp of each key. This API is only used by flush when
  // user-defined timestamps in MemTable only feature is enabled.
  InternalIterator* NewTimestampStrippingIterator(
      const ReadOptions&, UnownedPtr<const SeqnoToTimeMapping>, Arena* arena,
      const SliceTransform*, size_t) override {
    // TODO: support UDT
    assert(false);
    return NewErrorInternalIterator(
        Status::NotSupported(
            "WBWIMemTable does not support NewTimestampStrippingIterator."),
        arena);
  }

  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions&, SequenceNumber, bool) override {
    // TODO: support DeleteRange
    assert(!wbwi_->GetWriteBatch()->HasDeleteRange());
    return nullptr;
  }

  FragmentedRangeTombstoneIterator* NewTimestampStrippingRangeTombstoneIterator(
      const ReadOptions&, SequenceNumber, size_t) override {
    // TODO: support UDT
    assert(false);
    return nullptr;
  }

  // FIXME: not a good practice to use default parameter with virtual function
  using ReadOnlyMemTable::Get;
  bool Get(const LookupKey& key, std::string* value,
           PinnableWideColumns* columns, std::string* timestamp, Status* s,
           MergeContext* merge_context,
           SequenceNumber* max_covering_tombstone_seq, SequenceNumber* seq,
           const ReadOptions& read_opts, bool immutable_memtable,
           ReadCallback* callback = nullptr, bool* is_blob_index = nullptr,
           bool do_merge = true) override;

  void MultiGet(const ReadOptions& read_options, MultiGetRange* range,
                ReadCallback* callback, bool immutable_memtable) override;

  uint64_t NumEntries() const override { return num_entries_; }

  uint64_t NumDeletion() const override {
    // FIXME: this is used for stats and event logging
    return 0;
  }

  uint64_t NumRangeDeletion() const override {
    // FIXME
    assert(!wbwi_->GetWriteBatch()->HasDeleteRange());
    return 0;
  }

  uint64_t GetDataSize() const override {
    // FIXME: used in event logging in flush_job
    return 0;
  }

  SequenceNumber GetEarliestSequenceNumber() override {
    return assigned_seqno_.lower_bound;
  }

  bool IsEmpty() const override {
    // Ideally also check that wbwi contains updates from this CF. For now, we
    // only create WBWIMemTable for CFs with updates in wbwi.
    return wbwi_->GetWriteBatch()->Count() == 0;
  }

  SequenceNumber GetFirstSequenceNumber() override {
    return assigned_seqno_.lower_bound;
  }

  uint64_t GetMinLogContainingPrepSection() override {
    // FIXME: used to retain WAL with pending Prepare
    return min_prep_log_referenced_;
  }

  void MarkImmutable() override {}

  void MarkFlushed() override {}

  MemTableStats ApproximateStats(const Slice&, const Slice&) override {
    // FIXME: used for query planning
    return {};
  }

  const InternalKeyComparator& GetInternalKeyComparator() const override {
    return ikey_comparator_;
  }

  uint64_t ApproximateOldestKeyTime() const override {
    // FIXME: can use the time when this is added to the DB.
    return kUnknownOldestAncesterTime;
  }

  bool IsFragmentedRangeTombstonesConstructed() const override {
    assert(!wbwi_->GetWriteBatch()->HasDeleteRange());
    return true;
  }

  const Slice& GetNewestUDT() const override {
    // FIXME: support UDT
    assert(false);
    return newest_udt_;
  }

  // Assign a sequence number to the entries in this memtable.
  void AssignSequenceNumbers(const SeqnoRange& seqno_range) {
    // Not expecting to assign seqno multiple times.
    assert(assigned_seqno_.lower_bound == kMaxSequenceNumber);
    assert(assigned_seqno_.upper_bound == kMaxSequenceNumber);

    assigned_seqno_ = seqno_range;

    assert(assigned_seqno_.lower_bound <= assigned_seqno_.upper_bound);
    assert(assigned_seqno_.upper_bound != kMaxSequenceNumber);
  }

  void SetMinPrepLog(uint64_t min_prep_log) {
    min_prep_log_referenced_ = min_prep_log;
  }

 private:
  inline InternalIterator* NewIterator() const;

  Slice newest_udt_;
  std::shared_ptr<WriteBatchWithIndex> wbwi_;
  const Comparator* comparator_;
  InternalKeyComparator ikey_comparator_;
  SeqnoRange assigned_seqno_;
  const ImmutableMemTableOptions moptions_;
  SystemClock* clock_;
  uint64_t min_prep_log_referenced_{0};
  uint64_t num_entries_;
  // WBWI can contains updates to multiple CFs. `cf_id_` determines which CF
  // this memtable is for.
  uint32_t cf_id_;
};

class WBWIMemTableIterator final : public InternalIterator {
 public:
  WBWIMemTableIterator(std::unique_ptr<WBWIIterator>&& it,
                       const WBWIMemTable::SeqnoRange& assigned_seqno,
                       const Comparator* comparator, bool for_flush)
      : it_(std::move(it)),
        assigned_seqno_(assigned_seqno),
        comparator_(comparator),
        emit_overwritten_single_del_(for_flush) {
    assert(assigned_seqno_.lower_bound <= assigned_seqno_.upper_bound);
    assert(assigned_seqno_.upper_bound < kMaxSequenceNumber);
    s_.PermitUncheckedError();
  }

  // No copying allowed
  WBWIMemTableIterator(const WBWIMemTableIterator&) = delete;
  WBWIMemTableIterator& operator=(const WBWIMemTableIterator&) = delete;

  bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    it_->SeekToFirst();
    UpdateKey();
  }

  void SeekToLast() override {
    it_->SeekToLast();
    UpdateKey();
  }

  void Seek(const Slice& target) override {
    Slice target_user_key = ExtractUserKey(target);
    it_->Seek(target_user_key);
    if (it_->Valid()) {
      // compare seqno
      SequenceNumber seqno = GetInternalKeySeqno(target);
      assert(!emit_overwritten_single_del_);
      // For now all keys are assigned seqno_ub_, this may change after merge
      // is supported.
      assert(seqno <= assigned_seqno_.lower_bound ||
             seqno >= assigned_seqno_.upper_bound);
      if (seqno < assigned_seqno_.upper_bound &&
          comparator_->Compare(it_->Entry().key, target_user_key) == 0) {
        it_->Next();
        // TODO: cannot assume distinct keys once Merge is supported
        if (it_->Valid()) {
          assert(comparator_->Compare(it_->Entry().key, target_user_key) > 0);
        }
      }
    }
    UpdateKey();
  }

  void SeekForPrev(const Slice& target) override {
    Slice target_user_key = ExtractUserKey(target);
    it_->SeekForPrev(target_user_key);
    if (it_->Valid()) {
      SequenceNumber seqno = GetInternalKeySeqno(target);
      assert(seqno <= assigned_seqno_.lower_bound ||
             seqno >= assigned_seqno_.upper_bound);
      if (seqno > assigned_seqno_.upper_bound &&
          comparator_->Compare(it_->Entry().key, target_user_key) == 0) {
        it_->Prev();
        if (it_->Valid()) {
          // TODO: cannot assume distinct keys once Merge is supported
          assert(comparator_->Compare(it_->Entry().key, target_user_key) < 0);
        }
      }
    }
    UpdateKey();
  }

  void Next() override {
    // Only need to emit single deletion during flush. Since Flush does
    // sequential forward scan, we only need to emit single deletion in Next(),
    // and do not need to consider iterator direction change.
    assert(Valid());
    if (emit_overwritten_single_del_) {
      if (it_->HasOverWrittenSingleDel() && !at_overwritten_single_del_) {
        UpdateSingleDeleteKey();
        return;
      }
      at_overwritten_single_del_ = false;
    }

    it_->Next();
    UpdateKey();
  }

  bool NextAndGetResult(IterateResult* result) override {
    assert(Valid());
    Next();
    bool is_valid = Valid();
    if (is_valid) {
      result->key = key();
      result->bound_check_result = IterBoundCheck::kUnknown;
      result->value_prepared = true;
    }
    return is_valid;
  }

  void Prev() override {
    assert(Valid());
    it_->Prev();
    UpdateKey();
  }

  Slice key() const override {
    assert(Valid());
    return key_;
  }

  Slice value() const override {
    assert(Valid());
    // TODO: it_->Entry() is not trivial, cache it
    return it_->Entry().value;
  }

  Status status() const override {
    assert(it_->status().ok());
    return s_;
  }

  bool IsValuePinned() const override { return true; }

 private:
  static const std::unordered_map<WriteType, ValueType> WriteTypeToValueTypeMap;

  void UpdateKey() {
    valid_ = it_->Valid();
    if (!Valid()) {
      key_.clear();
      return;
    }
    auto t = WriteTypeToValueTypeMap.find(it_->Entry().type);
    assert(t != WriteTypeToValueTypeMap.end());
    if (t == WriteTypeToValueTypeMap.end()) {
      key_.clear();
      valid_ = false;
      s_ = Status::Corruption("Unexpected write_batch_with_index entry type " +
                              std::to_string(t->second));
      return;
    }
    key_buf_.SetInternalKey(it_->Entry().key, assigned_seqno_.upper_bound,
                            t->second);
    key_ = key_buf_.GetInternalKey();
  }

  void UpdateSingleDeleteKey() {
    assert(it_->Valid());
    assert(Valid());
    assert(assigned_seqno_.lower_bound < assigned_seqno_.upper_bound);
    key_buf_.SetInternalKey(it_->Entry().key, assigned_seqno_.upper_bound - 1,
                            kTypeSingleDeletion);
    key_ = key_buf_.GetInternalKey();
    at_overwritten_single_del_ = true;
  }

  std::unique_ptr<WBWIIterator> it_;
  const WBWIMemTable::SeqnoRange assigned_seqno_;
  const Comparator* comparator_;
  IterKey key_buf_;
  // The current internal key.
  Slice key_;
  Status s_;
  bool valid_ = false;
  bool at_overwritten_single_del_ = false;
  bool emit_overwritten_single_del_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
