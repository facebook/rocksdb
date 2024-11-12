//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include "db/memtable.h"
#include "rocksdb/utilities/write_batch_with_index.h"

namespace ROCKSDB_NAMESPACE {

class WBWIMemTableIterator final : public InternalIterator {
 public:
  WBWIMemTableIterator(std::unique_ptr<WBWIIterator>&& it, SequenceNumber seqno,
                       const Comparator* comparator)
      : it_(std::move(it)), global_seqno_(seqno), comparator_(comparator) {
    assert(seqno != kMaxSequenceNumber);
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
      if (seqno < global_seqno_ &&
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
      if (seqno > global_seqno_ &&
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
    assert(Valid());
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
    return it_->Entry().value;
  }

  Status status() const override {
    assert(it_->status().ok());
    return s_;
  }

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
    key_buf_.SetInternalKey(it_->Entry().key, global_seqno_, t->second);
    key_ = key_buf_.GetInternalKey();
  }

  std::unique_ptr<WBWIIterator> it_;
  // The sequence number of entries in this write batch.
  SequenceNumber global_seqno_;
  const Comparator* comparator_;
  IterKey key_buf_;
  // The current internal key.
  Slice key_;
  Status s_;
  bool valid_ = false;
};

class WBWIMemTable final : public ReadOnlyMemTable {
 public:
  WBWIMemTable(const std::shared_ptr<WriteBatchWithIndex>& wbwi,
               const Comparator* cmp, uint32_t cf_id,
               const ImmutableOptions* immutable_options,
               const MutableCFOptions* cf_options)
      : wbwi_(wbwi),
        comparator_(cmp),
        ikey_comparator_(comparator_),
        moptions_(*immutable_options, *cf_options),
        clock_(immutable_options->clock),
        cf_id_(cf_id) {}

  // No copying allowed
  WBWIMemTable(const WBWIMemTable&) = delete;
  WBWIMemTable& operator=(const WBWIMemTable&) = delete;

  ~WBWIMemTable() override = default;

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

  InternalIterator* NewIterator(
      const ReadOptions&, UnownedPtr<const SeqnoToTimeMapping>, Arena* arena,
      const SliceTransform* /* prefix_extractor */) override {
    // Ingested WBWIMemTable should have an assigned seqno
    assert(global_seqno_ != kMaxSequenceNumber);
    assert(arena);
    auto mem = arena->AllocateAligned(sizeof(WBWIMemTableIterator));
    return new (mem) WBWIMemTableIterator(
        std::unique_ptr<WBWIIterator>(wbwi_->NewIterator(cf_id_)),
        global_seqno_, comparator_);
  }

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

  uint64_t NumEntries() const override {
    // FIXME: used in
    // - verify number of entries processed during flush
    // - stats for estimate num entries and num entries in immutable memtables
    // - MemPurgeDecider
    return 0;
  }

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

  SequenceNumber GetEarliestSequenceNumber() override { return global_seqno_; }

  bool IsEmpty() const override {
    // Ideally also check that wbwi contains updates from this CF. For now, we
    // only create WBWIMemTable for CFs with updates in wbwi.
    return wbwi_->GetWriteBatch()->Count() == 0;
  }

  SequenceNumber GetFirstSequenceNumber() override { return global_seqno_; }

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
  void SetGlobalSequenceNumber(SequenceNumber global_seqno) {
    // Not expecting to assign seqno multiple times.
    assert(global_seqno_ == kMaxSequenceNumber);
    global_seqno_ = global_seqno;
  }

 private:
  InternalIterator* NewIterator() const {
    assert(global_seqno_ != kMaxSequenceNumber);
    return new WBWIMemTableIterator(
        std::unique_ptr<WBWIIterator>(wbwi_->NewIterator(cf_id_)),
        global_seqno_, comparator_);
  }

  Slice newest_udt_;
  std::shared_ptr<WriteBatchWithIndex> wbwi_;
  const Comparator* comparator_;
  InternalKeyComparator ikey_comparator_;
  SequenceNumber global_seqno_ = kMaxSequenceNumber;
  const ImmutableMemTableOptions moptions_;
  SystemClock* clock_;
  uint64_t min_prep_log_referenced_{0};
  // WBWI can contains updates to multiple CFs. `cf_id_` determines which CF
  // this memtable is for.
  uint32_t cf_id_;
};

}  // namespace ROCKSDB_NAMESPACE
