// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <limits>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "memtable/skiplist.h"
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/write_batch_with_index.h"

namespace ROCKSDB_NAMESPACE {

class WBWIIteratorImpl;
struct Options;
struct ImmutableOptions;

// when direction == forward
// * current_at_base_ <=> base_iterator > delta_iterator
// when direction == backwards
// * current_at_base_ <=> base_iterator < delta_iterator
// always:
// * equal_keys_ <=> base_iterator == delta_iterator
class BaseDeltaIterator : public Iterator {
 public:
  BaseDeltaIterator(ColumnFamilyHandle* column_family, Iterator* base_iterator,
                    WBWIIteratorImpl* delta_iterator,
                    const Comparator* comparator,
                    const ReadOptions* read_options);

  ~BaseDeltaIterator() override;

  bool Valid() const override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Seek(const Slice& k) override;
  void SeekForPrev(const Slice& k) override;
  void Next() override;
  void Prev() override;

  bool PrepareValue() override {
    assert(Valid());

    if (!allow_unprepared_value_) {
      return true;
    }

    if (!current_at_base_) {
      return true;
    }

    SetValueAndColumnsFromBase();

    return Valid();
  }

  Slice key() const override;
  Slice value() const override { return value_; }
  const WideColumns& columns() const override { return columns_; }
  Slice timestamp() const override;
  Status status() const override;
  void Invalidate(Status s);

 private:
  void AssertInvariants();
  void Advance();
  void AdvanceDelta();
  void AdvanceBase();
  bool BaseValid() const;
  bool DeltaValid() const;
  void ResetValueAndColumns();
  void SetValueAndColumnsFromBase();
  void SetValueAndColumnsFromDelta();
  void UpdateCurrent();

  bool forward_;
  bool current_at_base_;
  bool equal_keys_;
  bool allow_unprepared_value_;
  Status status_;
  ColumnFamilyHandle* column_family_;
  std::unique_ptr<Iterator> base_iterator_;
  std::unique_ptr<WBWIIteratorImpl> delta_iterator_;
  const Comparator* comparator_;  // not owned
  MergeContext merge_context_;
  std::string merge_result_;
  Slice value_;
  WideColumns columns_;
};

// Key used by skip list, as the binary searchable index of WriteBatchWithIndex.
struct WriteBatchIndexEntry {
  WriteBatchIndexEntry(size_t o, uint32_t c, size_t ko, size_t ksz)
      : offset(o),
        column_family(c),
        has_single_del(false),
        has_overwritten_single_del(false),
        key_offset(ko),
        key_size(ksz),
        search_key(nullptr) {}
  // Create a dummy entry as the search key. This index entry won't be backed
  // by an entry from the write batch, but a pointer to the search key. Or a
  // special flag of offset can indicate we are seek to first.
  // @_search_key: the search key
  // @_column_family: column family
  // @is_forward_direction: true for Seek(). False for SeekForPrev()
  // @is_seek_to_first: true if we seek to the beginning of the column family
  //                    _search_key should be null in this case.
  WriteBatchIndexEntry(const Slice* _search_key, uint32_t _column_family,
                       bool is_forward_direction, bool is_seek_to_first)
      // For SeekForPrev(), we need to make the dummy entry larger than any
      // entry who has the same search key. Otherwise, we'll miss those entries.
      : offset(is_forward_direction ? 0 : std::numeric_limits<size_t>::max()),
        column_family(_column_family),
        has_single_del(false),
        has_overwritten_single_del(false),
        key_offset(0),
        key_size(is_seek_to_first ? kFlagMinInCf : 0),
        search_key(_search_key) {
    assert(_search_key != nullptr || is_seek_to_first);
  }

  // If this flag appears in the key_size, it indicates a
  // key that is smaller than any other entry for the same column family.
  static const size_t kFlagMinInCf = std::numeric_limits<size_t>::max();

  bool is_min_in_cf() const {
    assert(key_size != kFlagMinInCf ||
           (key_offset == 0 && search_key == nullptr));
    return key_size == kFlagMinInCf;
  }

  // offset of an entry in write batch's string buffer. If this is a dummy
  // lookup key, in which case search_key != nullptr, offset is set to either
  // 0 or max, only for comparison purpose. Because when entries have the same
  // key, the entry with larger offset is larger, offset = 0 will make a seek
  // key small or equal than all the entries with the seek key, so that Seek()
  // will find all the entries of the same key. Similarly, offset = MAX will
  // make the entry just larger than all entries with the search key so
  // SeekForPrev() will see all the keys with the same key.
  size_t offset;
  uint32_t column_family;  // column family of the entry.
  bool has_single_del;     // whether single del was issued for this key
  bool has_overwritten_single_del;  // whether a single del for this key was
                                    // overwritten by another key
  size_t key_offset;       // offset of the key in write batch's string buffer.
  size_t key_size;         // size of the key. kFlagMinInCf indicates
                           // that this is a dummy look up entry for
                           // SeekToFirst() to the beginning of the column
                           // family. We use the flag here to save a boolean
                           // in the struct.

  const Slice* search_key;  // if not null, instead of reading keys from
                            // write batch, use it to compare. This is used
                            // for lookup key.
};

class ReadableWriteBatch : public WriteBatch {
 public:
  explicit ReadableWriteBatch(size_t reserved_bytes = 0, size_t max_bytes = 0,
                              size_t protection_bytes_per_key = 0,
                              size_t default_cf_ts_sz = 0)
      : WriteBatch(reserved_bytes, max_bytes, protection_bytes_per_key,
                   default_cf_ts_sz) {}
  // Retrieve some information from a write entry in the write batch, given
  // the start offset of the write entry.
  Status GetEntryFromDataOffset(size_t data_offset, WriteType* type, Slice* key,
                                Slice* value, Slice* blob, Slice* xid) const;
};

class WriteBatchEntryComparator {
 public:
  WriteBatchEntryComparator(const Comparator* _default_comparator,
                            const ReadableWriteBatch* write_batch)
      : default_comparator_(_default_comparator), write_batch_(write_batch) {}
  // Compare a and b. Return a negative value if a is less than b, 0 if they
  // are equal, and a positive value if a is greater than b
  int operator()(const WriteBatchIndexEntry* entry1,
                 const WriteBatchIndexEntry* entry2) const;

  int CompareKey(uint32_t column_family, const Slice& key1,
                 const Slice& key2) const;

  void SetComparatorForCF(uint32_t column_family_id,
                          const Comparator* comparator) {
    if (column_family_id >= cf_comparators_.size()) {
      cf_comparators_.resize(column_family_id + 1, nullptr);
    }
    cf_comparators_[column_family_id] = comparator;
  }

  const Comparator* default_comparator() { return default_comparator_; }

  const Comparator* GetComparator(
      const ColumnFamilyHandle* column_family) const;

  const Comparator* GetComparator(uint32_t column_family) const;

 private:
  const Comparator* const default_comparator_;
  std::vector<const Comparator*> cf_comparators_;
  const ReadableWriteBatch* const write_batch_;
};

using WriteBatchEntrySkipList =
    SkipList<WriteBatchIndexEntry*, const WriteBatchEntryComparator&>;

class WBWIIteratorImpl final : public WBWIIterator {
 public:
  enum Result : uint8_t {
    kFound,
    kDeleted,
    kNotFound,
    kMergeInProgress,
    kError
  };
  WBWIIteratorImpl(uint32_t column_family_id,
                   WriteBatchEntrySkipList* skip_list,
                   const ReadableWriteBatch* write_batch,
                   WriteBatchEntryComparator* comparator,
                   const Slice* iterate_lower_bound = nullptr,
                   const Slice* iterate_upper_bound = nullptr)
      : column_family_id_(column_family_id),
        skip_list_iter_(skip_list),
        write_batch_(write_batch),
        comparator_(comparator),
        iterate_lower_bound_(iterate_lower_bound),
        iterate_upper_bound_(iterate_upper_bound) {}

  ~WBWIIteratorImpl() override {}

  bool Valid() const override {
    return !out_of_bound_ && ValidRegardlessOfBoundLimit();
  }

  void SeekToFirst() override {
    if (iterate_lower_bound_ != nullptr) {
      WriteBatchIndexEntry search_entry(
          iterate_lower_bound_ /* search_key */, column_family_id_,
          true /* is_forward_direction */, false /* is_seek_to_first */);
      skip_list_iter_.Seek(&search_entry);
    } else {
      WriteBatchIndexEntry search_entry(
          nullptr /* search_key */, column_family_id_,
          true /* is_forward_direction */, true /* is_seek_to_first */);
      skip_list_iter_.Seek(&search_entry);
    }

    if (ValidRegardlessOfBoundLimit()) {
      out_of_bound_ = TestOutOfBound();
    }
  }

  void SeekToLast() override {
    WriteBatchIndexEntry search_entry =
        (iterate_upper_bound_ != nullptr)
            ? WriteBatchIndexEntry(
                  iterate_upper_bound_ /* search_key */, column_family_id_,
                  true /* is_forward_direction */, false /* is_seek_to_first */)
            : WriteBatchIndexEntry(
                  nullptr /* search_key */, column_family_id_ + 1,
                  true /* is_forward_direction */, true /* is_seek_to_first */);

    skip_list_iter_.Seek(&search_entry);
    if (!skip_list_iter_.Valid()) {
      skip_list_iter_.SeekToLast();
    } else {
      skip_list_iter_.Prev();
    }

    if (ValidRegardlessOfBoundLimit()) {
      out_of_bound_ = TestOutOfBound();
    }
  }

  void Seek(const Slice& key) override {
    if (BeforeLowerBound(&key)) {  // cap to prevent out of bound
      SeekToFirst();
      return;
    }

    WriteBatchIndexEntry search_entry(&key, column_family_id_,
                                      true /* is_forward_direction */,
                                      false /* is_seek_to_first */);
    skip_list_iter_.Seek(&search_entry);

    if (ValidRegardlessOfBoundLimit()) {
      out_of_bound_ = TestOutOfBound();
    }
  }

  void SeekForPrev(const Slice& key) override {
    if (AtOrAfterUpperBound(&key)) {  // cap to prevent out of bound
      SeekToLast();
      return;
    }

    WriteBatchIndexEntry search_entry(&key, column_family_id_,
                                      false /* is_forward_direction */,
                                      false /* is_seek_to_first */);
    skip_list_iter_.SeekForPrev(&search_entry);

    if (ValidRegardlessOfBoundLimit()) {
      out_of_bound_ = TestOutOfBound();
    }
  }

  void Next() override {
    skip_list_iter_.Next();
    if (ValidRegardlessOfBoundLimit()) {
      out_of_bound_ = TestOutOfBound();
    }
  }

  void Prev() override {
    skip_list_iter_.Prev();
    if (ValidRegardlessOfBoundLimit()) {
      out_of_bound_ = TestOutOfBound();
    }
  }

  WriteEntry Entry() const override;

  bool HasOverWrittenSingleDel() const override {
    assert(Valid());
    return skip_list_iter_.key()->has_overwritten_single_del;
  }

  Status status() const override {
    // this is in-memory data structure, so the only way status can be non-ok is
    // through memory corruption
    return Status::OK();
  }

  const WriteBatchIndexEntry* GetRawEntry() const {
    return skip_list_iter_.key();
  }

  bool MatchesKey(uint32_t cf_id, const Slice& key);

  // Moves the iterator to first entry of the previous key.
  void PrevKey();
  // Moves the iterator to first entry of the next key.
  void NextKey();

  // Moves the iterator to the Update (Put, PutEntity or Delete) for the current
  // key. If there is no Put/PutEntity/Delete, the Iterator will point to the
  // first entry for this key.
  // @return kFound if a Put/PutEntity was found for the key
  // @return kDeleted if a delete was found for the key
  // @return kMergeInProgress if only merges were found for the key
  // @return kError if an unsupported operation was found for the key
  // @return kNotFound if no operations were found for this key
  //
  Result FindLatestUpdate(const Slice& key, MergeContext* merge_context);
  Result FindLatestUpdate(MergeContext* merge_context);

 protected:
  void AdvanceKey(bool forward);

 private:
  uint32_t column_family_id_;
  WriteBatchEntrySkipList::Iterator skip_list_iter_;
  const ReadableWriteBatch* write_batch_;
  WriteBatchEntryComparator* comparator_;
  const Slice* iterate_lower_bound_;
  const Slice* iterate_upper_bound_;
  bool out_of_bound_ = false;

  bool TestOutOfBound() const {
    const Slice& curKey = Entry().key;
    return AtOrAfterUpperBound(&curKey) || BeforeLowerBound(&curKey);
  }

  bool ValidRegardlessOfBoundLimit() const {
    if (!skip_list_iter_.Valid()) {
      return false;
    }
    const WriteBatchIndexEntry* iter_entry = skip_list_iter_.key();
    return iter_entry != nullptr &&
           iter_entry->column_family == column_family_id_;
  }

  bool AtOrAfterUpperBound(const Slice* k) const {
    if (iterate_upper_bound_ == nullptr) {
      return false;
    }

    return comparator_->GetComparator(column_family_id_)
               ->CompareWithoutTimestamp(*k, /*a_has_ts=*/false,
                                         *iterate_upper_bound_,
                                         /*b_has_ts=*/false) >= 0;
  }

  bool BeforeLowerBound(const Slice* k) const {
    if (iterate_lower_bound_ == nullptr) {
      return false;
    }

    return comparator_->GetComparator(column_family_id_)
               ->CompareWithoutTimestamp(*k, /*a_has_ts=*/false,
                                         *iterate_lower_bound_,
                                         /*b_has_ts=*/false) < 0;
  }
};

class WriteBatchWithIndexInternal {
 public:
  static const Comparator* GetUserComparator(const WriteBatchWithIndex& wbwi,
                                             uint32_t cf_id);

  template <typename... ResultTs>
  static Status MergeKeyWithNoBaseValue(ColumnFamilyHandle* column_family,
                                        const Slice& key,
                                        const MergeContext& context,
                                        ResultTs... results) {
    const ImmutableOptions* ioptions = nullptr;

    const Status s = CheckAndGetImmutableOptions(column_family, &ioptions);
    if (!s.ok()) {
      return s;
    }

    assert(ioptions);

    // `op_failure_scope` (an output parameter) is not provided (set to
    // nullptr) since a failure must be propagated regardless of its value.
    return MergeHelper::TimedFullMerge(
        ioptions->merge_operator.get(), key, MergeHelper::kNoBaseValue,
        context.GetOperands(), ioptions->logger, ioptions->stats,
        ioptions->clock, /* update_num_ops_stats */ false,
        /* op_failure_scope */ nullptr, results...);
  }

  template <typename BaseTag, typename BaseT, typename... ResultTs>
  static Status MergeKeyWithBaseValue(ColumnFamilyHandle* column_family,
                                      const Slice& key, const BaseTag& base_tag,
                                      const BaseT& value,
                                      const MergeContext& context,
                                      ResultTs... results) {
    const ImmutableOptions* ioptions = nullptr;

    const Status s = CheckAndGetImmutableOptions(column_family, &ioptions);
    if (!s.ok()) {
      return s;
    }

    assert(ioptions);

    // `op_failure_scope` (an output parameter) is not provided (set to
    // nullptr) since a failure must be propagated regardless of its value.
    return MergeHelper::TimedFullMerge(
        ioptions->merge_operator.get(), key, base_tag, value,
        context.GetOperands(), ioptions->logger, ioptions->stats,
        ioptions->clock, /* update_num_ops_stats */ false,
        /* op_failure_scope */ nullptr, results...);
  }

  // If batch contains a value for key, store it in *value and return kFound.
  // If batch contains a deletion for key, return Deleted.
  // If batch contains Merge operations as the most recent entry for a key,
  //   and the merge process does not stop (not reaching a value or delete),
  //   prepend the current merge operands to *operands,
  //   and return kMergeInProgress
  // If batch does not contain this key, return kNotFound
  // Else, return kError on error with error Status stored in *s.
  static WBWIIteratorImpl::Result GetFromBatch(
      WriteBatchWithIndex* batch, ColumnFamilyHandle* column_family,
      const Slice& key, MergeContext* merge_context, std::string* value,
      Status* s);

  static WBWIIteratorImpl::Result GetEntityFromBatch(
      WriteBatchWithIndex* batch, ColumnFamilyHandle* column_family,
      const Slice& key, MergeContext* merge_context,
      PinnableWideColumns* columns, Status* s);

 private:
  static Status CheckAndGetImmutableOptions(ColumnFamilyHandle* column_family,
                                            const ImmutableOptions** ioptions);

  template <typename Traits>
  static WBWIIteratorImpl::Result GetFromBatchImpl(
      WriteBatchWithIndex* batch, ColumnFamilyHandle* column_family,
      const Slice& key, MergeContext* merge_context,
      typename Traits::OutputType* output, Status* s);
};

}  // namespace ROCKSDB_NAMESPACE
