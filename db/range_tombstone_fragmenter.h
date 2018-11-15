//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <list>
#include <memory>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "rocksdb/status.h"
#include "table/internal_iterator.h"

namespace rocksdb {

struct FragmentedRangeTombstoneList {
 public:
  FragmentedRangeTombstoneList(
      std::unique_ptr<InternalIterator> unfragmented_tombstones,
      const InternalKeyComparator& icmp, bool one_time_use,
      SequenceNumber snapshot = kMaxSequenceNumber);

  std::vector<RangeTombstone>::const_iterator begin() const {
    return tombstones_.begin();
  }

  std::vector<RangeTombstone>::const_iterator end() const {
    return tombstones_.end();
  }

  bool empty() const { return tombstones_.size() == 0; }

 private:
  // Given an ordered range tombstone iterator unfragmented_tombstones,
  // "fragment" the tombstones into non-overlapping pieces, and store them in
  // tombstones_.
  void FragmentTombstones(
      std::unique_ptr<InternalIterator> unfragmented_tombstones,
      const InternalKeyComparator& icmp, bool one_time_use,
      SequenceNumber snapshot = kMaxSequenceNumber);

  std::vector<RangeTombstone> tombstones_;
  std::list<std::string> pinned_slices_;
  PinnedIteratorsManager pinned_iters_mgr_;
};

// FragmentedRangeTombstoneIterator converts an InternalIterator of a range-del
// meta block into an iterator over non-overlapping tombstone fragments. The
// tombstone fragmentation process should be more efficient than the range
// tombstone collapsing algorithm in RangeDelAggregator because this leverages
// the internal key ordering already provided by the input iterator, if
// applicable (when the iterator is unsorted, a new sorted iterator is created
// before proceeding). If there are few overlaps, creating a
// FragmentedRangeTombstoneIterator should be O(n), while the RangeDelAggregator
// tombstone collapsing is always O(n log n).
class FragmentedRangeTombstoneIterator : public InternalIterator {
 public:
  FragmentedRangeTombstoneIterator(
      const FragmentedRangeTombstoneList* tombstones,
      const InternalKeyComparator& icmp);
  FragmentedRangeTombstoneIterator(
      const std::shared_ptr<const FragmentedRangeTombstoneList>& tombstones,
      const InternalKeyComparator& icmp);
  void SeekToFirst() override;
  void SeekToLast() override;
  void Seek(const Slice& target) override;
  void SeekForPrev(const Slice& target) override;
  void Next() override;
  void Prev() override;
  bool Valid() const override;
  Slice key() const override {
    MaybePinKey();
    return current_start_key_.Encode();
  }
  Slice value() const override { return pos_->end_key_; }
  bool IsKeyPinned() const override { return false; }
  bool IsValuePinned() const override { return true; }
  Status status() const override { return Status::OK(); }

  Slice user_key() const { return pos_->start_key_; }
  SequenceNumber seq() const { return pos_->seq_; }

 private:
  struct FragmentedRangeTombstoneComparator {
    explicit FragmentedRangeTombstoneComparator(const Comparator* c) : cmp(c) {}

    bool operator()(const RangeTombstone& a, const RangeTombstone& b) const {
      int user_key_cmp = cmp->Compare(a.start_key_, b.start_key_);
      if (user_key_cmp != 0) {
        return user_key_cmp < 0;
      }
      return a.seq_ > b.seq_;
    }

    const Comparator* cmp;
  };

  void MaybePinKey() const {
    if (pos_ != tombstones_->end() && pinned_pos_ != pos_) {
      current_start_key_.Set(pos_->start_key_, pos_->seq_, kTypeRangeDeletion);
      pinned_pos_ = pos_;
    }
  }

  void ParseKey(ParsedInternalKey* parsed) const {
    parsed->user_key = pos_->start_key_;
    parsed->sequence = pos_->seq_;
    parsed->type = kTypeRangeDeletion;
  }

  const FragmentedRangeTombstoneComparator tombstone_cmp_;
  const InternalKeyComparator* icmp_;
  const Comparator* ucmp_;
  std::shared_ptr<const FragmentedRangeTombstoneList> tombstones_ref_;
  const FragmentedRangeTombstoneList* tombstones_;
  std::vector<RangeTombstone>::const_iterator pos_;
  mutable std::vector<RangeTombstone>::const_iterator pinned_pos_;
  mutable InternalKey current_start_key_;
  PinnedIteratorsManager pinned_iters_mgr_;
};

SequenceNumber MaxCoveringTombstoneSeqnum(
    FragmentedRangeTombstoneIterator* tombstone_iter, const Slice& key,
    const Comparator* ucmp);

}  // namespace rocksdb
