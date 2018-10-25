//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/range_del_aggregator.h"
#include "util/heap.h"

#include <algorithm>

namespace rocksdb {

struct TombstoneStartKeyComparator {
  explicit TombstoneStartKeyComparator(const InternalKeyComparator* c)
      : cmp(c) {}

  bool operator()(const TruncatedRangeTombstone& a,
                  const TruncatedRangeTombstone& b) const {
    return cmp->Compare(a.start_key_, b.start_key_) < 0;
  }

  const InternalKeyComparator* cmp;
};

// An UncollapsedRangeDelMap is quick to create but slow to answer ShouldDelete
// queries.
class UncollapsedRangeDelMap : public RangeDelMap {
  typedef std::vector<TruncatedRangeTombstone> Rep;

  class Iterator : public RangeDelIterator {
    const Rep& rep_;
    Rep::const_iterator iter_;

   public:
    Iterator(const Rep& rep) : rep_(rep), iter_(rep.begin()) {}
    bool Valid() const override { return iter_ != rep_.end(); }
    void Next() override { iter_++; }

    void Seek(const Slice&) override {
      fprintf(stderr,
              "UncollapsedRangeDelMap::Iterator::Seek(Slice&) unimplemented\n");
      abort();
    }

    void Seek(const ParsedInternalKey&) override {
      fprintf(stderr,
              "UncollapsedRangeDelMap::Iterator::Seek(ParsedInternalKey&) "
              "unimplemented\n");
      abort();
    }

    RangeTombstone Tombstone() const override { return iter_->Tombstone(); }
  };

  Rep rep_;
  const InternalKeyComparator* icmp_;

 public:
  explicit UncollapsedRangeDelMap(const InternalKeyComparator* icmp)
      : icmp_(icmp) {}

  bool ShouldDelete(const ParsedInternalKey& parsed,
                    RangeDelPositioningMode mode) override {
    (void)mode;
    assert(mode == RangeDelPositioningMode::kFullScan);
    for (const auto& tombstone : rep_) {
      if (icmp_->Compare(parsed, tombstone.start_key_) < 0) {
        continue;
      }
      if (parsed.sequence < tombstone.seq_ &&
          icmp_->Compare(parsed, tombstone.end_key_) < 0) {
        return true;
      }
    }
    return false;
  }

  bool IsRangeOverlapped(const ParsedInternalKey& start,
                         const ParsedInternalKey& end) override {
    for (const auto& tombstone : rep_) {
      if (icmp_->Compare(start, tombstone.end_key_) < 0 &&
          icmp_->Compare(tombstone.start_key_, end) <= 0 &&
          icmp_->Compare(tombstone.start_key_, tombstone.end_key_) < 0) {
        return true;
      }
    }
    return false;
  }

  void AddTombstone(TruncatedRangeTombstone tombstone) override {
    rep_.emplace_back(tombstone);
  }

  size_t Size() const override { return rep_.size(); }

  void InvalidatePosition() override {}  // no-op

  std::unique_ptr<RangeDelIterator> NewIterator() override {
    std::sort(rep_.begin(), rep_.end(), TombstoneStartKeyComparator(icmp_));
    return std::unique_ptr<RangeDelIterator>(new Iterator(this->rep_));
  }
};

// A CollapsedRangeDelMap is slow to create but quick to answer ShouldDelete
// queries.
//
// An explanation of the design follows. Suppose we have tombstones [b, n) @ 1,
// [e, h) @ 2, [q, t) @ 2, and [g, k) @ 3. Visually, the tombstones look like
// this:
//
//     3:        g---k
//     2:     e---h        q--t
//     1:  b------------n
//
// The CollapsedRangeDelMap representation is based on the observation that
// wherever tombstones overlap, we need only store the tombstone with the
// largest seqno. From the perspective of a read at seqno 4 or greater, this set
// of tombstones is exactly equivalent:
//
//     3:        g---k
//     2:     e--g         q--t
//     1:  b--e      k--n
//
// Because these tombstones do not overlap, they can be efficiently represented
// in an ordered map from keys to sequence numbers. Each entry should be thought
// of as a transition from one tombstone to the next. In this example, the
// CollapsedRangeDelMap would store the following entries, in order:
//
//     b → 1, e → 2, g → 3, k → 1, n → 0, q → 2, t → 0
//
// If a tombstone ends before the next tombstone begins, a sentinel seqno of 0
// is installed to indicate that no tombstone exists. This occurs at keys n and
// t in the example above.
//
// To check whether a key K is covered by a tombstone, the map is binary
// searched for the last key less than K. K is covered iff the map entry has a
// larger seqno than K. As an example, consider the key h @ 4. It would be
// compared against the map entry g → 3 and determined to be uncovered. By
// contrast, the key h @ 2 would be determined to be covered.
class CollapsedRangeDelMap : public RangeDelMap {
  typedef std::map<ParsedInternalKey, SequenceNumber,
                   ParsedInternalKeyComparator>
      Rep;

  class Iterator : public RangeDelIterator {
    void MaybeSeekPastSentinel() {
      if (Valid() && iter_->second == 0) {
        iter_++;
      }
    }

    const Rep& rep_;
    Rep::const_iterator iter_;

   public:
    Iterator(const Rep& rep) : rep_(rep), iter_(rep.begin()) {}

    bool Valid() const override { return iter_ != rep_.end(); }

    void Next() override {
      iter_++;
      MaybeSeekPastSentinel();
    }

    void Seek(const Slice&) override {
      fprintf(stderr, "CollapsedRangeDelMap::Iterator::Seek(Slice&) unimplemented\n");
      abort();
    }

    void Seek(const ParsedInternalKey& target) override {
      iter_ = rep_.upper_bound(target);
      if (iter_ != rep_.begin()) {
        iter_--;
      }
      MaybeSeekPastSentinel();
    }

    RangeTombstone Tombstone() const override {
      assert(Valid());
      assert(std::next(iter_) != rep_.end());
      assert(iter_->second != 0);
      RangeTombstone tombstone;
      tombstone.start_key_ = iter_->first.user_key;
      tombstone.end_key_ = std::next(iter_)->first.user_key;
      tombstone.seq_ = iter_->second;
      return tombstone;
    }
  };

  Rep rep_;
  Rep::iterator iter_;
  const InternalKeyComparator* icmp_;

 public:
  explicit CollapsedRangeDelMap(const InternalKeyComparator* icmp)
    : rep_(ParsedInternalKeyComparator(icmp)),
      icmp_(icmp) {
    InvalidatePosition();
  }

  bool ShouldDelete(const ParsedInternalKey& parsed,
                    RangeDelPositioningMode mode) override {
    if (iter_ == rep_.end() &&
        (mode == RangeDelPositioningMode::kForwardTraversal ||
         mode == RangeDelPositioningMode::kBackwardTraversal)) {
      // invalid (e.g., if AddTombstones() changed the deletions), so need to
      // reseek
      mode = RangeDelPositioningMode::kBinarySearch;
    }
    switch (mode) {
      case RangeDelPositioningMode::kFullScan:
        assert(false);
      case RangeDelPositioningMode::kForwardTraversal:
        assert(iter_ != rep_.end());
        if (iter_ == rep_.begin() &&
            icmp_->Compare(parsed, iter_->first) < 0) {
          // before start of deletion intervals
          return false;
        }
        while (std::next(iter_) != rep_.end() &&
               icmp_->Compare(std::next(iter_)->first, parsed) <= 0) {
          ++iter_;
        }
        break;
      case RangeDelPositioningMode::kBackwardTraversal:
        assert(iter_ != rep_.end());
        while (iter_ != rep_.begin() &&
               icmp_->Compare(parsed, iter_->first) < 0) {
          --iter_;
        }
        if (iter_ == rep_.begin() &&
            icmp_->Compare(parsed, iter_->first) < 0) {
          // before start of deletion intervals
          return false;
        }
        break;
      case RangeDelPositioningMode::kBinarySearch:
        iter_ = rep_.upper_bound(parsed);
        if (iter_ == rep_.begin()) {
          // before start of deletion intervals
          return false;
        }
        --iter_;
        break;
    }
    assert(iter_ != rep_.end() &&
           icmp_->Compare(iter_->first, parsed) <= 0);
    assert(std::next(iter_) == rep_.end() ||
           icmp_->Compare(parsed, std::next(iter_)->first) < 0);
    return parsed.sequence < iter_->second;
  }

  bool IsRangeOverlapped(const ParsedInternalKey&,
                         const ParsedInternalKey&) override {
    // Unimplemented because the only client of this method, file ingestion,
    // uses uncollapsed maps.
    fprintf(stderr, "CollapsedRangeDelMap::IsRangeOverlapped unimplemented");
    abort();
  }

  void AddTombstone(TruncatedRangeTombstone t) override {
    if (icmp_->Compare(t.start_key_, t.end_key_) >= 0 || t.seq_ == 0) {
      // The tombstone covers no keys. Nothing to do.
      return;
    }

    auto it = rep_.upper_bound(t.start_key_);
    auto prev_seq = [&]() {
      return it == rep_.begin() ? 0 : std::prev(it)->second;
    };

    // end_seq stores the seqno of the last transition that the new tombstone
    // covered. This is the seqno that we'll install if we need to insert a
    // transition for the new tombstone's end key.
    SequenceNumber end_seq = 0;

    // In the diagrams below, the new tombstone is always [c, k) @ 2. The
    // existing tombstones are varied to depict different scenarios. Uppercase
    // letters are used to indicate points that exist in the map, while
    // lowercase letters are used to indicate points that do not exist in the
    // map. The location of the iterator is marked with a caret; it may point
    // off the end of the diagram to indicate that it is positioned at a
    // entry with a larger key whose specific key is irrelevant.

    if (t.seq_ > prev_seq()) {
      // The new tombstone's start point covers the existing tombstone:
      //
      //     3:                3: A--C           3:                3:
      //     2:    c---   OR   2:    c---   OR   2:    c---   OR   2: c------
      //     1: A--C           1:                1: A------        1: C------
      //                ^                 ^                 ^                  ^
      end_seq = prev_seq();
      Rep::iterator pit;
      if (it != rep_.begin() && (pit = std::prev(it)) != rep_.begin() &&
          icmp_->Compare(pit->first, t.start_key_) == 0 &&
          std::prev(pit)->second == t.seq_) {
        // The new tombstone starts at the end of an existing tombstone with an
        // identical seqno:
        //
        //     3:
        //     2: A--C---
        //     1:
        //                ^
        // Merge the tombstones by removing the existing tombstone's end key.
        it = rep_.erase(std::prev(it));
      } else {
        // Insert a new transition at the new tombstone's start point, or raise
        // the existing transition at that point to the new tombstone's seqno.
        rep_[t.start_key_] = t.seq_;  // operator[] will overwrite existing entry
      }
    } else {
      // The new tombstone's start point is covered by an existing tombstone:
      //
      //      3: A-----   OR    3: C------   OR
      //      2:   c---         2: c------         2: C------
      //                ^                  ^                  ^
      // Do nothing.
    }

    // Look at all the existing transitions that overlap the new tombstone.
    while (it != rep_.end() && icmp_->Compare(it->first, t.end_key_) < 0) {
      if (t.seq_ >= it->second) {
        // The transition is to an existing tombstone that the new tombstone
        // covers. Save the covered tombstone's seqno. We'll need to return to
        // it if the new tombstone ends before the existing tombstone.
        end_seq = it->second;

        if (t.seq_ == prev_seq()) {
          // The previous transition is to the seqno of the new tombstone:
          //
          //     3:                3:                3: --F
          //     2: C------   OR   2: C------   OR   2:   F----
          //     1:    F---        1: ---F           1:     H--
          //           ^                 ^                  ^
          //
          // Erase this transition. It's been superseded.
          it = rep_.erase(it);
          continue;  // skip increment; erase positions iterator correctly
        } else {
          // The previous transition is to a tombstone that covers the new
          // tombstone, but this transition is to a tombstone that is covered by
          // the new tombstone. That is, this is the end of a run of existing
          // tombstones that cover the new tombstone:
          //
          //     3: A---E     OR   3:  E-G
          //     2:   c----        2: ------
          //            ^                ^
          // Preserve this transition point, but raise it to the new tombstone's
          // seqno.
          it->second = t.seq_;
        }
      } else {
        // The transition is to an existing tombstone that covers the new
        // tombstone:
        //
        //     4:              4: --F
        //     3:   F--   OR   3:   F--
        //     2: -----        2: -----
        //          ^               ^
        // Do nothing.
      }
      ++it;
    }

    if (t.seq_ == prev_seq()) {
      // The new tombstone is unterminated in the map.
      if (it != rep_.end() && t.seq_ == it->second &&
          icmp_->Compare(it->first, t.end_key_) == 0) {
        // The new tombstone ends at the start of another tombstone with an
        // identical seqno. Merge the tombstones by removing the existing
        // tombstone's start key.
        rep_.erase(it);
      } else if (end_seq == prev_seq() ||
                 (it != rep_.end() && end_seq == it->second)) {
        // The new tombstone is implicitly ended because its end point is
        // contained within an existing tombstone with the same seqno:
        //
        //     2: ---k--N
        //              ^
      } else {
        // The new tombstone needs an explicit end point.
        //
        //     3:             OR   3: --G       OR   3: --G   K--
        //     2: C-------k        2:   G---k        2:   G---k
        //                  ^                 ^               ^
        // Install one that returns to the last seqno we covered. Because end
        // keys are exclusive, if there's an existing transition at t.end_key_,
        // it takes precedence over the transition that we install here.
        rep_.emplace(t.end_key_,
                     end_seq);  // emplace is a noop if existing entry
      }
    } else {
      // The new tombstone is implicitly ended because its end point is covered
      // by an existing tombstone with a higher seqno.
      //
      //     3:   I---M   OR   3: A-----------M
      //     2: ----k          2:   c-------k
      //              ^                       ^
      // Do nothing.
    }
  }

  size_t Size() const override { return rep_.empty() ? 0 : rep_.size() - 1; }

  void InvalidatePosition() override { iter_ = rep_.end(); }

  std::unique_ptr<RangeDelIterator> NewIterator() override {
    return std::unique_ptr<RangeDelIterator>(new Iterator(this->rep_));
  }
};

RangeDelAggregator::RangeDelAggregator(
    const InternalKeyComparator& icmp,
    const std::vector<SequenceNumber>& snapshots,
    bool collapse_deletions /* = true */)
    : upper_bound_(kMaxSequenceNumber),
      icmp_(icmp),
      collapse_deletions_(collapse_deletions) {
  InitRep(snapshots);
}

RangeDelAggregator::RangeDelAggregator(const InternalKeyComparator& icmp,
                                       SequenceNumber snapshot,
                                       bool collapse_deletions /* = false */)
    : upper_bound_(snapshot),
      icmp_(icmp),
      collapse_deletions_(collapse_deletions) {}

void RangeDelAggregator::InitRep(const std::vector<SequenceNumber>& snapshots) {
  assert(rep_ == nullptr);
  rep_.reset(new Rep());
  rep_->snapshots_ = snapshots;
  // Data newer than any snapshot falls in this catch-all stripe
  rep_->snapshots_.emplace_back(kMaxSequenceNumber);
  rep_->pinned_iters_mgr_.StartPinning();
}

std::unique_ptr<RangeDelMap> RangeDelAggregator::NewRangeDelMap() {
  RangeDelMap* tombstone_map;
  if (collapse_deletions_) {
    tombstone_map = new CollapsedRangeDelMap(&icmp_);
  } else {
    tombstone_map = new UncollapsedRangeDelMap(&icmp_);
  }
  return std::unique_ptr<RangeDelMap>(tombstone_map);
}

bool RangeDelAggregator::ShouldDeleteImpl(const Slice& internal_key,
                                          RangeDelPositioningMode mode) {
  assert(rep_ != nullptr);
  ParsedInternalKey parsed;
  if (!ParseInternalKey(internal_key, &parsed)) {
    assert(false);
    return false;
  }
  return ShouldDeleteImpl(parsed, mode);
}

bool RangeDelAggregator::ShouldDeleteImpl(const ParsedInternalKey& parsed,
                                          RangeDelPositioningMode mode) {
  assert(IsValueType(parsed.type));
  assert(rep_ != nullptr);
  auto* tombstone_map = GetRangeDelMapIfExists(parsed.sequence);
  if (tombstone_map == nullptr || tombstone_map->IsEmpty()) {
    return false;
  }
  return tombstone_map->ShouldDelete(parsed, mode);
}

bool RangeDelAggregator::IsRangeOverlapped(const Slice& start,
                                           const Slice& end) {
  // Unimplemented because the only client of this method, file ingestion,
  // uses uncollapsed maps.
  assert(!collapse_deletions_);
  if (rep_ == nullptr) {
    return false;
  }
  ParsedInternalKey start_ikey(start, kMaxSequenceNumber, kMaxValue);
  ParsedInternalKey end_ikey(end, 0, static_cast<ValueType>(0));
  for (const auto& stripe : rep_->stripe_map_) {
    if (stripe.second.first->IsRangeOverlapped(start_ikey, end_ikey)) {
      return true;
    }
  }
  return false;
}

Status RangeDelAggregator::AddTombstones(
    std::unique_ptr<InternalIterator> input,
    const InternalKey* smallest,
    const InternalKey* largest) {
  if (input == nullptr) {
    return Status::OK();
  }
  input->SeekToFirst();
  bool first_iter = true;
  while (input->Valid()) {
    if (first_iter) {
      if (rep_ == nullptr) {
        InitRep({upper_bound_});
      } else {
        InvalidateRangeDelMapPositions();
      }
      first_iter = false;
    }
    ParsedInternalKey parsed_key;
    bool parsed;
    if (input->IsKeyPinned()) {
      parsed = ParseInternalKey(input->key(), &parsed_key);
    } else {
      // The tombstone map holds slices into the iterator's memory. Make a
      // copy of the key if it is not pinned.
      rep_->pinned_slices_.emplace_back(input->key().data(),
                                        input->key().size());
      parsed = ParseInternalKey(rep_->pinned_slices_.back(), &parsed_key);
    }
    if (!parsed) {
      return Status::Corruption("Unable to parse range tombstone InternalKey");
    }
    Slice end_user_key;
    if (input->IsValuePinned()) {
      end_user_key = input->value();
    } else {
      // The tombstone map holds slices into the iterator's memory. Make a
      // copy of the value if it is not pinned.
      rep_->pinned_slices_.emplace_back(input->value().data(),
                                        input->value().size());
      end_user_key = rep_->pinned_slices_.back();
    }
    ParsedInternalKey start_key(parsed_key.user_key, kMaxSequenceNumber,
                                kMaxValue);
    ParsedInternalKey end_key(end_user_key, kMaxSequenceNumber, kMaxValue);
    // Truncate the tombstone to the range [smallest, largest].
    if (smallest != nullptr) {
      ParsedInternalKey parsed_smallest;
      if (ParseInternalKey(smallest->Encode(), &parsed_smallest) &&
          icmp_.Compare(start_key, parsed_smallest) < 0) {
        start_key.user_key = parsed_smallest.user_key;
        start_key.sequence = parsed_smallest.sequence;
      }
    }
    if (largest != nullptr) {
      ParsedInternalKey parsed_largest;
      if (ParseInternalKey(largest->Encode(), &parsed_largest) &&
          icmp_.Compare(end_key, parsed_largest) > 0) {
        end_key.user_key = parsed_largest.user_key;
        if (parsed_largest.sequence != kMaxSequenceNumber) {
          // The same user key straddles two adjacent sstables. To make sure we
          // can truncate to a range that includes the largest point key in the
          // first sstable, set the tombstone end key's sequence number to 1
          // less than the largest key.
          assert(parsed_largest.sequence != 0);
          end_key.sequence = parsed_largest.sequence - 1;
        } else {
          // The SST file boundary was artificially extended by a range tombstone.
          // We will not see any entries in this SST with this user key, so we
          // can leave the seqnum at kMaxSequenceNumber.
        }
      }
    }
    TruncatedRangeTombstone tombstone(start_key, end_key, parsed_key.sequence);
    GetRangeDelMap(parsed_key.sequence).AddTombstone(std::move(tombstone));
    input->Next();
  }
  if (!first_iter) {
    rep_->pinned_iters_mgr_.PinIterator(input.release(), false /* arena */);
  }
  return Status::OK();
}

void RangeDelAggregator::InvalidateRangeDelMapPositions() {
  if (rep_ == nullptr) {
    return;
  }
  for (auto& stripe : rep_->stripe_map_) {
    stripe.second.first->InvalidatePosition();
  }
}

RangeDelMap* RangeDelAggregator::GetRangeDelMapIfExists(SequenceNumber seq) {
  assert(rep_ != nullptr);
  // The stripe includes seqnum for the snapshot above and excludes seqnum for
  // the snapshot below.
  if (rep_->stripe_map_.empty()) {
    return nullptr;
  }
  StripeMap::iterator iter = rep_->stripe_map_.lower_bound(seq);
  if (iter == rep_->stripe_map_.end()) {
    return nullptr;
  }
  size_t snapshot_idx = iter->second.second;
  if (snapshot_idx > 0 && seq <= rep_->snapshots_[snapshot_idx - 1]) {
    return nullptr;
  }
  return iter->second.first.get();
}

RangeDelMap& RangeDelAggregator::GetRangeDelMap(SequenceNumber seq) {
  assert(rep_ != nullptr);
  // The stripe includes seqnum for the snapshot above and excludes seqnum for
  // the snapshot below.
  std::vector<SequenceNumber>::iterator iter =
      std::lower_bound(rep_->snapshots_.begin(), rep_->snapshots_.end(), seq);
  // catch-all stripe justifies this assertion in either of above cases
  assert(iter != rep_->snapshots_.end());
  if (rep_->stripe_map_.find(*iter) == rep_->stripe_map_.end()) {
    rep_->stripe_map_.emplace(
        *iter,
        std::make_pair(NewRangeDelMap(), iter - rep_->snapshots_.begin()));
  }
  return *rep_->stripe_map_[*iter].first;
}

bool RangeDelAggregator::IsEmpty() {
  if (rep_ == nullptr) {
    return true;
  }
  for (const auto& stripe : rep_->stripe_map_) {
    if (!stripe.second.first->IsEmpty()) {
      return false;
    }
  }
  return true;
}

bool RangeDelAggregator::AddFile(uint64_t file_number) {
  if (rep_ == nullptr) {
    return true;
  }
  return rep_->added_files_.emplace(file_number).second;
}

class MergingRangeDelIter : public RangeDelIterator {
 public:
  MergingRangeDelIter(const Comparator* c)
      : heap_(IterMinHeap(IterComparator(c))), current_(nullptr) {}

  void AddIterator(std::unique_ptr<RangeDelIterator> iter) {
    if (iter->Valid()) {
      heap_.push(iter.get());
      iters_.push_back(std::move(iter));
      current_ = heap_.top();
    }
  }

  bool Valid() const override { return current_ != nullptr; }

  void Next() override {
    current_->Next();
    if (current_->Valid()) {
      heap_.replace_top(current_);
    } else {
      heap_.pop();
    }
    current_ = heap_.empty() ? nullptr : heap_.top();
  }

  void Seek(const Slice& target) override {
    ParsedInternalKey ikey(target, kMaxSequenceNumber, kMaxValue);
    Seek(ikey);
  }

  void Seek(const ParsedInternalKey& target) override {
    heap_.clear();
    for (auto& iter : iters_) {
      iter->Seek(target);
      if (iter->Valid()) {
        heap_.push(iter.get());
      }
    }
    current_ = heap_.empty() ? nullptr : heap_.top();
  }

  RangeTombstone Tombstone() const override { return current_->Tombstone(); }

 private:
  struct IterComparator {
    IterComparator(const Comparator* c) : cmp(c) {}

    bool operator()(const RangeDelIterator* a,
                    const RangeDelIterator* b) const {
      // Note: counterintuitively, returning the tombstone with the larger start
      // key puts the tombstone with the smallest key at the top of the heap.
      return cmp->Compare(a->Tombstone().start_key_,
                          b->Tombstone().start_key_) > 0;
    }

    const Comparator* cmp;
  };

  typedef BinaryHeap<RangeDelIterator*, IterComparator> IterMinHeap;

  std::vector<std::unique_ptr<RangeDelIterator>> iters_;
  IterMinHeap heap_;
  RangeDelIterator* current_;
};

std::unique_ptr<RangeDelIterator> RangeDelAggregator::NewIterator() {
  std::unique_ptr<MergingRangeDelIter> iter(
      new MergingRangeDelIter(icmp_.user_comparator()));
  if (rep_ != nullptr) {
    for (const auto& stripe : rep_->stripe_map_) {
      iter->AddIterator(stripe.second.first->NewIterator());
    }
  }
  return std::move(iter);
}

}  // namespace rocksdb
