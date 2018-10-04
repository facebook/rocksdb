//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/range_tombstone_fragmenter.h"

#include <set>
#include <algorithm>
#include <functional>

#include "util/kv_map.h"
#include "util/autovector.h"

namespace rocksdb {

namespace {

struct ParsedInternalKeyComparator {
  ParsedInternalKeyComparator(const InternalKeyComparator* c) : cmp(c) {}

  bool operator()(const ParsedInternalKey& a,
                  const ParsedInternalKey& b) const {
    return cmp->Compare(a, b) < 0;
  }

  const InternalKeyComparator* cmp;
};

}

FragmentedRangeTombstoneIterator::FragmentedRangeTombstoneIterator(
    std::unique_ptr<InternalIterator> unfragmented_tombstones,
    const InternalKeyComparator& icmp, SequenceNumber snapshot)
    : tombstone_cmp_(icmp.user_comparator()), ucmp_(icmp.user_comparator()) {
  if (unfragmented_tombstones == nullptr) {
    pos_ = tombstones_.end();
    return;
  }
  Slice cur_start_key(nullptr, 0);
  auto cmp = ParsedInternalKeyComparator(&icmp);
  std::set<ParsedInternalKey, ParsedInternalKeyComparator> cur_end_keys(cmp);
  auto flush_current_tombstones = [&](const Slice& next_start_key) {
    auto it = cur_end_keys.begin();
    bool reached_next_start_key = false;
    for (; it != cur_end_keys.end() && !reached_next_start_key; ++it) {
      Slice cur_end_key = it->user_key;
      if (icmp.user_comparator()->Compare(cur_start_key, cur_end_key) ==
          0) {
        // Empty tombstone.
        continue;
      }
      if (icmp.user_comparator()->Compare(next_start_key, cur_end_key) <= 0) {
        reached_next_start_key = true;
        cur_end_keys.erase(cur_end_keys.begin(), it);
        cur_end_key = next_start_key;
      }
      SequenceNumber max_seqnum = 0;
      for (auto flush_it = it; flush_it != cur_end_keys.end(); ++flush_it) {
        max_seqnum = std::max(max_seqnum, flush_it->sequence);
      }
      tombstones_.push_back(
          RangeTombstone(cur_start_key, cur_end_key, max_seqnum));
      cur_start_key = cur_end_key;
    }
    if (!reached_next_start_key) {
      cur_end_keys.clear();
    }
    cur_start_key = next_start_key;
  };

  pinned_iters_mgr_.StartPinning();

  bool no_tombstones = true;
  for (unfragmented_tombstones->SeekToFirst(); unfragmented_tombstones->Valid();
       unfragmented_tombstones->Next()) {
    const Slice& ikey = unfragmented_tombstones->key();
    Slice tombstone_start_key = ExtractUserKey(ikey);
    SequenceNumber tombstone_seq = GetInternalKeySeqno(ikey);
    if (tombstone_seq > snapshot) {
      continue;
    }
    no_tombstones = false;

    Slice tombstone_end_key = unfragmented_tombstones->value();
    if (!unfragmented_tombstones->IsValuePinned()) {
      pinned_slices_.emplace_back(tombstone_end_key.data(),
                                  tombstone_end_key.size());
      tombstone_end_key = pinned_slices_.back();
    }
    if (!cur_end_keys.empty() && icmp.user_comparator()->Compare(
                                     cur_start_key, tombstone_start_key) != 0) {
      flush_current_tombstones(tombstone_start_key);
    }
    if (unfragmented_tombstones->IsKeyPinned()) {
      cur_start_key = tombstone_start_key;
    } else {
      pinned_slices_.emplace_back(tombstone_start_key.data(),
                                  tombstone_start_key.size());
      cur_start_key = pinned_slices_.back();
    }

    cur_end_keys.emplace(tombstone_end_key, tombstone_seq, kTypeRangeDeletion);
  }
  if (!cur_end_keys.empty()) {
    ParsedInternalKey last_end_key = *std::prev(cur_end_keys.end());
    flush_current_tombstones(last_end_key.user_key);
  }

  if (!no_tombstones) {
    pinned_iters_mgr_.PinIterator(unfragmented_tombstones.release(),
                                  false /* arena */);
  }

  // With this, the caller must Seek before the iterator is valid.
  pos_ = tombstones_.end();
}

void FragmentedRangeTombstoneIterator::SeekToFirst() {
  pos_ = tombstones_.begin();
}

void FragmentedRangeTombstoneIterator::SeekForPrev(const Slice& target) {
  if (tombstones_.empty()) {
    pos_ = tombstones_.end();
    return;
  }
  RangeTombstone search(target, target, kMaxSequenceNumber);
  pos_ = std::upper_bound(tombstones_.begin(), tombstones_.end(), search,
                          tombstone_cmp_);
  if (pos_ == tombstones_.begin() &&
      ucmp_->Compare(target, pos_->start_key_) < 0) {
    // Target before first tombstone.
    pos_ = tombstones_.end();
  } else if (pos_ == tombstones_.end() &&
             ucmp_->Compare(std::prev(pos_)->end_key_, target) <= 0) {
    // Target after last tombstone.
    pos_ = tombstones_.end();
  } else if (pos_ != tombstones_.begin() &&
             ucmp_->Compare(target, std::prev(pos_)->end_key_) < 0) {
    // Position the iterator at the earliest tombstone that covers the
    // target.
    --pos_;
  }
}

void FragmentedRangeTombstoneIterator::Next() {
  ++pos_;
}

bool FragmentedRangeTombstoneIterator::Valid() const {
  return pos_ != tombstones_.end();
}

Slice FragmentedRangeTombstoneIterator::start_key() const {
  return pos_->start_key_;
}

Slice FragmentedRangeTombstoneIterator::end_key() const {
  return pos_->end_key_;
}

SequenceNumber FragmentedRangeTombstoneIterator::seq() const {
  return pos_->seq_;
}

SequenceNumber FragmentedRangeTombstoneIterator::MaxCoveringTombstoneSeqnum(
    const Slice& key) {
  SeekForPrev(key);
  if (Valid() && ucmp_->Compare(start_key(), key) <= 0) {
    return seq();
  }
  return 0;
}

}  // namespace rocksdb
