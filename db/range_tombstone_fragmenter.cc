//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/range_tombstone_fragmenter.h"

#include <set>
#include <algorithm>
#include <functional>

#include <iostream>

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
    const InternalKeyComparator& icmp)
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
      Slice end_key = it->user_key;
      //std::cout << "current end key: " << (void*)end_key.data() << std::endl;
      if (icmp.user_comparator()->Compare(cur_start_key, end_key) ==
          0) {
        // Empty tombstone.
        continue;
      }
      if (icmp.user_comparator()->Compare(next_start_key, end_key) <= 0) {
        reached_next_start_key = true;
        cur_end_keys.erase(cur_end_keys.begin(), it);
        end_key = next_start_key;
      }
      autovector<SequenceNumber> seqnums_to_flush;
      for (auto flush_it = it; flush_it != cur_end_keys.end(); ++flush_it) {
        seqnums_to_flush.push_back(flush_it->sequence);
      }
      std::sort(seqnums_to_flush.begin(), seqnums_to_flush.end(),
                std::greater<SequenceNumber>());
      for (auto seqnum : seqnums_to_flush) {
        tombstones_.push_back(RangeTombstone(cur_start_key, end_key, seqnum));
      }
      cur_start_key = end_key;
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
    no_tombstones = false;
    const Slice& ikey = unfragmented_tombstones->key();
    Slice start_key = ExtractUserKey(ikey);
    SequenceNumber seq = GetInternalKeySeqno(ikey);

    Slice end_key = unfragmented_tombstones->value();
    if (!unfragmented_tombstones->IsValuePinned()) {
      pinned_slices_.emplace_back(unfragmented_tombstones->value().data(),
                                  unfragmented_tombstones->value().size());
      end_key = pinned_slices_.back();
    }
    //std::cout << "cur start key ptr before: " << (void*)cur_start_key.data() << std::endl;
    if (!cur_end_keys.empty() &&
        icmp.user_comparator()->Compare(cur_start_key, start_key) != 0) {
      flush_current_tombstones(start_key);
    }
    if (unfragmented_tombstones->IsKeyPinned()) {
      cur_start_key = start_key;
    } else {
      pinned_slices_.emplace_back(start_key.data(), start_key.size());
      cur_start_key = pinned_slices_.back();
    }
    //std::cout << "cur start key ptr after: " << (void*)cur_start_key.data()<< std::endl;

    cur_end_keys.emplace(end_key, seq, kTypeRangeDeletion);
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
  } else if (pos_ != tombstones_.begin()) {
    // Step backwards until we reach the earliest tombstone that covers the
    // target, or the earliest tombstone that starts after the target if no
    // covering tombstone exists.
    while (pos_ != tombstones_.begin() &&
        ucmp_->Compare(target, std::prev(pos_)->end_key_) < 0) {
      --pos_;
    }
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
