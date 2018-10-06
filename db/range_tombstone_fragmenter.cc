//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/range_tombstone_fragmenter.h"

#include <set>
#include <algorithm>
#include <functional>

#include "util/kv_map.h"

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

void SeekToHighestSeqnum(InternalIterator* iter, const Comparator* ucmp) {
  if (!iter->Valid()) {
    return;
  }
  Slice cur_key = ExtractUserKey(iter->key());
  std::string pinned_key(cur_key.data(), cur_key.size());
  while (iter->Valid() &&
         ucmp->Compare(pinned_key, ExtractUserKey(iter->key())) == 0) {
    iter->Prev();
  }
  if (!iter->Valid()) {
    iter->SeekToFirst();
  } else {
    iter->Next();
  }
  assert(ucmp->Compare(ExtractUserKey(iter->key()), pinned_key) == 0);
}

}  // anonymous namespace

FragmentedRangeTombstoneIterator::FragmentedRangeTombstoneIterator(
    std::unique_ptr<InternalIterator> unfragmented_tombstones,
    const InternalKeyComparator& icmp, SequenceNumber snapshot)
    : tombstone_cmp_(icmp.user_comparator()),
      icmp_(&icmp),
      ucmp_(icmp.user_comparator()) {
  if (unfragmented_tombstones == nullptr) {
    pos_ = tombstones_.end();
    return;
  }
  Slice cur_start_key(nullptr, 0);
  auto cmp = ParsedInternalKeyComparator(&icmp);

  // Stores the end keys and sequence numbers of range tombstones with a start
  // key of cur_start_key. Provides an ordering by end key for use in
  // flush_current_tombstones.
  std::set<ParsedInternalKey, ParsedInternalKeyComparator> cur_end_keys(cmp);

  // Given the next start key in unfragmented_tombstones,
  // flush_current_tombstones writes every tombstone fragment that starts and
  // ends with a key before next_start_key.
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
      // Flush only the tombstone fragment with the highest sequence number.
      tombstones_.push_back(
          RangeTombstone(cur_start_key, cur_end_key, max_seqnum));
      cur_start_key = cur_end_key;
    }
    if (!reached_next_start_key) {
      // There is a gap between the last flushed tombstone fragment and the next
      // tombstone's start key. Remove the remaining end keys in the working
      // set.
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
      // The tombstone is not visible by this snapshot.
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
      // The start key has changed. Flush all tombstones that start before this
      // new start key.
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
  UpdateKey();
}

void FragmentedRangeTombstoneIterator::SeekToLast() {
  pos_ = tombstones_.end();
  Prev();
}

void FragmentedRangeTombstoneIterator::Seek(const Slice& target) {
  if (tombstones_.empty()) {
    pos_ = tombstones_.end();
    return;
  }
  RangeTombstone search(target, target, kMaxSequenceNumber);
  pos_ = std::lower_bound(tombstones_.begin(), tombstones_.end(), search,
                          tombstone_cmp_);
  UpdateKey();
}

void FragmentedRangeTombstoneIterator::SeekForPrev(const Slice& target) {
  SeekForPrevImpl(target, icmp_);
}

void FragmentedRangeTombstoneIterator::Next() {
  ++pos_;
  if (pos_ != tombstones_.end()) {
    UpdateKey();
  }
}

void FragmentedRangeTombstoneIterator::Prev() {
  if (pos_ == tombstones_.begin()) {
    pos_ = tombstones_.end();
    return;
  }
  --pos_;
  UpdateKey();
}

bool FragmentedRangeTombstoneIterator::Valid() const {
  return pos_ != tombstones_.end();
}

SequenceNumber MaxCoveringTombstoneSeqnum(
    InternalIterator* tombstone_iter,
    const Slice& key, const Comparator* ucmp) {
  InternalKey ikey(key, 0, kTypeRangeDeletion);
  tombstone_iter->SeekForPrev(ikey.Encode());
  SeekToHighestSeqnum(tombstone_iter, ucmp);
  if (tombstone_iter->Valid() &&
      ucmp->Compare(key, tombstone_iter->value()) < 0) {
    return GetInternalKeySeqno(tombstone_iter->key());
  }
  return 0;
}

}  // namespace rocksdb
