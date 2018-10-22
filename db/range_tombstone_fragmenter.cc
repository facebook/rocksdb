//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/range_tombstone_fragmenter.h"

#include <set>
#include <algorithm>
#include <functional>

#include <stdio.h>
#include <inttypes.h>

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
  std::vector<RangeTombstone> ordered_tombstones;
  for (unfragmented_tombstones->SeekToFirst(); unfragmented_tombstones->Valid();
       unfragmented_tombstones->Next()) {
    const Slice& ikey = unfragmented_tombstones->key();
    Slice tombstone_start_key = ExtractUserKey(ikey);
    Slice tombstone_end_key = unfragmented_tombstones->value();
    SequenceNumber tombstone_seq = GetInternalKeySeqno(ikey);
    if (tombstone_seq > snapshot) {
      // The tombstone is not visible by this snapshot.
      continue;
    }
    no_tombstones = false;

    if (!unfragmented_tombstones->IsKeyPinned()) {
      pinned_slices_.emplace_back(tombstone_start_key.data(),
                                  tombstone_start_key.size());
      tombstone_start_key = pinned_slices_.back();
    }
    if (!unfragmented_tombstones->IsValuePinned()) {
      pinned_slices_.emplace_back(tombstone_end_key.data(),
                                  tombstone_end_key.size());
      tombstone_end_key = pinned_slices_.back();
    }
    ordered_tombstones.emplace_back(tombstone_start_key, tombstone_end_key,
                                    tombstone_seq);
  }
  std::sort(ordered_tombstones.begin(), ordered_tombstones.end(), tombstone_cmp_);

  for (const auto& tombstone : ordered_tombstones) {
    if (!cur_end_keys.empty() &&
        icmp.user_comparator()->Compare(cur_start_key, tombstone.start_key_) !=
            0) {
      // The start key has changed. Flush all tombstones that start before
      // this new start key.
      flush_current_tombstones(tombstone.start_key_);
    }
    cur_start_key = tombstone.start_key_;
    cur_end_keys.emplace(tombstone.end_key_, tombstone.seq_,
                         kTypeRangeDeletion);
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
  pinned_pos_ = tombstones_.end();
}

void FragmentedRangeTombstoneIterator::SeekToFirst() {
  pos_ = tombstones_.begin();
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
  RangeTombstone search(ExtractUserKey(target), ExtractUserKey(target),
                        GetInternalKeySeqno(target));
  pos_ = std::lower_bound(tombstones_.begin(), tombstones_.end(), search,
                          tombstone_cmp_);
}

void FragmentedRangeTombstoneIterator::SeekForPrev(const Slice& target) {
  Seek(target);
  if (!Valid()) {
    SeekToLast();
  }
  ParsedInternalKey parsed_target;
  if (!ParseInternalKey(target, &parsed_target)) {
    assert(false);
  }
  ParsedInternalKey parsed_start_key;
  ParseKey(&parsed_start_key);
  while (Valid() && icmp_->Compare(parsed_target, parsed_start_key) < 0) {
    Prev();
    ParseKey(&parsed_start_key);
  }
}

void FragmentedRangeTombstoneIterator::Next() {
  ++pos_;
}

void FragmentedRangeTombstoneIterator::Prev() {
  if (pos_ == tombstones_.begin()) {
    pos_ = tombstones_.end();
    return;
  }
  --pos_;
}

bool FragmentedRangeTombstoneIterator::Valid() const {
  return pos_ != tombstones_.end();
}

SequenceNumber MaxCoveringTombstoneSeqnum(
    FragmentedRangeTombstoneIterator* tombstone_iter,
    const Slice& lookup_key, const Comparator* ucmp) {
  SequenceNumber snapshot = GetInternalKeySeqno(lookup_key);
  Slice user_key = ExtractUserKey(lookup_key);

  tombstone_iter->Seek(lookup_key);
  SequenceNumber highest_covering_seqnum = 0;
  if (!tombstone_iter->Valid()) {
    // Seeked past the last tombstone
    tombstone_iter->Prev();
  }
  while (tombstone_iter->Valid() &&
         ucmp->Compare(user_key, tombstone_iter->value()) < 0) {
    if (tombstone_iter->seq() <= snapshot &&
        ucmp->Compare(tombstone_iter->user_key(), user_key) <= 0) {
      highest_covering_seqnum = std::max(
          highest_covering_seqnum, tombstone_iter->seq());
    }
    tombstone_iter->Prev();
  }
  return highest_covering_seqnum;
}

}  // namespace rocksdb
