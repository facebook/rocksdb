//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/range_tombstone_fragmenter.h"

#include <algorithm>
#include <functional>
#include <set>

#include <inttypes.h>
#include <stdio.h>

#include "util/autovector.h"
#include "util/kv_map.h"
#include "util/vector_iterator.h"

namespace rocksdb {

FragmentedRangeTombstoneList::FragmentedRangeTombstoneList(
    std::unique_ptr<InternalIterator> unfragmented_tombstones,
    const InternalKeyComparator& icmp, bool one_time_use,
    SequenceNumber snapshot) {
  if (unfragmented_tombstones == nullptr) {
    return;
  }
  bool is_sorted = true;
  int num_tombstones = 0;
  InternalKey pinned_last_start_key;
  Slice last_start_key;
  for (unfragmented_tombstones->SeekToFirst(); unfragmented_tombstones->Valid();
       unfragmented_tombstones->Next(), num_tombstones++) {
    if (num_tombstones > 0 &&
        icmp.Compare(last_start_key, unfragmented_tombstones->key()) > 0) {
      is_sorted = false;
      break;
    }
    if (unfragmented_tombstones->IsKeyPinned()) {
      last_start_key = unfragmented_tombstones->key();
    } else {
      pinned_last_start_key.DecodeFrom(unfragmented_tombstones->key());
      last_start_key = pinned_last_start_key.Encode();
    }
  }
  if (is_sorted) {
    FragmentTombstones(std::move(unfragmented_tombstones), icmp, one_time_use,
                       snapshot);
    return;
  }

  // Sort the tombstones before fragmenting them.
  std::vector<std::string> keys, values;
  keys.reserve(num_tombstones);
  values.reserve(num_tombstones);
  for (unfragmented_tombstones->SeekToFirst(); unfragmented_tombstones->Valid();
       unfragmented_tombstones->Next()) {
    keys.emplace_back(unfragmented_tombstones->key().data(),
                      unfragmented_tombstones->key().size());
    values.emplace_back(unfragmented_tombstones->value().data(),
                        unfragmented_tombstones->value().size());
  }
  // VectorIterator implicitly sorts by key during construction.
  auto iter = std::unique_ptr<VectorIterator>(
      new VectorIterator(std::move(keys), std::move(values), &icmp));
  FragmentTombstones(std::move(iter), icmp, one_time_use, snapshot);
}

void FragmentedRangeTombstoneList::FragmentTombstones(
    std::unique_ptr<InternalIterator> unfragmented_tombstones,
    const InternalKeyComparator& icmp, bool one_time_use,
    SequenceNumber snapshot) {
  Slice cur_start_key(nullptr, 0);
  auto cmp = ParsedInternalKeyComparator(&icmp);

  // Stores the end keys and sequence numbers of range tombstones with a start
  // key less than or equal to cur_start_key. Provides an ordering by end key
  // for use in flush_current_tombstones.
  std::set<ParsedInternalKey, ParsedInternalKeyComparator> cur_end_keys(cmp);

  // Given the next start key in unfragmented_tombstones,
  // flush_current_tombstones writes every tombstone fragment that starts
  // and ends with a key before next_start_key, and starts with a key greater
  // than or equal to cur_start_key.
  auto flush_current_tombstones = [&](const Slice& next_start_key) {
    auto it = cur_end_keys.begin();
    bool reached_next_start_key = false;
    for (; it != cur_end_keys.end() && !reached_next_start_key; ++it) {
      Slice cur_end_key = it->user_key;
      if (icmp.user_comparator()->Compare(cur_start_key, cur_end_key) == 0) {
        // Empty tombstone.
        continue;
      }
      if (icmp.user_comparator()->Compare(next_start_key, cur_end_key) <= 0) {
        // All of the end keys in [it, cur_end_keys.end()) are after
        // next_start_key, so the tombstones they represent can be used in
        // fragments that start with keys greater than or equal to
        // next_start_key. However, the end keys we already passed will not be
        // used in any more tombstone fragments.
        //
        // Remove the fully fragmented tombstones and stop iteration after a
        // final round of flushing to preserve the tombstones we can create more
        // fragments from.
        reached_next_start_key = true;
        cur_end_keys.erase(cur_end_keys.begin(), it);
        cur_end_key = next_start_key;
      }

      // Flush a range tombstone fragment [cur_start_key, cur_end_key), which
      // should not overlap with the last-flushed tombstone fragment.
      assert(tombstones_.empty() ||
             icmp.user_comparator()->Compare(tombstones_.back().end_key_,
                                               cur_start_key) <= 0);

      if (one_time_use) {
        SequenceNumber max_seqnum = 0;
        for (auto flush_it = it; flush_it != cur_end_keys.end(); ++flush_it) {
          max_seqnum = std::max(max_seqnum, flush_it->sequence);
        }

        // Flush only the tombstone fragment with the highest sequence number.
        tombstones_.push_back(
            RangeTombstone(cur_start_key, cur_end_key, max_seqnum));
      } else {
        // Sort the sequence numbers of the tombstones being fragmented in
        // descending order, and then flush them in that order.
        autovector<SequenceNumber> seqnums_to_flush;
        for (auto flush_it = it; flush_it != cur_end_keys.end(); ++flush_it) {
          seqnums_to_flush.push_back(flush_it->sequence);
        }
        std::sort(seqnums_to_flush.begin(), seqnums_to_flush.end(),
                  std::greater<SequenceNumber>());
        for (const auto seq : seqnums_to_flush) {
          tombstones_.push_back(
              RangeTombstone(cur_start_key, cur_end_key, seq));
        }
      }
      cur_start_key = cur_end_key;
    }
    if (!reached_next_start_key) {
      // There is a gap between the last flushed tombstone fragment and
      // the next tombstone's start key. Remove all the end keys in
      // the working set, since we have fully fragmented their corresponding
      // tombstones.
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
    if (one_time_use && tombstone_seq > snapshot) {
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
      // The start key has changed. Flush all tombstones that start before
      // this new start key.
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
}

FragmentedRangeTombstoneIterator::FragmentedRangeTombstoneIterator(
    const FragmentedRangeTombstoneList* tombstones,
    const InternalKeyComparator& icmp)
    : tombstone_cmp_(icmp.user_comparator()),
      icmp_(&icmp),
      ucmp_(icmp.user_comparator()),
      tombstones_(tombstones) {
  assert(tombstones_ != nullptr);
  pos_ = tombstones_->end();
  pinned_pos_ = tombstones_->end();
}

FragmentedRangeTombstoneIterator::FragmentedRangeTombstoneIterator(
    const std::shared_ptr<const FragmentedRangeTombstoneList>& tombstones,
    const InternalKeyComparator& icmp)
    : tombstone_cmp_(icmp.user_comparator()),
      icmp_(&icmp),
      ucmp_(icmp.user_comparator()),
      tombstones_ref_(tombstones),
      tombstones_(tombstones_ref_.get()) {
  assert(tombstones_ != nullptr);
  pos_ = tombstones_->end();
  pinned_pos_ = tombstones_->end();
}

void FragmentedRangeTombstoneIterator::SeekToFirst() {
  pos_ = tombstones_->begin();
}

void FragmentedRangeTombstoneIterator::SeekToLast() {
  pos_ = tombstones_->end();
  Prev();
}

void FragmentedRangeTombstoneIterator::Seek(const Slice& target) {
  if (tombstones_->empty()) {
    pos_ = tombstones_->end();
    return;
  }
  RangeTombstone search(ExtractUserKey(target), ExtractUserKey(target),
                        GetInternalKeySeqno(target));
  pos_ = std::lower_bound(tombstones_->begin(), tombstones_->end(), search,
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

void FragmentedRangeTombstoneIterator::Next() { ++pos_; }

void FragmentedRangeTombstoneIterator::Prev() {
  if (pos_ == tombstones_->begin()) {
    pos_ = tombstones_->end();
    return;
  }
  --pos_;
}

bool FragmentedRangeTombstoneIterator::Valid() const {
  return tombstones_ != nullptr && pos_ != tombstones_->end();
}

SequenceNumber MaxCoveringTombstoneSeqnum(
    FragmentedRangeTombstoneIterator* tombstone_iter, const Slice& lookup_key,
    const Comparator* ucmp) {
  if (tombstone_iter == nullptr) {
    return 0;
  }

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
      highest_covering_seqnum =
          std::max(highest_covering_seqnum, tombstone_iter->seq());
    }
    tombstone_iter->Prev();
  }
  return highest_covering_seqnum;
}

}  // namespace rocksdb
