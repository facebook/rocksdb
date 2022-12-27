//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "table/compaction_merging_iterator.h"

namespace ROCKSDB_NAMESPACE {
void CompactionMergingIterator::SeekToFirst() {
  minHeap_.clear();
  status_ = Status::OK();
  for (auto& child : children_) {
    child.iter.SeekToFirst();
    AddToMinHeapOrCheckStatus(&child);
  }

  for (size_t i = 0; i < range_tombstone_iters_.size(); ++i) {
    if (range_tombstone_iters_[i]) {
      range_tombstone_iters_[i]->SeekToFirst();
      InsertRangeTombstoneAtLevel(i);
    }
  }

  FindNextVisibleKey();
  current_ = CurrentForward();
}

void CompactionMergingIterator::Seek(const Slice& target) {
  minHeap_.clear();
  status_ = Status::OK();
  for (auto& child : children_) {
    child.iter.Seek(target);
    AddToMinHeapOrCheckStatus(&child);
  }

  ParsedInternalKey pik;
  ParseInternalKey(target, &pik, false /* log_err_key */)
      .PermitUncheckedError();
  for (size_t i = 0; i < range_tombstone_iters_.size(); ++i) {
    if (range_tombstone_iters_[i]) {
      range_tombstone_iters_[i]->Seek(pik.user_key);
      // For compaction, output keys should all be after seek target.
      while (range_tombstone_iters_[i]->Valid() &&
             comparator_->Compare(range_tombstone_iters_[i]->start_key(), pik) <
                 0) {
        range_tombstone_iters_[i]->Next();
      }
      InsertRangeTombstoneAtLevel(i);
    }
  }

  FindNextVisibleKey();
  current_ = CurrentForward();
}

void CompactionMergingIterator::Next() {
  assert(Valid());
  // For the heap modifications below to be correct, current_ must be the
  // current top of the heap.
  assert(current_ == CurrentForward());
  // as the current points to the current record. move the iterator forward.
  if (current_->type == HeapItem::ITERATOR) {
    current_->iter.Next();
    if (current_->iter.Valid()) {
      // current is still valid after the Next() call above.  Call
      // replace_top() to restore the heap property.  When the same child
      // iterator yields a sequence of keys, this is cheap.
      assert(current_->iter.status().ok());
      minHeap_.replace_top(current_);
    } else {
      // current stopped being valid, remove it from the heap.
      considerStatus(current_->iter.status());
      minHeap_.pop();
    }
  } else {
    assert(current_->type == HeapItem::DELETE_RANGE_START);
    size_t level = current_->level;
    assert(range_tombstone_iters_[level]);
    range_tombstone_iters_[level]->Next();
    if (range_tombstone_iters_[level]->Valid()) {
      pinned_heap_item_[level].SetTombstoneForCompaction(
          range_tombstone_iters_[level]->start_key());
      minHeap_.replace_top(&pinned_heap_item_[level]);
    } else {
      minHeap_.pop();
    }
  }
  FindNextVisibleKey();
  current_ = CurrentForward();
}

void CompactionMergingIterator::FindNextVisibleKey() {
  // IsDeleteRangeSentinelKey() here means file boundary sentinel keys.
  while (!minHeap_.empty() && minHeap_.top()->IsDeleteRangeSentinelKey()) {
    HeapItem* current = minHeap_.top();
    // range tombstone start keys from the same SSTable should have been
    // exhausted
    assert(!range_tombstone_iters_[current->level] ||
           !range_tombstone_iters_[current->level]->Valid());
    // iter is a LevelIterator, and it enters a new SST file in the Next()
    // call here.
    current->iter.Next();
    if (current->iter.Valid()) {
      assert(current->iter.status().ok());
      minHeap_.replace_top(current);
    } else {
      minHeap_.pop();
    }
    if (range_tombstone_iters_[current->level]) {
      InsertRangeTombstoneAtLevel(current->level);
    }
  }
}
void CompactionMergingIterator::AddToMinHeapOrCheckStatus(HeapItem* child) {
  if (child->iter.Valid()) {
    assert(child->iter.status().ok());
    minHeap_.push(child);
  } else {
    considerStatus(child->iter.status());
  }
}

InternalIterator* NewCompactionMergingIterator(
    const InternalKeyComparator* comparator, InternalIterator** children, int n,
    std::vector<std::pair<TruncatedRangeDelIterator*,
                          TruncatedRangeDelIterator***>>& range_tombstone_iters,
    Arena* arena) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyInternalIterator<Slice>(arena);
  } else {
    if (arena == nullptr) {
      return new CompactionMergingIterator(comparator, children, n, false,
                                           range_tombstone_iters);
    } else {
      auto mem = arena->AllocateAligned(sizeof(CompactionMergingIterator));
      return new (mem) CompactionMergingIterator(comparator, children, n, true,
                                                 range_tombstone_iters);
    }
  }
}
}  // namespace ROCKSDB_NAMESPACE
