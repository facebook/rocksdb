//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional
//  grant of patent rights can be found in the PATENTS file in the same
//  directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.  Use of
// this source code is governed by a BSD-style license that can be found
// in the LICENSE file. See the AUTHORS file for names of contributors.
//
// InlineSkipList is derived from SkipList (skiplist.h), but it optimizes
// the memory layout by requiring that the key storage be allocated through
// the skip list instance.  For the common case of SkipList<const char*,
// Cmp> this saves 1 pointer per skip list node and gives better cache
// locality, at the expense of wasted padding from using AllocateAligned
// instead of Allocate for the keys.  The unused padding will be from
// 0 to sizeof(void*)-1 bytes, and the space savings are sizeof(void*)
// bytes, so despite the padding the space used is always less than
// SkipList<const char*, ..>.
//
// Thread safety -------------
//
// Writes via Insert require external synchronization, most likely a mutex.
// InsertConcurrently can be safely called concurrently with reads and
// with other concurrent inserts.  Reads require a guarantee that the
// InlineSkipList will not be destroyed while the read is in progress.
// Apart from that, reads progress without any internal locking or
// synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the InlineSkipList is
// destroyed.  This is trivially guaranteed by the code since we never
// delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the InlineSkipList.
// Only Insert() modifies the list, and it is careful to initialize a
// node and use release-stores to publish the nodes in one or more lists.
//
// ... prev vs. next pointer ordering ...
//

#pragma once
#include <assert.h>
#include <stdlib.h>
#include <atomic>
#include "port/port.h"
#include "util/allocator.h"
#include "util/random.h"

namespace rocksdb {

template <class Comparator>
class InlineSkipList {
 private:
  struct Node;

 public:
  // Create a new InlineSkipList object that will use "cmp" for comparing
  // keys, and will allocate memory using "*allocator".  Objects allocated
  // in the allocator must remain allocated for the lifetime of the
  // skiplist object.
  explicit InlineSkipList(Comparator cmp, Allocator* allocator,
                          int32_t max_height = 12,
                          int32_t branching_factor = 4);

  // Allocates a key and a skip-list node, returning a pointer to the key
  // portion of the node.  This method is thread-safe if the allocator
  // is thread-safe.
  char* AllocateKey(size_t key_size);

  // Inserts a key allocated by AllocateKey, after the actual key value
  // has been filled in.
  //
  // REQUIRES: nothing that compares equal to key is currently in the list.
  // REQUIRES: no concurrent calls to INSERT
  void Insert(const char* key);

  // Like Insert, but external synchronization is not required.
  void InsertConcurrently(const char* key);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const char* key) const;

  // Return estimated number of entries smaller than `key`.
  uint64_t EstimateCount(const char* key) const;

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const InlineSkipList* list);

    // Change the underlying skiplist used for this iterator
    // This enables us not changing the iterator without deallocating
    // an old one and then allocating a new one
    void SetList(const InlineSkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const char* key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const char* target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    const InlineSkipList* list_;
    Node* node_;
    // Intentionally copyable
  };

 private:
  enum MaxPossibleHeightEnum : uint16_t { kMaxPossibleHeight = 32 };

  const uint16_t kMaxHeight_;
  const uint16_t kBranching_;
  const uint32_t kScaledInverseBranching_;

  // Immutable after construction
  Comparator const compare_;
  Allocator* const allocator_;  // Allocator used for allocations of nodes

  Node* const head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  std::atomic<int> max_height_;  // Height of the entire list

  // Used for optimizing sequential insert patterns.  Tricky.  prev_height_
  // of zero means prev_ is undefined.  Otherwise: prev_[i] for i up
  // to max_height_ - 1 (inclusive) is the predecessor of prev_[0], and
  // prev_height_ is the height of prev_[0].  prev_[0] can only be equal
  // to head when max_height_ and prev_height_ are both 1.
  Node** prev_;
  std::atomic<int32_t> prev_height_;

  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  int RandomHeight();

  Node* AllocateNode(size_t key_size, int height);

  bool Equal(const char* a, const char* b) const {
    return (compare_(a, b) == 0);
  }

  // Return true if key is greater than the data stored in "n".  Null n
  // is considered infinite.
  bool KeyIsAfterNode(const char* key, Node* n) const;

  // Returns the earliest node with a key >= key.
  // Return nullptr if there is no such node.
  Node* FindGreaterOrEqual(const char* key) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  // Fills prev[level] with pointer to previous node at "level" for every
  // level in [0..max_height_-1], if prev is non-null.
  Node* FindLessThan(const char* key, Node** prev = nullptr) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  // Traverses a single level of the list, setting *out_prev to the last
  // node before the key and *out_next to the first node after. Assumes
  // that the key is not present in the skip list. On entry, before should
  // point to a node that is before the key, and after should point to
  // a node that is after the key.  after should be nullptr if a good after
  // node isn't conveniently available.
  void FindLevelSplice(const char* key, Node* before, Node* after, int level,
                       Node** out_prev, Node** out_next);

  // No copying allowed
  InlineSkipList(const InlineSkipList&);
  InlineSkipList& operator=(const InlineSkipList&);
};

// Implementation details follow

// The Node data type is more of a pointer into custom-managed memory than
// a traditional C++ struct.  The key is stored in the bytes immediately
// after the struct, and the next_ pointers for nodes with height > 1 are
// stored immediately _before_ the struct.  This avoids the need to include
// any pointer or sizing data, which reduces per-node memory overheads.
template <class Comparator>
struct InlineSkipList<Comparator>::Node {
  // Stores the height of the node in the memory location normally used for
  // next_[0].  This is used for passing data from AllocateKey to Insert.
  void StashHeight(const int height) {
    assert(sizeof(int) <= sizeof(next_[0]));
    memcpy(&next_[0], &height, sizeof(int));
  }

  // Retrieves the value passed to StashHeight.  Undefined after a call
  // to SetNext or NoBarrier_SetNext.
  int UnstashHeight() const {
    int rv;
    memcpy(&rv, &next_[0], sizeof(int));
    return rv;
  }

  const char* Key() const { return reinterpret_cast<const char*>(&next_[1]); }

  // Accessors/mutators for links.  Wrapped in methods so we can add
  // the appropriate barriers as necessary, and perform the necessary
  // addressing trickery for storing links below the Node in memory.
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return (next_[-n].load(std::memory_order_acquire));
  }

  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[-n].store(x, std::memory_order_release);
  }

  bool CASNext(int n, Node* expected, Node* x) {
    assert(n >= 0);
    return next_[-n].compare_exchange_strong(expected, x);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return next_[-n].load(std::memory_order_relaxed);
  }

  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[-n].store(x, std::memory_order_relaxed);
  }

 private:
  // next_[0] is the lowest level link (level 0).  Higher levels are
  // stored _earlier_, so level 1 is at next_[-1].
  std::atomic<Node*> next_[1];
};

template <class Comparator>
inline InlineSkipList<Comparator>::Iterator::Iterator(
    const InlineSkipList* list) {
  SetList(list);
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::SetList(
    const InlineSkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template <class Comparator>
inline bool InlineSkipList<Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template <class Comparator>
inline const char* InlineSkipList<Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->Key();
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->Key());
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::Seek(const char* target) {
  node_ = list_->FindGreaterOrEqual(target);
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <class Comparator>
int InlineSkipList<Comparator>::RandomHeight() {
  auto rnd = Random::GetTLSInstance();

  // Increase height with probability 1 in kBranching
  int height = 1;
  while (height < kMaxHeight_ && height < kMaxPossibleHeight &&
         rnd->Next() < kScaledInverseBranching_) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight_);
  assert(height <= kMaxPossibleHeight);
  return height;
}

template <class Comparator>
bool InlineSkipList<Comparator>::KeyIsAfterNode(const char* key,
                                                Node* n) const {
  // nullptr n is considered infinite
  return (n != nullptr) && (compare_(n->Key(), key) < 0);
}

template <class Comparator>
typename InlineSkipList<Comparator>::Node*
InlineSkipList<Comparator>::FindGreaterOrEqual(const char* key) const {
  // Note: It looks like we could reduce duplication by implementing
  // this function as FindLessThan(key)->Next(0), but we wouldn't be able
  // to exit early on equality and the result wouldn't even be correct.
  // A concurrent insert might occur after FindLessThan(key) but before
  // we get a chance to call Next(0).
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  Node* last_bigger = nullptr;
  while (true) {
    Node* next = x->Next(level);
    // Make sure the lists are sorted
    assert(x == head_ || next == nullptr || KeyIsAfterNode(next->Key(), x));
    // Make sure we haven't overshot during our search
    assert(x == head_ || KeyIsAfterNode(key, x));
    int cmp = (next == nullptr || next == last_bigger)
                  ? 1
                  : compare_(next->Key(), key);
    if (cmp == 0 || (cmp > 0 && level == 0)) {
      return next;
    } else if (cmp < 0) {
      // Keep searching in this list
      x = next;
    } else {
      // Switch to next list, reuse compare_() result
      last_bigger = next;
      level--;
    }
  }
}

template <class Comparator>
typename InlineSkipList<Comparator>::Node*
InlineSkipList<Comparator>::FindLessThan(const char* key, Node** prev) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  // KeyIsAfter(key, last_not_after) is definitely false
  Node* last_not_after = nullptr;
  while (true) {
    Node* next = x->Next(level);
    assert(x == head_ || next == nullptr || KeyIsAfterNode(next->Key(), x));
    assert(x == head_ || KeyIsAfterNode(key, x));
    if (next != last_not_after && KeyIsAfterNode(key, next)) {
      // Keep searching in this list
      x = next;
    } else {
      if (prev != nullptr) {
        prev[level] = x;
      }
      if (level == 0) {
        return x;
      } else {
        // Switch to next list, reuse KeyIUsAfterNode() result
        last_not_after = next;
        level--;
      }
    }
  }
}

template <class Comparator>
typename InlineSkipList<Comparator>::Node*
InlineSkipList<Comparator>::FindLast() const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <class Comparator>
uint64_t InlineSkipList<Comparator>::EstimateCount(const char* key) const {
  uint64_t count = 0;

  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->Key(), key) < 0);
    Node* next = x->Next(level);
    if (next == nullptr || compare_(next->Key(), key) >= 0) {
      if (level == 0) {
        return count;
      } else {
        // Switch to next list
        count *= kBranching_;
        level--;
      }
    } else {
      x = next;
      count++;
    }
  }
}

template <class Comparator>
InlineSkipList<Comparator>::InlineSkipList(const Comparator cmp,
                                           Allocator* allocator,
                                           int32_t max_height,
                                           int32_t branching_factor)
    : kMaxHeight_(max_height),
      kBranching_(branching_factor),
      kScaledInverseBranching_((Random::kMaxNext + 1) / kBranching_),
      compare_(cmp),
      allocator_(allocator),
      head_(AllocateNode(0, max_height)),
      max_height_(1),
      prev_height_(1) {
  assert(max_height > 0 && kMaxHeight_ == static_cast<uint32_t>(max_height));
  assert(branching_factor > 1 &&
         kBranching_ == static_cast<uint32_t>(branching_factor));
  assert(kScaledInverseBranching_ > 0);
  // Allocate the prev_ Node* array, directly from the passed-in allocator.
  // prev_ does not need to be freed, as its life cycle is tied up with
  // the allocator as a whole.
  prev_ = reinterpret_cast<Node**>(
      allocator_->AllocateAligned(sizeof(Node*) * kMaxHeight_));
  for (int i = 0; i < kMaxHeight_; i++) {
    head_->SetNext(i, nullptr);
    prev_[i] = head_;
  }
}

template <class Comparator>
char* InlineSkipList<Comparator>::AllocateKey(size_t key_size) {
  return const_cast<char*>(AllocateNode(key_size, RandomHeight())->Key());
}

template <class Comparator>
typename InlineSkipList<Comparator>::Node*
InlineSkipList<Comparator>::AllocateNode(size_t key_size, int height) {
  auto prefix = sizeof(std::atomic<Node*>) * (height - 1);

  // prefix is space for the height - 1 pointers that we store before
  // the Node instance (next_[-(height - 1) .. -1]).  Node starts at
  // raw + prefix, and holds the bottom-mode (level 0) skip list pointer
  // next_[0].  key_size is the bytes for the key, which comes just after
  // the Node.
  char* raw = allocator_->AllocateAligned(prefix + sizeof(Node) + key_size);
  Node* x = reinterpret_cast<Node*>(raw + prefix);

  // Once we've linked the node into the skip list we don't actually need
  // to know its height, because we can implicitly use the fact that we
  // traversed into a node at level h to known that h is a valid level
  // for that node.  We need to convey the height to the Insert step,
  // however, so that it can perform the proper links.  Since we're not
  // using the pointers at the moment, StashHeight temporarily borrow
  // storage from next_[0] for that purpose.
  x->StashHeight(height);
  return x;
}

template <class Comparator>
void InlineSkipList<Comparator>::Insert(const char* key) {
  // InsertConcurrently often can't maintain the prev_ invariants, so
  // it just sets prev_height_ to zero, letting us know that we should
  // ignore it.  A relaxed load suffices here because write thread
  // synchronization separates Insert calls from InsertConcurrently calls.
  auto prev_height = prev_height_.load(std::memory_order_relaxed);

  // fast path for sequential insertion
  if (prev_height > 0 && !KeyIsAfterNode(key, prev_[0]->NoBarrier_Next(0)) &&
      (prev_[0] == head_ || KeyIsAfterNode(key, prev_[0]))) {
    assert(prev_[0] != head_ || (prev_height == 1 && GetMaxHeight() == 1));

    // Outside of this method prev_[1..max_height_] is the predecessor
    // of prev_[0], and prev_height_ refers to prev_[0].  Inside Insert
    // prev_[0..max_height - 1] is the predecessor of key.  Switch from
    // the external state to the internal
    for (int i = 1; i < prev_height; i++) {
      prev_[i] = prev_[0];
    }
  } else {
    // TODO(opt): we could use a NoBarrier predecessor search as an
    // optimization for architectures where memory_order_acquire needs
    // a synchronization instruction.  Doesn't matter on x86
    FindLessThan(key, prev_);
  }

  // Our data structure does not allow duplicate insertion
  assert(prev_[0]->Next(0) == nullptr || !Equal(key, prev_[0]->Next(0)->Key()));

  // Find the Node that we placed before the key in AllocateKey
  Node* x = reinterpret_cast<Node*>(const_cast<char*>(key)) - 1;
  int height = x->UnstashHeight();
  assert(height >= 1 && height <= kMaxHeight_);

  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      prev_[i] = head_;
    }

    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (nullptr), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since nullptr sorts after all
    // keys.  In the latter case the reader will use the new node.
    max_height_.store(height, std::memory_order_relaxed);
  }

  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev_[i]->NoBarrier_Next(i));
    prev_[i]->SetNext(i, x);
  }
  prev_[0] = x;
  prev_height_.store(height, std::memory_order_relaxed);
}

template <class Comparator>
void InlineSkipList<Comparator>::FindLevelSplice(const char* key, Node* before,
                                                 Node* after, int level,
                                                 Node** out_prev,
                                                 Node** out_next) {
  while (true) {
    Node* next = before->Next(level);
    assert(before == head_ || next == nullptr ||
           KeyIsAfterNode(next->Key(), before));
    assert(before == head_ || KeyIsAfterNode(key, before));
    if (next == after || !KeyIsAfterNode(key, next)) {
      // found it
      *out_prev = before;
      *out_next = next;
      return;
    }
    before = next;
  }
}

template <class Comparator>
void InlineSkipList<Comparator>::InsertConcurrently(const char* key) {
  Node* x = reinterpret_cast<Node*>(const_cast<char*>(key)) - 1;
  int height = x->UnstashHeight();
  assert(height >= 1 && height <= kMaxHeight_);

  // We don't have a lock-free algorithm for updating prev_, but we do have
  // the option of invalidating the entire sequential-insertion cache.
  // prev_'s invariant is that prev_[i] (i > 0) is the predecessor of
  // prev_[0] at that level.  We're only going to violate that if height
  // > 1 and key lands after prev_[height - 1] but before prev_[0].
  // Comparisons are pretty expensive, so an easier version is to just
  // clear the cache if height > 1.  We only write to prev_height_ if the
  // nobody else has, to avoid invalidating the root of the skip list in
  // all of the other CPU caches.
  if (height > 1 && prev_height_.load(std::memory_order_relaxed) != 0) {
    prev_height_.store(0, std::memory_order_relaxed);
  }

  int max_height = max_height_.load(std::memory_order_relaxed);
  while (height > max_height) {
    if (max_height_.compare_exchange_strong(max_height, height)) {
      // successfully updated it
      max_height = height;
      break;
    }
    // else retry, possibly exiting the loop because somebody else
    // increased it
  }
  assert(max_height <= kMaxPossibleHeight);

  Node* prev[kMaxPossibleHeight + 1];
  Node* next[kMaxPossibleHeight + 1];
  prev[max_height] = head_;
  next[max_height] = nullptr;
  for (int i = max_height - 1; i >= 0; --i) {
    FindLevelSplice(key, prev[i + 1], next[i + 1], i, &prev[i], &next[i]);
  }
  for (int i = 0; i < height; ++i) {
    while (true) {
      x->NoBarrier_SetNext(i, next[i]);
      if (prev[i]->CASNext(i, next[i], x)) {
        // success
        break;
      }
      // CAS failed, we need to recompute prev and next. It is unlikely
      // to be helpful to try to use a different level as we redo the
      // search, because it should be unlikely that lots of nodes have
      // been inserted between prev[i] and next[i]. No point in using
      // next[i] as the after hint, because we know it is stale.
      FindLevelSplice(key, prev[i], nullptr, i, &prev[i], &next[i]);
    }
  }
}

template <class Comparator>
bool InlineSkipList<Comparator>::Contains(const char* key) const {
  Node* x = FindGreaterOrEqual(key);
  if (x != nullptr && Equal(key, x->Key())) {
    return true;
  } else {
    return false;
  }
}

}  // namespace rocksdb
