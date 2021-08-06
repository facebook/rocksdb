//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/memtable.h"
#include "memory/arena.h"
#include "memtable/inlineskiplist.h"
#include "rocksdb/memtablerep.h"

namespace ROCKSDB_NAMESPACE {
namespace {
class SkipListRep : public MemTableRep {
  InlineSkipList<const MemTableRep::KeyComparator&> skip_list_;
  const MemTableRep::KeyComparator& cmp_;
  const SliceTransform* transform_;
  const size_t lookahead_;

  friend class LookaheadIterator;
public:
 explicit SkipListRep(const MemTableRep::KeyComparator& compare,
                      Allocator* allocator, const SliceTransform* transform,
                      const size_t lookahead)
     : MemTableRep(allocator),
       skip_list_(compare, allocator),
       cmp_(compare),
       transform_(transform),
       lookahead_(lookahead) {}

 KeyHandle Allocate(const size_t len, char** buf) override {
   *buf = skip_list_.AllocateKey(len);
   return static_cast<KeyHandle>(*buf);
 }

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
 void Insert(KeyHandle handle) override {
   skip_list_.Insert(static_cast<char*>(handle));
 }

 bool InsertKey(KeyHandle handle) override {
   return skip_list_.Insert(static_cast<char*>(handle));
 }

 void InsertWithHint(KeyHandle handle, void** hint) override {
   skip_list_.InsertWithHint(static_cast<char*>(handle), hint);
 }

 bool InsertKeyWithHint(KeyHandle handle, void** hint) override {
   return skip_list_.InsertWithHint(static_cast<char*>(handle), hint);
 }

 void InsertWithHintConcurrently(KeyHandle handle, void** hint) override {
   skip_list_.InsertWithHintConcurrently(static_cast<char*>(handle), hint);
 }

 bool InsertKeyWithHintConcurrently(KeyHandle handle, void** hint) override {
   return skip_list_.InsertWithHintConcurrently(static_cast<char*>(handle),
                                                hint);
 }

 void InsertConcurrently(KeyHandle handle) override {
   skip_list_.InsertConcurrently(static_cast<char*>(handle));
 }

 bool InsertKeyConcurrently(KeyHandle handle) override {
   return skip_list_.InsertConcurrently(static_cast<char*>(handle));
 }

  // Returns true iff an entry that compares equal to key is in the list.
 bool Contains(const char* key) const override {
   return skip_list_.Contains(key);
 }

 size_t ApproximateMemoryUsage() override {
   // All memory is allocated through allocator; nothing to report here
   return 0;
 }

 void Get(const LookupKey& k, void* callback_args,
          bool (*callback_func)(void* arg, const char* entry)) override {
   SkipListRep::Iterator iter(&skip_list_);
   Slice dummy_slice;
   for (iter.Seek(dummy_slice, k.memtable_key().data());
        iter.Valid() && callback_func(callback_args, iter.key()); iter.Next()) {
   }
 }

  uint64_t ApproximateNumEntries(const Slice& start_ikey,
                                 const Slice& end_ikey) override {
    std::string tmp;
    uint64_t start_count =
        skip_list_.EstimateCount(EncodeKey(&tmp, start_ikey));
    uint64_t end_count = skip_list_.EstimateCount(EncodeKey(&tmp, end_ikey));
    return (end_count >= start_count) ? (end_count - start_count) : 0;
  }

  void UniqueRandomSample(const uint64_t& num_entries,
                          const uint64_t& target_sample_size,
                          std::unordered_set<const char*>* entries) override {
    entries->clear();
    // Avoid divide-by-0.
    assert(target_sample_size > 0);
    assert(num_entries > 0);
    // NOTE: the size of entries is not enforced to be exactly
    // target_sample_size at the end of this function, it might be slightly
    // greater or smaller.
    SkipListRep::Iterator iter(&skip_list_);
    // There are two methods to create the subset of samples (size m)
    // from the table containing N elements:
    // 1-Iterate through N, and sample a Bernoulli distribution of p=m/N.
    // 2-Pick m random elements without repetition.
    // We pick Option 2 when m<N/10 and
    // Option 1 when m > N/10.
    if (target_sample_size > num_entries / 10) {
      // Option 1: iterate through each element, and store it in sample
      // subset if Bernoulli dist returns true.
      Random* rnd = Random::GetTLSInstance();
      int n = (target_sample_size > num_entries)
                  ? 1
                  : static_cast<int>(num_entries / target_sample_size);
      for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        if (rnd->OneIn(n)) {
          entries->insert(iter.key());
        }
      }
    } else {
      // Option 2: pick m random elements
      // If Option 2 is picked, then target_sample_size<N/10
      // Using a set spares the need to check for duplicates.
      for (uint64_t i = 0; i < target_sample_size; i++) {
        // We give it 5 attempts to find a non-duplicate
        // At worst, for the final pick , when m=N/10 there is 10%
        // chances to find a duplicate. So chances to fail finding
        // a non-duplicate after 5 attempts is 0.001%.
        for (uint64_t j = 0; j < 5; j++) {
          if (entries->find(iter.key()) == entries->end()) {
            entries->insert(iter.key());
            break;
          }
        }
      }
    }
  }

  ~SkipListRep() override {}

  // Iteration over the contents of a skip list
  class Iterator : public MemTableRep::Iterator {
    InlineSkipList<const MemTableRep::KeyComparator&>::Iterator iter_;

   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(
        const InlineSkipList<const MemTableRep::KeyComparator&>* list)
        : iter_(list) {}

    ~Iterator() override {}

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const override { return iter_.Valid(); }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const char* key() const override { return iter_.key(); }

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next() override { iter_.Next(); }

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev() override { iter_.Prev(); }

    // Advance to the first entry with a key >= target
    void Seek(const Slice& user_key, const char* memtable_key) override {
      if (memtable_key != nullptr) {
        iter_.Seek(memtable_key);
      } else {
        iter_.Seek(EncodeKey(&tmp_, user_key));
      }
    }

    // Retreat to the last entry with a key <= target
    void SeekForPrev(const Slice& user_key, const char* memtable_key) override {
      if (memtable_key != nullptr) {
        iter_.SeekForPrev(memtable_key);
      } else {
        iter_.SeekForPrev(EncodeKey(&tmp_, user_key));
      }
    }

    void RandomSeek() override { iter_.RandomSeek(); }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst() override { iter_.SeekToFirst(); }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast() override { iter_.SeekToLast(); }

   protected:
    std::string tmp_;       // For passing to EncodeKey
  };

  // Iterator over the contents of a skip list which also keeps track of the
  // previously visited node. In Seek(), it examines a few nodes after it
  // first, falling back to O(log n) search from the head of the list only if
  // the target key hasn't been found.
  class LookaheadIterator : public MemTableRep::Iterator {
   public:
    explicit LookaheadIterator(const SkipListRep& rep) :
        rep_(rep), iter_(&rep_.skip_list_), prev_(iter_) {}

    ~LookaheadIterator() override {}

    bool Valid() const override { return iter_.Valid(); }

    const char* key() const override {
      assert(Valid());
      return iter_.key();
    }

    void Next() override {
      assert(Valid());

      bool advance_prev = true;
      if (prev_.Valid()) {
        auto k1 = rep_.UserKey(prev_.key());
        auto k2 = rep_.UserKey(iter_.key());

        if (k1.compare(k2) == 0) {
          // same user key, don't move prev_
          advance_prev = false;
        } else if (rep_.transform_) {
          // only advance prev_ if it has the same prefix as iter_
          auto t1 = rep_.transform_->Transform(k1);
          auto t2 = rep_.transform_->Transform(k2);
          advance_prev = t1.compare(t2) == 0;
        }
      }

      if (advance_prev) {
        prev_ = iter_;
      }
      iter_.Next();
    }

    void Prev() override {
      assert(Valid());
      iter_.Prev();
      prev_ = iter_;
    }

    void Seek(const Slice& internal_key, const char* memtable_key) override {
      const char *encoded_key =
        (memtable_key != nullptr) ?
            memtable_key : EncodeKey(&tmp_, internal_key);

      if (prev_.Valid() && rep_.cmp_(encoded_key, prev_.key()) >= 0) {
        // prev_.key() is smaller or equal to our target key; do a quick
        // linear search (at most lookahead_ steps) starting from prev_
        iter_ = prev_;

        size_t cur = 0;
        while (cur++ <= rep_.lookahead_ && iter_.Valid()) {
          if (rep_.cmp_(encoded_key, iter_.key()) <= 0) {
            return;
          }
          Next();
        }
      }

      iter_.Seek(encoded_key);
      prev_ = iter_;
    }

    void SeekForPrev(const Slice& internal_key,
                     const char* memtable_key) override {
      const char* encoded_key = (memtable_key != nullptr)
                                    ? memtable_key
                                    : EncodeKey(&tmp_, internal_key);
      iter_.SeekForPrev(encoded_key);
      prev_ = iter_;
    }

    void SeekToFirst() override {
      iter_.SeekToFirst();
      prev_ = iter_;
    }

    void SeekToLast() override {
      iter_.SeekToLast();
      prev_ = iter_;
    }

   protected:
    std::string tmp_;       // For passing to EncodeKey

   private:
    const SkipListRep& rep_;
    InlineSkipList<const MemTableRep::KeyComparator&>::Iterator iter_;
    InlineSkipList<const MemTableRep::KeyComparator&>::Iterator prev_;
  };

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override {
    if (lookahead_ > 0) {
      void *mem =
        arena ? arena->AllocateAligned(sizeof(SkipListRep::LookaheadIterator))
              : operator new(sizeof(SkipListRep::LookaheadIterator));
      return new (mem) SkipListRep::LookaheadIterator(*this);
    } else {
      void *mem =
        arena ? arena->AllocateAligned(sizeof(SkipListRep::Iterator))
              : operator new(sizeof(SkipListRep::Iterator));
      return new (mem) SkipListRep::Iterator(&skip_list_);
    }
  }
};
}

MemTableRep* SkipListFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform* transform, Logger* /*logger*/) {
  return new SkipListRep(compare, allocator, transform, lookahead_);
}

}  // namespace ROCKSDB_NAMESPACE
