//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "rocksdb/memtablerep.h"
#include "db/memtable.h"
#include "db/skiplist.h"

namespace rocksdb {
namespace {
class SkipListRep : public MemTableRep {
  SkipList<const char*, MemTableRep::KeyComparator&> skip_list_;
public:
  explicit SkipListRep(MemTableRep::KeyComparator& compare, Arena* arena)
    : skip_list_(compare, arena) {
}

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  virtual void Insert(const char* key) override {
    skip_list_.Insert(key);
  }

  // Returns true iff an entry that compares equal to key is in the list.
  virtual bool Contains(const char* key) const override {
    return skip_list_.Contains(key);
  }

  virtual size_t ApproximateMemoryUsage() override {
    // All memory is allocated through arena; nothing to report here
    return 0;
  }

  virtual ~SkipListRep() override { }

  // Iteration over the contents of a skip list
  class Iterator : public MemTableRep::Iterator {
    SkipList<const char*, MemTableRep::KeyComparator&>::Iterator iter_;
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(
      const SkipList<const char*, MemTableRep::KeyComparator&>* list
    ) : iter_(list) { }

    virtual ~Iterator() override { }

    // Returns true iff the iterator is positioned at a valid node.
    virtual bool Valid() const override {
      return iter_.Valid();
    }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    virtual const char* key() const override {
      return iter_.key();
    }

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual void Next() override {
      iter_.Next();
    }

    // Advances to the previous position.
    // REQUIRES: Valid()
    virtual void Prev() override {
      iter_.Prev();
    }

    // Advance to the first entry with a key >= target
    virtual void Seek(const char* target) override {
      iter_.Seek(target);
    }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToFirst() override {
      iter_.SeekToFirst();
    }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToLast() override {
      iter_.SeekToLast();
    }
  };

  // Unhide default implementations of GetIterator
  using MemTableRep::GetIterator;

  virtual std::shared_ptr<MemTableRep::Iterator> GetIterator() override {
    return std::make_shared<SkipListRep::Iterator>(&skip_list_);
  }
};
}

std::shared_ptr<MemTableRep> SkipListFactory::CreateMemTableRep (
  MemTableRep::KeyComparator& compare, Arena* arena) {
    return std::shared_ptr<MemTableRep>(new SkipListRep(compare, arena));
}

} // namespace rocksdb
