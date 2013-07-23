#ifndef STORAGE_LEVELDB_DB_SKIPLISTREP_H_
#define STORAGE_LEVELDB_DB_SKIPLISTREP_H_

#include "leveldb/memtablerep.h"
#include "db/memtable.h"
#include "db/skiplist.h"

namespace leveldb {

class Arena;

class SkipListRep : public MemTableRep {
  Arena arena_;
  SkipList<const char*, MemTableRep::KeyComparator&> skip_list_;
public:
  explicit SkipListRep(MemTableRep::KeyComparator& compare)
    : skip_list_(compare, &arena_) { }

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  virtual void Insert(const char* key) {
    skip_list_.Insert(key);
  }

  // Returns true iff an entry that compares equal to key is in the list.
  virtual bool Contains(const char* key) const {
    return skip_list_.Contains(key);
  }

  virtual size_t ApproximateMemoryUsage() {
    return arena_.MemoryUsage();
  }

  virtual ~SkipListRep() { }

  // Iteration over the contents of a skip list
  class Iterator : public MemTableRep::Iterator {
    SkipList<const char*, MemTableRep::KeyComparator&>::Iterator iter_;
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(
      const SkipList<const char*, MemTableRep::KeyComparator&>* list
    ) : iter_(list) { }

    virtual ~Iterator() { }

    // Returns true iff the iterator is positioned at a valid node.
    virtual bool Valid() const {
      return iter_.Valid();
    }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    virtual const char* key() const {
      return iter_.key();
    }

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual void Next() {
      iter_.Next();
    }

    // Advances to the previous position.
    // REQUIRES: Valid()
    virtual void Prev() {
      iter_.Prev();
    }

    // Advance to the first entry with a key >= target
    virtual void Seek(const char* target) {
      iter_.Seek(target);
    }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToFirst() {
      iter_.SeekToFirst();
    }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToLast() {
      iter_.SeekToLast();
    }
  };

  virtual std::shared_ptr<MemTableRep::Iterator> GetIterator() {
    return std::shared_ptr<MemTableRep::Iterator>(
      new SkipListRep::Iterator(&skip_list_)
    );
  }
};

class SkipListFactory : public MemTableRepFactory {
public:
  virtual std::shared_ptr<MemTableRep> CreateMemTableRep (
    MemTableRep::KeyComparator& compare) {
    return std::shared_ptr<MemTableRep>(new SkipListRep(compare));
  }
};

}

#endif // STORAGE_LEVELDB_DB_SKIPLISTREP_H_
