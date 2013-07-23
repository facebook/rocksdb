// This file contains the interface that must be implemented by any collection
// to be used as the backing store for a MemTable. Such a collection must
// satisfy the following properties:
//  (1) It does not store duplicate items.
//  (2) It uses MemTableRep::KeyComparator to compare items for iteration and
//     equality.
//  (3) It can be accessed concurrently by multiple readers but need not support
//      concurrent writes.
//  (4) Items are never deleted.
// The liberal use of assertions is encouraged to enforce (1).

#ifndef STORAGE_LEVELDB_DB_TABLE_H_
#define STORAGE_LEVELDB_DB_TABLE_H_

#include <memory>

namespace leveldb {

class MemTableRep {
 public:
  // KeyComparator(a, b) returns a negative value if a is less than b, 0 if they
  // are equal, and a positive value if b is greater than a
  class KeyComparator {
  public:
    virtual int operator()(const char* a, const char* b) const = 0;
    virtual ~KeyComparator() { }
  };

  // Insert key into the collection. (The caller will pack key and value into a
  // single buffer and pass that in as the parameter to Insert)
  // REQUIRES: nothing that compares equal to key is currently in the
  // collection.
  virtual void Insert(const char* key) = 0;

  // Returns true iff an entry that compares equal to key is in the collection.
  virtual bool Contains(const char* key) const = 0;

  // Returns an estimate of the number of bytes of data in use by this
  // data structure.
  virtual size_t ApproximateMemoryUsage() = 0;

  virtual ~MemTableRep() { }

  // Iteration over the contents of a skip collection
  class Iterator {
   public:
    // Initialize an iterator over the specified collection.
    // The returned iterator is not valid.
    // explicit Iterator(const MemTableRep* collection);
    virtual ~Iterator() { };

    // Returns true iff the iterator is positioned at a valid node.
    virtual bool Valid() const = 0;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    virtual const char* key() const = 0;

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual void Next() = 0;

    // Advances to the previous position.
    // REQUIRES: Valid()
    virtual void Prev() = 0;

    // Advance to the first entry with a key >= target
    virtual void Seek(const char* target) = 0;

    // Position at the first entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToFirst() = 0;

    // Position at the last entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToLast() = 0;
  };

  virtual std::shared_ptr<Iterator> GetIterator() = 0;
};

class MemTableRepFactory {
 public:
  virtual ~MemTableRepFactory() { };
  virtual std::shared_ptr<MemTableRep> CreateMemTableRep(
    MemTableRep::KeyComparator&) = 0;
};

}

#endif // STORAGE_LEVELDB_DB_TABLE_H_
