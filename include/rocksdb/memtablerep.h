// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file contains the interface that must be implemented by any collection
// to be used as the backing store for a MemTable. Such a collection must
// satisfy the following properties:
//  (1) It does not store duplicate items.
//  (2) It uses MemTableRep::KeyComparator to compare items for iteration and
//     equality.
//  (3) It can be accessed concurrently by multiple readers and can support
//     during reads. However, it needn't support multiple concurrent writes.
//  (4) Items are never deleted.
// The liberal use of assertions is encouraged to enforce (1).
//
// The factory will be passed an Arena object when a new MemTableRep is
// requested. The API for this object is in rocksdb/arena.h.
//
// Users can implement their own memtable representations. We include three
// types built in:
//  - SkipListRep: This is the default; it is backed by a skip list.
//  - HashSkipListRep: The memtable rep that is best used for keys that are
//  structured like "prefix:suffix" where iteration within a prefix is
//  common and iteration across different prefixes is rare. It is backed by
//  a hash map where each bucket is a skip list.
//  - VectorRep: This is backed by an unordered std::vector. On iteration, the
// vector is sorted. It is intelligent about sorting; once the MarkReadOnly()
// has been called, the vector will only be sorted once. It is optimized for
// random-write-heavy workloads.
//
// The last four implementations are designed for situations in which
// iteration over the entire collection is rare since doing so requires all the
// keys to be copied into a sorted data structure.

#pragma once

#include <memory>
#include <stdint.h>

namespace rocksdb {

class Arena;
class LookupKey;
class Slice;
class SliceTransform;

class MemTableRep {
 public:
  // KeyComparator provides a means to compare keys, which are internal keys
  // concatenated with values.
  class KeyComparator {
   public:
    // Compare a and b. Return a negative value if a is less than b, 0 if they
    // are equal, and a positive value if a is greater than b
    virtual int operator()(const char* prefix_len_key1,
                           const char* prefix_len_key2) const = 0;

    virtual int operator()(const char* prefix_len_key,
                           const Slice& key) const = 0;

    virtual ~KeyComparator() { }
  };

  // Insert key into the collection. (The caller will pack key and value into a
  // single buffer and pass that in as the parameter to Insert)
  // REQUIRES: nothing that compares equal to key is currently in the
  // collection.
  virtual void Insert(const char* key) = 0;

  // Returns true iff an entry that compares equal to key is in the collection.
  virtual bool Contains(const char* key) const = 0;

  // Notify this table rep that it will no longer be added to. By default, does
  // nothing.
  virtual void MarkReadOnly() { }

  // Look up key from the mem table, since the first key in the mem table whose
  // user_key matches the one given k, call the function callback_func(), with
  // callback_args directly forwarded as the first parameter, and the mem table
  // key as the second parameter. If the return value is false, then terminates.
  // Otherwise, go through the next key.
  // It's safe for Get() to terminate after having finished all the potential
  // key for the k.user_key(), or not.
  //
  // Default:
  // Get() function with a default value of dynamically construct an iterator,
  // seek and call the call back function.
  virtual void Get(const LookupKey& k, void* callback_args,
                   bool (*callback_func)(void* arg, const char* entry));

  // Report an approximation of how much memory has been used other than memory
  // that was allocated through the arena.
  virtual size_t ApproximateMemoryUsage() = 0;

  virtual ~MemTableRep() { }

  // Iteration over the contents of a skip collection
  class Iterator {
   public:
    // Initialize an iterator over the specified collection.
    // The returned iterator is not valid.
    // explicit Iterator(const MemTableRep* collection);
    virtual ~Iterator() {}

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
    virtual void Seek(const Slice& internal_key, const char* memtable_key) = 0;

    // Position at the first entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToFirst() = 0;

    // Position at the last entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToLast() = 0;
  };

  // Return an iterator over the keys in this representation.
  virtual Iterator* GetIterator() = 0;

  // Return an iterator over at least the keys with the specified user key. The
  // iterator may also allow access to other keys, but doesn't have to. Default:
  // GetIterator().
  virtual Iterator* GetIterator(const Slice& user_key) { return GetIterator(); }

  // Return an iterator over at least the keys with the specified prefix. The
  // iterator may also allow access to other keys, but doesn't have to. Default:
  // GetIterator().
  virtual Iterator* GetPrefixIterator(const Slice& prefix) {
    return GetIterator();
  }

  // Return an iterator that has a special Seek semantics. The result of
  // a Seek might only include keys with the same prefix as the target key.
  virtual Iterator* GetDynamicPrefixIterator() { return GetIterator(); }

 protected:
  // When *key is an internal key concatenated with the value, returns the
  // user key.
  virtual Slice UserKey(const char* key) const;
};

// This is the base class for all factories that are used by RocksDB to create
// new MemTableRep objects
class MemTableRepFactory {
 public:
  virtual ~MemTableRepFactory() {}
  virtual MemTableRep* CreateMemTableRep(MemTableRep::KeyComparator&,
                                         Arena*) = 0;
  virtual const char* Name() const = 0;
};

// This creates MemTableReps that are backed by an std::vector. On iteration,
// the vector is sorted. This is useful for workloads where iteration is very
// rare and writes are generally not issued after reads begin.
//
// Parameters:
//   count: Passed to the constructor of the underlying std::vector of each
//     VectorRep. On initialization, the underlying array will be at least count
//     bytes reserved for usage.
class VectorRepFactory : public MemTableRepFactory {
  const size_t count_;

 public:
  explicit VectorRepFactory(size_t count = 0) : count_(count) { }
  virtual MemTableRep* CreateMemTableRep(MemTableRep::KeyComparator&,
                                         Arena*) override;
  virtual const char* Name() const override {
    return "VectorRepFactory";
  }
};

// This uses a skip list to store keys. It is the default.
class SkipListFactory : public MemTableRepFactory {
 public:
  virtual MemTableRep* CreateMemTableRep(MemTableRep::KeyComparator&,
                                         Arena*) override;
  virtual const char* Name() const override {
    return "SkipListFactory";
  }
};

// This class contains a fixed array of buckets, each
// pointing to a skiplist (null if the bucket is empty).
// bucket_count: number of fixed array buckets
// skiplist_height: the max height of the skiplist
// skiplist_branching_factor: probabilistic size ratio between adjacent
//                            link lists in the skiplist
extern MemTableRepFactory* NewHashSkipListRepFactory(
  const SliceTransform* transform, size_t bucket_count = 1000000,
  int32_t skiplist_height = 4, int32_t skiplist_branching_factor = 4
);

// The factory is to create memtables with a hashed linked list:
// it contains a fixed array of buckets, each pointing to a sorted single
// linked list (null if the bucket is empty).
// bucket_count: number of fixed array buckets
extern MemTableRepFactory* NewHashLinkListRepFactory(
    const SliceTransform* transform, size_t bucket_count = 50000);

}  // namespace rocksdb
