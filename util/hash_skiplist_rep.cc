//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include "rocksdb/memtablerep.h"
#include "rocksdb/arena.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "port/port.h"
#include "port/atomic_pointer.h"
#include "util/murmurhash.h"
#include "db/skiplist.h"

namespace rocksdb {
namespace {

class HashSkipListRep : public MemTableRep {
 public:
  HashSkipListRep(MemTableRep::KeyComparator& compare, Arena* arena,
    const SliceTransform* transform, size_t bucket_size);

  virtual void Insert(const char* key) override;

  virtual bool Contains(const char* key) const override;

  virtual size_t ApproximateMemoryUsage() override;

  virtual ~HashSkipListRep();

  virtual std::shared_ptr<MemTableRep::Iterator> GetIterator() override;

  virtual std::shared_ptr<MemTableRep::Iterator> GetIterator(
      const Slice& slice) override;

  virtual std::shared_ptr<MemTableRep::Iterator> GetPrefixIterator(
      const Slice& prefix) override;

  virtual std::shared_ptr<MemTableRep::Iterator> GetDynamicPrefixIterator()
      override;

 private:
  friend class DynamicIterator;
  typedef SkipList<const char*, MemTableRep::KeyComparator&> Bucket;

  size_t bucket_size_;

  // Maps slices (which are transformed user keys) to buckets of keys sharing
  // the same transform.
  port::AtomicPointer* buckets_;

  // The user-supplied transform whose domain is the user keys.
  const SliceTransform* transform_;

  MemTableRep::KeyComparator& compare_;
  // immutable after construction
  Arena* const arena_;

  inline size_t GetHash(const Slice& slice) const {
    return MurmurHash(slice.data(), slice.size(), 0) % bucket_size_;
  }
  inline Bucket* GetBucket(size_t i) const {
    return static_cast<Bucket*>(buckets_[i].Acquire_Load());
  }
  inline Bucket* GetBucket(const Slice& slice) const {
    return GetBucket(GetHash(slice));
  }
  // Get a bucket from buckets_. If the bucket hasn't been initialized yet,
  // initialize it before returning.
  Bucket* GetInitializedBucket(const Slice& transformed);

  class Iterator : public MemTableRep::Iterator {
   public:
    explicit Iterator(Bucket* list, bool own_list = true)
      : list_(list),
        iter_(list),
        own_list_(own_list) {}

    virtual ~Iterator() {
      // if we own the list, we should also delete it
      if (own_list_) {
        assert(list_ != nullptr);
        delete list_;
      }
    }

    // Returns true iff the iterator is positioned at a valid node.
    virtual bool Valid() const {
      return list_ != nullptr && iter_.Valid();
    }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    virtual const char* key() const {
      assert(Valid());
      return iter_.key();
    }

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual void Next() {
      assert(Valid());
      iter_.Next();
    }

    // Advances to the previous position.
    // REQUIRES: Valid()
    virtual void Prev() {
      assert(Valid());
      iter_.Prev();
    }

    // Advance to the first entry with a key >= target
    virtual void Seek(const char* target) {
      if (list_ != nullptr) {
        iter_.Seek(target);
      }
    }

    // Position at the first entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToFirst() {
      if (list_ != nullptr) {
        iter_.SeekToFirst();
      }
    }

    // Position at the last entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToLast() {
      if (list_ != nullptr) {
        iter_.SeekToLast();
      }
    }
   protected:
    void Reset(Bucket* list) {
      if (own_list_) {
        assert(list_ != nullptr);
        delete list_;
      }
      list_ = list;
      iter_.SetList(list);
      own_list_ = false;
    }
   private:
    // if list_ is nullptr, we should NEVER call any methods on iter_
    // if list_ is nullptr, this Iterator is not Valid()
    Bucket* list_;
    Bucket::Iterator iter_;
    // here we track if we own list_. If we own it, we are also
    // responsible for it's cleaning. This is a poor man's shared_ptr
    bool own_list_;
  };

  class DynamicIterator : public HashSkipListRep::Iterator {
   public:
    explicit DynamicIterator(const HashSkipListRep& memtable_rep)
      : HashSkipListRep::Iterator(nullptr, false),
        memtable_rep_(memtable_rep) {}

    // Advance to the first entry with a key >= target
    virtual void Seek(const char* target) {
      auto transformed = memtable_rep_.transform_->Transform(
        memtable_rep_.UserKey(target));
      Reset(memtable_rep_.GetBucket(transformed));
      HashSkipListRep::Iterator::Seek(target);
    }

    // Position at the first entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToFirst() {
      // Prefix iterator does not support total order.
      // We simply set the iterator to invalid state
      Reset(nullptr);
    }

    // Position at the last entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToLast() {
      // Prefix iterator does not support total order.
      // We simply set the iterator to invalid state
      Reset(nullptr);
    }
   private:
    // the underlying memtable
    const HashSkipListRep& memtable_rep_;
  };

  class EmptyIterator : public MemTableRep::Iterator {
    // This is used when there wasn't a bucket. It is cheaper than
    // instantiating an empty bucket over which to iterate.
   public:
    EmptyIterator() { }
    virtual bool Valid() const {
      return false;
    }
    virtual const char* key() const {
      assert(false);
      return nullptr;
    }
    virtual void Next() { }
    virtual void Prev() { }
    virtual void Seek(const char* target) { }
    virtual void SeekToFirst() { }
    virtual void SeekToLast() { }
   private:
  };

  std::shared_ptr<EmptyIterator> empty_iterator_;
};

HashSkipListRep::HashSkipListRep(MemTableRep::KeyComparator& compare,
    Arena* arena, const SliceTransform* transform, size_t bucket_size)
  : bucket_size_(bucket_size),
    transform_(transform),
    compare_(compare),
    arena_(arena),
    empty_iterator_(std::make_shared<EmptyIterator>()) {

  buckets_ = new port::AtomicPointer[bucket_size];

  for (size_t i = 0; i < bucket_size_; ++i) {
    buckets_[i].NoBarrier_Store(nullptr);
  }
}

HashSkipListRep::~HashSkipListRep() {
  delete[] buckets_;
}

HashSkipListRep::Bucket* HashSkipListRep::GetInitializedBucket(
    const Slice& transformed) {
  size_t hash = GetHash(transformed);
  auto bucket = GetBucket(hash);
  if (bucket == nullptr) {
    auto addr = arena_->AllocateAligned(sizeof(Bucket));
    bucket = new (addr) Bucket(compare_, arena_);
    buckets_[hash].Release_Store(static_cast<void*>(bucket));
  }
  return bucket;
}

void HashSkipListRep::Insert(const char* key) {
  assert(!Contains(key));
  auto transformed = transform_->Transform(UserKey(key));
  auto bucket = GetInitializedBucket(transformed);
  bucket->Insert(key);
}

bool HashSkipListRep::Contains(const char* key) const {
  auto transformed = transform_->Transform(UserKey(key));
  auto bucket = GetBucket(transformed);
  if (bucket == nullptr) {
    return false;
  }
  return bucket->Contains(key);
}

size_t HashSkipListRep::ApproximateMemoryUsage() {
  return sizeof(buckets_);
}

std::shared_ptr<MemTableRep::Iterator> HashSkipListRep::GetIterator() {
  auto list = new Bucket(compare_, arena_);
  for (size_t i = 0; i < bucket_size_; ++i) {
    auto bucket = GetBucket(i);
    if (bucket != nullptr) {
      Bucket::Iterator itr(bucket);
      for (itr.SeekToFirst(); itr.Valid(); itr.Next()) {
        list->Insert(itr.key());
      }
    }
  }
  return std::make_shared<Iterator>(list);
}

std::shared_ptr<MemTableRep::Iterator> HashSkipListRep::GetPrefixIterator(
  const Slice& prefix) {
  auto bucket = GetBucket(prefix);
  if (bucket == nullptr) {
    return empty_iterator_;
  }
  return std::make_shared<Iterator>(bucket, false);
}

std::shared_ptr<MemTableRep::Iterator> HashSkipListRep::GetIterator(
    const Slice& slice) {
  return GetPrefixIterator(transform_->Transform(slice));
}

std::shared_ptr<MemTableRep::Iterator>
    HashSkipListRep::GetDynamicPrefixIterator() {
  return std::make_shared<DynamicIterator>(*this);
}

} // anon namespace

class HashSkipListRepFactory : public MemTableRepFactory {
 public:
  explicit HashSkipListRepFactory(const SliceTransform* transform,
      size_t bucket_count = 1000000)
    : transform_(transform),
      bucket_count_(bucket_count) { }

  virtual ~HashSkipListRepFactory() { delete transform_; }

  virtual std::shared_ptr<MemTableRep> CreateMemTableRep(
      MemTableRep::KeyComparator& compare, Arena* arena) override {
    return std::make_shared<HashSkipListRep>(compare, arena, transform_,
        bucket_count_);
  }

  virtual const char* Name() const override {
    return "HashSkipListRepFactory";
  }

  const SliceTransform* GetTransform() { return transform_; }

 private:
  const SliceTransform* transform_;
  const size_t bucket_count_;
};

MemTableRepFactory* NewHashSkipListRepFactory(
    const SliceTransform* transform, size_t bucket_count) {
  return new HashSkipListRepFactory(transform, bucket_count);
}

} // namespace rocksdb
