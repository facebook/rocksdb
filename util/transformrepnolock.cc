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

class TransformRepNoLock : public MemTableRep {
 public:
  TransformRepNoLock(MemTableRep::KeyComparator& compare, Arena* arena,
    const SliceTransform* transform, size_t bucket_size);

  virtual void Insert(const char* key) override;

  virtual bool Contains(const char* key) const override;

  virtual size_t ApproximateMemoryUsage() override;

  virtual ~TransformRepNoLock();

  virtual std::shared_ptr<MemTableRep::Iterator> GetIterator() override;

  virtual std::shared_ptr<MemTableRep::Iterator> GetIterator(
    const Slice& slice) override;

  std::shared_ptr<MemTableRep::Iterator> GetTransformIterator(
    const Slice& transformed);

 private:
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
        delete list_;
      }
    };

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

    // Position at the first entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToFirst() {
      iter_.SeekToFirst();
    }

    // Position at the last entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToLast() {
      iter_.SeekToLast();
    }
   private:
    Bucket* list_;
    Bucket::Iterator iter_;
    // here we track if we own list_. If we own it, we are also
    // responsible for it's cleaning. This is a poor man's shared_ptr
    bool own_list_;
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

class PrefixHashRepNoLock : public TransformRepNoLock {
 public:
  PrefixHashRepNoLock(MemTableRep::KeyComparator& compare, Arena* arena,
    const SliceTransform* transform, size_t bucket_size)
  : TransformRepNoLock(compare, arena, transform, bucket_size) { }

  virtual std::shared_ptr<MemTableRep::Iterator> GetPrefixIterator(
    const Slice& prefix) override;
};

TransformRepNoLock::TransformRepNoLock(MemTableRep::KeyComparator& compare,
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

TransformRepNoLock::~TransformRepNoLock() {
  delete[] buckets_;
}

TransformRepNoLock::Bucket* TransformRepNoLock::GetInitializedBucket(
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

void TransformRepNoLock::Insert(const char* key) {
  assert(!Contains(key));
  auto transformed = transform_->Transform(UserKey(key));
  auto bucket = GetInitializedBucket(transformed);
  bucket->Insert(key);
}

bool TransformRepNoLock::Contains(const char* key) const {
  auto transformed = transform_->Transform(UserKey(key));
  auto bucket = GetBucket(transformed);
  if (bucket == nullptr) {
    return false;
  }
  return bucket->Contains(key);
}

size_t TransformRepNoLock::ApproximateMemoryUsage() {
  return sizeof(buckets_);
}

std::shared_ptr<MemTableRep::Iterator> TransformRepNoLock::GetIterator() {
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

std::shared_ptr<MemTableRep::Iterator> TransformRepNoLock::GetTransformIterator(
  const Slice& transformed) {
  auto bucket = GetBucket(transformed);
  if (bucket == nullptr) {
    return empty_iterator_;
  }
  return std::make_shared<Iterator>(bucket, false);
}

std::shared_ptr<MemTableRep::Iterator> TransformRepNoLock::GetIterator(
  const Slice& slice) {
  auto transformed = transform_->Transform(slice);
  return GetTransformIterator(transformed);
}

} // anon namespace

std::shared_ptr<MemTableRep> TransformRepNoLockFactory::CreateMemTableRep(
  MemTableRep::KeyComparator& compare, Arena* arena) {
  return std::make_shared<TransformRepNoLock>(compare, arena, transform_,
    bucket_count_);
}

std::shared_ptr<MemTableRep> PrefixHashRepNoLockFactory::CreateMemTableRep(
  MemTableRep::KeyComparator& compare, Arena* arena) {
  return std::make_shared<PrefixHashRepNoLock>(compare, arena, transform_,
    bucket_count_);
}

std::shared_ptr<MemTableRep::Iterator> PrefixHashRepNoLock::GetPrefixIterator(
  const Slice& prefix) {
  return TransformRepNoLock::GetTransformIterator(prefix);
}

} // namespace rocksdb
