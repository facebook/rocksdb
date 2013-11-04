//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <unordered_map>
#include <set>
#include <vector>
#include <algorithm>
#include <iostream>

#include "rocksdb/memtablerep.h"
#include "rocksdb/arena.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "port/port.h"
#include "util/mutexlock.h"
#include "util/murmurhash.h"
#include "util/stl_wrappers.h"

namespace std {
template <>
struct hash<rocksdb::Slice> {
  size_t operator()(const rocksdb::Slice& slice) const {
    return MurmurHash(slice.data(), slice.size(), 0);
  }
};
}

namespace rocksdb {
namespace {

using namespace stl_wrappers;

class TransformRep : public MemTableRep {
 public:
  TransformRep(const KeyComparator& compare, Arena* arena,
    const SliceTransform* transform, size_t bucket_size,
    size_t num_locks);

  virtual void Insert(const char* key) override;

  virtual bool Contains(const char* key) const override;

  virtual size_t ApproximateMemoryUsage() override;

  virtual ~TransformRep() { }

  virtual std::shared_ptr<MemTableRep::Iterator> GetIterator() override;

  virtual std::shared_ptr<MemTableRep::Iterator> GetIterator(
    const Slice& slice) override;

  virtual std::shared_ptr<MemTableRep::Iterator> GetDynamicPrefixIterator()
      override {
    return std::make_shared<DynamicPrefixIterator>(*this);
  }

  std::shared_ptr<MemTableRep::Iterator> GetTransformIterator(
    const Slice& transformed);

 private:
  friend class DynamicPrefixIterator;
  typedef std::set<const char*, Compare> Bucket;
  typedef std::unordered_map<Slice, std::shared_ptr<Bucket>> BucketMap;

  // Maps slices (which are transformed user keys) to buckets of keys sharing
  // the same transform.
  BucketMap buckets_;

  // rwlock_ protects access to the buckets_ data structure itself. Each bucket
  // has its own read-write lock as well.
  mutable port::RWMutex rwlock_;

  // Keep track of approximately how much memory is being used.
  size_t memory_usage_ = 0;

  // The user-supplied transform whose domain is the user keys.
  const SliceTransform* transform_;

  // Get a bucket from buckets_. If the bucket hasn't been initialized yet,
  // initialize it before returning. Must be externally synchronized.
  std::shared_ptr<Bucket>& GetBucket(const Slice& transformed);

  port::RWMutex* GetLock(const Slice& transformed) const;

  mutable std::vector<port::RWMutex> locks_;

  const KeyComparator& compare_;

  class Iterator : public MemTableRep::Iterator {
   public:
    explicit Iterator(std::shared_ptr<Bucket> items);

    virtual ~Iterator() { };

    // Returns true iff the iterator is positioned at a valid node.
    virtual bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    virtual const char* key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    virtual void Prev();

    // Advance to the first entry with a key >= target
    virtual void Seek(const char* target);

    // Position at the first entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToFirst();

    // Position at the last entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToLast();
   private:
    std::shared_ptr<Bucket> items_;
    Bucket::const_iterator cit_;
  };

  class EmptyIterator : public MemTableRep::Iterator {
    // This is used when there wasn't a bucket. It is cheaper than
    // instantiating an empty bucket over which to iterate.
   public:
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
    static std::shared_ptr<EmptyIterator> GetInstance();
   private:
    static std::shared_ptr<EmptyIterator> instance;
    EmptyIterator() { }
  };

  class TransformIterator : public Iterator {
   public:
    explicit TransformIterator(std::shared_ptr<Bucket> items,
      port::RWMutex* rwlock);
    virtual ~TransformIterator() { }
   private:
    const ReadLock l_;
  };


  class DynamicPrefixIterator : public MemTableRep::Iterator {
   private:
    // the underlying memtable rep
    const TransformRep& memtable_rep_;
    // the result of a prefix seek
    std::unique_ptr<MemTableRep::Iterator> bucket_iterator_;

   public:
    explicit DynamicPrefixIterator(const TransformRep& memtable_rep)
    : memtable_rep_(memtable_rep) {}

    virtual ~DynamicPrefixIterator() { };

    // Returns true iff the iterator is positioned at a valid node.
    virtual bool Valid() const {
      return bucket_iterator_ && bucket_iterator_->Valid();
    }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    virtual const char* key() const {
      assert(Valid());
      return bucket_iterator_->key();
    }

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual void Next() {
      assert(Valid());
      bucket_iterator_->Next();
    }

    // Advances to the previous position.
    // REQUIRES: Valid()
    virtual void Prev() {
      assert(Valid());
      bucket_iterator_->Prev();
    }

    // Advance to the first entry with a key >= target within the
    // same bucket as target
    virtual void Seek(const char* target) {
      Slice prefix = memtable_rep_.transform_->Transform(
        memtable_rep_.UserKey(target));

      ReadLock l(&memtable_rep_.rwlock_);
      auto bucket = memtable_rep_.buckets_.find(prefix);
      if (bucket == memtable_rep_.buckets_.end()) {
        bucket_iterator_.reset(nullptr);
      } else {
        bucket_iterator_.reset(
          new TransformIterator(bucket->second, memtable_rep_.GetLock(prefix)));
        bucket_iterator_->Seek(target);
      }
    }

    // Position at the first entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToFirst() {
      // Prefix iterator does not support total order.
      // We simply set the iterator to invalid state
      bucket_iterator_.reset(nullptr);
    }

    // Position at the last entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToLast() {
      // Prefix iterator does not support total order.
      // We simply set the iterator to invalid state
      bucket_iterator_.reset(nullptr);
    }
  };
};

class PrefixHashRep : public TransformRep {
 public:
  PrefixHashRep(const KeyComparator& compare, Arena* arena,
    const SliceTransform* transform, size_t bucket_size,
    size_t num_locks)
  : TransformRep(compare, arena, transform,
    bucket_size, num_locks) { }

  virtual std::shared_ptr<MemTableRep::Iterator> GetPrefixIterator(
    const Slice& prefix) override;
};

std::shared_ptr<TransformRep::Bucket>& TransformRep::GetBucket(
  const Slice& transformed) {
  WriteLock l(&rwlock_);
  auto& bucket = buckets_[transformed];
  if (!bucket) {
    bucket.reset(
      new decltype(buckets_)::mapped_type::element_type(Compare(compare_)));
    // To memory_usage_ we add the size of the std::set and the size of the
    // std::pair (decltype(buckets_)::value_type) which includes the
    // Slice and the std::shared_ptr
    memory_usage_ += sizeof(*bucket) +
                     sizeof(decltype(buckets_)::value_type);
  }
  return bucket;
}

port::RWMutex* TransformRep::GetLock(const Slice& transformed) const {
  return &locks_[std::hash<Slice>()(transformed) % locks_.size()];
}

TransformRep::TransformRep(const KeyComparator& compare, Arena* arena,
  const SliceTransform* transform, size_t bucket_size,
  size_t num_locks)
  : buckets_(bucket_size),
    transform_(transform),
    locks_(num_locks),
    compare_(compare) { }

void TransformRep::Insert(const char* key) {
  assert(!Contains(key));
  auto transformed = transform_->Transform(UserKey(key));
  auto& bucket = GetBucket(transformed);
  WriteLock bl(GetLock(transformed));
  bucket->insert(key);
  memory_usage_ += sizeof(key);
}

bool TransformRep::Contains(const char* key) const {
  ReadLock l(&rwlock_);
  auto transformed = transform_->Transform(UserKey(key));
  auto bucket = buckets_.find(transformed);
  if (bucket == buckets_.end()) {
    return false;
  }
  ReadLock bl(GetLock(transformed));
  return bucket->second->count(key) != 0;
}

size_t TransformRep::ApproximateMemoryUsage() {
  return memory_usage_;
}

std::shared_ptr<TransformRep::EmptyIterator>
  TransformRep::EmptyIterator::GetInstance() {
  if (!instance) {
    instance.reset(new TransformRep::EmptyIterator);
  }
  return instance;
}

TransformRep::Iterator::Iterator(std::shared_ptr<Bucket> items)
  : items_(items),
    cit_(items_->begin()) { }

// Returns true iff the iterator is positioned at a valid node.
bool TransformRep::Iterator::Valid() const {
  return cit_ != items_->end();
}

// Returns the key at the current position.
// REQUIRES: Valid()
const char* TransformRep::Iterator::key() const {
  assert(Valid());
  return *cit_;
}

// Advances to the next position.
// REQUIRES: Valid()
void TransformRep::Iterator::Next() {
  assert(Valid());
  if (cit_ == items_->end()) {
    return;
  }
  ++cit_;
}

// Advances to the previous position.
// REQUIRES: Valid()
void TransformRep::Iterator::Prev() {
  assert(Valid());
  if (cit_ == items_->begin()) {
    // If you try to go back from the first element, the iterator should be
    // invalidated. So we set it to past-the-end. This means that you can
    // treat the container circularly.
    cit_ = items_->end();
  } else {
    --cit_;
  }
}

// Advance to the first entry with a key >= target
void TransformRep::Iterator::Seek(const char* target) {
  cit_ = items_->lower_bound(target);
}

// Position at the first entry in collection.
// Final state of iterator is Valid() iff collection is not empty.
void TransformRep::Iterator::SeekToFirst() {
  cit_ = items_->begin();
}

void TransformRep::Iterator::SeekToLast() {
  cit_ = items_->end();
  if (items_->size() != 0) {
    --cit_;
  }
}

TransformRep::TransformIterator::TransformIterator(
  std::shared_ptr<Bucket> items, port::RWMutex* rwlock)
  : Iterator(items), l_(rwlock) { }

std::shared_ptr<MemTableRep::Iterator> TransformRep::GetIterator() {
  auto items = std::make_shared<Bucket>(Compare(compare_));
  // Hold read locks on all locks
  ReadLock l(&rwlock_);
  std::for_each(locks_.begin(), locks_.end(), [] (port::RWMutex& lock) {
    lock.ReadLock();
  });
  for (auto& bucket : buckets_) {
    items->insert(bucket.second->begin(), bucket.second->end());
  }
  std::for_each(locks_.begin(), locks_.end(), [] (port::RWMutex& lock) {
    lock.Unlock();
  });
  return std::make_shared<Iterator>(std::move(items));
}

std::shared_ptr<MemTableRep::Iterator> TransformRep::GetTransformIterator(
  const Slice& transformed) {
  ReadLock l(&rwlock_);
  auto bucket = buckets_.find(transformed);
  if (bucket == buckets_.end()) {
    return EmptyIterator::GetInstance();
  }
  return std::make_shared<TransformIterator>(bucket->second,
    GetLock(transformed));
}

std::shared_ptr<MemTableRep::Iterator> TransformRep::GetIterator(
  const Slice& slice) {
  auto transformed = transform_->Transform(slice);
  return GetTransformIterator(transformed);
}

std::shared_ptr<TransformRep::EmptyIterator>
  TransformRep::EmptyIterator::instance;

} // anon namespace

std::shared_ptr<MemTableRep> TransformRepFactory::CreateMemTableRep(
  MemTableRep::KeyComparator& compare, Arena* arena) {
  return std::make_shared<TransformRep>(compare, arena, transform_,
    bucket_count_, num_locks_);
}

std::shared_ptr<MemTableRep> PrefixHashRepFactory::CreateMemTableRep(
  MemTableRep::KeyComparator& compare, Arena* arena) {
  return std::make_shared<PrefixHashRep>(compare, arena, transform_,
    bucket_count_, num_locks_);
}

std::shared_ptr<MemTableRep::Iterator> PrefixHashRep::GetPrefixIterator(
  const Slice& prefix) {
  return TransformRep::GetTransformIterator(prefix);
}

} // namespace rocksdb
