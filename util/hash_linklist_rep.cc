//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#ifndef ROCKSDB_LITE
#include "util/hash_linklist_rep.h"

#include "rocksdb/memtablerep.h"
#include "util/arena.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "port/port.h"
#include "port/atomic_pointer.h"
#include "util/murmurhash.h"
#include "db/memtable.h"
#include "db/skiplist.h"

namespace rocksdb {
namespace {

typedef const char* Key;

struct Node {
  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  Node* Next() {
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return reinterpret_cast<Node*>(next_.Acquire_Load());
  }
  void SetNext(Node* x) {
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_.Release_Store(x);
  }
  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next() {
    return reinterpret_cast<Node*>(next_.NoBarrier_Load());
  }

  void NoBarrier_SetNext(Node* x) {
    next_.NoBarrier_Store(x);
  }

 private:
  port::AtomicPointer next_;
 public:
  char key[0];
};

class HashLinkListRep : public MemTableRep {
 public:
  HashLinkListRep(const MemTableRep::KeyComparator& compare, Arena* arena,
                  const SliceTransform* transform, size_t bucket_size,
                  size_t huge_page_tlb_size);

  virtual KeyHandle Allocate(const size_t len, char** buf) override;

  virtual void Insert(KeyHandle handle) override;

  virtual bool Contains(const char* key) const override;

  virtual size_t ApproximateMemoryUsage() override;

  virtual void Get(const LookupKey& k, void* callback_args,
                   bool (*callback_func)(void* arg,
                                         const char* entry)) override;

  virtual ~HashLinkListRep();

  virtual MemTableRep::Iterator* GetIterator() override;

  virtual MemTableRep::Iterator* GetIterator(const Slice& slice) override;

  virtual MemTableRep::Iterator* GetDynamicPrefixIterator() override;

 private:
  friend class DynamicIterator;
  typedef SkipList<const char*, const MemTableRep::KeyComparator&> FullList;

  size_t bucket_size_;

  // Maps slices (which are transformed user keys) to buckets of keys sharing
  // the same transform.
  port::AtomicPointer* buckets_;

  // The user-supplied transform whose domain is the user keys.
  const SliceTransform* transform_;

  const MemTableRep::KeyComparator& compare_;

  bool BucketContains(Node* head, const Slice& key) const;

  Slice GetPrefix(const Slice& internal_key) const {
    return transform_->Transform(ExtractUserKey(internal_key));
  }

  size_t GetHash(const Slice& slice) const {
    return MurmurHash(slice.data(), slice.size(), 0) % bucket_size_;
  }

  Node* GetBucket(size_t i) const {
    return static_cast<Node*>(buckets_[i].Acquire_Load());
  }

  Node* GetBucket(const Slice& slice) const {
    return GetBucket(GetHash(slice));
  }

  bool Equal(const Slice& a, const Key& b) const {
    return (compare_(b, a) == 0);
  }


  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  bool KeyIsAfterNode(const Slice& internal_key, const Node* n) const {
    // nullptr n is considered infinite
    return (n != nullptr) && (compare_(n->key, internal_key) < 0);
  }

  bool KeyIsAfterNode(const Key& key, const Node* n) const {
    // nullptr n is considered infinite
    return (n != nullptr) && (compare_(n->key, key) < 0);
  }


  Node* FindGreaterOrEqualInBucket(Node* head, const Slice& key) const;

  class FullListIterator : public MemTableRep::Iterator {
   public:
    explicit FullListIterator(FullList* list, Arena* arena)
      : iter_(list), full_list_(list), arena_(arena) {}

    virtual ~FullListIterator() {
    }

    // Returns true iff the iterator is positioned at a valid node.
    virtual bool Valid() const {
      return iter_.Valid();
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
    virtual void Seek(const Slice& internal_key, const char* memtable_key) {
      const char* encoded_key =
          (memtable_key != nullptr) ?
              memtable_key : EncodeKey(&tmp_, internal_key);
      iter_.Seek(encoded_key);
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
    FullList::Iterator iter_;
    // To destruct with the iterator.
    std::unique_ptr<FullList> full_list_;
    std::unique_ptr<Arena> arena_;
    std::string tmp_;       // For passing to EncodeKey
  };

  class Iterator : public MemTableRep::Iterator {
   public:
    explicit Iterator(const HashLinkListRep* const hash_link_list_rep,
                      Node* head) :
        hash_link_list_rep_(hash_link_list_rep), head_(head), node_(nullptr) {
    }

    virtual ~Iterator() {
    }

    // Returns true iff the iterator is positioned at a valid node.
    virtual bool Valid() const {
      return node_ != nullptr;
    }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    virtual const char* key() const {
      assert(Valid());
      return node_->key;
    }

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual void Next() {
      assert(Valid());
      node_ = node_->Next();
    }

    // Advances to the previous position.
    // REQUIRES: Valid()
    virtual void Prev() {
      // Prefix iterator does not support total order.
      // We simply set the iterator to invalid state
      Reset(nullptr);
    }

    // Advance to the first entry with a key >= target
    virtual void Seek(const Slice& internal_key, const char* memtable_key) {
      node_ = hash_link_list_rep_->FindGreaterOrEqualInBucket(head_,
                                                              internal_key);
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

   protected:
    void Reset(Node* head) {
      head_ = head;
      node_ = nullptr;
    }
   private:
    friend class HashLinkListRep;
    const HashLinkListRep* const hash_link_list_rep_;
    Node* head_;
    Node* node_;
    std::string tmp_;       // For passing to EncodeKey

    virtual void SeekToHead() {
      node_ = head_;
    }
  };

  class DynamicIterator : public HashLinkListRep::Iterator {
   public:
    explicit DynamicIterator(HashLinkListRep& memtable_rep)
      : HashLinkListRep::Iterator(&memtable_rep, nullptr),
        memtable_rep_(memtable_rep) {}

    // Advance to the first entry with a key >= target
    virtual void Seek(const Slice& k, const char* memtable_key) {
      auto transformed = memtable_rep_.GetPrefix(k);
      Reset(memtable_rep_.GetBucket(transformed));
      HashLinkListRep::Iterator::Seek(k, memtable_key);
    }

   private:
    // the underlying memtable
    const HashLinkListRep& memtable_rep_;
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
    virtual void Seek(const Slice& user_key, const char* memtable_key) { }
    virtual void SeekToFirst() { }
    virtual void SeekToLast() { }
   private:
  };
};

HashLinkListRep::HashLinkListRep(const MemTableRep::KeyComparator& compare,
                                 Arena* arena, const SliceTransform* transform,
                                 size_t bucket_size, size_t huge_page_tlb_size)
    : MemTableRep(arena),
      bucket_size_(bucket_size),
      transform_(transform),
      compare_(compare) {
  char* mem = arena_->AllocateAligned(sizeof(port::AtomicPointer) * bucket_size,
                                      huge_page_tlb_size);

  buckets_ = new (mem) port::AtomicPointer[bucket_size];

  for (size_t i = 0; i < bucket_size_; ++i) {
    buckets_[i].NoBarrier_Store(nullptr);
  }
}

HashLinkListRep::~HashLinkListRep() {
}

KeyHandle HashLinkListRep::Allocate(const size_t len, char** buf) {
  char* mem = arena_->AllocateAligned(sizeof(Node) + len);
  Node* x = new (mem) Node();
  *buf = x->key;
  return static_cast<void*>(x);
}

void HashLinkListRep::Insert(KeyHandle handle) {
  Node* x = static_cast<Node*>(handle);
  assert(!Contains(x->key));
  Slice internal_key = GetLengthPrefixedSlice(x->key);
  auto transformed = GetPrefix(internal_key);
  auto& bucket = buckets_[GetHash(transformed)];
  Node* head = static_cast<Node*>(bucket.Acquire_Load());

  if (!head) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(nullptr);
    bucket.Release_Store(static_cast<void*>(x));
    return;
  }

  Node* cur = head;
  Node* prev = nullptr;
  while (true) {
    if (cur == nullptr) {
      break;
    }
    Node* next = cur->Next();
    // Make sure the lists are sorted.
    // If x points to head_ or next points nullptr, it is trivially satisfied.
    assert((cur == head) || (next == nullptr) ||
           KeyIsAfterNode(next->key, cur));
    if (KeyIsAfterNode(internal_key, cur)) {
      // Keep searching in this list
      prev = cur;
      cur = next;
    } else {
      break;
    }
  }

  // Our data structure does not allow duplicate insertion
  assert(cur == nullptr || !Equal(x->key, cur->key));

  // NoBarrier_SetNext() suffices since we will add a barrier when
  // we publish a pointer to "x" in prev[i].
  x->NoBarrier_SetNext(cur);

  if (prev) {
    prev->SetNext(x);
  } else {
    bucket.Release_Store(static_cast<void*>(x));
  }
}

bool HashLinkListRep::Contains(const char* key) const {
  Slice internal_key = GetLengthPrefixedSlice(key);

  auto transformed = GetPrefix(internal_key);
  auto bucket = GetBucket(transformed);
  if (bucket == nullptr) {
    return false;
  }
  return BucketContains(bucket, internal_key);
}

size_t HashLinkListRep::ApproximateMemoryUsage() {
  // Memory is always allocated from the arena.
  return 0;
}

void HashLinkListRep::Get(const LookupKey& k, void* callback_args,
                          bool (*callback_func)(void* arg, const char* entry)) {
  auto transformed = transform_->Transform(k.user_key());
  auto bucket = GetBucket(transformed);
  if (bucket != nullptr) {
    Iterator iter(this, bucket);
    for (iter.Seek(k.internal_key(), nullptr);
         iter.Valid() && callback_func(callback_args, iter.key());
         iter.Next()) {
    }
  }
}

MemTableRep::Iterator* HashLinkListRep::GetIterator() {
  // allocate a new arena of similar size to the one currently in use
  Arena* new_arena = new Arena(arena_->BlockSize());
  auto list = new FullList(compare_, new_arena);
  for (size_t i = 0; i < bucket_size_; ++i) {
    auto bucket = GetBucket(i);
    if (bucket != nullptr) {
      Iterator itr(this, bucket);
      for (itr.SeekToHead(); itr.Valid(); itr.Next()) {
        list->Insert(itr.key());
      }
    }
  }
  return new FullListIterator(list, new_arena);
}

MemTableRep::Iterator* HashLinkListRep::GetIterator(const Slice& slice) {
  auto bucket = GetBucket(transform_->Transform(slice));
  if (bucket == nullptr) {
    return new EmptyIterator();
  }
  return new Iterator(this, bucket);
}

MemTableRep::Iterator* HashLinkListRep::GetDynamicPrefixIterator() {
  return new DynamicIterator(*this);
}

bool HashLinkListRep::BucketContains(Node* head, const Slice& user_key) const {
  Node* x = FindGreaterOrEqualInBucket(head, user_key);
  return (x != nullptr && Equal(user_key, x->key));
}

Node* HashLinkListRep::FindGreaterOrEqualInBucket(Node* head,
                                                  const Slice& key) const {
  Node* x = head;
  while (true) {
    if (x == nullptr) {
      return x;
    }
    Node* next = x->Next();
    // Make sure the lists are sorted.
    // If x points to head_ or next points nullptr, it is trivially satisfied.
    assert((x == head) || (next == nullptr) || KeyIsAfterNode(next->key, x));
    if (KeyIsAfterNode(key, x)) {
      // Keep searching in this list
      x = next;
    } else {
      break;
    }
  }
  return x;
}

} // anon namespace

MemTableRep* HashLinkListRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Arena* arena,
    const SliceTransform* transform) {
  return new HashLinkListRep(compare, arena, transform, bucket_count_,
                             huge_page_tlb_size_);
}

MemTableRepFactory* NewHashLinkListRepFactory(size_t bucket_count,
                                              size_t huge_page_tlb_size) {
  return new HashLinkListRepFactory(bucket_count, huge_page_tlb_size);
}

} // namespace rocksdb
#endif  // ROCKSDB_LITE
