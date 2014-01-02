//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "rocksdb/cache.h"
#include "port/port.h"
#include "util/autovector.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace rocksdb {

Cache::~Cache() {
}

namespace {

// LRU cache implementation

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
struct LRUHandle {
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;      // TODO(opt): Only allow uint32_t?
  size_t key_length;
  uint32_t refs;
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
  char key_data[1];   // Beginning of key

  Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) {
      return *(reinterpret_cast<Slice*>(value));
    } else {
      return Slice(key_data, key_length);
    }
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h;
    if (old == nullptr) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != nullptr) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  LRUHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != nullptr &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 16;
    while (new_length < elems_ * 1.5) {
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }
  void SetRemoveScanCountLimit(size_t remove_scan_count_limit) {
    remove_scan_count_limit_ = remove_scan_count_limit;
  }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  // Although in some platforms the update of size_t is atomic, to make sure
  // GetUsage() works correctly under any platforms, we'll protect this
  // function with mutex.
  size_t GetUsage() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* e);
  // Just reduce the reference count by 1.
  // Return true if last reference
  bool Unref(LRUHandle* e);
  // Call deleter and free
  void FreeEntry(LRUHandle* e);

  // Initialized before use.
  size_t capacity_;
  uint32_t remove_scan_count_limit_;

  // mutex_ protects the following state.
  // We don't count mutex_ as the cache's internal state so semantically we
  // don't mind mutex_ invoking the non-const actions.
  mutable port::Mutex mutex_;
  size_t usage_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  LRUHandle lru_;

  HandleTable table_;
};

LRUCache::LRUCache()
    : usage_(0) {
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
}

LRUCache::~LRUCache() {
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;
    assert(e->refs == 1);  // Error if caller has an unreleased handle
    if (Unref(e)) {
      FreeEntry(e);
    }
    e = next;
  }
}

bool LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  return e->refs == 0;
}

void LRUCache::FreeEntry(LRUHandle* e) {
  assert(e->refs == 0);
  (*e->deleter)(e->key(), e->value);
  free(e);
}

void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
  usage_ -= e->charge;
}

void LRUCache::LRU_Append(LRUHandle* e) {
  // Make "e" newest entry by inserting just before lru_
  e->next = &lru_;
  e->prev = lru_.prev;
  e->prev->next = e;
  e->next->prev = e;
  usage_ += e->charge;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    e->refs++;
    LRU_Remove(e);
    LRU_Append(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Release(Cache::Handle* handle) {
  LRUHandle* e = reinterpret_cast<LRUHandle*>(handle);
  bool last_reference = false;
  {
    MutexLock l(&mutex_);
    last_reference = Unref(e);
  }
  if (last_reference) {
    FreeEntry(e);
  }
}

Cache::Handle* LRUCache::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {

  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size()));
  autovector<LRUHandle*> last_reference_list;

  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->refs = 2;  // One from LRUCache, one for the returned handle
  memcpy(e->key_data, key.data(), key.size());

  {
    MutexLock l(&mutex_);

    LRU_Append(e);

    LRUHandle* old = table_.Insert(e);
    if (old != nullptr) {
      LRU_Remove(old);
      if (Unref(old)) {
        last_reference_list.push_back(old);
      }
    }

    if (remove_scan_count_limit_ > 0) {
      // Try to free the space by evicting the entries that are only
      // referenced by the cache first.
      LRUHandle* cur = lru_.next;
      for (unsigned int scanCount = 0;
           usage_ > capacity_ && cur != &lru_
           && scanCount < remove_scan_count_limit_; scanCount++) {
        LRUHandle* next = cur->next;
        if (cur->refs <= 1) {
          LRU_Remove(cur);
          table_.Remove(cur->key(), cur->hash);
          if (Unref(cur)) {
            last_reference_list.push_back(cur);
          }
        }
        cur = next;
      }
    }

    // Free the space following strict LRU policy until enough space
    // is freed.
    while (usage_ > capacity_ && lru_.next != &lru_) {
      LRUHandle* old = lru_.next;
      LRU_Remove(old);
      table_.Remove(old->key(), old->hash);
      if (Unref(old)) {
        last_reference_list.push_back(old);
      }
    }
  }

  // we free the entries here outside of mutex for
  // performance reasons
  for (auto entry : last_reference_list) {
    FreeEntry(entry);
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
  LRUHandle* e;
  bool last_reference = false;
  {
    MutexLock l(&mutex_);
    e = table_.Remove(key, hash);
    if (e != nullptr) {
      LRU_Remove(e);
      last_reference = Unref(e);
    }
  }
  // mutex not held here
  // last_reference will only be true if e != nullptr
  if (last_reference) {
    FreeEntry(e);
  }
}

static int kNumShardBits = 4;          // default values, can be overridden
static int kRemoveScanCountLimit = 0; // default values, can be overridden

class ShardedLRUCache : public Cache {
 private:
  LRUCache* shards_;
  port::Mutex id_mutex_;
  uint64_t last_id_;
  int num_shard_bits_;
  size_t capacity_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  uint32_t Shard(uint32_t hash) {
    // Note, hash >> 32 yields hash in gcc, not the zero we expect!
    return (num_shard_bits_ > 0) ? (hash >> (32 - num_shard_bits_)) : 0;
  }

  void init(size_t capacity, int numbits, int removeScanCountLimit) {
    num_shard_bits_ = numbits;
    capacity_ = capacity;
    int num_shards = 1 << num_shard_bits_;
    shards_ = new LRUCache[num_shards];
    const size_t per_shard = (capacity + (num_shards - 1)) / num_shards;
    for (int s = 0; s < num_shards; s++) {
      shards_[s].SetCapacity(per_shard);
      shards_[s].SetRemoveScanCountLimit(removeScanCountLimit);
    }
  }

 public:
  explicit ShardedLRUCache(size_t capacity)
      : last_id_(0) {
    init(capacity, kNumShardBits, kRemoveScanCountLimit);
  }
  ShardedLRUCache(size_t capacity, int num_shard_bits,
                  int removeScanCountLimit)
     : last_id_(0) {
    init(capacity, num_shard_bits, removeScanCountLimit);
  }
  virtual ~ShardedLRUCache() {
    delete[] shards_;
  }
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shards_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    return shards_[Shard(hash)].Lookup(key, hash);
  }
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shards_[Shard(h->hash)].Release(handle);
  }
  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    shards_[Shard(hash)].Erase(key, hash);
  }
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  virtual size_t GetCapacity() const {
    return capacity_;
  }
  virtual size_t GetUsage() const {
    // We will not lock the cache when getting the usage from shards.
    // for (size_t i = 0; i < num_shard_bits_; ++i)
    int num_shards = 1 << num_shard_bits_;
    size_t usage = 0;
    for (int s = 0; s < num_shards; s++) {
      usage += shards_[s].GetUsage();
    }
    return usage;
  }
};

}  // end anonymous namespace

shared_ptr<Cache> NewLRUCache(size_t capacity) {
  return NewLRUCache(capacity, kNumShardBits);
}

shared_ptr<Cache> NewLRUCache(size_t capacity, int num_shard_bits) {
  return NewLRUCache(capacity, num_shard_bits, kRemoveScanCountLimit);
}

shared_ptr<Cache> NewLRUCache(size_t capacity, int num_shard_bits,
                              int removeScanCountLimit) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  return std::make_shared<ShardedLRUCache>(capacity,
                                           num_shard_bits,
                                           removeScanCountLimit);
}

}  // namespace rocksdb
