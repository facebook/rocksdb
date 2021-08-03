// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable cache sizing, etc.)

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "rocksdb/memory_allocator.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class Cache;
struct ConfigOptions;
class SecondaryCache;

extern const bool kDefaultToAdaptiveMutex;

enum CacheMetadataChargePolicy {
  kDontChargeCacheMetadata,
  kFullChargeCacheMetadata
};
const CacheMetadataChargePolicy kDefaultCacheMetadataChargePolicy =
    kFullChargeCacheMetadata;

struct LRUCacheOptions {
  // Capacity of the cache.
  size_t capacity = 0;

  // Cache is sharded into 2^num_shard_bits shards,
  // by hash of key. Refer to NewLRUCache for further
  // information.
  int num_shard_bits = -1;

  // If strict_capacity_limit is set,
  // insert to the cache will fail when cache is full.
  bool strict_capacity_limit = false;

  // Percentage of cache reserved for high priority entries.
  // If greater than zero, the LRU list will be split into a high-pri
  // list and a low-pri list. High-pri entries will be inserted to the
  // tail of high-pri list, while low-pri entries will be first inserted to
  // the low-pri list (the midpoint). This is referred to as
  // midpoint insertion strategy to make entries that never get hit in cache
  // age out faster.
  //
  // See also
  // BlockBasedTableOptions::cache_index_and_filter_blocks_with_high_priority.
  double high_pri_pool_ratio = 0.5;

  // If non-nullptr will use this allocator instead of system allocator when
  // allocating memory for cache blocks. Call this method before you start using
  // the cache!
  //
  // Caveat: when the cache is used as block cache, the memory allocator is
  // ignored when dealing with compression libraries that allocate memory
  // internally (currently only XPRESS).
  std::shared_ptr<MemoryAllocator> memory_allocator;

  // Whether to use adaptive mutexes for cache shards. Note that adaptive
  // mutexes need to be supported by the platform in order for this to have any
  // effect. The default value is true if RocksDB is compiled with
  // -DROCKSDB_DEFAULT_TO_ADAPTIVE_MUTEX, false otherwise.
  bool use_adaptive_mutex = kDefaultToAdaptiveMutex;

  CacheMetadataChargePolicy metadata_charge_policy =
      kDefaultCacheMetadataChargePolicy;

  // A SecondaryCache instance to use a the non-volatile tier
  std::shared_ptr<SecondaryCache> secondary_cache;

  LRUCacheOptions() {}
  LRUCacheOptions(size_t _capacity, int _num_shard_bits,
                  bool _strict_capacity_limit, double _high_pri_pool_ratio,
                  std::shared_ptr<MemoryAllocator> _memory_allocator = nullptr,
                  bool _use_adaptive_mutex = kDefaultToAdaptiveMutex,
                  CacheMetadataChargePolicy _metadata_charge_policy =
                      kDefaultCacheMetadataChargePolicy)
      : capacity(_capacity),
        num_shard_bits(_num_shard_bits),
        strict_capacity_limit(_strict_capacity_limit),
        high_pri_pool_ratio(_high_pri_pool_ratio),
        memory_allocator(std::move(_memory_allocator)),
        use_adaptive_mutex(_use_adaptive_mutex),
        metadata_charge_policy(_metadata_charge_policy) {}
};

// Create a new cache with a fixed size capacity. The cache is sharded
// to 2^num_shard_bits shards, by hash of the key. The total capacity
// is divided and evenly assigned to each shard. If strict_capacity_limit
// is set, insert to the cache will fail when cache is full. User can also
// set percentage of the cache reserves for high priority entries via
// high_pri_pool_pct.
// num_shard_bits = -1 means it is automatically determined: every shard
// will be at least 512KB and number of shard bits will not exceed 6.
extern std::shared_ptr<Cache> NewLRUCache(
    size_t capacity, int num_shard_bits = -1,
    bool strict_capacity_limit = false, double high_pri_pool_ratio = 0.5,
    std::shared_ptr<MemoryAllocator> memory_allocator = nullptr,
    bool use_adaptive_mutex = kDefaultToAdaptiveMutex,
    CacheMetadataChargePolicy metadata_charge_policy =
        kDefaultCacheMetadataChargePolicy);

extern std::shared_ptr<Cache> NewLRUCache(const LRUCacheOptions& cache_opts);

// Similar to NewLRUCache, but create a cache based on CLOCK algorithm with
// better concurrent performance in some cases. See util/clock_cache.cc for
// more detail.
//
// Return nullptr if it is not supported.
//
// BROKEN: ClockCache is known to have bugs that could lead to crash or
// corruption, so should not be used until fixed. Use NewLRUCache instead.
extern std::shared_ptr<Cache> NewClockCache(
    size_t capacity, int num_shard_bits = -1,
    bool strict_capacity_limit = false,
    CacheMetadataChargePolicy metadata_charge_policy =
        kDefaultCacheMetadataChargePolicy);

class Cache {
 public:
  // Depending on implementation, cache entries with high priority could be less
  // likely to get evicted than low priority entries.
  enum class Priority { HIGH, LOW };

  // A set of callbacks to allow objects in the primary block cache to be
  // be persisted in a secondary cache. The purpose of the secondary cache
  // is to support other ways of caching the object, such as persistent or
  // compressed data, that may require the object to be parsed and transformed
  // in some way. Since the primary cache holds C++ objects and the secondary
  // cache may only hold flat data that doesn't need relocation, these
  // callbacks need to be provided by the user of the block
  // cache to do the conversion.
  // The CacheItemHelper is passed to Insert() and Lookup(). It has pointers
  // to callback functions for size, saving and deletion of the
  // object. The callbacks are defined in C-style in order to make them
  // stateless and not add to the cache metadata size.
  // Saving multiple std::function objects will take up 32 bytes per
  // function, even if its not bound to an object and does no capture.
  //
  // All the callbacks are C-style function pointers in order to simplify
  // lifecycle management. Objects in the cache can outlive the parent DB,
  // so anything required for these operations should be contained in the
  // object itself.
  //
  // The SizeCallback takes a void* pointer to the object and returns the size
  // of the persistable data. It can be used by the secondary cache to allocate
  // memory if needed.
  using SizeCallback = size_t (*)(void* obj);

  // The SaveToCallback takes a void* object pointer and saves the persistable
  // data into a buffer. The secondary cache may decide to not store it in a
  // contiguous buffer, in which case this callback will be called multiple
  // times with increasing offset
  using SaveToCallback = Status (*)(void* from_obj, size_t from_offset,
                                    size_t length, void* out);

  // A function pointer type for custom destruction of an entry's
  // value. The Cache is responsible for copying and reclaiming space
  // for the key, but values are managed by the caller.
  using DeleterFn = void (*)(const Slice& key, void* value);

  // A struct with pointers to helper functions for spilling items from the
  // cache into the secondary cache. May be extended in the future. An
  // instance of this struct is expected to outlive the cache.
  struct CacheItemHelper {
    SizeCallback size_cb;
    SaveToCallback saveto_cb;
    DeleterFn del_cb;

    CacheItemHelper() : size_cb(nullptr), saveto_cb(nullptr), del_cb(nullptr) {}
    CacheItemHelper(SizeCallback _size_cb, SaveToCallback _saveto_cb,
                    DeleterFn _del_cb)
        : size_cb(_size_cb), saveto_cb(_saveto_cb), del_cb(_del_cb) {}
  };

  // The CreateCallback is passed by the block cache user to Lookup(). It
  // takes in a buffer from the NVM cache and constructs an object using
  // it. The callback doesn't have ownership of the buffer and should
  // copy the contents into its own buffer.
  // typedef std::function<Status(void* buf, size_t size, void** out_obj,
  //                             size_t* charge)>
  //    CreateCallback;
  using CreateCallback = std::function<Status(void* buf, size_t size,
                                              void** out_obj, size_t* charge)>;

  Cache(std::shared_ptr<MemoryAllocator> allocator = nullptr)
      : memory_allocator_(std::move(allocator)) {}
  // No copying allowed
  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;

  // Creates a new Cache based on the input value string and returns the result.
  // Currently, this method can be used to create LRUCaches only
  // @param config_options
  // @param value  The value might be:
  //   - an old-style cache ("1M") -- equivalent to NewLRUCache(1024*102(
  //   - Name-value option pairs -- "capacity=1M; num_shard_bits=4;
  //     For the LRUCache, the values are defined in LRUCacheOptions.
  // @param result The new Cache object
  // @return OK if the cache was successfully created
  // @return NotFound if an invalid name was specified in the value
  // @return InvalidArgument if either the options were not valid
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& value,
                                 std::shared_ptr<Cache>* result);

  // Destroys all existing entries by calling the "deleter"
  // function that was passed via the Insert() function.
  //
  // @See Insert
  virtual ~Cache() {}

  // Opaque handle to an entry stored in the cache.
  struct Handle {};

  // The type of the Cache
  virtual const char* Name() const = 0;

  // Insert a mapping from key->value into the volatile cache only
  // and assign it // the specified charge against the total cache capacity.
  // If strict_capacity_limit is true and cache reaches its full capacity,
  // return Status::Incomplete.
  //
  // If handle is not nullptr, returns a handle that corresponds to the
  // mapping. The caller must call this->Release(handle) when the returned
  // mapping is no longer needed. In case of error caller is responsible to
  // cleanup the value (i.e. calling "deleter").
  //
  // If handle is nullptr, it is as if Release is called immediately after
  // insert. In case of error value will be cleanup.
  //
  // When the inserted entry is no longer needed, the key and
  // value will be passed to "deleter" which must delete the value.
  // (The Cache is responsible for copying and reclaiming space for
  // the key.)
  virtual Status Insert(const Slice& key, void* value, size_t charge,
                        DeleterFn deleter, Handle** handle = nullptr,
                        Priority priority = Priority::LOW) = 0;

  // If the cache has no mapping for "key", returns nullptr.
  //
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  // If stats is not nullptr, relative tickers could be used inside the
  // function.
  virtual Handle* Lookup(const Slice& key, Statistics* stats = nullptr) = 0;

  // Increments the reference count for the handle if it refers to an entry in
  // the cache. Returns true if refcount was incremented; otherwise, returns
  // false.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual bool Ref(Handle* handle) = 0;

  /**
   * Release a mapping returned by a previous Lookup(). A released entry might
   * still  remain in cache in case it is later looked up by others. If
   * force_erase is set then it also erase it from the cache if there is no
   * other reference to  it. Erasing it should call the deleter function that
   * was provided when the
   * entry was inserted.
   *
   * Returns true if the entry was also erased.
   */
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual bool Release(Handle* handle, bool force_erase = false) = 0;

  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void* Value(Handle* handle) = 0;

  // If the cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void Erase(const Slice& key) = 0;
  // Return a new numeric id.  May be used by multiple clients who are
  // sharding the same cache to partition the key space.  Typically the
  // client will allocate a new id at startup and prepend the id to
  // its cache keys.
  virtual uint64_t NewId() = 0;

  // sets the maximum configured capacity of the cache. When the new
  // capacity is less than the old capacity and the existing usage is
  // greater than new capacity, the implementation will do its best job to
  // purge the released entries from the cache in order to lower the usage
  virtual void SetCapacity(size_t capacity) = 0;

  // Set whether to return error on insertion when cache reaches its full
  // capacity.
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) = 0;

  // Get the flag whether to return error on insertion when cache reaches its
  // full capacity.
  virtual bool HasStrictCapacityLimit() const = 0;

  // returns the maximum configured capacity of the cache
  virtual size_t GetCapacity() const = 0;

  // returns the memory size for the entries residing in the cache.
  virtual size_t GetUsage() const = 0;

  // returns the memory size for a specific entry in the cache.
  virtual size_t GetUsage(Handle* handle) const = 0;

  // returns the memory size for the entries in use by the system
  virtual size_t GetPinnedUsage() const = 0;

  // returns the charge for the specific entry in the cache.
  virtual size_t GetCharge(Handle* handle) const = 0;

  // Returns the deleter for the specified entry. This might seem useless
  // as the Cache itself is responsible for calling the deleter, but
  // the deleter can essentially verify that a cache entry is of an
  // expected type from an expected code source.
  virtual DeleterFn GetDeleter(Handle* handle) const = 0;

  // Call this on shutdown if you want to speed it up. Cache will disown
  // any underlying data and will not free it on delete. This call will leak
  // memory - call this only if you're shutting down the process.
  // Any attempts of using cache after this call will fail terribly.
  // Always delete the DB object before calling this method!
  virtual void DisownData(){
      // default implementation is noop
  }

  struct ApplyToAllEntriesOptions {
    // If the Cache uses locks, setting `average_entries_per_lock` to
    // a higher value suggests iterating over more entries each time a lock
    // is acquired, likely reducing the time for ApplyToAllEntries but
    // increasing latency for concurrent users of the Cache. Setting
    // `average_entries_per_lock` to a smaller value could be helpful if
    // callback is relatively expensive, such as using large data structures.
    size_t average_entries_per_lock = 256;
  };

  // Apply a callback to all entries in the cache. The Cache must ensure
  // thread safety but does not guarantee that a consistent snapshot of all
  // entries is iterated over if other threads are operating on the Cache
  // also.
  virtual void ApplyToAllEntries(
      const std::function<void(const Slice& key, void* value, size_t charge,
                               DeleterFn deleter)>& callback,
      const ApplyToAllEntriesOptions& opts) = 0;

  // DEPRECATED version of above. (Default implementation uses above.)
  virtual void ApplyToAllCacheEntries(void (*callback)(void* value,
                                                       size_t charge),
                                      bool /*thread_safe*/) {
    ApplyToAllEntries([callback](const Slice&, void* value, size_t charge,
                                 DeleterFn) { callback(value, charge); },
                      {});
  }

  // Remove all entries.
  // Prerequisite: no entry is referenced.
  virtual void EraseUnRefEntries() = 0;

  virtual std::string GetPrintableOptions() const { return ""; }

  MemoryAllocator* memory_allocator() const { return memory_allocator_.get(); }

  // EXPERIMENTAL
  // The following APIs are experimental and might change in the future.
  // The Insert and Lookup APIs below are intended to allow cached objects
  // to be demoted/promoted between the primary block cache and a secondary
  // cache. The secondary cache could be a non-volatile cache, and will
  // likely store the object in a different representation more suitable
  // for on disk storage. They rely on a per object CacheItemHelper to do
  // the conversions.
  // The secondary cache may persist across process and system restarts,
  // and may even be moved between hosts. Therefore, the cache key must
  // be repeatable across restarts/reboots, and globally unique if
  // multiple DBs share the same cache and the set of DBs can change
  // over time.

  // Insert a mapping from key->value into the cache and assign it
  // the specified charge against the total cache capacity.
  // If strict_capacity_limit is true and cache reaches its full capacity,
  // return Status::Incomplete.
  //
  // The helper argument is saved by the cache and will be used when the
  // inserted object is evicted or promoted to the secondary cache. It,
  // therefore, must outlive the cache.
  //
  // If handle is not nullptr, returns a handle that corresponds to the
  // mapping. The caller must call this->Release(handle) when the returned
  // mapping is no longer needed. In case of error caller is responsible to
  // cleanup the value (i.e. calling "deleter").
  //
  // If handle is nullptr, it is as if Release is called immediately after
  // insert. In case of error value will be cleanup.
  //
  // Regardless of whether the item was inserted into the cache,
  // it will attempt to insert it into the secondary cache if one is
  // configured, and the helper supports it.
  // The cache implementation must support a secondary cache, otherwise
  // the item is only inserted into the primary cache. It may
  // defer the insertion to the secondary cache as it sees fit.
  //
  // When the inserted entry is no longer needed, the key and
  // value will be passed to "deleter".
  virtual Status Insert(const Slice& key, void* value,
                        const CacheItemHelper* helper, size_t charge,
                        Handle** handle = nullptr,
                        Priority priority = Priority::LOW) {
    if (!helper) {
      return Status::InvalidArgument();
    }
    return Insert(key, value, charge, helper->del_cb, handle, priority);
  }

  // Lookup the key in the primary and secondary caches (if one is configured).
  // The create_cb callback function object will be used to contruct the
  // cached object.
  // If none of the caches have the mapping for the key, returns nullptr.
  // Else, returns a handle that corresponds to the mapping.
  //
  // This call may promote the object from the secondary cache (if one is
  // configured, and has the given key) to the primary cache.
  //
  // The helper argument should be provided if the caller wants the lookup
  // to include the secondary cache (if one is configured) and the object,
  // if it exists, to be promoted to the primary cache. The helper may be
  // saved and used later when the object is evicted. Therefore, it must
  // outlive the cache.
  //
  // The handle returned may not be ready. The caller should call IsReady()
  // to check if the item value is ready, and call Wait() or WaitAll() if
  // its not ready. The caller should then call Value() to check if the
  // item was successfully retrieved. If unsuccessful (perhaps due to an
  // IO error), Value() will return nullptr.
  virtual Handle* Lookup(const Slice& key, const CacheItemHelper* /*helper_cb*/,
                         const CreateCallback& /*create_cb*/,
                         Priority /*priority*/, bool /*wait*/,
                         Statistics* stats = nullptr) {
    return Lookup(key, stats);
  }

  // Release a mapping returned by a previous Lookup(). The "useful"
  // parameter specifies whether the data was actually used or not,
  // which may be used by the cache implementation to decide whether
  // to consider it as a hit for retention purposes.
  virtual bool Release(Handle* handle, bool /*useful*/, bool force_erase) {
    return Release(handle, force_erase);
  }

  // Determines if the handle returned by Lookup() has a valid value yet. The
  // call is not thread safe and should be called only by someone holding a
  // reference to the handle.
  virtual bool IsReady(Handle* /*handle*/) { return true; }

  // If the handle returned by Lookup() is not ready yet, wait till it
  // becomes ready.
  // Note: A ready handle doesn't necessarily mean it has a valid value. The
  // user should call Value() and check for nullptr.
  virtual void Wait(Handle* /*handle*/) {}

  // Wait for a vector of handles to become ready. As with Wait(), the user
  // should check the Value() of each handle for nullptr. This call is not
  // thread safe and should only be called by the caller holding a reference
  // to each of the handles.
  virtual void WaitAll(std::vector<Handle*>& /*handles*/) {}

 private:
  std::shared_ptr<MemoryAllocator> memory_allocator_;
};

}  // namespace ROCKSDB_NAMESPACE
