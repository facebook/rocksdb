// Copyright (c) 2021, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <stdint.h>

#include <memory>
#include <string>

#include "rocksdb/cache.h"
#include "rocksdb/customizable.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// A handle for lookup result. The handle may not be immediately ready or
// have a valid value. The caller must call isReady() to determine if its
// ready, and call Wait() in order to block until it becomes ready.
// The caller must call value() after it becomes ready to determine if the
// handle successfullly read the item.
class SecondaryCacheResultHandle {
 public:
  virtual ~SecondaryCacheResultHandle() {}

  // Returns whether the handle is ready or not
  virtual bool IsReady() = 0;

  // Block until handle becomes ready
  virtual void Wait() = 0;

  // Return the value. If nullptr, it means the lookup was unsuccessful
  virtual void* Value() = 0;

  // Return the size of value
  virtual size_t Size() = 0;
};

// SecondaryCache
//
// Cache interface for caching blocks on a secondary tier (which can include
// non-volatile media, or alternate forms of caching such as compressed data)
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class SecondaryCache : public Customizable {
 public:
  virtual ~SecondaryCache() {}

  static const char* Type() { return "SecondaryCache"; }
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& id,
                                 std::shared_ptr<SecondaryCache>* result);

  // Insert the given value into this cache. The value is not written
  // directly. Rather, the SaveToCallback provided by helper_cb will be
  // used to extract the persistable data in value, which will be written
  // to this tier. The implementation may or may not write it to cache
  // depending on the admission control policy, even if the return status is
  // success.
  virtual Status Insert(const Slice& key, void* value,
                        const Cache::CacheItemHelper* helper) = 0;

  // Lookup the data for the given key in this cache. The create_cb
  // will be used to create the object. The handle returned may not be
  // ready yet, unless wait=true, in which case Lookup() will block until
  // the handle is ready
  virtual std::unique_ptr<SecondaryCacheResultHandle> Lookup(
      const Slice& key, const Cache::CreateCallback& create_cb, bool wait) = 0;

  // At the discretion of the implementation, erase the data associated
  // with key
  virtual void Erase(const Slice& key) = 0;

  // Wait for a collection of handles to become ready
  virtual void WaitAll(std::vector<SecondaryCacheResultHandle*> handles) = 0;

  virtual std::string GetPrintableOptions() const override = 0;
};

}  // namespace ROCKSDB_NAMESPACE
