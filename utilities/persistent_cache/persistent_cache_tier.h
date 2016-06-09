//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#ifndef ROCKSDB_LITE

#include <limits>
#include <list>
#include <map>
#include <string>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/status.h"
#include "util/histogram.h"

// Persistent Cache
//
// Persistent cache is tiered key-value cache that can use persistent medium. It
// is a generic design and can leverage any storage medium -- disk/SSD/NVM/RAM.
// The code has been kept generic but significant benchmark/design/development
// time has been spent to make sure the cache performs appropriately for
// respective storage medium.
// The file defines
// PersistentCacheTier    : Implementation that handles individual cache tier
// PersistentTieresCache  : Implementation that handles all tiers as a logical
//                          unit
//
// PersistentTieredCache architecture:
// +--------------------------+ PersistentCacheTier that handles multiple tiers
// | +----------------+       |
// | | RAM            | PersistentCacheTier that handles RAM (VolatileCacheImpl)
// | +----------------+       |
// |   | next                 |
// |   v                      |
// | +----------------+       |
// | | NVM            | PersistentCacheTier implementation that handles NVM
// | +----------------+ (BlockCacheImpl)
// |   | next                 |
// |   V                      |
// | +----------------+       |
// | | LE-SSD         | PersistentCacheTier implementation that handles LE-SSD
// | +----------------+ (BlockCacheImpl)
// |   |                      |
// |   V                      |
// |  null                    |
// +--------------------------+
//               |
//               V
//              null
namespace rocksdb {

// Persistent Cache Tier
//
// This a logical abstraction that defines a tier of the persistent cache. Tiers
// can be stacked over one another. PersistentCahe provides the basic definition
// for accessing/storing in the cache. PersistentCacheTier extends the interface
// to enable management and stacking of tiers.
class PersistentCacheTier : public PersistentCache {
 public:
  typedef std::shared_ptr<PersistentCacheTier> Tier;
  typedef std::map<std::string, double> TierStats;

  virtual ~PersistentCacheTier() {}

  // Open the persistent cache tier
  virtual Status Open();

  // Close the persistent cache tier
  virtual Status Close();

  // Flush the pending writes
  virtual void Flush();

  // Reserve space up to 'size' bytes
  virtual bool Reserve(const size_t size);

  // Erase a key from the cache
  virtual bool Erase(const Slice& key);

  // Print stats to string recursively
  virtual std::string PrintStats();

  // Expose stats
  virtual std::vector<TierStats> Stats() = 0;

  // Insert to page cache
  virtual Status Insert(const Slice& page_key, const char* data,
                        const size_t size) = 0;

  // Lookup page cache by page identifier
  virtual Status Lookup(const Slice& page_key, std::unique_ptr<char[]>* data,
                        size_t* size) = 0;

  // Return a reference to next tier
  virtual Tier& next_tier() { return next_tier_; }

  // Set the value for next tier
  virtual void set_next_tier(const Tier& tier) {
    assert(!next_tier_);
    next_tier_ = tier;
  }

 private:
  Tier next_tier_;  // next tier
};

// PersistentTieredCache
//
// Abstraction that helps you construct a tiers of persistent caches as a
// unified cache. The tier(s) of cache will act a single tier for management
// ease and support PersistentCache methods for accessing data.
class PersistentTieredCache : public PersistentCacheTier {
 public:
  virtual ~PersistentTieredCache();

  Status Open() override;
  Status Close() override;
  void Flush() override;
  bool Erase(const Slice& key) override;
  std::string PrintStats() override;
  Status Insert(const Slice& page_key, const char* data,
                const size_t size) override;
  Status Lookup(const Slice& page_key, std::unique_ptr<char[]>* data,
                size_t* size) override;

  void AddTier(const Tier& tier);

  Tier& next_tier() override {
    auto it = tiers_.end();
    return (*it)->next_tier();
  }

  void set_next_tier(const Tier& tier) override {
    auto it = tiers_.end();
    (*it)->set_next_tier(tier);
  }

 protected:
  std::list<Tier> tiers_;  // list of tiers top-down
};

}  // namespace rocksdb

#endif
