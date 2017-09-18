#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "cachelib_nvm/nvm/EvictionPolicy.hpp"
#include "cachelib_nvm/nvm/Types.hpp"

namespace facebook {
namespace cachelib {

enum class OpenMode {
  Read,
  Write,
};

enum class OpenStatus {
  // Writer can proceed
  Ready,

  // There is another writer that has finish before this writer can proceed
  Retry,
};

// In both lock modes proper draining flags are set to stop writer/reader and
// writers accessing the region.
enum class LockMode {
  AllowReaders,
  Exclusive,
};

// Region manager doesn't have internal locks. External caller must take care
// of locking.
class RegionManager {
 public:
  struct Stats {
    uint32_t reclaimCount{};
    uint64_t usedRegionSize{};
    uint64_t usedRegionDataSize{};
    uint64_t allocatedMemory{};
  };

  RegionManager(uint64_t regions,
                uint32_t regionSize,
                std::unique_ptr<EvictionPolicy> policy);
  RegionManager(const RegionManager&) = delete;
  RegionManager operator=(const RegionManager&) = delete;

  EvictionPolicy& getPolicy() const {
    return *policy_;
  }

  // Gets a free region if any
  RegionId getFree();

  // Gets a region to evict
  RegionId evict();

  // Opens region for read/write
  OpenStatus open(RegionId rid, OpenMode mode);

  // Close only OpenStatus::Ready regions
  void close(RegionId rid, OpenMode mode);

  void recordHit(RegionId rid) {
    policy_->recordHit(rid);
  }

  // Not executed under lock. It's some stats, don't care about it much.
  // Atomic x86-64 anyways because of memory model.
  void traceAllocation(RegionId rid, uint32_t size) {
    regions_[rid.index()].dataSize += size;
  }

  void trackHits(RegionId rid) {
    policy_->track(rid);
  }

  RegionId offsetToRegion(uint64_t offset) const {
    // TODO: Limit to power of 2?
    return RegionId(offset / regionSize_);
  }

  uint64_t absoluteOffset(RegionId rid, uint32_t offset) const {
    return offset + rid.index() * regionSize_;
  }

  void setKlass(RegionId rid, uint32_t klass) {
    regions_[rid.index()].klass = klass;
  }

  uint32_t getKlass(RegionId rid) const {
    return regions_[rid.index()].klass;
  }

  uint32_t regionSize() const { return regionSize_; }

  bool tryLock(RegionId rid, LockMode mode);

  void unlock(RegionId rid) {
    auto& region = regions_[rid.index()];
    region.flags = 0;
  }

  Stats collectStats() const;

 private:
  static constexpr uint32_t kLock = 1;
  static constexpr uint32_t kBlockWriters = 2;
  static constexpr uint32_t kBlockReaders = 4;

  struct Region {
    uint32_t flags{};
    uint32_t klass{};

    // Readers/writers counters are limited to max concurrency. 16 bits
    // are more than enough.
    uint16_t readers{};
    uint16_t writers{};

    uint32_t dataSize{};
  };

  // Although it is not necessary, but power of two @Region size allows fast
  // index in the @regions_ array as well as @offsetToRegion. Most important
  // that these computation happen under lock.
  static_assert(sizeof(Region) == 16, "performance warning");

  const uint64_t regionSize_{};
  // Do not mutate @regions_ vector other than in the constructor
  std::vector<Region> regions_;
  uint32_t free_{};
  uint32_t reclaimCount_{};
  std::unique_ptr<EvictionPolicy> policy_;
};

}
}
