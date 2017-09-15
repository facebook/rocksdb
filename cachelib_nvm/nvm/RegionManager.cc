#include "RegionManager.hpp"

#include "cachelib_nvm/common/Logging.hpp"
#include "cachelib_nvm/nvm/Utils.hpp"

namespace facebook {
namespace cachelib {
RegionManager::RegionManager(
    uint64_t regions,
    uint32_t regionSize,
    std::unique_ptr<EvictionPolicy> policy)
    : regionSize_{regionSize},
      regions_(regions),
      free_(regions_.size()),
      policy_{std::move(policy)} {
#if CACHELIB_LOGGING
  VLOG(1) << regions_.size() << " regions, " << regionSize_ << " bytes each";
#endif
}

RegionId RegionManager::getFree() {
  RegionId rid;
  if (free_ > 0) {
    rid = RegionId(regions_.size() - free_);
    free_--;
  }
  return rid;
}

RegionId RegionManager::evict() {
  auto rid = policy_->evict();
  reclaimCount_++;
  return rid;
}

OpenStatus RegionManager::open(RegionId rid, OpenMode mode) {
  auto& region = regions_[rid.index()];
  if (region.flags & kLock) {
    return OpenStatus::Retry;
  }
  if (mode == OpenMode::Read) {
    if (region.flags & kBlockReaders) {
      return OpenStatus::Retry;
    }
    region.readers++;
  } else {
    if (region.flags & kBlockWriters) {
      return OpenStatus::Retry;
    }
    region.writers++;
  }
  return OpenStatus::Ready;
}

void RegionManager::close(RegionId rid, OpenMode mode) {
  auto& region = regions_[rid.index()];
  if (mode == OpenMode::Read) {
    assert(region.readers > 0);
    region.readers--;
  } else {
    assert(region.writers > 0);
    region.writers--;
  }
}

bool RegionManager::tryLock(RegionId rid, LockMode mode) {
  auto& region = regions_[rid.index()];
  if (region.flags & kLock) {
    return false;
  }
  if (mode == LockMode::AllowReaders) {
    if (region.writers > 0) {
      region.flags |= kBlockWriters;
      return false;
    }
  } else {
    if (region.writers + region.readers > 0) {
      region.flags |= kBlockWriters | kBlockReaders;
      return false;
    }
  }
  region.flags |= kLock;
  return true;
}

RegionManager::Stats RegionManager::collectStats() const {
  Stats stats;
  stats.reclaimCount = reclaimCount_;
  auto usedCount = regions_.size() - free_;
  uint32_t i = 0;
  for (const auto& r : regions_) {
    if (i >= usedCount) {
      break;
    }
    stats.usedRegionSize += regionSize_;
    stats.usedRegionDataSize += r.dataSize;
    i++;
  }
  stats.allocatedMemory = sizeof(*this) + util::memorySize(regions_);
  return stats;
}
}
}
