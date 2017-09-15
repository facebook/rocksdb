#pragma once

#include "cachelib_nvm/nvm/Types.hpp"

namespace facebook {
namespace cachelib {
class EvictionPolicy {
 public:
  virtual ~EvictionPolicy() = default;

  // add a new region for tracking
  virtual void track(RegionId id) = 0;

  // record a hit for this region
  virtual void recordHit(RegionId id) = 0;

  // evict a region and stop tracking
  virtual RegionId evict() = 0;
};
}
}
