#pragma once

#include <cstddef>
#include <cstdint>

namespace facebook {
namespace cachelib {

struct EvictionAgeStat {
  // the age of the oldest element in seconds
  uint64_t oldestElementAge;

  // this is the estimated age after removing a slab worth of elements
  uint64_t projectedAge;
};

struct MMContainerStat {
  // number of elements in the container.
  size_t size;

  // what is the unix timestamp in seconds of the oldest element existing in
  // the container.
  uint64_t oldestTimeSec;

  // number of times we recycled allocations from this container.
  uint64_t evictions;

  // number of lock hits by inserts into the LRU
  uint64_t numLockByInserts;

  // number of lock hits by recordAccess
  uint64_t numLockByRecordAccesses;

  // number of lock hits by removes
  uint64_t numLockByRemoves;

  // TODO: Make the MMContainerStat generic by moving the Lru/2Q specific
  // stats inside MMType and exporting them through a generic stats interface.
  // number of hits in each lru.
  uint64_t numHotAccesses;
  uint64_t numColdAccesses;
  uint64_t numWarmAccesses;
};
} // namespace cachelib
} // namespace facebook
