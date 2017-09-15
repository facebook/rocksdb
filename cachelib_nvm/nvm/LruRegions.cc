#include "LruRegions.hpp"

#include <algorithm>
#include <utility>

#include "cachelib_nvm/common/Logging.hpp"

namespace facebook {
namespace cachelib {

LruRegions::LruRegions(uint32_t regions)
    : lru_{MMType::Config(0, true, true), Node::PtrCompressor()} {
  nodes_.reserve(regions);
  for (uint32_t i = 0; i < regions; i++) {
    nodes_.emplace_back(RegionId{i});
  }
}

void LruRegions::recordHit(RegionId id) {
  lru_.recordAccess(nodes_[id.index()], AccessMode::kRead);
}

void LruRegions::track(RegionId id) {
  lru_.add(nodes_[id.index()]);
}

RegionId LruRegions::evict() {
  auto it = lru_.getEvictionIterator();
  auto id = it->getId();
  lru_.remove(it);
  return id;
}

}
}
