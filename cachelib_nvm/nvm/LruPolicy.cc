#include "LruPolicy.hpp"

#include <algorithm>
#include <atomic>
#include <utility>

#include "cachelib_nvm/common/Logging.hpp"

namespace facebook {
namespace cachelib {

LruPolicy::LruPolicy(uint32_t regions)
    : lru_{MMType::Config(0, true, true), Node::PtrCompressor()} {
  nodes_.reserve(regions);
  for (uint32_t i = 0; i < regions; i++) {
    nodes_.emplace_back(RegionId{i});
  }
}

void LruPolicy::recordHit(RegionId id) {
  lru_.recordAccess(nodes_[id.index()], AccessMode::kRead);
}

void LruPolicy::track(RegionId id) {
  lru_.add(nodes_[id.index()]);
}

RegionId LruPolicy::evict() {
  auto it = lru_.getEvictionIterator();
  auto id = it->getId();
  lru_.remove(it);
  return id;
}

}
}
