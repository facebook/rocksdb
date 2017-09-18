#include "FifoPolicy.hpp"

#include "cachelib_nvm/common/Logging.hpp"

namespace facebook {
namespace cachelib {

void FifoPolicy::recordHit(RegionId id) {
  // Empty
}

void FifoPolicy::track(RegionId id) {
  // Empty
}

RegionId FifoPolicy::evict() {
  auto rid = RegionId{reclaim_};
  reclaim_ = (reclaim_ + 1) % regions_;
  return rid;
}

}
}
