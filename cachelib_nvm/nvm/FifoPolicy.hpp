#pragma once

#include "cachelib_nvm/nvm/EvictionPolicy.hpp"

namespace facebook {
namespace cachelib {

// Simple FIFO policy for region reclamation
class FifoPolicy : public EvictionPolicy {
 public:
  explicit FifoPolicy(uint32_t regions): regions_{regions} {}
  ~FifoPolicy() override = default;

  void recordHit(RegionId id) override;
  void track(RegionId id) override;
  RegionId evict() override;

 private:
  const uint32_t regions_{};
  uint32_t reclaim_{};
};

}
}
