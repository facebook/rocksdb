#pragma once

#include <cstdint>

namespace facebook {
namespace cachelib {

using HashFunction = uint64_t (*)(const uint8_t*, uint32_t);

class RegionId {
 public:
  RegionId() = default;
  explicit RegionId(uint32_t idx) : idx_{idx} {}

  bool valid() const noexcept {
    return idx_ != kInvalid;
  }

  uint32_t index() const noexcept {
   return idx_;
  }

  bool operator==(const RegionId& other) const noexcept {
    return idx_ == other.idx_;
  }

  bool operator!=(const RegionId& other) const noexcept {
    return !(*this == other);
  }

 private:
  static constexpr uint32_t kInvalid{~0u};
  uint32_t idx_{kInvalid};
};

// Members ordered the way to reduce padding
struct NvmSlot {
  RegionId rid;
  uint32_t size{};
  uint64_t offset{};

  NvmSlot() = default;
  NvmSlot(RegionId r, uint64_t o, uint32_t s): rid{r}, size{s}, offset{o} {}
};
}
}
