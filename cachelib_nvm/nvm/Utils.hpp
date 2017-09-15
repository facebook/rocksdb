#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <vector>

#include "cachelib_nvm/common/CompilerUtils.hpp"

namespace facebook {
namespace cachelib {
namespace util {
template <typename T>
struct DestroyDeleter {
  void operator()(T* ptr) const {
    ptr->destroy();
  }
};

inline constexpr bool isPowTwo(uint64_t n) {
  return (n & (n - 1)) == 0;
}

inline constexpr size_t alignSize(size_t size, size_t boundary) {
  assert(isPowTwo(boundary));
  return (size + (boundary - 1)) & ~(boundary - 1);
}

template <typename T>
size_t memorySize(const std::vector<T>& v) {
  return sizeof(T) * v.capacity();
}

template <typename IterIn, typename IterOut, typename Cmp>
void topN(size_t n, IterIn first, IterIn last, IterOut res, Cmp cmp) {
  std::partial_sort(first, first + n, last, cmp);
  std::copy(first, first + n, res);
}
}
}
}
