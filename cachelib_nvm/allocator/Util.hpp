#pragma once

#include <ctime>
#include <chrono>

namespace facebook {
namespace cachelib {
namespace util {
inline std::time_t getCurrentTimeSec() {
  auto dur = std::chrono::high_resolution_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::seconds>(dur).count();
}
}
}
}
