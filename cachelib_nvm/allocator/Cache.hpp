#pragma once

namespace facebook {
namespace cachelib {
// The mode in which the cache was accessed. This can be used by containers
// to differentiate between the access modes and do appropriate action.
enum class AccessMode {
  kRead,
  kWrite
};

// enum value to indicate if the removal from the MMContainer was an eviction
// or not.
enum class RemoveContext {
  kEviction,
  kNormal
};
}
}
