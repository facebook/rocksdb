/**
 * Copyright 2012 Facebook
 * @author Tudor Bosman (tudorb@fb.com)
 */

#ifndef THRIFT_LIB_CPP_REFLECTION_H_
#define THRIFT_LIB_CPP_REFLECTION_H_

#include <cstddef>
#include <cstdint>

#include "reflection_types.h"

namespace apache {
namespace thrift {
namespace reflection {

namespace detail {
const size_t kTypeBits = 5;
const uint64_t kTypeMask = (1ULL << kTypeBits) - 1;
}  // namespace detail

inline int64_t makeTypeId(Type type, uint64_t hash) {
  return static_cast<int64_t>((hash & ~detail::kTypeMask) | type);
}

inline Type getType(int64_t typeId) {
  return static_cast<Type>(typeId & detail::kTypeMask);
}

inline bool isBaseType(Type type) {
  return type <= TYPE_DOUBLE;
}

}  // namespace reflection
}  // namespace thrift
}  // namespace apache

#endif /* THRIFT_LIB_CPP_REFLECTION_H_ */

