//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <folly/Traits.h>

#include <cstdint>
#include <cstring>
#include <type_traits>

namespace folly {

template <
    typename To,
    typename From,
    _t<std::enable_if<
        sizeof(From) == sizeof(To) && std::is_trivial<To>::value &&
            is_trivially_copyable<From>::value,
        int>> = 0>
To bit_cast(const From& src) noexcept {
  To to;
  std::memcpy(&to, &src, sizeof(From));
  return to;
}

} // namespace folly

