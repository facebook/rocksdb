//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>

namespace folly {

//  Memory locations within the same cache line are subject to destructive
//  interference, also known as false sharing, which is when concurrent
//  accesses to these different memory locations from different cores, where at
//  least one of the concurrent accesses is or involves a store operation,
//  induce contention and harm performance.
//
//  Microbenchmarks indicate that pairs of cache lines also see destructive
//  interference under heavy use of atomic operations, as observed for atomic
//  increment on Sandy Bridge.
//
//  We assume a cache line size of 64, so we use a cache line pair size of 128
//  to avoid destructive interference.
//
//  mimic: std::hardware_destructive_interference_size, C++17
constexpr std::size_t hardware_destructive_interference_size = 128;

//  Memory locations within the same cache line are subject to constructive
//  interference, also known as true sharing, which is when accesses to some
//  memory locations induce all memory locations within the same cache line to
//  be cached, benefiting subsequent accesses to different memory locations
//  within the same cache line and heping performance.
//
//  mimic: std::hardware_constructive_interference_size, C++17
constexpr std::size_t hardware_constructive_interference_size = 64;

} // namespace folly

