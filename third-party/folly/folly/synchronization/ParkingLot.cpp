//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <folly/synchronization/ParkingLot.h>

#include <array>

namespace folly {
namespace parking_lot_detail {

Bucket& Bucket::bucketFor(uint64_t key) {
  constexpr size_t const kNumBuckets = 4096;

  // Statically allocating this lets us use this in allocation-sensitive
  // contexts. This relies on the assumption that std::mutex won't dynamically
  // allocate memory, which we assume to be the case on Linux and iOS.
  static Indestructible<std::array<Bucket, kNumBuckets>> gBuckets;
  return (*gBuckets)[key % kNumBuckets];
}

std::atomic<uint64_t> idallocator{0};

} // namespace parking_lot_detail
} // namespace folly
