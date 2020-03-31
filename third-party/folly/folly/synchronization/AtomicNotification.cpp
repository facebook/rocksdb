//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <folly/synchronization/AtomicNotification.h>

#include <cstdint>

namespace folly {
namespace detail {
namespace atomic_notification {

// ParkingLot instance used for the atomic_wait() family of functions
//
// This has been defined as a static object (as opposed to allocated to avoid
// destruction order problems) because of possible uses coming from
// allocation-sensitive contexts.
ParkingLot<std::uint32_t> parkingLot;

} // namespace atomic_notification
} // namespace detail
} // namespace folly
