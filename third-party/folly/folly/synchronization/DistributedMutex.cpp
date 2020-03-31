//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <folly/synchronization/DistributedMutex.h>

namespace folly {
namespace detail {
namespace distributed_mutex {

template class DistributedMutex<std::atomic, true>;

} // namespace distributed_mutex
} // namespace detail
} // namespace folly
