//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <folly/synchronization/DistributedMutex.h>
#include <folly/synchronization/detail/ProxyLockable.h>

/**
 * Specializations for DistributedMutex allow us to use it like a normal
 * mutex.  Even though it has a non-usual interface
 */
namespace std {
template <template <typename> class Atom, bool TimePublishing>
class unique_lock<
    ::folly::detail::distributed_mutex::DistributedMutex<Atom, TimePublishing>>
    : public ::folly::detail::ProxyLockableUniqueLock<
          ::folly::detail::distributed_mutex::
              DistributedMutex<Atom, TimePublishing>> {
 public:
  using ::folly::detail::ProxyLockableUniqueLock<
      ::folly::detail::distributed_mutex::
          DistributedMutex<Atom, TimePublishing>>::ProxyLockableUniqueLock;
};

template <template <typename> class Atom, bool TimePublishing>
class lock_guard<
    ::folly::detail::distributed_mutex::DistributedMutex<Atom, TimePublishing>>
    : public ::folly::detail::ProxyLockableLockGuard<
          ::folly::detail::distributed_mutex::
              DistributedMutex<Atom, TimePublishing>> {
 public:
  using ::folly::detail::ProxyLockableLockGuard<
      ::folly::detail::distributed_mutex::
          DistributedMutex<Atom, TimePublishing>>::ProxyLockableLockGuard;
};
} // namespace std
