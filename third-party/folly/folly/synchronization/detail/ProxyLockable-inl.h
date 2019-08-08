//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <folly/Optional.h>
#include <folly/Portability.h>
#include <folly/Utility.h>

#include <cassert>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace folly {
namespace detail {
namespace proxylockable_detail {
template <typename Bool>
void throwIfAlreadyLocked(Bool&& locked) {
  if (kIsDebug && locked) {
    throw std::system_error{
        std::make_error_code(std::errc::resource_deadlock_would_occur)};
  }
}

template <typename Bool>
void throwIfNotLocked(Bool&& locked) {
  if (kIsDebug && !locked) {
    throw std::system_error{
        std::make_error_code(std::errc::operation_not_permitted)};
  }
}

template <typename Bool>
void throwIfNoMutex(Bool&& mutex) {
  if (kIsDebug && !mutex) {
    throw std::system_error{
        std::make_error_code(std::errc::operation_not_permitted)};
  }
}
} // namespace proxylockable_detail

template <typename Mutex>
ProxyLockableUniqueLock<Mutex>::~ProxyLockableUniqueLock() {
  if (owns_lock()) {
    unlock();
  }
}

template <typename Mutex>
ProxyLockableUniqueLock<Mutex>::ProxyLockableUniqueLock(
    mutex_type& mtx) noexcept {
  proxy_.emplace(mtx.lock());
  mutex_ = std::addressof(mtx);
}

template <typename Mutex>
ProxyLockableUniqueLock<Mutex>::ProxyLockableUniqueLock(
    ProxyLockableUniqueLock&& a) noexcept {
  *this = std::move(a);
}

template <typename Mutex>
ProxyLockableUniqueLock<Mutex>& ProxyLockableUniqueLock<Mutex>::operator=(
    ProxyLockableUniqueLock&& other) noexcept {
  proxy_ = std::move(other.proxy_);
  mutex_ = folly::exchange(other.mutex_, nullptr);
  return *this;
}

template <typename Mutex>
ProxyLockableUniqueLock<Mutex>::ProxyLockableUniqueLock(
    mutex_type& mtx,
    std::defer_lock_t) noexcept {
  mutex_ = std::addressof(mtx);
}

template <typename Mutex>
ProxyLockableUniqueLock<Mutex>::ProxyLockableUniqueLock(
    mutex_type& mtx,
    std::try_to_lock_t) {
  mutex_ = std::addressof(mtx);
  if (auto state = mtx.try_lock()) {
    proxy_.emplace(std::move(state));
  }
}

template <typename Mutex>
template <typename Rep, typename Period>
ProxyLockableUniqueLock<Mutex>::ProxyLockableUniqueLock(
    mutex_type& mtx,
    const std::chrono::duration<Rep, Period>& duration) {
  mutex_ = std::addressof(mtx);
  if (auto state = mtx.try_lock_for(duration)) {
    proxy_.emplace(std::move(state));
  }
}

template <typename Mutex>
template <typename Clock, typename Duration>
ProxyLockableUniqueLock<Mutex>::ProxyLockableUniqueLock(
    mutex_type& mtx,
    const std::chrono::time_point<Clock, Duration>& time) {
  mutex_ = std::addressof(mtx);
  if (auto state = mtx.try_lock_until(time)) {
    proxy_.emplace(std::move(state));
  }
}

template <typename Mutex>
void ProxyLockableUniqueLock<Mutex>::lock() {
  proxylockable_detail::throwIfAlreadyLocked(proxy_);
  proxylockable_detail::throwIfNoMutex(mutex_);

  proxy_.emplace(mutex_->lock());
}

template <typename Mutex>
void ProxyLockableUniqueLock<Mutex>::unlock() {
  proxylockable_detail::throwIfNoMutex(mutex_);
  proxylockable_detail::throwIfNotLocked(proxy_);

  mutex_->unlock(std::move(*proxy_));
  proxy_.reset();
}

template <typename Mutex>
bool ProxyLockableUniqueLock<Mutex>::try_lock() {
  proxylockable_detail::throwIfNoMutex(mutex_);
  proxylockable_detail::throwIfAlreadyLocked(proxy_);

  if (auto state = mutex_->try_lock()) {
    proxy_.emplace(std::move(state));
    return true;
  }

  return false;
}

template <typename Mutex>
template <typename Rep, typename Period>
bool ProxyLockableUniqueLock<Mutex>::try_lock_for(
    const std::chrono::duration<Rep, Period>& duration) {
  proxylockable_detail::throwIfNoMutex(mutex_);
  proxylockable_detail::throwIfAlreadyLocked(proxy_);

  if (auto state = mutex_->try_lock_for(duration)) {
    proxy_.emplace(std::move(state));
    return true;
  }

  return false;
}

template <typename Mutex>
template <typename Clock, typename Duration>
bool ProxyLockableUniqueLock<Mutex>::try_lock_until(
    const std::chrono::time_point<Clock, Duration>& time) {
  proxylockable_detail::throwIfNoMutex(mutex_);
  proxylockable_detail::throwIfAlreadyLocked(proxy_);

  if (auto state = mutex_->try_lock_until(time)) {
    proxy_.emplace(std::move(state));
    return true;
  }

  return false;
}

template <typename Mutex>
void ProxyLockableUniqueLock<Mutex>::swap(
    ProxyLockableUniqueLock& other) noexcept {
  std::swap(mutex_, other.mutex_);
  std::swap(proxy_, other.proxy_);
}

template <typename Mutex>
typename ProxyLockableUniqueLock<Mutex>::mutex_type*
ProxyLockableUniqueLock<Mutex>::mutex() const noexcept {
  return mutex_;
}

template <typename Mutex>
typename ProxyLockableUniqueLock<Mutex>::proxy_type*
ProxyLockableUniqueLock<Mutex>::proxy() const noexcept {
  return proxy_ ? std::addressof(proxy_.value()) : nullptr;
}

template <typename Mutex>
bool ProxyLockableUniqueLock<Mutex>::owns_lock() const noexcept {
  return proxy_.has_value();
}

template <typename Mutex>
ProxyLockableUniqueLock<Mutex>::operator bool() const noexcept {
  return owns_lock();
}

template <typename Mutex>
ProxyLockableLockGuard<Mutex>::ProxyLockableLockGuard(mutex_type& mtx)
    : ProxyLockableUniqueLock<Mutex>{mtx} {}

} // namespace detail
} // namespace folly
