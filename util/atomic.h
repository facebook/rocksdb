//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Background:
// std::atomic is somewhat easy to misuse:
// * Implicit conversion to T using std::memory_order_seq_cst, along with
// memory order parameter defaults, make it easy to accidentally mix sequential
// consistency ordering with acquire/release memory ordering. See
// "The single total order might not be consistent with happens-before" at
// https://en.cppreference.com/w/cpp/atomic/memory_order
// * It's easy to use nonsensical (UB) combinations like store with
// std::memory_order_acquire.
// For such reasons, we provide wrappers below to make safe usage easier.

// Wrapper around std::atomic to avoid certain bugs (see Background above).
//
// This relaxed-only wrapper is intended for atomics that do not need
// ordering constraints with other data reads/writes aside from those
// necessary for computing data values or given by other happens-before
// relationships. For example, a cross-thread counter that never returns
// the same result can be a RelaxedAtomic.
template <typename T>
class RelaxedAtomic {
 public:
  explicit RelaxedAtomic(T initial = {}) : v_(initial) {}
  void StoreRelaxed(T desired) { v_.store(desired, std::memory_order_relaxed); }
  T LoadRelaxed() const { return v_.load(std::memory_order_relaxed); }
  bool CasWeakRelaxed(T& expected, T desired) {
    return v_.compare_exchange_weak(expected, desired,
                                    std::memory_order_relaxed);
  }
  bool CasStrongRelaxed(T& expected, T desired) {
    return v_.compare_exchange_strong(expected, desired,
                                      std::memory_order_relaxed);
  }
  T ExchangeRelaxed(T desired) {
    return v_.exchange(desired, std::memory_order_relaxed);
  }
  T FetchAddRelaxed(T operand) {
    return v_.fetch_add(operand, std::memory_order_relaxed);
  }
  T FetchSubRelaxed(T operand) {
    return v_.fetch_sub(operand, std::memory_order_relaxed);
  }
  T FetchAndRelaxed(T operand) {
    return v_.fetch_and(operand, std::memory_order_relaxed);
  }
  T FetchOrRelaxed(T operand) {
    return v_.fetch_or(operand, std::memory_order_relaxed);
  }
  T FetchXorRelaxed(T operand) {
    return v_.fetch_xor(operand, std::memory_order_relaxed);
  }

 protected:
  std::atomic<T> v_;
};

// Wrapper around std::atomic to avoid certain bugs (see Background above).
//
// Except for some unusual cases requiring sequential consistency, this is
// a general-purpose atomic. Relaxed operations can be mixed in as appropriate.
template <typename T>
class AcqRelAtomic : public RelaxedAtomic<T> {
 public:
  explicit AcqRelAtomic(T initial = {}) : RelaxedAtomic<T>(initial) {}
  void Store(T desired) {
    RelaxedAtomic<T>::v_.store(desired, std::memory_order_release);
  }
  T Load() const {
    return RelaxedAtomic<T>::v_.load(std::memory_order_acquire);
  }
  bool CasWeak(T& expected, T desired) {
    return RelaxedAtomic<T>::v_.compare_exchange_weak(
        expected, desired, std::memory_order_acq_rel);
  }
  bool CasStrong(T& expected, T desired) {
    return RelaxedAtomic<T>::v_.compare_exchange_strong(
        expected, desired, std::memory_order_acq_rel);
  }
  T Exchange(T desired) {
    return RelaxedAtomic<T>::v_.exchange(desired, std::memory_order_acq_rel);
  }
  T FetchAdd(T operand) {
    return RelaxedAtomic<T>::v_.fetch_add(operand, std::memory_order_acq_rel);
  }
  T FetchSub(T operand) {
    return RelaxedAtomic<T>::v_.fetch_sub(operand, std::memory_order_acq_rel);
  }
  T FetchAnd(T operand) {
    return RelaxedAtomic<T>::v_.fetch_and(operand, std::memory_order_acq_rel);
  }
  T FetchOr(T operand) {
    return RelaxedAtomic<T>::v_.fetch_or(operand, std::memory_order_acq_rel);
  }
  T FetchXor(T operand) {
    return RelaxedAtomic<T>::v_.fetch_xor(operand, std::memory_order_acq_rel);
  }
};

}  // namespace ROCKSDB_NAMESPACE
