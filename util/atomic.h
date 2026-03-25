//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <cstdint>
#include <type_traits>

#include "monitoring/stress_trace.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Background:
// std::atomic is somewhat easy to misuse:
// * Implicit conversion to T makes it easy to use an unnecessarily strong
// memory ordering (std::memory_order_seq_cst) and to hide atomic operations
// that should be evident on reading the code.
// * Similarly, defaulting to std::memory_order_seq_cst for atomic operations
// makes it easy to use unnecessarily strong orderings. (It's always safe if
// some ordering is safe, but it's better to be intentional and thoughtful when
// carefully optimizing code with atomics.) Legitimate needs for seq_cst vs.
// acq_rel are rare, such as drawing inferences across two atomics in
// implementing hazard pointers.
// * It's easy to use nonsensical (UB) combinations like store with
// std::memory_order_acquire. Getting these right in development is an
// unnecessary cognitive overhead even if they are caught by UBSAN.
//
// For such reasons, we provide wrappers below to make clear and explicit
// usage of atomics easier.

// Helper to cast atomic values to uint64_t for trace logging.
// Handles integers, enums, bools, and pointers uniformly.
namespace atomic_trace_detail {
template <typename T>
ROCKSDB_NO_INSTRUMENT inline uint64_t ToTraceVal(T v) {
  if constexpr (std::is_pointer_v<T>) {
    return reinterpret_cast<uint64_t>(v);
  } else {
    return static_cast<uint64_t>(v);
  }
}
}  // namespace atomic_trace_detail

// Wrapper around std::atomic for better code clarity (see Background above).
//
// This relaxed-only wrapper is intended for atomics that are not used to
// synchronize other data across threads (only the atomic data), so can always
// used relaxed memory ordering. For example, a cross-thread counter that never
// returns the same result can be a RelaxedAtomic.
template <typename T>
class RelaxedAtomic {
 public:
  explicit RelaxedAtomic(T initial = {}) : v_(initial) {}
  void StoreRelaxed(T desired) {
    STRESS_TRACE_BINARY(stress_trace::TraceEventType::kAtomicStore, this,
                        atomic_trace_detail::ToTraceVal(desired), 0);
    v_.store(desired, std::memory_order_relaxed);
  }
  T LoadRelaxed() const { return v_.load(std::memory_order_relaxed); }
  bool CasWeakRelaxed(T& expected, T desired) {
    bool ok = v_.compare_exchange_weak(expected, desired,
                                       std::memory_order_relaxed);
    STRESS_TRACE_BINARY(stress_trace::TraceEventType::kAtomicCas, this,
                        atomic_trace_detail::ToTraceVal(ok ? desired : expected),
                        ok);
    return ok;
  }
  bool CasStrongRelaxed(T& expected, T desired) {
    bool ok = v_.compare_exchange_strong(expected, desired,
                                         std::memory_order_relaxed);
    STRESS_TRACE_BINARY(stress_trace::TraceEventType::kAtomicCas, this,
                        atomic_trace_detail::ToTraceVal(ok ? desired : expected),
                        ok);
    return ok;
  }
  T ExchangeRelaxed(T desired) {
    T old = v_.exchange(desired, std::memory_order_relaxed);
    STRESS_TRACE_BINARY(stress_trace::TraceEventType::kAtomicXchg, this,
                        atomic_trace_detail::ToTraceVal(old),
                        atomic_trace_detail::ToTraceVal(desired));
    return old;
  }
  T FetchAddRelaxed(T operand) {
    T old = v_.fetch_add(operand, std::memory_order_relaxed);
    STRESS_TRACE_BINARY(stress_trace::TraceEventType::kAtomicAdd, this,
                        atomic_trace_detail::ToTraceVal(old),
                        atomic_trace_detail::ToTraceVal(operand));
    return old;
  }
  T FetchSubRelaxed(T operand) {
    T old = v_.fetch_sub(operand, std::memory_order_relaxed);
    STRESS_TRACE_BINARY(stress_trace::TraceEventType::kAtomicSub, this,
                        atomic_trace_detail::ToTraceVal(old),
                        atomic_trace_detail::ToTraceVal(operand));
    return old;
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

// A reasonably general-purpose wrapper around std::atomic for better code
// clarity (see Background above).
//
// Operations use std::memory_order_acq_rel by default (or just acquire or just
// release for read-only and write-only operations), but relaxed operations are
// also available and can be mixed in when appropriate.
//
// Future: add std::memory_order_seqcst variants like StoreSeqCst if/when
// there's a need for them (rare). No distinct type is needed because the
// distinction between acq_rel and seq_cst is more about where it is used in
// combination with other atomics than the atomic itself.
template <typename T>
class Atomic : public RelaxedAtomic<T> {
 public:
  explicit Atomic(T initial = {}) : RelaxedAtomic<T>(initial) {}
  void Store(T desired) {
    STRESS_TRACE_BINARY(stress_trace::TraceEventType::kAtomicStore, this,
                        atomic_trace_detail::ToTraceVal(desired), 0);
    RelaxedAtomic<T>::v_.store(desired, std::memory_order_release);
  }
  T Load() const {
    T val = RelaxedAtomic<T>::v_.load(std::memory_order_acquire);
    STRESS_TRACE_BINARY(stress_trace::TraceEventType::kAtomicLoad, this,
                        atomic_trace_detail::ToTraceVal(val), 0);
    return val;
  }
  bool CasWeak(T& expected, T desired) {
    bool ok = RelaxedAtomic<T>::v_.compare_exchange_weak(
        expected, desired, std::memory_order_acq_rel);
    STRESS_TRACE_BINARY(stress_trace::TraceEventType::kAtomicCas, this,
                        atomic_trace_detail::ToTraceVal(ok ? desired : expected),
                        ok);
    return ok;
  }
  bool CasStrong(T& expected, T desired) {
    bool ok = RelaxedAtomic<T>::v_.compare_exchange_strong(
        expected, desired, std::memory_order_acq_rel);
    STRESS_TRACE_BINARY(stress_trace::TraceEventType::kAtomicCas, this,
                        atomic_trace_detail::ToTraceVal(ok ? desired : expected),
                        ok);
    return ok;
  }
  T Exchange(T desired) {
    T old = RelaxedAtomic<T>::v_.exchange(desired, std::memory_order_acq_rel);
    STRESS_TRACE_BINARY(stress_trace::TraceEventType::kAtomicXchg, this,
                        atomic_trace_detail::ToTraceVal(old),
                        atomic_trace_detail::ToTraceVal(desired));
    return old;
  }
  T FetchAdd(T operand) {
    T old = RelaxedAtomic<T>::v_.fetch_add(operand, std::memory_order_acq_rel);
    STRESS_TRACE_BINARY(stress_trace::TraceEventType::kAtomicAdd, this,
                        atomic_trace_detail::ToTraceVal(old),
                        atomic_trace_detail::ToTraceVal(operand));
    return old;
  }
  T FetchSub(T operand) {
    T old = RelaxedAtomic<T>::v_.fetch_sub(operand, std::memory_order_acq_rel);
    STRESS_TRACE_BINARY(stress_trace::TraceEventType::kAtomicSub, this,
                        atomic_trace_detail::ToTraceVal(old),
                        atomic_trace_detail::ToTraceVal(operand));
    return old;
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
