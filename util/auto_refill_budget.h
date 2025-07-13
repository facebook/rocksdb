//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <memory>

#include "rocksdb/options.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

// AutoRefillBudget is a template class that maintains a budget of type T
// that automatically refills at regular intervals using a window
// approach.
template <typename T>
class AutoRefillBudget {
 public:
  // Constructor
  // refill_amount: Amount to refill each period
  // refill_period_us: Refill period in microseconds
  // clock: System clock for timing (optional, uses default if nullptr)
  AutoRefillBudget(T refill_amount, int64_t refill_period_us,
                   const std::shared_ptr<SystemClock>& clock = nullptr)
      : refill_amount_(refill_amount),
        refill_period_us_(refill_period_us),
        available_budget_(refill_amount),
        clock_(clock ? clock : SystemClock::Default()) {
    next_refill_us_.store(NowMicrosMonotonic(), std::memory_order_relaxed);
  }

  virtual ~AutoRefillBudget() = default;

  bool TryConsume(T amount) {
    RefillBudgetIfNeeded();

    T current_budget = available_budget_.load(std::memory_order_acquire);
    while (current_budget >= amount) {
      if (available_budget_.compare_exchange_weak(
              current_budget, current_budget - amount,
              std::memory_order_acq_rel, std::memory_order_acquire)) {
        return true;
      }
    }
    return false;
  }
  double GetRate() { return refill_amount_ / (refill_period_us_ / 1000000.0); }
  T GetAvailableBudget() {
    RefillBudgetIfNeeded();
    return available_budget_.load(std::memory_order_acquire);
  }
  T GetTotalBudget() { return refill_amount_.load(std::memory_order_relaxed); }
  T GetRefillAmount() const {
    return refill_amount_.load(std::memory_order_relaxed);
  }
  int64_t GetRefillPeriodUs() const {
    return refill_period_us_.load(std::memory_order_relaxed);
  }
  void SetRefillParameters(T refill_amount, int64_t refill_period_us) {
    refill_amount_.store(refill_amount, std::memory_order_relaxed);
    refill_period_us_.store(refill_period_us, std::memory_order_relaxed);
  }
  void Reset() {
    available_budget_.store(refill_amount_, std::memory_order_release);
    next_refill_us_.store(NowMicrosMonotonic(), std::memory_order_release);
  }
  // For testing purposes
  void TEST_SetClock(std::shared_ptr<SystemClock> clock) {
    clock_ = std::move(clock);
    next_refill_us_.store(NowMicrosMonotonic(), std::memory_order_release);
  }

 private:
  // Refill budget based on elapsed time using atomic operations
  void RefillBudgetIfNeeded() {
    int64_t now_us = NowMicrosMonotonic();
    int64_t next_refill = next_refill_us_.load(std::memory_order_acquire);

    if (now_us >= next_refill) {
      // Try to update next_refill_us_ atomically to claim the refill
      // operation
      int64_t refill_period = refill_period_us_.load(std::memory_order_relaxed);
      int64_t new_next_refill = now_us + refill_period;

      if (next_refill_us_.compare_exchange_strong(next_refill, new_next_refill,
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
        auto refill_amount = refill_amount_.load(std::memory_order_relaxed);
        available_budget_.store(refill_amount, std::memory_order_release);
      }
    }
  }

  uint64_t NowMicrosMonotonic() { return clock_->NowNanos() / std::milli::den; }

  std::atomic<T> refill_amount_;
  std::atomic<int64_t> refill_period_us_;
  std::atomic<T> available_budget_;
  std::atomic<int64_t> next_refill_us_;

  // System clock for timing
  std::shared_ptr<SystemClock> clock_;
};

// Common type aliases for convenience
using IOBudget = AutoRefillBudget<size_t>;
using CPUBudget = AutoRefillBudget<size_t>;

class CPUIOBudgetFactory {
 public:
  // Create a new IOBudget instance
  virtual std::pair<std::shared_ptr<IOBudget>, std::shared_ptr<CPUBudget>>
  GetBudget() = 0;
  virtual ~CPUIOBudgetFactory() = default;
  virtual Options GetOptions() = 0;
};
}  // namespace ROCKSDB_NAMESPACE
