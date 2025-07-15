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
template <typename T>
class Budget {
 public:
  Budget(T amount, int64_t period_us)
      : amount_(amount), period_us_(period_us){};
  double GetRate() { return amount_ / (period_us_ / 1000000.0); }

 private:
  T amount_;
  int64_t period_us_;
};
using IOBudget = Budget<size_t>;
using CPUBudget = Budget<size_t>;

class CPUIOBudgetFactory {
 public:
  // Create a new IOBudget instance
  virtual std::pair<std::shared_ptr<IOBudget>, std::shared_ptr<CPUBudget>>
  GetBudget() = 0;
  virtual ~CPUIOBudgetFactory() = default;
  virtual Options GetOptions() = 0;
};
}  // namespace ROCKSDB_NAMESPACE
