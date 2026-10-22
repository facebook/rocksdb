//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/threadpool.h"
#include "util/semaphore.h"

namespace ROCKSDB_NAMESPACE {

// Fans a materialization batch across a fixed executor and joins it on the
// calling thread. Create() returns null unless the configured pool has workers,
// allowing callers to leave their existing inline path completely unchanged.
class ParallelAsyncIOMaterializer {
 public:
  static std::shared_ptr<ParallelAsyncIOMaterializer> Create(
      std::shared_ptr<ThreadPool> pool) {
    const int background_threads =
        pool != nullptr ? pool->GetBackgroundThreads() : 0;
    if (background_threads <= 0) {
      return nullptr;
    }
    return std::shared_ptr<ParallelAsyncIOMaterializer>(
        new ParallelAsyncIOMaterializer(
            std::move(pool), static_cast<size_t>(background_threads)));
  }

  // Invokes materialize_range(begin, end) for disjoint contiguous ranges and
  // returns only after every range has completed.
  template <typename MaterializeRange>
  void Materialize(size_t item_count,
                   MaterializeRange&& materialize_range) const {
    assert(item_count > 0);
    const size_t task_count = std::min(item_count, worker_count_);
    auto completion = std::make_shared<BatchCompletion>(task_count);
    auto materialize = std::make_shared<std::decay_t<MaterializeRange>>(
        std::forward<MaterializeRange>(materialize_range));

    for (size_t task = 0; task < task_count; ++task) {
      const size_t begin = item_count * task / task_count;
      const size_t end = item_count * (task + 1) / task_count;
      pool_->SubmitJob([completion, materialize, begin, end]() {
        (*materialize)(begin, end);
        if (completion->remaining_tasks.fetch_sub(
                1, std::memory_order_acq_rel) == 1) {
          completion->done.Release();
        }
      });
    }
    completion->done.Acquire();
  }

 private:
  struct BatchCompletion {
    explicit BatchCompletion(size_t task_count) : remaining_tasks(task_count) {}

    std::atomic<size_t> remaining_tasks;
    BinarySemaphore done{0};
  };

  ParallelAsyncIOMaterializer(std::shared_ptr<ThreadPool> pool,
                              size_t worker_count)
      : pool_(std::move(pool)), worker_count_(worker_count) {}

  std::shared_ptr<ThreadPool> pool_;
  const size_t worker_count_;
};

}  // namespace ROCKSDB_NAMESPACE
