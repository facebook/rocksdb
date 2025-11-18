//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/io_dispatcher_imp.h"

#include <atomic>
#include <functional>
#include <memory>

#include "rocksdb/threadpool.h"

namespace ROCKSDB_NAMESPACE {

struct IODispatcherImpl::Impl {
  explicit Impl(int num_threads)
      : thread_pool_(NewThreadPool(num_threads)), queue_len_(0) {}

  ~Impl() {
    thread_pool_->WaitForJobsAndJoinAllThreads();
    delete thread_pool_;
  }

  void SubmitJob(std::function<void()>&& job) {
    queue_len_.fetch_add(1, std::memory_order_relaxed);
    thread_pool_->SubmitJob([this, job = std::move(job)]() {
      job();
      queue_len_.fetch_sub(1, std::memory_order_relaxed);
    });
  }

  unsigned int GetQueueLen() const {
    return queue_len_.load(std::memory_order_relaxed) +
           thread_pool_->GetQueueLen();
  }

  ThreadPool* thread_pool_;
  std::atomic<unsigned int> queue_len_;
};

IODispatcherImpl::IODispatcherImpl(int num_threads)
    : impl_(new Impl(num_threads)) {}

IODispatcherImpl::~IODispatcherImpl() = default;

void IODispatcherImpl::SubmitJob(const std::function<void()>& job) {
  auto copy(job);
  impl_->SubmitJob(std::move(copy));
}

void IODispatcherImpl::SubmitJob(std::function<void()>&& job) {
  impl_->SubmitJob(std::move(job));
}

unsigned int IODispatcherImpl::GetQueueLen() const {
  return impl_->GetQueueLen();
}

IODispatcher* NewIODispatcher(int num_threads) {
  return new IODispatcherImpl(num_threads);
}

}  // namespace ROCKSDB_NAMESPACE
