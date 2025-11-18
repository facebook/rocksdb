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
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <vector>

#include "port/port.h"
#include "rocksdb/io_dispatcher.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/cachable_entry.h"
#include "table/format.h"

namespace ROCKSDB_NAMESPACE {

struct IODispatcherImpl::Impl {
  struct JobItem {
    std::shared_ptr<IOJob> job;
    std::shared_ptr<JobHandle> handle;
  };

  Impl(int num_threads);
  ~Impl();

  void JoinThreads();
  void WorkerThread(size_t thread_id);
  void ProcessJob(JobItem& job);

  JobHandle SubmitJob(std::shared_ptr<IOJob> job);
  unsigned int GetQueueLen() const {
    return queue_len_.load(std::memory_order_relaxed);
  }

 private:
  using JobQueue = std::deque<JobItem>;

  int num_threads_;
  std::atomic_uint queue_len_;
  bool exit_all_threads_;

  JobQueue queue_;
  std::mutex mu_;
  std::condition_variable cv_;
  std::vector<port::Thread> worker_threads_;
};

IODispatcherImpl::Impl::Impl(int num_threads)
    : num_threads_(num_threads),
      queue_len_(0),
      exit_all_threads_(false),
      queue_(),
      mu_(),
      cv_(),
      worker_threads_() {
  for (int i = 0; i < num_threads_; ++i) {
    worker_threads_.emplace_back(&IODispatcherImpl::Impl::WorkerThread, this,
                                 i);
  }
}

IODispatcherImpl::Impl::~Impl() { JoinThreads(); }

void IODispatcherImpl::Impl::JoinThreads() {
  {
    std::unique_lock<std::mutex> lock(mu_);
    exit_all_threads_ = true;
  }
  cv_.notify_all();

  for (auto& thread : worker_threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  worker_threads_.clear();
}

void IODispatcherImpl::Impl::WorkerThread(size_t thread_id) {
  (void)thread_id;
  while (true) {
    JobItem job;
    {
      std::unique_lock<std::mutex> lock(mu_);
      cv_.wait(lock, [this] { return exit_all_threads_ || !queue_.empty(); });

      if (exit_all_threads_ && queue_.empty()) {
        break;
      }

      if (!queue_.empty()) {
        job = std::move(queue_.front());
        queue_.pop_front();
        queue_len_.store(static_cast<unsigned int>(queue_.size()),
                         std::memory_order_relaxed);
      }
    }

    ProcessJob(job);
  }
}

void IODispatcherImpl::Impl::ProcessJob(JobItem& item) {
  std::shared_ptr<IOJob> job = item.job;
  std::shared_ptr<JobHandle> handle = item.handle;
  // Step 1: Check cache and pin cached blocks
  std::vector<size_t> block_indices_to_read;
  handle->pinned_blocks.reserve(job->block_handles.size());
  for (size_t i = 0; i < job->block_handles.size(); ++i) {
    const auto& data_block_handle = job->block_handles[i];

    // Lookup and pin block in cache
    // Using Block_kData type for data blocks
    Status s = job->table->LookupAndPinBlocksInCache<Block_kData>(
        *job->read_options, data_block_handle,
        &(handle->pinned_blocks)[i].As<Block_kData>());

    if (!s.ok()) {
      continue;
    }

    if (!(handle->pinned_blocks)[i].GetValue()) {
      // Block not in cache - needs to be read from disk
      block_indices_to_read.emplace_back(i);
    }
  }

  // Step 2: Handle blocks not in cache
  // In the original implementation (block_based_table_iterator.cc),
  // blocks not in cache would be read via PrepareIORequests and ExecuteIO.
  // For now, this is left as future work since it requires:
  // - File handle from the table
  // - IO options
  // - Potential async IO handling
  // - Block creation and insertion into cache

  // The current implementation handles the cache lookup and pinning phase.
  // Blocks found in cache are now pinned and available in job->pinned_blocks.
  // Blocks not in cache (in block_indices_to_read) would need additional IO.
}

JobHandle IODispatcherImpl::Impl::SubmitJob(std::shared_ptr<IOJob> job) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (exit_all_threads_) {
      return JobHandle{};
    }

    queue_.push_back(JobItem{job});
    queue_len_.store(static_cast<unsigned int>(queue_.size()),
                     std::memory_order_relaxed);
  }
  cv_.notify_one();

  return JobHandle{};
}

IODispatcherImpl::IODispatcherImpl(int num_threads)
    : impl_(new Impl(num_threads)) {}

IODispatcherImpl::~IODispatcherImpl() = default;

JobHandle IODispatcherImpl::SubmitJob(std::shared_ptr<IOJob> job) {
  return impl_->SubmitJob(job);
}

unsigned int IODispatcherImpl::GetQueueLen() const {
  return impl_->GetQueueLen();
}

IODispatcher* NewIODispatcher(int num_threads) {
  return new IODispatcherImpl(num_threads);
}

}  // namespace ROCKSDB_NAMESPACE
