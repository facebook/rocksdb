//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#if USE_COROUTINES
#include "util/async_file_reader.h"

// RocksDB-Cloud contribution: restructured to use MultiReadAsync

namespace ROCKSDB_NAMESPACE {
bool AsyncFileReader::MultiReadAsyncImpl(ReadAwaiter* awaiter) {
  if (tail_) {
    tail_->next_ = awaiter;
  }
  tail_ = awaiter;
  if (!head_) {
    head_ = awaiter;
  }
  awaiter->next_ = nullptr;

  num_reqs_ += awaiter->num_reqs_;
  awaiter->io_handle_.resize(awaiter->num_reqs_);
  awaiter->del_fn_.resize(awaiter->num_reqs_);
  size_t num_io_handles = awaiter->num_reqs_;
  IOStatus s = awaiter->file_->MultiReadAsync(
      awaiter->read_reqs_, awaiter->num_reqs_, awaiter->opts_,
      [](const FSReadRequest* reqs, size_t n_reqs, void* cb_arg) {
        FSReadRequest* read_reqs = static_cast<FSReadRequest*>(cb_arg);
        if (read_reqs != reqs) {
          for (size_t idx = 0; idx < n_reqs; idx++) {
            read_reqs[idx].status = reqs[idx].status;
            read_reqs[idx].result = reqs[idx].result;
          }
        }
      },
      awaiter->read_reqs_, (void**)&awaiter->io_handle_[0], &num_io_handles,
      &awaiter->del_fn_[0],
      /*aligned_buf=*/nullptr);
  if (!s.ok()) {
    assert(num_io_handles == 0);
    // For any non-ok status, the FileSystem will not call the callback
    // So let's update the status ourselves assuming the whole batch failed.
    for (size_t i = 0; i < awaiter->num_reqs_; ++i) {
      awaiter->read_reqs_[i].status = s;
    }
  }
  assert(num_io_handles <= awaiter->num_reqs_);
  awaiter->io_handle_.resize(num_io_handles);
  awaiter->del_fn_.resize(num_io_handles);
  return true;
}

void AsyncFileReader::Wait() {
  if (!head_) {
    return;
  }

  // TODO: No need to copy if we have 1 awaiter.
  // Poll API seems to encourage inefficiency.
  std::vector<void*> io_handles;
  io_handles.reserve(num_reqs_);

  ReadAwaiter* waiter = head_;
  do {
    for (size_t i = 0; i < waiter->io_handle_.size(); ++i) {
      if (waiter->io_handle_[i]) {
        io_handles.push_back(waiter->io_handle_[i]);
      }
    }
  } while (waiter != tail_ && (waiter = waiter->next_));

  IOStatus s = IOStatus::OK();
  if (io_handles.size() > 0) {
    StopWatch sw(SystemClock::Default().get(), stats_, POLL_WAIT_MICROS);
    s = fs_->Poll(io_handles, io_handles.size());
  }

  do {
    waiter = head_;
    head_ = waiter->next_;

    for (size_t i = 0; i < waiter->io_handle_.size(); ++i) {
      if (waiter->io_handle_[i] && waiter->del_fn_[i]) {
        waiter->del_fn_[i](waiter->io_handle_[i]);
      }
    }
    if (!s.ok()) {
      for (size_t i = 0; i < waiter->num_reqs_; ++i) {
        if (waiter->read_reqs_[i].status.ok()) {
          // Override the request status with the Poll error
          waiter->read_reqs_[i].status = s;
        }
      }
    }
    waiter->awaiting_coro_.resume();
  } while (waiter != tail_);
  head_ = tail_ = nullptr;
  RecordInHistogram(stats_, MULTIGET_IO_BATCH_SIZE, num_reqs_);
  num_reqs_ = 0;
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // USE_COROUTINES
