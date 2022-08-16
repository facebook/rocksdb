//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <atomic>
#include <sstream>
#include <string>

#include "env/file_system_tracer.h"
#include "port/port.h"
#include "rocksdb/file_system.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"
#include "util/aligned_buffer.h"

namespace ROCKSDB_NAMESPACE {
class Statistics;
class HistogramImpl;
class SystemClock;

using AlignedBuf = std::unique_ptr<char[]>;

// Align the request r according to alignment and return the aligned result.
FSReadRequest Align(const FSReadRequest& r, size_t alignment);

// Try to merge src to dest if they have overlap.
//
// Each request represents an inclusive interval [offset, offset + len].
// If the intervals have overlap, update offset and len to represent the
// merged interval, and return true.
// Otherwise, do nothing and return false.
bool TryMerge(FSReadRequest* dest, const FSReadRequest& src);

// RandomAccessFileReader is a wrapper on top of FSRandomAccessFile. It is
// responsible for:
// - Handling Buffered and Direct reads appropriately.
// - Rate limiting compaction reads.
// - Notifying any interested listeners on the completion of a read.
// - Updating IO stats.
class RandomAccessFileReader {
 private:
#ifndef ROCKSDB_LITE
  void NotifyOnFileReadFinish(
      uint64_t offset, size_t length,
      const FileOperationInfo::StartTimePoint& start_ts,
      const FileOperationInfo::FinishTimePoint& finish_ts,
      const Status& status) const {
    FileOperationInfo info(FileOperationType::kRead, file_name_, start_ts,
                           finish_ts, status, file_temperature_);
    info.offset = offset;
    info.length = length;

    for (auto& listener : listeners_) {
      listener->OnFileReadFinish(info);
    }
    info.status.PermitUncheckedError();
  }

  void NotifyOnIOError(const IOStatus& io_status, FileOperationType operation,
                       const std::string& file_path, size_t length,
                       uint64_t offset) const {
    if (listeners_.empty()) {
      return;
    }
    IOErrorInfo io_error_info(io_status, operation, file_path, length, offset);

    for (auto& listener : listeners_) {
      listener->OnIOError(io_error_info);
    }
    io_status.PermitUncheckedError();
  }

#endif  // ROCKSDB_LITE

  bool ShouldNotifyListeners() const { return !listeners_.empty(); }

  FSRandomAccessFilePtr file_;
  std::string file_name_;
  SystemClock* clock_;
  Statistics* stats_;
  uint32_t hist_type_;
  HistogramImpl* file_read_hist_;
  RateLimiter* rate_limiter_;
  std::vector<std::shared_ptr<EventListener>> listeners_;
  const Temperature file_temperature_;
  const bool is_last_level_;

  struct ReadAsyncInfo {
    ReadAsyncInfo(std::function<void(const FSReadRequest&, void*)> cb,
                  void* cb_arg, uint64_t start_time)
        : cb_(cb),
          cb_arg_(cb_arg),
          start_time_(start_time),
          user_scratch_(nullptr),
          user_aligned_buf_(nullptr),
          user_offset_(0),
          user_len_(0),
          is_aligned_(false) {}

    std::function<void(const FSReadRequest&, void*)> cb_;
    void* cb_arg_;
    uint64_t start_time_;
#ifndef ROCKSDB_LITE
    FileOperationInfo::StartTimePoint fs_start_ts_;
#endif
    // Below fields stores the parameters passed by caller in case of direct_io.
    char* user_scratch_;
    AlignedBuf* user_aligned_buf_;
    uint64_t user_offset_;
    size_t user_len_;
    Slice user_result_;
    // Used in case of direct_io
    AlignedBuffer buf_;
    bool is_aligned_;
  };

 public:
  explicit RandomAccessFileReader(
      std::unique_ptr<FSRandomAccessFile>&& raf, const std::string& _file_name,
      SystemClock* clock = nullptr,
      const std::shared_ptr<IOTracer>& io_tracer = nullptr,
      Statistics* stats = nullptr, uint32_t hist_type = 0,
      HistogramImpl* file_read_hist = nullptr,
      RateLimiter* rate_limiter = nullptr,
      const std::vector<std::shared_ptr<EventListener>>& listeners = {},
      Temperature file_temperature = Temperature::kUnknown,
      bool is_last_level = false)
      : file_(std::move(raf), io_tracer, _file_name),
        file_name_(std::move(_file_name)),
        clock_(clock),
        stats_(stats),
        hist_type_(hist_type),
        file_read_hist_(file_read_hist),
        rate_limiter_(rate_limiter),
        listeners_(),
        file_temperature_(file_temperature),
        is_last_level_(is_last_level) {
#ifndef ROCKSDB_LITE
    std::for_each(listeners.begin(), listeners.end(),
                  [this](const std::shared_ptr<EventListener>& e) {
                    if (e->ShouldBeNotifiedOnFileIO()) {
                      listeners_.emplace_back(e);
                    }
                  });
#else  // !ROCKSDB_LITE
    (void)listeners;
#endif
  }

  static IOStatus Create(const std::shared_ptr<FileSystem>& fs,
                         const std::string& fname, const FileOptions& file_opts,
                         std::unique_ptr<RandomAccessFileReader>* reader,
                         IODebugContext* dbg);
  RandomAccessFileReader(const RandomAccessFileReader&) = delete;
  RandomAccessFileReader& operator=(const RandomAccessFileReader&) = delete;

  // In non-direct IO mode,
  // 1. if using mmap, result is stored in a buffer other than scratch;
  // 2. if not using mmap, result is stored in the buffer starting from scratch.
  //
  // In direct IO mode, an aligned buffer is allocated internally.
  // 1. If aligned_buf is null, then results are copied to the buffer
  // starting from scratch;
  // 2. Otherwise, scratch is not used and can be null, the aligned_buf owns
  // the internally allocated buffer on return, and the result refers to a
  // region in aligned_buf.
  //
  // `rate_limiter_priority` is used to charge the internal rate limiter when
  // enabled. The special value `Env::IO_TOTAL` makes this operation bypass the
  // rate limiter.
  IOStatus Read(const IOOptions& opts, uint64_t offset, size_t n, Slice* result,
                char* scratch, AlignedBuf* aligned_buf,
                Env::IOPriority rate_limiter_priority) const;

  // REQUIRES:
  // num_reqs > 0, reqs do not overlap, and offsets in reqs are increasing.
  // In non-direct IO mode, aligned_buf should be null;
  // In direct IO mode, aligned_buf stores the aligned buffer allocated inside
  // MultiRead, the result Slices in reqs refer to aligned_buf.
  //
  // `rate_limiter_priority` will be used to charge the internal rate limiter.
  // It is not yet supported so the client must provide the special value
  // `Env::IO_TOTAL` to bypass the rate limiter.
  IOStatus MultiRead(const IOOptions& opts, FSReadRequest* reqs,
                     size_t num_reqs, AlignedBuf* aligned_buf,
                     Env::IOPriority rate_limiter_priority) const;

  IOStatus Prefetch(uint64_t offset, size_t n,
                    const Env::IOPriority rate_limiter_priority) const {
    IOOptions opts;
    opts.rate_limiter_priority = rate_limiter_priority;
    return file_->Prefetch(offset, n, opts, nullptr);
  }

  FSRandomAccessFile* file() { return file_.get(); }

  const std::string& file_name() const { return file_name_; }

  bool use_direct_io() const { return file_->use_direct_io(); }

  IOStatus PrepareIOOptions(const ReadOptions& ro, IOOptions& opts);

  IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                     std::function<void(const FSReadRequest&, void*)> cb,
                     void* cb_arg, void** io_handle, IOHandleDeleter* del_fn,
                     AlignedBuf* aligned_buf);

  void ReadAsyncCallback(const FSReadRequest& req, void* cb_arg);
};
}  // namespace ROCKSDB_NAMESPACE
