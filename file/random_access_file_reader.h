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

// RandomAccessFileReader is a wrapper on top of Env::RnadomAccessFile. It is
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
                           finish_ts, status);
    info.offset = offset;
    info.length = length;

    for (auto& listener : listeners_) {
      listener->OnFileReadFinish(info);
    }
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

 public:
  explicit RandomAccessFileReader(
      std::unique_ptr<FSRandomAccessFile>&& raf, const std::string& _file_name,
      SystemClock* clock = nullptr,
      const std::shared_ptr<IOTracer>& io_tracer = nullptr,
      Statistics* stats = nullptr, uint32_t hist_type = 0,
      HistogramImpl* file_read_hist = nullptr,
      RateLimiter* rate_limiter = nullptr,
      const std::vector<std::shared_ptr<EventListener>>& listeners = {})
      : file_(std::move(raf), io_tracer, _file_name),
        file_name_(std::move(_file_name)),
        clock_(clock),
        stats_(stats),
        hist_type_(hist_type),
        file_read_hist_(file_read_hist),
        rate_limiter_(rate_limiter),
        listeners_() {
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

  static Status Create(const std::shared_ptr<FileSystem>& fs,
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
  Status Read(const IOOptions& opts, uint64_t offset, size_t n, Slice* result,
              char* scratch, AlignedBuf* aligned_buf,
              bool for_compaction = false) const;

  // REQUIRES:
  // num_reqs > 0, reqs do not overlap, and offsets in reqs are increasing.
  // In non-direct IO mode, aligned_buf should be null;
  // In direct IO mode, aligned_buf stores the aligned buffer allocated inside
  // MultiRead, the result Slices in reqs refer to aligned_buf.
  Status MultiRead(const IOOptions& opts, FSReadRequest* reqs, size_t num_reqs,
                   AlignedBuf* aligned_buf) const;

  Status Prefetch(uint64_t offset, size_t n) const {
    return file_->Prefetch(offset, n, IOOptions(), nullptr);
  }

  FSRandomAccessFile* file() { return file_.get(); }

  const std::string& file_name() const { return file_name_; }

  bool use_direct_io() const { return file_->use_direct_io(); }

  IOStatus PrepareIOOptions(const ReadOptions& ro, IOOptions& opts);
};
}  // namespace ROCKSDB_NAMESPACE
