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
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/listener.h"
#include "rocksdb/rate_limiter.h"
#include "util/aligned_buffer.h"

namespace ROCKSDB_NAMESPACE {
class Statistics;
class HistogramImpl;

// RandomAccessFileReader is a wrapper on top of Env::RnadomAccessFile. It is
// responsible for:
// - Handling Buffered and Direct reads appropriately.
// - Rate limiting compaction reads.
// - Notifying any interested listeners on the completion of a read.
// - Updating IO stats.
class RandomAccessFileReader {
 private:
#ifndef ROCKSDB_LITE
  void NotifyOnFileReadFinish(uint64_t offset, size_t length,
                              const FileOperationInfo::TimePoint& start_ts,
                              const FileOperationInfo::TimePoint& finish_ts,
                              const Status& status) const {
    FileOperationInfo info(file_name_, start_ts, finish_ts);
    info.offset = offset;
    info.length = length;
    info.status = status;

    for (auto& listener : listeners_) {
      listener->OnFileReadFinish(info);
    }
  }
#endif  // ROCKSDB_LITE

  bool ShouldNotifyListeners() const { return !listeners_.empty(); }

  std::unique_ptr<FSRandomAccessFile> file_;
  std::string file_name_;
  Env* env_;
  Statistics* stats_;
  uint32_t hist_type_;
  HistogramImpl* file_read_hist_;
  RateLimiter* rate_limiter_;
  std::vector<std::shared_ptr<EventListener>> listeners_;

 public:
  explicit RandomAccessFileReader(
      std::unique_ptr<FSRandomAccessFile>&& raf, std::string _file_name,
      Env* env = nullptr, Statistics* stats = nullptr, uint32_t hist_type = 0,
      HistogramImpl* file_read_hist = nullptr,
      RateLimiter* rate_limiter = nullptr,
      const std::vector<std::shared_ptr<EventListener>>& listeners = {})
      : file_(std::move(raf)),
        file_name_(std::move(_file_name)),
        env_(env),
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

  RandomAccessFileReader(RandomAccessFileReader&& o) ROCKSDB_NOEXCEPT {
    *this = std::move(o);
  }

  RandomAccessFileReader& operator=(RandomAccessFileReader&& o)
      ROCKSDB_NOEXCEPT {
    file_ = std::move(o.file_);
    env_ = std::move(o.env_);
    stats_ = std::move(o.stats_);
    hist_type_ = std::move(o.hist_type_);
    file_read_hist_ = std::move(o.file_read_hist_);
    rate_limiter_ = std::move(o.rate_limiter_);
    return *this;
  }

  RandomAccessFileReader(const RandomAccessFileReader&) = delete;
  RandomAccessFileReader& operator=(const RandomAccessFileReader&) = delete;

  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch,
              bool for_compaction = false) const;

  Status MultiRead(FSReadRequest* reqs, size_t num_reqs) const;

  Status Prefetch(uint64_t offset, size_t n) const {
    return file_->Prefetch(offset, n, IOOptions(), nullptr);
  }

  FSRandomAccessFile* file() { return file_.get(); }

  std::string file_name() const { return file_name_; }

  bool use_direct_io() const { return file_->use_direct_io(); }
};
}  // namespace ROCKSDB_NAMESPACE
