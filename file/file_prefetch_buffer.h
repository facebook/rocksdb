//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <algorithm>
#include <atomic>
#include <sstream>
#include <string>

#include "file/readahead_file_info.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "util/aligned_buffer.h"

namespace ROCKSDB_NAMESPACE {

#define DEAFULT_DECREMENT 8 * 1024

struct IOOptions;
class RandomAccessFileReader;

// FilePrefetchBuffer is a smart buffer to store and read data from a file.
class FilePrefetchBuffer {
 public:
  static const int kMinNumFileReadsToStartAutoReadahead = 2;
  static const size_t kInitAutoReadaheadSize = 8 * 1024;

  // Constructor.
  //
  // All arguments are optional.
  // readahead_size     : the initial readahead size.
  // max_readahead_size : the maximum readahead size.
  //   If max_readahead_size > readahead_size, the readahead size will be
  //   doubled on every IO until max_readahead_size is hit.
  //   Typically this is set as a multiple of readahead_size.
  //   max_readahead_size should be greater than equal to readahead_size.
  // enable : controls whether reading from the buffer is enabled.
  //   If false, TryReadFromCache() always return false, and we only take stats
  //   for the minimum offset if track_min_offset = true.
  // track_min_offset : Track the minimum offset ever read and collect stats on
  //   it. Used for adaptable readahead of the file footer/metadata.
  // implicit_auto_readahead : Readahead is enabled implicitly by rocksdb after
  //   doing sequential scans for two times.
  //
  // Automatic readhead is enabled for a file if readahead_size
  // and max_readahead_size are passed in.
  // A user can construct a FilePrefetchBuffer without any arguments, but use
  // `Prefetch` to load data into the buffer.
  FilePrefetchBuffer(size_t readahead_size = 0, size_t max_readahead_size = 0,
                     bool enable = true, bool track_min_offset = false,
                     bool implicit_auto_readahead = false)
      : buffer_offset_(0),
        readahead_size_(readahead_size),
        max_readahead_size_(max_readahead_size),
        min_offset_read_(port::kMaxSizet),
        enable_(enable),
        track_min_offset_(track_min_offset),
        implicit_auto_readahead_(implicit_auto_readahead),
        prev_offset_(0),
        prev_len_(0),
        num_file_reads_(kMinNumFileReadsToStartAutoReadahead + 1) {}

  // Load data into the buffer from a file.
  // reader                : the file reader.
  // offset                : the file offset to start reading from.
  // n                     : the number of bytes to read.
  // rate_limiter_priority : rate limiting priority, or `Env::IO_TOTAL` to
  //                         bypass.
  Status Prefetch(const IOOptions& opts, RandomAccessFileReader* reader,
                  uint64_t offset, size_t n,
                  Env::IOPriority rate_limiter_priority);

  // Tries returning the data for a file read from this buffer if that data is
  // in the buffer.
  // It handles tracking the minimum read offset if track_min_offset = true.
  // It also does the exponential readahead when readahead_size is set as part
  // of the constructor.
  //
  // opts                  : the IO options to use.
  // reader                : the file reader.
  // offset                : the file offset.
  // n                     : the number of bytes.
  // result                : output buffer to put the data into.
  // s                     : output status.
  // rate_limiter_priority : rate limiting priority, or `Env::IO_TOTAL` to
  //                         bypass.
  // for_compaction        : true if cache read is done for compaction read.
  bool TryReadFromCache(const IOOptions& opts, RandomAccessFileReader* reader,
                        uint64_t offset, size_t n, Slice* result, Status* s,
                        Env::IOPriority rate_limiter_priority,
                        bool for_compaction = false);

  // The minimum `offset` ever passed to TryReadFromCache(). This will nly be
  // tracked if track_min_offset = true.
  size_t min_offset_read() const { return min_offset_read_; }

  // Called in case of implicit auto prefetching.
  void UpdateReadPattern(const uint64_t& offset, const size_t& len,
                         bool is_adaptive_readahead = false) {
    if (is_adaptive_readahead) {
      // Since this block was eligible for prefetch but it was found in
      // cache, so check and decrease the readahead_size by 8KB (default)
      // if eligible.
      DecreaseReadAheadIfEligible(offset, len);
    }
    prev_offset_ = offset;
    prev_len_ = len;
  }

  bool IsBlockSequential(const size_t& offset) {
    return (prev_len_ == 0 || (prev_offset_ + prev_len_ == offset));
  }

  // Called in case of implicit auto prefetching.
  void ResetValues() {
    num_file_reads_ = 1;
    readahead_size_ = kInitAutoReadaheadSize;
  }

  void GetReadaheadState(ReadaheadFileInfo::ReadaheadInfo* readahead_info) {
    readahead_info->readahead_size = readahead_size_;
    readahead_info->num_file_reads = num_file_reads_;
  }

  void DecreaseReadAheadIfEligible(uint64_t offset, size_t size,
                                   size_t value = DEAFULT_DECREMENT) {
    // Decrease the readahead_size if
    // - its enabled internally by RocksDB (implicit_auto_readahead_) and,
    // - readahead_size is greater than 0 and,
    // - this block would have called prefetch API if not found in cache for
    //   which conditions are:
    //   - few/no bytes are in buffer and,
    //   - block is sequential with the previous read and,
    //   - num_file_reads_ + 1 (including this read) >
    //   kMinNumFileReadsToStartAutoReadahead
    if (implicit_auto_readahead_ && readahead_size_ > 0) {
      if ((offset + size > buffer_offset_ + buffer_.CurrentSize()) &&
          IsBlockSequential(offset) &&
          (num_file_reads_ + 1 > kMinNumFileReadsToStartAutoReadahead)) {
        size_t initial_auto_readahead_size = kInitAutoReadaheadSize;
        readahead_size_ =
            std::max(initial_auto_readahead_size,
                     (readahead_size_ >= value ? readahead_size_ - value : 0));
      }
    }
  }

 private:
  AlignedBuffer buffer_;
  uint64_t buffer_offset_;
  size_t readahead_size_;
  // FilePrefetchBuffer object won't be created from Iterator flow if
  // max_readahead_size_ = 0.
  size_t max_readahead_size_;
  // The minimum `offset` ever passed to TryReadFromCache().
  size_t min_offset_read_;
  // if false, TryReadFromCache() always return false, and we only take stats
  // for track_min_offset_ if track_min_offset_ = true
  bool enable_;
  // If true, track minimum `offset` ever passed to TryReadFromCache(), which
  // can be fetched from min_offset_read().
  bool track_min_offset_;

  // implicit_auto_readahead is enabled by rocksdb internally after 2
  // sequential IOs.
  bool implicit_auto_readahead_;
  uint64_t prev_offset_;
  size_t prev_len_;
  int64_t num_file_reads_;
};
}  // namespace ROCKSDB_NAMESPACE
