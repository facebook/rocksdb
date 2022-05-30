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
#include "monitoring/statistics.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "util/aligned_buffer.h"

namespace ROCKSDB_NAMESPACE {

#define DEAFULT_DECREMENT 8 * 1024

struct IOOptions;
class RandomAccessFileReader;

struct BufferInfo {
  AlignedBuffer buffer_;
  uint64_t offset_ = 0;
};

// FilePrefetchBuffer is a smart buffer to store and read data from a file.
class FilePrefetchBuffer {
 public:
  static const int kMinNumFileReadsToStartAutoReadahead = 2;

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
  // async_io : When async_io is enabled, if it's implicit_auto_readahead, it
  //   prefetches data asynchronously in second buffer while curr_ is being
  //   consumed.
  //
  // Automatic readhead is enabled for a file if readahead_size
  // and max_readahead_size are passed in.
  // A user can construct a FilePrefetchBuffer without any arguments, but use
  // `Prefetch` to load data into the buffer.
  FilePrefetchBuffer(size_t readahead_size = 0, size_t max_readahead_size = 0,
                     bool enable = true, bool track_min_offset = false,
                     bool implicit_auto_readahead = false,
                     bool async_io = false, FileSystem* fs = nullptr,
                     SystemClock* clock = nullptr, Statistics* stats = nullptr)
      : curr_(0),
        readahead_size_(readahead_size),
        initial_auto_readahead_size_(readahead_size),
        max_readahead_size_(max_readahead_size),
        min_offset_read_(std::numeric_limits<size_t>::max()),
        enable_(enable),
        track_min_offset_(track_min_offset),
        implicit_auto_readahead_(implicit_auto_readahead),
        prev_offset_(0),
        prev_len_(0),
        num_file_reads_(kMinNumFileReadsToStartAutoReadahead + 1),
        io_handle_(nullptr),
        del_fn_(nullptr),
        async_read_in_progress_(false),
        async_io_(async_io),
        fs_(fs),
        clock_(clock),
        stats_(stats) {
    // If async_io_ is enabled, data is asynchronously filled in second buffer
    // while curr_ is being consumed. If data is overlapping in two buffers,
    // data is copied to third buffer to return continuous buffer.
    bufs_.resize(3);
    (void)async_io_;
  }

  ~FilePrefetchBuffer() {
    // Abort any pending async read request before destroying the class object.
    if (async_read_in_progress_ && fs_ != nullptr) {
      std::vector<void*> handles;
      handles.emplace_back(io_handle_);
      Status s = fs_->AbortIO(handles);
      assert(s.ok());
    }

    // Prefetch buffer bytes discarded.
    uint64_t bytes_discarded = 0;
    if (bufs_[curr_].buffer_.CurrentSize() != 0) {
      bytes_discarded = bufs_[curr_].buffer_.CurrentSize();
    }
    if (bufs_[curr_ ^ 1].buffer_.CurrentSize() != 0) {
      bytes_discarded += bufs_[curr_ ^ 1].buffer_.CurrentSize();
    }
    RecordInHistogram(stats_, PREFETCHED_BYTES_DISCARDED, bytes_discarded);

    // Release io_handle_.
    if (io_handle_ != nullptr && del_fn_ != nullptr) {
      del_fn_(io_handle_);
      io_handle_ = nullptr;
      del_fn_ = nullptr;
    }
  }

  // Load data into the buffer from a file.
  // reader                : the file reader.
  // offset                : the file offset to start reading from.
  // n                     : the number of bytes to read.
  // rate_limiter_priority : rate limiting priority, or `Env::IO_TOTAL` to
  //                         bypass.
  // is_async_read         : if the data should be prefetched by calling read
  //                         asynchronously. It should be set true when called
  //                         from TryReadFromCache.
  Status Prefetch(const IOOptions& opts, RandomAccessFileReader* reader,
                  uint64_t offset, size_t n,
                  Env::IOPriority rate_limiter_priority);

  // Request for reading the data from a file asynchronously.
  // If data already exists in the buffer, result will be updated.
  // reader                : the file reader.
  // offset                : the file offset to start reading from.
  // n                     : the number of bytes to read.
  // rate_limiter_priority : rate limiting priority, or `Env::IO_TOTAL` to
  //                         bypass.
  // result                : if data already exists in the buffer, result will
  //                         be updated with the data.
  //
  // If data already exist in the buffer, it will return Status::OK, otherwise
  // it will send asynchronous request and return Status::TryAgain.
  Status PrefetchAsync(const IOOptions& opts, RandomAccessFileReader* reader,
                       uint64_t offset, size_t n,
                       Env::IOPriority rate_limiter_priority, Slice* result);

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

  bool TryReadFromCacheAsync(const IOOptions& opts,
                             RandomAccessFileReader* reader, uint64_t offset,
                             size_t n, Slice* result, Status* status,
                             Env::IOPriority rate_limiter_priority);

  // The minimum `offset` ever passed to TryReadFromCache(). This will nly be
  // tracked if track_min_offset = true.
  size_t min_offset_read() const { return min_offset_read_; }

  // Called in case of implicit auto prefetching.
  void UpdateReadPattern(const uint64_t& offset, const size_t& len,
                         bool decrease_readaheadsize) {
    if (decrease_readaheadsize) {
      // Since this block was eligible for prefetch but it was found in
      // cache, so check and decrease the readahead_size by 8KB (default)
      // if eligible.
      DecreaseReadAheadIfEligible(offset, len);
    }
    prev_offset_ = offset;
    prev_len_ = len;
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
      if ((offset + size >
           bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize()) &&
          IsBlockSequential(offset) &&
          (num_file_reads_ + 1 > kMinNumFileReadsToStartAutoReadahead)) {
        readahead_size_ =
            std::max(initial_auto_readahead_size_,
                     (readahead_size_ >= value ? readahead_size_ - value : 0));
      }
    }
  }

  // Callback function passed to underlying FS in case of asynchronous reads.
  void PrefetchAsyncCallback(const FSReadRequest& req, void* cb_arg);

 private:
  // Calculates roundoff offset and length to be prefetched based on alignment
  // and data present in buffer_. It also allocates new buffer or refit tail if
  // required.
  void CalculateOffsetAndLen(size_t alignment, uint64_t offset,
                             size_t roundup_len, size_t index, bool refit_tail,
                             uint64_t& chunk_len);

  // It calls Poll API if any there is any pending asynchronous request. It then
  // checks if data is in any buffer. It clears the outdated data and swaps the
  // buffers if required.
  void PollAndUpdateBuffersIfNeeded(uint64_t offset);

  Status PrefetchAsyncInternal(const IOOptions& opts,
                               RandomAccessFileReader* reader, uint64_t offset,
                               size_t length, size_t readahead_size,
                               Env::IOPriority rate_limiter_priority,
                               bool& copy_to_third_buffer);

  Status Read(const IOOptions& opts, RandomAccessFileReader* reader,
              Env::IOPriority rate_limiter_priority, uint64_t read_len,
              uint64_t chunk_len, uint64_t rounddown_start, uint32_t index);

  Status ReadAsync(const IOOptions& opts, RandomAccessFileReader* reader,
                   Env::IOPriority rate_limiter_priority, uint64_t read_len,
                   uint64_t chunk_len, uint64_t rounddown_start,
                   uint32_t index);

  // Copy the data from src to third buffer.
  void CopyDataToBuffer(uint32_t src, uint64_t& offset, size_t& length);

  bool IsBlockSequential(const size_t& offset) {
    return (prev_len_ == 0 || (prev_offset_ + prev_len_ == offset));
  }

  // Called in case of implicit auto prefetching.
  void ResetValues() {
    num_file_reads_ = 1;
    readahead_size_ = initial_auto_readahead_size_;
  }

  bool IsEligibleForPrefetch(uint64_t offset, size_t n) {
    // Prefetch only if this read is sequential otherwise reset readahead_size_
    // to initial value.
    if (!IsBlockSequential(offset)) {
      UpdateReadPattern(offset, n, false /*decrease_readaheadsize*/);
      ResetValues();
      return false;
    }
    num_file_reads_++;
    if (num_file_reads_ <= kMinNumFileReadsToStartAutoReadahead) {
      UpdateReadPattern(offset, n, false /*decrease_readaheadsize*/);
      return false;
    }
    return true;
  }

  std::vector<BufferInfo> bufs_;
  // curr_ represents the index for bufs_ indicating which buffer is being
  // consumed currently.
  uint32_t curr_;
  size_t readahead_size_;
  size_t initial_auto_readahead_size_;
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

  // io_handle_ is allocated and used by underlying file system in case of
  // asynchronous reads.
  void* io_handle_;
  IOHandleDeleter del_fn_;
  bool async_read_in_progress_;
  bool async_io_;
  FileSystem* fs_;
  SystemClock* clock_;
  Statistics* stats_;
};
}  // namespace ROCKSDB_NAMESPACE
