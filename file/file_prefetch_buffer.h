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
#include "util/autovector.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

#define DEFAULT_DECREMENT 8 * 1024

struct IOOptions;
class RandomAccessFileReader;

struct BufferInfo {
  AlignedBuffer buffer_;

  uint64_t offset_ = 0;

  // Below parameters are used in case of async read flow.
  // Length requested for in ReadAsync.
  size_t async_req_len_ = 0;

  // async_read_in_progress can be used as mutex. Callback can update the buffer
  // and its size but async_read_in_progress is only set by main thread.
  bool async_read_in_progress_ = false;

  // io_handle is allocated and used by underlying file system in case of
  // asynchronous reads.
  void* io_handle_ = nullptr;

  IOHandleDeleter del_fn_ = nullptr;

  // pos represents the index of this buffer in vector of BufferInfo.
  uint32_t pos_ = 0;
};

// FilePrefetchBuffer is a smart buffer to store and read data from a file.
class FilePrefetchBuffer {
 public:
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
                     bool implicit_auto_readahead = false,
                     uint64_t num_file_reads = 0,
                     uint64_t num_file_reads_for_auto_readahead = 0,
                     FileSystem* fs = nullptr, SystemClock* clock = nullptr,
                     Statistics* stats = nullptr)
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
        num_file_reads_for_auto_readahead_(num_file_reads_for_auto_readahead),
        num_file_reads_(num_file_reads),
        explicit_prefetch_submitted_(false),
        fs_(fs),
        clock_(clock),
        stats_(stats) {
    assert((num_file_reads_ >= num_file_reads_for_auto_readahead_ + 1) ||
           (num_file_reads_ == 0));
    // If ReadOptions.async_io is enabled, data is asynchronously filled in
    // second buffer while curr_ is being consumed. If data is overlapping in
    // two buffers, data is copied to third buffer to return continuous buffer.
    bufs_.resize(3);
    for (uint32_t i = 0; i < 2; i++) {
      bufs_[i].pos_ = i;
    }
  }

  ~FilePrefetchBuffer() {
    // Abort any pending async read request before destroying the class object.
    if (fs_ != nullptr) {
      std::vector<void*> handles;
      for (uint32_t i = 0; i < 2; i++) {
        if (bufs_[i].async_read_in_progress_ &&
            bufs_[i].io_handle_ != nullptr) {
          handles.emplace_back(bufs_[i].io_handle_);
        }
      }
      if (!handles.empty()) {
        StopWatch sw(clock_, stats_, ASYNC_PREFETCH_ABORT_MICROS);
        Status s = fs_->AbortIO(handles);
        assert(s.ok());
      }
    }

    // Prefetch buffer bytes discarded.
    uint64_t bytes_discarded = 0;
    // Iterated over 2 buffers.
    for (int i = 0; i < 2; i++) {
      int first = i;
      int second = i ^ 1;

      if (DoesBufferContainData(first)) {
        // If last block was read completely from first and some bytes in
        // first buffer are still unconsumed.
        if (prev_offset_ >= bufs_[first].offset_ &&
            prev_offset_ + prev_len_ <
                bufs_[first].offset_ + bufs_[first].buffer_.CurrentSize()) {
          bytes_discarded += bufs_[first].buffer_.CurrentSize() -
                             (prev_offset_ + prev_len_ - bufs_[first].offset_);
        }
        // If data was in second buffer and some/whole block bytes were read
        // from second buffer.
        else if (prev_offset_ < bufs_[first].offset_ &&
                 !DoesBufferContainData(second)) {
          // If last block read was completely from different buffer, this
          // buffer is unconsumed.
          if (prev_offset_ + prev_len_ <= bufs_[first].offset_) {
            bytes_discarded += bufs_[first].buffer_.CurrentSize();
          }
          // If last block read overlaps with this buffer and some data is
          // still unconsumed and previous buffer (second) is not cleared.
          else if (prev_offset_ + prev_len_ > bufs_[first].offset_ &&
                   bufs_[first].offset_ + bufs_[first].buffer_.CurrentSize() ==
                       bufs_[second].offset_) {
            bytes_discarded += bufs_[first].buffer_.CurrentSize() -
                               (/*bytes read from this buffer=*/prev_len_ -
                                (bufs_[first].offset_ - prev_offset_));
          }
        }
      }
    }

    for (uint32_t i = 0; i < 2; i++) {
      // Release io_handle.
      DestroyAndClearIOHandle(i);
    }
    RecordInHistogram(stats_, PREFETCHED_BYTES_DISCARDED, bytes_discarded);
  }

  // Load data into the buffer from a file.
  // reader                : the file reader.
  // offset                : the file offset to start reading from.
  // n                     : the number of bytes to read.
  // rate_limiter_priority : rate limiting priority, or `Env::IO_TOTAL` to
  //                         bypass.
  Status Prefetch(const IOOptions& opts, RandomAccessFileReader* reader,
                  uint64_t offset, size_t n,
                  Env::IOPriority rate_limiter_priority);

  // Request for reading the data from a file asynchronously.
  // If data already exists in the buffer, result will be updated.
  // reader                : the file reader.
  // offset                : the file offset to start reading from.
  // n                     : the number of bytes to read.
  // result                : if data already exists in the buffer, result will
  //                         be updated with the data.
  //
  // If data already exist in the buffer, it will return Status::OK, otherwise
  // it will send asynchronous request and return Status::TryAgain.
  Status PrefetchAsync(const IOOptions& opts, RandomAccessFileReader* reader,
                       uint64_t offset, size_t n, Slice* result);

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
    explicit_prefetch_submitted_ = false;
  }

  void GetReadaheadState(ReadaheadFileInfo::ReadaheadInfo* readahead_info) {
    readahead_info->readahead_size = readahead_size_;
    readahead_info->num_file_reads = num_file_reads_;
  }

  void DecreaseReadAheadIfEligible(uint64_t offset, size_t size,
                                   size_t value = DEFAULT_DECREMENT) {
    // Decrease the readahead_size if
    // - its enabled internally by RocksDB (implicit_auto_readahead_) and,
    // - readahead_size is greater than 0 and,
    // - this block would have called prefetch API if not found in cache for
    //   which conditions are:
    //   - few/no bytes are in buffer and,
    //   - block is sequential with the previous read and,
    //   - num_file_reads_ + 1 (including this read) >
    //   num_file_reads_for_auto_readahead_
    size_t curr_size = bufs_[curr_].async_read_in_progress_
                           ? bufs_[curr_].async_req_len_
                           : bufs_[curr_].buffer_.CurrentSize();
    if (implicit_auto_readahead_ && readahead_size_ > 0) {
      if ((offset + size > bufs_[curr_].offset_ + curr_size) &&
          IsBlockSequential(offset) &&
          (num_file_reads_ + 1 > num_file_reads_for_auto_readahead_)) {
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
                             size_t roundup_len, uint32_t index,
                             bool refit_tail, uint64_t& chunk_len);

  void AbortIOIfNeeded(uint64_t offset);

  void AbortAllIOs();

  void UpdateBuffersIfNeeded(uint64_t offset);

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
                   uint64_t read_len, uint64_t rounddown_start, uint32_t index);

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

  // Called in case of implicit auto prefetching.
  bool IsEligibleForPrefetch(uint64_t offset, size_t n) {
    // Prefetch only if this read is sequential otherwise reset readahead_size_
    // to initial value.
    if (!IsBlockSequential(offset)) {
      UpdateReadPattern(offset, n, false /*decrease_readaheadsize*/);
      ResetValues();
      return false;
    }
    num_file_reads_++;

    // Since async request was submitted in last call directly by calling
    // PrefetchAsync, it skips num_file_reads_ check as this call is to poll the
    // data submitted in previous call.
    if (explicit_prefetch_submitted_) {
      return true;
    }
    if (num_file_reads_ <= num_file_reads_for_auto_readahead_) {
      UpdateReadPattern(offset, n, false /*decrease_readaheadsize*/);
      return false;
    }
    return true;
  }

  // Helper functions.
  bool IsDataBlockInBuffer(uint64_t offset, size_t length, uint32_t index) {
    return (offset >= bufs_[index].offset_ &&
            offset + length <=
                bufs_[index].offset_ + bufs_[index].buffer_.CurrentSize());
  }
  bool IsOffsetInBuffer(uint64_t offset, uint32_t index) {
    return (offset >= bufs_[index].offset_ &&
            offset < bufs_[index].offset_ + bufs_[index].buffer_.CurrentSize());
  }
  bool DoesBufferContainData(uint32_t index) {
    return bufs_[index].buffer_.CurrentSize() > 0;
  }
  bool IsBufferOutdated(uint64_t offset, uint32_t index) {
    return (
        !bufs_[index].async_read_in_progress_ && DoesBufferContainData(index) &&
        offset >= bufs_[index].offset_ + bufs_[index].buffer_.CurrentSize());
  }
  bool IsBufferOutdatedWithAsyncProgress(uint64_t offset, uint32_t index) {
    return (bufs_[index].async_read_in_progress_ &&
            bufs_[index].io_handle_ != nullptr &&
            offset >= bufs_[index].offset_ + bufs_[index].async_req_len_);
  }
  bool IsOffsetInBufferWithAsyncProgress(uint64_t offset, uint32_t index) {
    return (bufs_[index].async_read_in_progress_ &&
            offset >= bufs_[index].offset_ &&
            offset < bufs_[index].offset_ + bufs_[index].async_req_len_);
  }

  bool IsSecondBuffEligibleForPrefetching() {
    uint32_t second = curr_ ^ 1;
    if (bufs_[second].async_read_in_progress_) {
      return false;
    }
    assert(!bufs_[curr_].async_read_in_progress_);

    if (DoesBufferContainData(curr_) && DoesBufferContainData(second) &&
        (bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize() ==
         bufs_[second].offset_)) {
      return false;
    }
    bufs_[second].buffer_.Clear();
    return true;
  }

  void DestroyAndClearIOHandle(uint32_t index) {
    if (bufs_[index].io_handle_ != nullptr && bufs_[index].del_fn_ != nullptr) {
      bufs_[index].del_fn_(bufs_[index].io_handle_);
      bufs_[index].io_handle_ = nullptr;
      bufs_[index].del_fn_ = nullptr;
    }
    bufs_[index].async_read_in_progress_ = false;
  }

  Status HandleOverlappingData(const IOOptions& opts,
                               RandomAccessFileReader* reader, uint64_t offset,
                               size_t length, size_t readahead_size,
                               Env::IOPriority rate_limiter_priority,
                               bool& copy_to_third_buffer, uint64_t& tmp_offset,
                               size_t& tmp_length);

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
  // num_file_reads_ and num_file_reads_for_auto_readahead_ is only used when
  // implicit_auto_readahead_ is set.
  uint64_t num_file_reads_for_auto_readahead_;
  uint64_t num_file_reads_;

  // If explicit_prefetch_submitted_ is set then it indicates RocksDB called
  // PrefetchAsync to submit request. It needs to call TryReadFromCacheAsync to
  // poll the submitted request without checking if data is sequential and
  // num_file_reads_.
  bool explicit_prefetch_submitted_;

  FileSystem* fs_;
  SystemClock* clock_;
  Statistics* stats_;
};
}  // namespace ROCKSDB_NAMESPACE
