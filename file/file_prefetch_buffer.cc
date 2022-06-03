//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "file/file_prefetch_buffer.h"

#include <algorithm>

#include "file/random_access_file_reader.h"
#include "monitoring/histogram.h"
#include "monitoring/iostats_context_imp.h"
#include "port/port.h"
#include "test_util/sync_point.h"
#include "util/random.h"
#include "util/rate_limiter.h"

namespace ROCKSDB_NAMESPACE {

void FilePrefetchBuffer::CalculateOffsetAndLen(size_t alignment,
                                               uint64_t offset,
                                               size_t roundup_len, size_t index,
                                               bool refit_tail,
                                               uint64_t& chunk_len) {
  uint64_t chunk_offset_in_buffer = 0;
  bool copy_data_to_new_buffer = false;
  // Check if requested bytes are in the existing buffer_.
  // If only a few bytes exist -- reuse them & read only what is really needed.
  //     This is typically the case of incremental reading of data.
  // If no bytes exist in buffer -- full pread.
  if (bufs_[index].buffer_.CurrentSize() > 0 &&
      offset >= bufs_[index].offset_ &&
      offset <= bufs_[index].offset_ + bufs_[index].buffer_.CurrentSize()) {
    // Only a few requested bytes are in the buffer. memmove those chunk of
    // bytes to the beginning, and memcpy them back into the new buffer if a
    // new buffer is created.
    chunk_offset_in_buffer = Rounddown(
        static_cast<size_t>(offset - bufs_[index].offset_), alignment);
    chunk_len = static_cast<uint64_t>(bufs_[index].buffer_.CurrentSize()) -
                chunk_offset_in_buffer;
    assert(chunk_offset_in_buffer % alignment == 0);
    // assert(chunk_len % alignment == 0);
    assert(chunk_offset_in_buffer + chunk_len <=
           bufs_[index].offset_ + bufs_[index].buffer_.CurrentSize());
    if (chunk_len > 0) {
      copy_data_to_new_buffer = true;
    } else {
      // this reset is not necessary, but just to be safe.
      chunk_offset_in_buffer = 0;
    }
  }

  // Create a new buffer only if current capacity is not sufficient, and memcopy
  // bytes from old buffer if needed (i.e., if chunk_len is greater than 0).
  if (bufs_[index].buffer_.Capacity() < roundup_len) {
    bufs_[index].buffer_.Alignment(alignment);
    bufs_[index].buffer_.AllocateNewBuffer(
        static_cast<size_t>(roundup_len), copy_data_to_new_buffer,
        chunk_offset_in_buffer, static_cast<size_t>(chunk_len));
  } else if (chunk_len > 0 && refit_tail) {
    // New buffer not needed. But memmove bytes from tail to the beginning since
    // chunk_len is greater than 0.
    bufs_[index].buffer_.RefitTail(static_cast<size_t>(chunk_offset_in_buffer),
                                   static_cast<size_t>(chunk_len));
  } else if (chunk_len > 0) {
    // For async prefetching, it doesn't call RefitTail with chunk_len > 0.
    // Allocate new buffer if needed because aligned buffer calculate remaining
    // buffer as capacity_ - cursize_ which might not be the case in this as we
    // are not refitting.
    // TODO akanksha: Update the condition when asynchronous prefetching is
    // stable.
    bufs_[index].buffer_.Alignment(alignment);
    bufs_[index].buffer_.AllocateNewBuffer(
        static_cast<size_t>(roundup_len), copy_data_to_new_buffer,
        chunk_offset_in_buffer, static_cast<size_t>(chunk_len));
  }
}

Status FilePrefetchBuffer::Read(const IOOptions& opts,
                                RandomAccessFileReader* reader,
                                Env::IOPriority rate_limiter_priority,
                                uint64_t read_len, uint64_t chunk_len,
                                uint64_t rounddown_start, uint32_t index) {
  Slice result;
  Status s = reader->Read(opts, rounddown_start + chunk_len, read_len, &result,
                          bufs_[index].buffer_.BufferStart() + chunk_len,
                          nullptr, rate_limiter_priority);
#ifndef NDEBUG
  if (result.size() < read_len) {
    // Fake an IO error to force db_stress fault injection to ignore
    // truncated read errors
    IGNORE_STATUS_IF_ERROR(Status::IOError());
  }
#endif
  if (!s.ok()) {
    return s;
  }

  // Update the buffer offset and size.
  bufs_[index].offset_ = rounddown_start;
  bufs_[index].buffer_.Size(static_cast<size_t>(chunk_len) + result.size());
  return s;
}

Status FilePrefetchBuffer::ReadAsync(const IOOptions& opts,
                                     RandomAccessFileReader* reader,
                                     Env::IOPriority rate_limiter_priority,
                                     uint64_t read_len, uint64_t chunk_len,
                                     uint64_t rounddown_start, uint32_t index) {
  // callback for async read request.
  auto fp = std::bind(&FilePrefetchBuffer::PrefetchAsyncCallback, this,
                      std::placeholders::_1, std::placeholders::_2);
  FSReadRequest req;
  Slice result;
  req.len = read_len;
  req.offset = rounddown_start + chunk_len;
  req.result = result;
  req.scratch = bufs_[index].buffer_.BufferStart() + chunk_len;
  Status s = reader->ReadAsync(req, opts, fp, nullptr /*cb_arg*/, &io_handle_,
                               &del_fn_, rate_limiter_priority);
  req.status.PermitUncheckedError();
  if (s.ok()) {
    async_read_in_progress_ = true;
  }
  return s;
}

Status FilePrefetchBuffer::Prefetch(const IOOptions& opts,
                                    RandomAccessFileReader* reader,
                                    uint64_t offset, size_t n,
                                    Env::IOPriority rate_limiter_priority) {
  if (!enable_ || reader == nullptr) {
    return Status::OK();
  }
  TEST_SYNC_POINT("FilePrefetchBuffer::Prefetch:Start");

  if (offset + n <= bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize()) {
    // All requested bytes are already in the curr_ buffer. So no need to Read
    // again.
    return Status::OK();
  }

  size_t alignment = reader->file()->GetRequiredBufferAlignment();
  size_t offset_ = static_cast<size_t>(offset);
  uint64_t rounddown_offset = Rounddown(offset_, alignment);
  uint64_t roundup_end = Roundup(offset_ + n, alignment);
  uint64_t roundup_len = roundup_end - rounddown_offset;
  assert(roundup_len >= alignment);
  assert(roundup_len % alignment == 0);

  uint64_t chunk_len = 0;
  CalculateOffsetAndLen(alignment, offset, roundup_len, curr_,
                        true /*refit_tail*/, chunk_len);
  size_t read_len = static_cast<size_t>(roundup_len - chunk_len);

  Status s = Read(opts, reader, rate_limiter_priority, read_len, chunk_len,
                  rounddown_offset, curr_);
  return s;
}

// Copy data from src to third buffer.
void FilePrefetchBuffer::CopyDataToBuffer(uint32_t src, uint64_t& offset,
                                          size_t& length) {
  if (length == 0) {
    return;
  }
  uint64_t copy_offset = (offset - bufs_[src].offset_);
  size_t copy_len = 0;
  if (offset + length <=
      bufs_[src].offset_ + bufs_[src].buffer_.CurrentSize()) {
    // All the bytes are in src.
    copy_len = length;
  } else {
    copy_len = bufs_[src].buffer_.CurrentSize() - copy_offset;
  }

  memcpy(bufs_[2].buffer_.BufferStart() + bufs_[2].buffer_.CurrentSize(),
         bufs_[src].buffer_.BufferStart() + copy_offset, copy_len);

  bufs_[2].buffer_.Size(bufs_[2].buffer_.CurrentSize() + copy_len);

  // Update offset and length.
  offset += copy_len;
  length -= copy_len;

  // length > 0 indicates it has consumed all data from the src buffer and it
  // still needs to read more other buffer.
  if (length > 0) {
    bufs_[src].buffer_.Clear();
  }
}

void FilePrefetchBuffer::PollAndUpdateBuffersIfNeeded(uint64_t offset) {
  if (async_read_in_progress_ && fs_ != nullptr) {
    // Wait for prefetch data to complete.
    // No mutex is needed as PrefetchAsyncCallback updates the result in second
    // buffer and FilePrefetchBuffer should wait for Poll before accessing the
    // second buffer.
    std::vector<void*> handles;
    handles.emplace_back(io_handle_);
    StopWatch sw(clock_, stats_, POLL_WAIT_MICROS);
    fs_->Poll(handles, 1).PermitUncheckedError();
  }

  // Reset and Release io_handle_ after the Poll API as request has been
  // completed.
  async_read_in_progress_ = false;
  if (io_handle_ != nullptr && del_fn_ != nullptr) {
    del_fn_(io_handle_);
    io_handle_ = nullptr;
    del_fn_ = nullptr;
  }

  // Index of second buffer.
  uint32_t second = curr_ ^ 1;

  // First clear the buffers if it contains outdated data. Outdated data can be
  // because previous sequential reads were read from the cache instead of these
  // buffer.
  {
    if (bufs_[curr_].buffer_.CurrentSize() > 0 &&
        offset >= bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize()) {
      bufs_[curr_].buffer_.Clear();
    }
    if (bufs_[second].buffer_.CurrentSize() > 0 &&
        offset >= bufs_[second].offset_ + bufs_[second].buffer_.CurrentSize()) {
      bufs_[second].buffer_.Clear();
    }
  }

  // If data is in second buffer, make it curr_. Second buffer can be either
  // partial filled or full.
  if (bufs_[second].buffer_.CurrentSize() > 0 &&
      offset >= bufs_[second].offset_ &&
      offset < bufs_[second].offset_ + bufs_[second].buffer_.CurrentSize()) {
    // Clear the curr_ as buffers have been swapped and curr_ contains the
    // outdated data and switch the buffers.
    bufs_[curr_].buffer_.Clear();
    curr_ = curr_ ^ 1;
  }
}

// If async_read = true:
// async_read is enabled in case of sequential reads. So when
// buffers are switched, we clear the curr_ buffer as we assume the data has
// been consumed because of sequential reads.
//
// Scenarios for prefetching asynchronously:
// Case1: If both buffers are empty, prefetch n bytes
//        synchronously in curr_
//        and prefetch readahead_size_/2 async in second buffer.
// Case2: If second buffer has partial or full data, make it current and
//        prefetch readahead_size_/2 async in second buffer. In case of
//        partial data, prefetch remaining bytes from size n synchronously to
//        fulfill the requested bytes request.
// Case3: If curr_ has partial data, prefetch remaining bytes from size n
//        synchronously in curr_ to fulfill the requested bytes request and
//        prefetch readahead_size_/2 bytes async in second buffer.
// Case4: If data is in both buffers, copy requested data from curr_ and second
//        buffer to third buffer. If all requested bytes have been copied, do
//        the asynchronous prefetching in second buffer.
Status FilePrefetchBuffer::PrefetchAsyncInternal(
    const IOOptions& opts, RandomAccessFileReader* reader, uint64_t offset,
    size_t length, size_t readahead_size, Env::IOPriority rate_limiter_priority,
    bool& copy_to_third_buffer) {
  if (!enable_) {
    return Status::OK();
  }

  TEST_SYNC_POINT("FilePrefetchBuffer::PrefetchAsyncInternal:Start");

  PollAndUpdateBuffersIfNeeded(offset);

  // If all the requested bytes are in curr_, it will go for async prefetching
  // only.
  if (bufs_[curr_].buffer_.CurrentSize() > 0 &&
      offset + length <=
          bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize()) {
    offset += length;
    length = 0;
  }

  Status s;
  size_t prefetch_size = length + readahead_size;
  size_t alignment = reader->file()->GetRequiredBufferAlignment();
  // Index of second buffer.
  uint32_t second = curr_ ^ 1;

  // Data is overlapping i.e. some of the data is in curr_ buffer and remaining
  // in second buffer.
  if (bufs_[curr_].buffer_.CurrentSize() > 0 &&
      bufs_[second].buffer_.CurrentSize() > 0 &&
      offset >= bufs_[curr_].offset_ &&
      offset < bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize() &&
      offset + length > bufs_[second].offset_) {
    // Allocate new buffer to third buffer;
    bufs_[2].buffer_.Clear();
    bufs_[2].buffer_.Alignment(alignment);
    bufs_[2].buffer_.AllocateNewBuffer(length);
    bufs_[2].offset_ = offset;
    copy_to_third_buffer = true;

    // Move data from curr_ buffer to third.
    CopyDataToBuffer(curr_, offset, length);
    if (length == 0) {
      // Requested data has been copied and curr_ still has unconsumed data.
      return s;
    }
    CopyDataToBuffer(second, offset, length);
    // Length == 0: All the requested data has been copied to third buffer. It
    // should go for only async prefetching.
    // Length > 0: More data needs to be consumed so it will continue async and
    // sync prefetching and copy the remaining data to third buffer in the end.
    // swap the buffers.
    curr_ = curr_ ^ 1;
    // Update prefetch_size as length has been updated in CopyDataToBuffer.
    prefetch_size = length + readahead_size;
  }

  size_t _offset = static_cast<size_t>(offset);
  second = curr_ ^ 1;

  // offset and size alignment for curr_ buffer with synchronous prefetching
  uint64_t rounddown_start1 = Rounddown(_offset, alignment);
  uint64_t roundup_end1 = Roundup(_offset + prefetch_size, alignment);
  uint64_t roundup_len1 = roundup_end1 - rounddown_start1;
  assert(roundup_len1 >= alignment);
  assert(roundup_len1 % alignment == 0);
  uint64_t chunk_len1 = 0;
  uint64_t read_len1 = 0;

  // For length == 0, skip the synchronous prefetching. read_len1 will be 0.
  if (length > 0) {
    CalculateOffsetAndLen(alignment, offset, roundup_len1, curr_,
                          false /*refit_tail*/, chunk_len1);
    assert(roundup_len1 >= chunk_len1);
    read_len1 = static_cast<size_t>(roundup_len1 - chunk_len1);
  }
  {
    // offset and size alignment for second buffer for asynchronous
    // prefetching
    uint64_t rounddown_start2 = roundup_end1;
    uint64_t roundup_end2 =
        Roundup(rounddown_start2 + readahead_size, alignment);

    // For length == 0, do the asynchronous prefetching in second instead of
    // synchronous prefetching in curr_.
    if (length == 0) {
      rounddown_start2 =
          bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize();
      roundup_end2 = Roundup(rounddown_start2 + prefetch_size, alignment);
    }

    uint64_t roundup_len2 = roundup_end2 - rounddown_start2;
    uint64_t chunk_len2 = 0;
    CalculateOffsetAndLen(alignment, rounddown_start2, roundup_len2, second,
                          false /*refit_tail*/, chunk_len2);

    // Update the buffer offset.
    bufs_[second].offset_ = rounddown_start2;
    assert(roundup_len2 >= chunk_len2);
    uint64_t read_len2 = static_cast<size_t>(roundup_len2 - chunk_len2);
    ReadAsync(opts, reader, rate_limiter_priority, read_len2, chunk_len2,
              rounddown_start2, second)
        .PermitUncheckedError();
  }

  if (read_len1 > 0) {
    s = Read(opts, reader, rate_limiter_priority, read_len1, chunk_len1,
             rounddown_start1, curr_);
    if (!s.ok()) {
      return s;
    }
  }
  // Copy remaining requested bytes to third_buffer.
  if (copy_to_third_buffer && length > 0) {
    CopyDataToBuffer(curr_, offset, length);
  }
  return s;
}

bool FilePrefetchBuffer::TryReadFromCache(const IOOptions& opts,
                                          RandomAccessFileReader* reader,
                                          uint64_t offset, size_t n,
                                          Slice* result, Status* status,
                                          Env::IOPriority rate_limiter_priority,
                                          bool for_compaction /* = false */) {
  if (track_min_offset_ && offset < min_offset_read_) {
    min_offset_read_ = static_cast<size_t>(offset);
  }
  if (!enable_ || (offset < bufs_[curr_].offset_)) {
    return false;
  }

  // If the buffer contains only a few of the requested bytes:
  //    If readahead is enabled: prefetch the remaining bytes + readahead bytes
  //        and satisfy the request.
  //    If readahead is not enabled: return false.
  TEST_SYNC_POINT_CALLBACK("FilePrefetchBuffer::TryReadFromCache",
                           &readahead_size_);
  if (offset + n > bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize()) {
    if (readahead_size_ > 0) {
      Status s;
      assert(reader != nullptr);
      assert(max_readahead_size_ >= readahead_size_);
      if (for_compaction) {
        s = Prefetch(opts, reader, offset, std::max(n, readahead_size_),
                     rate_limiter_priority);
      } else {
        if (implicit_auto_readahead_) {
          if (!IsEligibleForPrefetch(offset, n)) {
            // Ignore status as Prefetch is not called.
            s.PermitUncheckedError();
            return false;
          }
        }
        s = Prefetch(opts, reader, offset, n + readahead_size_,
                     rate_limiter_priority);
      }
      if (!s.ok()) {
        if (status) {
          *status = s;
        }
#ifndef NDEBUG
        IGNORE_STATUS_IF_ERROR(s);
#endif
        return false;
      }
      readahead_size_ = std::min(max_readahead_size_, readahead_size_ * 2);
    } else {
      return false;
    }
  }
  UpdateReadPattern(offset, n, false /*decrease_readaheadsize*/);

  uint64_t offset_in_buffer = offset - bufs_[curr_].offset_;
  *result = Slice(bufs_[curr_].buffer_.BufferStart() + offset_in_buffer, n);
  return true;
}

// TODO akanksha: Merge this function with TryReadFromCache once async
// functionality is stable.
bool FilePrefetchBuffer::TryReadFromCacheAsync(
    const IOOptions& opts, RandomAccessFileReader* reader, uint64_t offset,
    size_t n, Slice* result, Status* status,
    Env::IOPriority rate_limiter_priority) {
  assert(async_io_);

  if (track_min_offset_ && offset < min_offset_read_) {
    min_offset_read_ = static_cast<size_t>(offset);
  }

  if (!enable_) {
    return false;
  }

  // In case of async_io_, offset can be less than bufs_[curr_].offset_ because
  // of reads not sequential and PrefetchAsync can be called for any block and
  // RocksDB will call TryReadFromCacheAsync after PrefetchAsync to Poll for
  // requested bytes.
  if (bufs_[curr_].buffer_.CurrentSize() > 0 && offset < bufs_[curr_].offset_ &&
      prev_len_ != 0) {
    return false;
  }

  bool prefetched = false;
  bool copy_to_third_buffer = false;
  // If the buffer contains only a few of the requested bytes:
  //    If readahead is enabled: prefetch the remaining bytes + readahead bytes
  //        and satisfy the request.
  //    If readahead is not enabled: return false.
  TEST_SYNC_POINT_CALLBACK("FilePrefetchBuffer::TryReadFromCache",
                           &readahead_size_);
  if (offset < bufs_[curr_].offset_ ||
      offset + n > bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize()) {
    if (readahead_size_ > 0) {
      Status s;
      assert(reader != nullptr);
      assert(max_readahead_size_ >= readahead_size_);

      if (implicit_auto_readahead_) {
        if (!IsEligibleForPrefetch(offset, n)) {
          // Ignore status as Prefetch is not called.
          s.PermitUncheckedError();
          return false;
        }
      }

      // Prefetch n + readahead_size_/2 synchronously as remaining
      // readahead_size_/2 will be prefetched asynchronously.
      s = PrefetchAsyncInternal(opts, reader, offset, n, readahead_size_ / 2,
                                rate_limiter_priority, copy_to_third_buffer);
      if (!s.ok()) {
        if (status) {
          *status = s;
        }
#ifndef NDEBUG
        IGNORE_STATUS_IF_ERROR(s);
#endif
        return false;
      }
      prefetched = true;
    } else {
      return false;
    }
  }
  UpdateReadPattern(offset, n, false /*decrease_readaheadsize*/);

  uint32_t index = curr_;
  if (copy_to_third_buffer) {
    index = 2;
  }
  uint64_t offset_in_buffer = offset - bufs_[index].offset_;
  *result = Slice(bufs_[index].buffer_.BufferStart() + offset_in_buffer, n);
  if (prefetched) {
    readahead_size_ = std::min(max_readahead_size_, readahead_size_ * 2);
  }
  return true;
}

void FilePrefetchBuffer::PrefetchAsyncCallback(const FSReadRequest& req,
                                               void* /*cb_arg*/) {
  uint32_t index = curr_ ^ 1;

#ifndef NDEBUG
  if (req.result.size() < req.len) {
    // Fake an IO error to force db_stress fault injection to ignore
    // truncated read errors
    IGNORE_STATUS_IF_ERROR(Status::IOError());
  }
  IGNORE_STATUS_IF_ERROR(req.status);
#endif

  if (req.status.ok()) {
    if (req.offset + req.result.size() <=
        bufs_[index].offset_ + bufs_[index].buffer_.CurrentSize()) {
      // All requested bytes are already in the buffer. So no need to update.
      return;
    }
    if (req.offset < bufs_[index].offset_) {
      // Next block to be read has changed (Recent read was not a sequential
      // read). So ignore this read.
      return;
    }
    size_t current_size = bufs_[index].buffer_.CurrentSize();
    bufs_[index].buffer_.Size(current_size + req.result.size());
  }
}

Status FilePrefetchBuffer::PrefetchAsync(const IOOptions& opts,
                                         RandomAccessFileReader* reader,
                                         uint64_t offset, size_t n,
                                         Env::IOPriority rate_limiter_priority,
                                         Slice* result) {
  assert(reader != nullptr);
  if (!enable_) {
    return Status::NotSupported();
  }
  TEST_SYNC_POINT("FilePrefetchBuffer::PrefetchAsync:Start");

  PollAndUpdateBuffersIfNeeded(offset);

  // Index of second buffer.
  uint32_t second = curr_ ^ 1;

  // Since PrefetchAsync can be called on non sequential reads. So offset can
  // be less than buffers' offset. In that case it clears the buffer and
  // prefetch that block.
  if (bufs_[curr_].buffer_.CurrentSize() > 0 && offset < bufs_[curr_].offset_) {
    bufs_[curr_].buffer_.Clear();
  }

  // All requested bytes are already in the curr_ buffer. So no need to Read
  // again.
  if (bufs_[curr_].buffer_.CurrentSize() > 0 &&
      offset + n <= bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize()) {
    uint64_t offset_in_buffer = offset - bufs_[curr_].offset_;
    *result = Slice(bufs_[curr_].buffer_.BufferStart() + offset_in_buffer, n);
    return Status::OK();
  }

  Status s;
  size_t alignment = reader->file()->GetRequiredBufferAlignment();

  // TODO akanksha: Handle the scenario if data is overlapping in 2 buffers.
  // Currently, tt covers 2 scenarios. Either one buffer (curr_) has no data or
  // it has partial data. It ignores the contents in second buffer (overlapping
  // data in 2 buffers) and send the request to re-read that data again.

  // Clear the second buffer in order to do asynchronous prefetching.
  bufs_[second].buffer_.Clear();

  size_t offset_to_read = static_cast<size_t>(offset);
  uint64_t rounddown_start = 0;
  uint64_t roundup_end = 0;

  if (bufs_[curr_].buffer_.CurrentSize() == 0) {
    // Prefetch full data.
    rounddown_start = Rounddown(offset_to_read, alignment);
    roundup_end = Roundup(offset_to_read + n, alignment);
  } else {
    // Prefetch remaining data.
    size_t rem_length = n - (bufs_[curr_].buffer_.CurrentSize() -
                             (offset - bufs_[curr_].offset_));
    rounddown_start = bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize();
    roundup_end = Roundup(rounddown_start + rem_length, alignment);
  }

  uint64_t roundup_len = roundup_end - rounddown_start;
  assert(roundup_len >= alignment);
  assert(roundup_len % alignment == 0);

  uint64_t chunk_len = 0;
  CalculateOffsetAndLen(alignment, rounddown_start, roundup_len, second, false,
                        chunk_len);

  // Update the buffer offset.
  bufs_[second].offset_ = rounddown_start;
  assert(roundup_len >= chunk_len);

  size_t read_len = static_cast<size_t>(roundup_len - chunk_len);

  s = ReadAsync(opts, reader, rate_limiter_priority, read_len, chunk_len,
                rounddown_start, second);

  if (!s.ok()) {
    return s;
  }

  // Update read pattern so that TryReadFromCacheAsync call be called to Poll
  // the data. It will return without polling if blocks are not sequential.
  UpdateReadPattern(offset, n, /*decrease_readaheadsize=*/false);
  prev_len_ = 0;

  return Status::TryAgain();
}
}  // namespace ROCKSDB_NAMESPACE
