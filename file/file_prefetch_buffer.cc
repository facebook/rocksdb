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
#include <cassert>

#include "file/random_access_file_reader.h"
#include "monitoring/histogram.h"
#include "monitoring/iostats_context_imp.h"
#include "port/port.h"
#include "test_util/sync_point.h"
#include "util/random.h"
#include "util/rate_limiter_impl.h"

namespace ROCKSDB_NAMESPACE {

void FilePrefetchBuffer::CalculateOffsetAndLen(size_t alignment,
                                               uint64_t offset,
                                               size_t roundup_len,
                                               uint32_t index, bool refit_tail,
                                               uint64_t& chunk_len) {
  uint64_t chunk_offset_in_buffer = 0;
  bool copy_data_to_new_buffer = false;
  // Check if requested bytes are in the existing buffer_.
  // If only a few bytes exist -- reuse them & read only what is really needed.
  //     This is typically the case of incremental reading of data.
  // If no bytes exist in buffer -- full pread.
  if (DoesBufferContainData(index) && IsOffsetInBuffer(offset, index)) {
    // Only a few requested bytes are in the buffer. memmove those chunk of
    // bytes to the beginning, and memcpy them back into the new buffer if a
    // new buffer is created.
    chunk_offset_in_buffer = Rounddown(
        static_cast<size_t>(offset - bufs_[index].offset_), alignment);
    chunk_len = static_cast<uint64_t>(bufs_[index].buffer_.CurrentSize()) -
                chunk_offset_in_buffer;
    assert(chunk_offset_in_buffer % alignment == 0);
    assert(chunk_len % alignment == 0);
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
                                uint64_t read_len, uint64_t chunk_len,
                                uint64_t start_offset, uint32_t index) {
  Slice result;
  char* to_buf = bufs_[index].buffer_.BufferStart() + chunk_len;
  Status s = reader->Read(opts, start_offset + chunk_len, read_len, &result,
                          to_buf, /*aligned_buf=*/nullptr);
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
  if (result.data() != to_buf) {
    // If the read is coming from some other buffer already in memory (such as
    // mmap) then it would be inefficient to create another copy in this
    // FilePrefetchBuffer. The caller is expected to exclude this case.
    assert(false);
    return Status::Corruption("File read didn't populate our buffer");
  }

  if (usage_ == FilePrefetchBufferUsage::kUserScanPrefetch) {
    RecordTick(stats_, PREFETCH_BYTES, read_len);
  }
  // Update the buffer offset and size.
  bufs_[index].offset_ = start_offset;
  bufs_[index].buffer_.Size(static_cast<size_t>(chunk_len) + result.size());
  return s;
}

Status FilePrefetchBuffer::ReadAsync(const IOOptions& opts,
                                     RandomAccessFileReader* reader,
                                     uint64_t read_len, uint64_t start_offset,
                                     uint32_t index) {
  TEST_SYNC_POINT("FilePrefetchBuffer::ReadAsync");
  // callback for async read request.
  auto fp = std::bind(&FilePrefetchBuffer::PrefetchAsyncCallback, this,
                      std::placeholders::_1, std::placeholders::_2);
  FSReadRequest req;
  Slice result;
  req.len = read_len;
  req.offset = start_offset;
  req.result = result;
  req.scratch = bufs_[index].buffer_.BufferStart();
  bufs_[index].async_req_len_ = req.len;

  Status s =
      reader->ReadAsync(req, opts, fp, &(bufs_[index].pos_),
                        &(bufs_[index].io_handle_), &(bufs_[index].del_fn_),
                        /*aligned_buf=*/nullptr);
  req.status.PermitUncheckedError();
  if (s.ok()) {
    RecordTick(stats_, PREFETCH_BYTES, read_len);
    bufs_[index].async_read_in_progress_ = true;
  }
  return s;
}

Status FilePrefetchBuffer::Prefetch(const IOOptions& opts,
                                    RandomAccessFileReader* reader,
                                    uint64_t offset, size_t n) {
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
  uint64_t rounddown_offset = offset, roundup_end = 0, chunk_len = 0;
  size_t read_len = 0;

  ReadAheadSizeTuning(/*read_curr_block=*/true, /*refit_tail=*/true,
                      rounddown_offset, curr_, alignment, 0, n,
                      rounddown_offset, roundup_end, read_len, chunk_len);

  Status s;
  if (read_len > 0) {
    s = Read(opts, reader, read_len, chunk_len, rounddown_offset, curr_);
  }

  if (usage_ == FilePrefetchBufferUsage::kTableOpenPrefetchTail && s.ok()) {
    RecordInHistogram(stats_, TABLE_OPEN_PREFETCH_TAIL_READ_BYTES, read_len);
  }
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
  if (IsDataBlockInBuffer(offset, length, src)) {
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
    bufs_[src].ClearBuffer();
  }
}

// Clear the buffers if it contains outdated data. Outdated data can be
// because previous sequential reads were read from the cache instead of these
// buffer. In that case outdated IOs should be aborted.
void FilePrefetchBuffer::AbortIOIfNeeded(uint64_t offset) {
  uint32_t second = curr_ ^ 1;
  std::vector<void*> handles;
  autovector<uint32_t> buf_pos;
  if (IsBufferOutdatedWithAsyncProgress(offset, curr_)) {
    handles.emplace_back(bufs_[curr_].io_handle_);
    buf_pos.emplace_back(curr_);
  }
  if (IsBufferOutdatedWithAsyncProgress(offset, second)) {
    handles.emplace_back(bufs_[second].io_handle_);
    buf_pos.emplace_back(second);
  }
  if (!handles.empty()) {
    StopWatch sw(clock_, stats_, ASYNC_PREFETCH_ABORT_MICROS);
    Status s = fs_->AbortIO(handles);
    assert(s.ok());
  }

  for (auto& pos : buf_pos) {
    // Release io_handle.
    DestroyAndClearIOHandle(pos);
  }

  if (bufs_[second].io_handle_ == nullptr) {
    bufs_[second].async_read_in_progress_ = false;
  }

  if (bufs_[curr_].io_handle_ == nullptr) {
    bufs_[curr_].async_read_in_progress_ = false;
  }
}

void FilePrefetchBuffer::AbortAllIOs() {
  uint32_t second = curr_ ^ 1;
  std::vector<void*> handles;
  for (uint32_t i = 0; i < 2; i++) {
    if (bufs_[i].async_read_in_progress_ && bufs_[i].io_handle_ != nullptr) {
      handles.emplace_back(bufs_[i].io_handle_);
    }
  }
  if (!handles.empty()) {
    StopWatch sw(clock_, stats_, ASYNC_PREFETCH_ABORT_MICROS);
    Status s = fs_->AbortIO(handles);
    assert(s.ok());
  }

  // Release io_handles.
  if (bufs_[curr_].io_handle_ != nullptr && bufs_[curr_].del_fn_ != nullptr) {
    DestroyAndClearIOHandle(curr_);
  } else {
    bufs_[curr_].async_read_in_progress_ = false;
  }

  if (bufs_[second].io_handle_ != nullptr && bufs_[second].del_fn_ != nullptr) {
    DestroyAndClearIOHandle(second);
  } else {
    bufs_[second].async_read_in_progress_ = false;
  }
}

// Clear the buffers if it contains outdated data. Outdated data can be
// because previous sequential reads were read from the cache instead of these
// buffer.
void FilePrefetchBuffer::UpdateBuffersIfNeeded(uint64_t offset, size_t length) {
  uint32_t second = curr_ ^ 1;

  if (IsBufferOutdated(offset, curr_)) {
    bufs_[curr_].ClearBuffer();
  }
  if (IsBufferOutdated(offset, second)) {
    bufs_[second].ClearBuffer();
  }

  {
    // In case buffers do not align, reset second buffer if requested data needs
    // to be read in second buffer.
    if (!bufs_[second].async_read_in_progress_ &&
        !bufs_[curr_].async_read_in_progress_) {
      if (DoesBufferContainData(curr_)) {
        if (bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize() !=
            bufs_[second].offset_) {
          if (DoesBufferContainData(second) &&
              IsOffsetInBuffer(offset, curr_) &&
              (offset + length >
               bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize())) {
            bufs_[second].ClearBuffer();
          }
        }
      } else {
        if (DoesBufferContainData(second) &&
            !IsOffsetInBuffer(offset, second)) {
          bufs_[second].ClearBuffer();
        }
      }
    }
  }

  // If data starts from second buffer, make it curr_. Second buffer can be
  // either partial filled, full or async read is in progress.
  if (bufs_[second].async_read_in_progress_) {
    if (IsOffsetInBufferWithAsyncProgress(offset, second)) {
      curr_ = curr_ ^ 1;
    }
  } else {
    if (DoesBufferContainData(second) && IsOffsetInBuffer(offset, second)) {
      assert(bufs_[curr_].async_read_in_progress_ ||
             bufs_[curr_].buffer_.CurrentSize() == 0);
      curr_ = curr_ ^ 1;
    }
  }
}

void FilePrefetchBuffer::PollAndUpdateBuffersIfNeeded(uint64_t offset,
                                                      size_t length) {
  if (bufs_[curr_].async_read_in_progress_ && fs_ != nullptr) {
    if (bufs_[curr_].io_handle_ != nullptr) {
      // Wait for prefetch data to complete.
      // No mutex is needed as async_read_in_progress behaves as mutex and is
      // updated by main thread only.
      std::vector<void*> handles;
      handles.emplace_back(bufs_[curr_].io_handle_);
      StopWatch sw(clock_, stats_, POLL_WAIT_MICROS);
      fs_->Poll(handles, 1).PermitUncheckedError();
    }

    // Reset and Release io_handle after the Poll API as request has been
    // completed.
    DestroyAndClearIOHandle(curr_);
  }
  UpdateBuffersIfNeeded(offset, length);
}

// ReadAheadSizeTuning API calls readaheadsize_cb_
// (BlockBasedTableIterator::BlockCacheLookupForReadAheadSize) to lookup in the
// cache and tune the start and end offsets based on cache hits/misses.
//
// Arguments -
// read_curr_block   :   True if this call was due to miss in the cache and
//                         FilePrefetchBuffer wants to read that block
//                         synchronously.
//                       False if current call is to prefetch additional data in
//                         extra buffers through ReadAsync API.
// prev_buf_end_offset : End offset of the previous buffer. It's used in case
//                       of ReadAsync to make sure it doesn't read anything from
//                       previous buffer which is already prefetched.
void FilePrefetchBuffer::ReadAheadSizeTuning(
    bool read_curr_block, bool refit_tail, uint64_t prev_buf_end_offset,
    uint32_t index, size_t alignment, size_t length, size_t readahead_size,
    uint64_t& start_offset, uint64_t& end_offset, size_t& read_len,
    uint64_t& chunk_len) {
  uint64_t updated_start_offset = Rounddown(start_offset, alignment);
  uint64_t updated_end_offset =
      Roundup(start_offset + length + readahead_size, alignment);
  uint64_t initial_end_offset = updated_end_offset;
  uint64_t initial_start_offset = updated_start_offset;

  // Callback to tune the start and end offsets.
  if (readaheadsize_cb_ != nullptr && readahead_size > 0) {
    readaheadsize_cb_(read_curr_block, updated_start_offset,
                      updated_end_offset);
  }

  // read_len will be 0 and there is nothing to read/prefetch.
  if (updated_start_offset == updated_end_offset) {
    UpdateReadAheadTrimmedStat((initial_end_offset - initial_start_offset),
                               (updated_end_offset - updated_start_offset));
    return;
  }

  assert(updated_start_offset < updated_end_offset);

  if (!read_curr_block) {
    // Handle the case when callback added block handles which are already
    // prefetched and nothing new needs to be prefetched. In that case end
    // offset updated by callback will be less than prev_buf_end_offset which
    // means data has been already prefetched.
    if (updated_end_offset <= prev_buf_end_offset) {
      start_offset = end_offset = prev_buf_end_offset;
      UpdateReadAheadTrimmedStat((initial_end_offset - initial_start_offset),
                                 (end_offset - start_offset));
      return;
    }
  }

  // Realign if start and end offsets are not aligned after tuning.
  start_offset = Rounddown(updated_start_offset, alignment);
  end_offset = Roundup(updated_end_offset, alignment);

  if (!read_curr_block && start_offset < prev_buf_end_offset) {
    // Previous buffer already contains the data till prev_buf_end_offset
    // because of alignment. Update the start offset after that to avoid
    // prefetching it again.
    start_offset = prev_buf_end_offset;
  }

  uint64_t roundup_len = end_offset - start_offset;

  CalculateOffsetAndLen(alignment, start_offset, roundup_len, index, refit_tail,
                        chunk_len);
  assert(roundup_len >= chunk_len);

  // Update the buffer offset.
  bufs_[index].offset_ = start_offset;
  // Update the initial end offset of this buffer which will be the starting
  // offset of next prefetch.
  bufs_[index].initial_end_offset_ = initial_end_offset;
  read_len = static_cast<size_t>(roundup_len - chunk_len);

  UpdateReadAheadTrimmedStat((initial_end_offset - initial_start_offset),
                             (end_offset - start_offset));
}

Status FilePrefetchBuffer::HandleOverlappingData(
    const IOOptions& opts, RandomAccessFileReader* reader, uint64_t offset,
    size_t length, size_t readahead_size, bool& copy_to_third_buffer,
    uint64_t& tmp_offset, size_t& tmp_length) {
  Status s;
  size_t alignment = reader->file()->GetRequiredBufferAlignment();
  uint32_t second;

  // Check if the first buffer has the required offset and the async read is
  // still in progress. This should only happen if a prefetch was initiated
  // by Seek, but the next access is at another offset.
  if (bufs_[curr_].async_read_in_progress_ &&
      IsOffsetInBufferWithAsyncProgress(offset, curr_)) {
    PollAndUpdateBuffersIfNeeded(offset, length);
  }
  second = curr_ ^ 1;

  // If data is overlapping over two buffers, copy the data from curr_ and
  // call ReadAsync on curr_.
  if (!bufs_[curr_].async_read_in_progress_ && DoesBufferContainData(curr_) &&
      IsOffsetInBuffer(offset, curr_) &&
      (/*Data extends over curr_ buffer and second buffer either has data or in
         process of population=*/
       (offset + length > bufs_[second].offset_) &&
       (bufs_[second].async_read_in_progress_ ||
        DoesBufferContainData(second)))) {
    // Allocate new buffer to third buffer;
    bufs_[2].ClearBuffer();
    bufs_[2].buffer_.Alignment(alignment);
    bufs_[2].buffer_.AllocateNewBuffer(length);
    bufs_[2].offset_ = offset;
    copy_to_third_buffer = true;

    CopyDataToBuffer(curr_, tmp_offset, tmp_length);

    // Call async prefetching on curr_ since data has been consumed in curr_
    // only if requested data lies within second buffer.
    size_t second_size = bufs_[second].async_read_in_progress_
                             ? bufs_[second].async_req_len_
                             : bufs_[second].buffer_.CurrentSize();
    uint64_t start_offset = bufs_[second].initial_end_offset_;
    // Second buffer might be out of bound if first buffer already prefetched
    // that data.
    if (tmp_offset + tmp_length <= bufs_[second].offset_ + second_size) {
      size_t read_len = 0;
      uint64_t end_offset = start_offset, chunk_len = 0;

      ReadAheadSizeTuning(/*read_curr_block=*/false, /*refit_tail=*/false,
                          bufs_[second].offset_ + second_size, curr_, alignment,
                          /*length=*/0, readahead_size, start_offset,
                          end_offset, read_len, chunk_len);
      if (read_len > 0) {
        s = ReadAsync(opts, reader, read_len, start_offset, curr_);
        if (!s.ok()) {
          DestroyAndClearIOHandle(curr_);
          bufs_[curr_].ClearBuffer();
          return s;
        }
      }
    }
    curr_ = curr_ ^ 1;
  }
  return s;
}
// If async_io is enabled in case of sequential reads, PrefetchAsyncInternal is
// called. When buffers are switched, we clear the curr_ buffer as we assume the
// data has been consumed because of sequential reads.
// Data in buffers will always be sequential with curr_ following second and
// not vice versa.
//
// Scenarios for prefetching asynchronously:
// Case1: If both buffers are empty, prefetch n + readahead_size_/2 bytes
//        synchronously in curr_ and prefetch readahead_size_/2 async in second
//        buffer.
// Case2: If second buffer has partial or full data, make it current and
//        prefetch readahead_size_/2 async in second buffer. In case of
//        partial data, prefetch remaining bytes from size n synchronously to
//        fulfill the requested bytes request.
// Case3: If curr_ has partial data, prefetch remaining bytes from size n
//        synchronously in curr_ to fulfill the requested bytes request and
//        prefetch readahead_size_/2 bytes async in second buffer.
// Case4: (Special case) If data is in both buffers, copy requested data from
//        curr_, send async request on curr_, wait for poll to fill second
//        buffer (if any), and copy remaining data from second buffer to third
//        buffer.
Status FilePrefetchBuffer::PrefetchAsyncInternal(const IOOptions& opts,
                                                 RandomAccessFileReader* reader,
                                                 uint64_t offset, size_t length,
                                                 size_t readahead_size,
                                                 bool& copy_to_third_buffer) {
  if (!enable_) {
    return Status::OK();
  }

  TEST_SYNC_POINT("FilePrefetchBuffer::PrefetchAsyncInternal:Start");

  size_t alignment = reader->file()->GetRequiredBufferAlignment();
  Status s;
  uint64_t tmp_offset = offset;
  size_t tmp_length = length;
  size_t original_length = length;

  // 1. Abort IO and swap buffers if needed to point curr_ to first buffer with
  // data.
  if (!explicit_prefetch_submitted_) {
    AbortIOIfNeeded(offset);
  }
  UpdateBuffersIfNeeded(offset, length);

  // 2. Handle overlapping data over two buffers. If data is overlapping then
  //    during this call:
  //   - data from curr_ is copied into third buffer,
  //   - curr_ is send for async prefetching of further data if second buffer
  //     contains remaining requested data or in progress for async prefetch,
  //   - switch buffers and curr_ now points to second buffer to copy remaining
  //     data.
  s = HandleOverlappingData(opts, reader, offset, length, readahead_size,
                            copy_to_third_buffer, tmp_offset, tmp_length);
  if (!s.ok()) {
    return s;
  }

  // 3. Call Poll only if data is needed for the second buffer.
  //    - Return if whole data is in curr_ and second buffer is in progress or
  //      already full.
  //    - If second buffer is empty, it will go for ReadAsync for second buffer.
  if (!bufs_[curr_].async_read_in_progress_ && DoesBufferContainData(curr_) &&
      IsDataBlockInBuffer(offset, length, curr_)) {
    // Whole data is in curr_.
    UpdateBuffersIfNeeded(offset, length);
    if (!IsSecondBuffEligibleForPrefetching()) {
      UpdateStats(/*found_in_buffer=*/true, original_length);
      return s;
    }
  } else {
    // After poll request, curr_ might be empty because of IOError in
    // callback while reading or may contain required data.
    PollAndUpdateBuffersIfNeeded(offset, length);
  }

  if (copy_to_third_buffer) {
    offset = tmp_offset;
    length = tmp_length;
  }

  // 4. After polling and swapping buffers, if all the requested bytes are in
  // curr_, it will only go for async prefetching.
  // copy_to_third_buffer is a special case so it will be handled separately.
  if (!copy_to_third_buffer && DoesBufferContainData(curr_) &&
      IsDataBlockInBuffer(offset, length, curr_)) {
    offset += length;
    length = 0;

    // Since async request was submitted directly by calling PrefetchAsync in
    // last call, we don't need to prefetch further as this call is to poll
    // the data submitted in previous call.
    if (explicit_prefetch_submitted_) {
      return s;
    }
    if (!IsSecondBuffEligibleForPrefetching()) {
      UpdateStats(/*found_in_buffer=*/true, original_length);
      return s;
    }
  }

  uint32_t second = curr_ ^ 1;
  assert(!bufs_[curr_].async_read_in_progress_);

  // In case because of some IOError curr_ got empty, abort IO for second as
  // well. Otherwise data might not align if more data needs to be read in curr_
  // which might overlap with second buffer.
  if (!DoesBufferContainData(curr_) && bufs_[second].async_read_in_progress_) {
    if (bufs_[second].io_handle_ != nullptr) {
      std::vector<void*> handles;
      handles.emplace_back(bufs_[second].io_handle_);
      {
        StopWatch sw(clock_, stats_, ASYNC_PREFETCH_ABORT_MICROS);
        Status status = fs_->AbortIO(handles);
        assert(status.ok());
      }
    }
    DestroyAndClearIOHandle(second);
    bufs_[second].ClearBuffer();
  }

  // 5. Data is overlapping i.e. some of the data has been copied to third
  // buffer and remaining will be updated below.
  if (copy_to_third_buffer && DoesBufferContainData(curr_)) {
    CopyDataToBuffer(curr_, offset, length);

    // Length == 0: All the requested data has been copied to third buffer and
    // it has already gone for async prefetching. It can return without doing
    // anything further.
    // Length > 0: More data needs to be consumed so it will continue async
    // and sync prefetching and copy the remaining data to third buffer in the
    // end.
    if (length == 0) {
      UpdateStats(/*found_in_buffer=*/true, original_length);
      return s;
    }
  }

  // 6. Go for ReadAsync and Read (if needed).
  assert(!bufs_[second].async_read_in_progress_ &&
         !DoesBufferContainData(second));

  // offset and size alignment for curr_ buffer with synchronous prefetching
  uint64_t start_offset1 = offset, end_offset1 = 0, chunk_len1 = 0;
  size_t read_len1 = 0;

  // For length == 0, skip the synchronous prefetching. read_len1 will be 0.
  if (length > 0) {
    ReadAheadSizeTuning(/*read_curr_block=*/true, /*refit_tail=*/false,
                        start_offset1, curr_, alignment, length, readahead_size,
                        start_offset1, end_offset1, read_len1, chunk_len1);
    UpdateStats(/*found_in_buffer=*/false,
                /*length_found=*/original_length - length);
  } else {
    end_offset1 = bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize();
    UpdateStats(/*found_in_buffer=*/true, original_length);
  }

  // Prefetch in second buffer only if readahead_size > 0.
  if (readahead_size > 0) {
    // offset and size alignment for second buffer for asynchronous
    // prefetching.
    uint64_t start_offset2 = bufs_[curr_].initial_end_offset_;

      // Find updated readahead size after tuning
      size_t read_len2 = 0;
      uint64_t end_offset2 = start_offset2, chunk_len2 = 0;
      ReadAheadSizeTuning(/*read_curr_block=*/false, /*refit_tail=*/false,
                          /*prev_buf_end_offset=*/end_offset1, second,
                          alignment,
                          /*length=*/0, readahead_size, start_offset2,
                          end_offset2, read_len2, chunk_len2);
      if (read_len2 > 0) {
        s = ReadAsync(opts, reader, read_len2, start_offset2, second);
        if (!s.ok()) {
          DestroyAndClearIOHandle(second);
          bufs_[second].ClearBuffer();
          return s;
        }
    }
  }

  if (read_len1 > 0) {
    s = Read(opts, reader, read_len1, chunk_len1, start_offset1, curr_);
    if (!s.ok()) {
      if (bufs_[second].io_handle_ != nullptr) {
        std::vector<void*> handles;
        handles.emplace_back(bufs_[second].io_handle_);
        {
          StopWatch sw(clock_, stats_, ASYNC_PREFETCH_ABORT_MICROS);
          Status status = fs_->AbortIO(handles);
          assert(status.ok());
        }
      }
      DestroyAndClearIOHandle(second);
      bufs_[second].ClearBuffer();
      bufs_[curr_].ClearBuffer();
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
                                          bool for_compaction /* = false */) {
  bool ret = TryReadFromCacheUntracked(opts, reader, offset, n, result, status,
                                       for_compaction);
  if (usage_ == FilePrefetchBufferUsage::kTableOpenPrefetchTail && enable_) {
    if (ret) {
      RecordTick(stats_, TABLE_OPEN_PREFETCH_TAIL_HIT);
    } else {
      RecordTick(stats_, TABLE_OPEN_PREFETCH_TAIL_MISS);
    }
  }
  return ret;
}

bool FilePrefetchBuffer::TryReadFromCacheUntracked(
    const IOOptions& opts, RandomAccessFileReader* reader, uint64_t offset,
    size_t n, Slice* result, Status* status,
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
        s = Prefetch(opts, reader, offset, std::max(n, readahead_size_));
      } else {
        if (IsOffsetInBuffer(offset, curr_)) {
          RecordTick(stats_, PREFETCH_BYTES_USEFUL,
                     bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize() -
                         offset);
        }
        if (implicit_auto_readahead_) {
          if (!IsEligibleForPrefetch(offset, n)) {
            // Ignore status as Prefetch is not called.
            s.PermitUncheckedError();
            return false;
          }
        }
        s = Prefetch(opts, reader, offset, n + readahead_size_);
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
  } else if (!for_compaction) {
    RecordTick(stats_, PREFETCH_HITS);
    RecordTick(stats_, PREFETCH_BYTES_USEFUL, n);
  }
  UpdateReadPattern(offset, n, false /*decrease_readaheadsize*/);

  uint64_t offset_in_buffer = offset - bufs_[curr_].offset_;
  *result = Slice(bufs_[curr_].buffer_.BufferStart() + offset_in_buffer, n);
  return true;
}

bool FilePrefetchBuffer::TryReadFromCacheAsync(const IOOptions& opts,
                                               RandomAccessFileReader* reader,
                                               uint64_t offset, size_t n,
                                               Slice* result, Status* status) {
  bool ret =
      TryReadFromCacheAsyncUntracked(opts, reader, offset, n, result, status);
  if (usage_ == FilePrefetchBufferUsage::kTableOpenPrefetchTail && enable_) {
    if (ret) {
      RecordTick(stats_, TABLE_OPEN_PREFETCH_TAIL_HIT);
    } else {
      RecordTick(stats_, TABLE_OPEN_PREFETCH_TAIL_MISS);
    }
  }
  return ret;
}

bool FilePrefetchBuffer::TryReadFromCacheAsyncUntracked(
    const IOOptions& opts, RandomAccessFileReader* reader, uint64_t offset,
    size_t n, Slice* result, Status* status) {
  if (track_min_offset_ && offset < min_offset_read_) {
    min_offset_read_ = static_cast<size_t>(offset);
  }

  if (!enable_) {
    return false;
  }

  if (explicit_prefetch_submitted_) {
    // explicit_prefetch_submitted_ is special case where it expects request
    // submitted in PrefetchAsync should match with this request. Otherwise
    // buffers will be outdated.
    // Random offset called. So abort the IOs.
    if (prev_offset_ != offset) {
      AbortAllIOs();
      bufs_[curr_].ClearBuffer();
      bufs_[curr_ ^ 1].ClearBuffer();
      explicit_prefetch_submitted_ = false;
      return false;
    }
  }

  if (!explicit_prefetch_submitted_ && offset < bufs_[curr_].offset_) {
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

  if (explicit_prefetch_submitted_ ||
      (bufs_[curr_].async_read_in_progress_ ||
       offset + n >
           bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize())) {
    // In case readahead_size is trimmed (=0), we still want to poll the data
    // submitted with explicit_prefetch_submitted_=true.
    if (readahead_size_ > 0 || explicit_prefetch_submitted_) {
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
                                copy_to_third_buffer);
      explicit_prefetch_submitted_ = false;
      if (!s.ok()) {
        if (status) {
          *status = s;
        }
#ifndef NDEBUG
        IGNORE_STATUS_IF_ERROR(s);
#endif
        return false;
      }
      prefetched = explicit_prefetch_submitted_ ? false : true;
    } else {
      return false;
    }
  } else {
    UpdateStats(/*found_in_buffer=*/true, n);
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
                                               void* cb_arg) {
  uint32_t index = *(static_cast<uint32_t*>(cb_arg));
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
      // All requested bytes are already in the buffer or no data is read
      // because of EOF. So no need to update.
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
                                         Slice* result) {
  assert(reader != nullptr);
  if (!enable_) {
    return Status::NotSupported();
  }

  TEST_SYNC_POINT("FilePrefetchBuffer::PrefetchAsync:Start");

  num_file_reads_ = 0;
  explicit_prefetch_submitted_ = false;
  bool is_eligible_for_prefetching = false;

  if (readahead_size_ > 0 &&
      (!implicit_auto_readahead_ ||
       num_file_reads_ >= num_file_reads_for_auto_readahead_)) {
      is_eligible_for_prefetching = true;
  }

  // 1. Cancel any pending async read to make code simpler as buffers can be out
  // of sync.
  AbortAllIOs();

  // 2. Clear outdated data.
  UpdateBuffersIfNeeded(offset, n);
  uint32_t second = curr_ ^ 1;

  // - Since PrefetchAsync can be called on non sequential reads. So offset can
  //   be less than curr_ buffers' offset. In that case it clears both
  //   buffers.
  // - In case of tuning of readahead_size, on Reseek, we have to clear both
  //   buffers otherwise, we may end up with inconsistent BlockHandles in queue
  //   and data in buffer.
  if (readaheadsize_cb_ != nullptr ||
      (DoesBufferContainData(curr_) && !IsOffsetInBuffer(offset, curr_))) {
    bufs_[curr_].ClearBuffer();
    bufs_[second].ClearBuffer();
  }

  UpdateReadPattern(offset, n, /*decrease_readaheadsize=*/false);

  bool data_found = false;

  // 3. If curr_ has full data.
  if (DoesBufferContainData(curr_) && IsDataBlockInBuffer(offset, n, curr_)) {
    uint64_t offset_in_buffer = offset - bufs_[curr_].offset_;
    *result = Slice(bufs_[curr_].buffer_.BufferStart() + offset_in_buffer, n);
    data_found = true;
    UpdateStats(/*found_in_buffer=*/true, n);

    // Update num_file_reads_ as TryReadFromCacheAsync won't be called for
    // poll and update num_file_reads_ if data is found.
    num_file_reads_++;

    // 3.1 If second also has some data or is not eligible for prefetching,
    // return.
    if (!is_eligible_for_prefetching || DoesBufferContainData(second)) {
      return Status::OK();
    }
  } else {
    // Partial data in curr_.
    bufs_[curr_].ClearBuffer();
  }
  bufs_[second].ClearBuffer();

  std::string msg;

  Status s;
  size_t alignment = reader->file()->GetRequiredBufferAlignment();
  size_t readahead_size = is_eligible_for_prefetching ? readahead_size_ / 2 : 0;
  size_t offset_to_read = static_cast<size_t>(offset);
  uint64_t start_offset1 = offset, end_offset1 = 0, start_offset2 = 0,
           chunk_len1 = 0;
  size_t read_len1 = 0, read_len2 = 0;

  // - If curr_ is empty.
  //   - Call async read for full data +  readahead_size on curr_.
  //   - Call async read for readahead_size on second if eligible.
  // - If curr_ is filled.
  //   - readahead_size on second.
  // Calculate length and offsets for reading.
  if (!DoesBufferContainData(curr_)) {
    uint64_t roundup_len1;
    // Prefetch full data + readahead_size in curr_.
    if (is_eligible_for_prefetching || reader->use_direct_io()) {
      ReadAheadSizeTuning(/*read_curr_block=*/true, /*refit_tail=*/false,
                          /*prev_buf_end_offset=*/start_offset1, curr_,
                          alignment, n, readahead_size, start_offset1,
                          end_offset1, read_len1, chunk_len1);
    } else {
      // No alignment or extra prefetching.
      start_offset1 = offset_to_read;
      end_offset1 = offset_to_read + n;
      roundup_len1 = end_offset1 - start_offset1;
      CalculateOffsetAndLen(alignment, start_offset1, roundup_len1, curr_,
                            false, chunk_len1);
      assert(chunk_len1 == 0);
      assert(roundup_len1 >= chunk_len1);
      read_len1 = static_cast<size_t>(roundup_len1);
      bufs_[curr_].offset_ = start_offset1;
    }
  }

  if (is_eligible_for_prefetching) {
    start_offset2 = bufs_[curr_].initial_end_offset_;
    // Second buffer might be out of bound if first buffer already prefetched
    // that data.

      uint64_t end_offset2 = start_offset2, chunk_len2 = 0;
      ReadAheadSizeTuning(/*read_curr_block=*/false, /*refit_tail=*/false,
                          /*prev_buf_end_offset=*/end_offset1, second,
                          alignment,
                          /*length=*/0, readahead_size, start_offset2,
                          end_offset2, read_len2, chunk_len2);
  }

  if (read_len1) {
    s = ReadAsync(opts, reader, read_len1, start_offset1, curr_);
    if (!s.ok()) {
      DestroyAndClearIOHandle(curr_);
      bufs_[curr_].ClearBuffer();
      return s;
    }
    explicit_prefetch_submitted_ = true;
    prev_len_ = 0;
  }

  if (read_len2) {
    TEST_SYNC_POINT("FilePrefetchBuffer::PrefetchAsync:ExtraPrefetching");
    s = ReadAsync(opts, reader, read_len2, start_offset2, second);
    if (!s.ok()) {
      DestroyAndClearIOHandle(second);
      bufs_[second].ClearBuffer();
      return s;
    }
    readahead_size_ = std::min(max_readahead_size_, readahead_size_ * 2);
  }
  return (data_found ? Status::OK() : Status::TryAgain());
}

}  // namespace ROCKSDB_NAMESPACE
