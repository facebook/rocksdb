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

void FilePrefetchBuffer::PrepareBufferForRead(BufferInfo* buf, size_t alignment,
                                              uint64_t offset,
                                              size_t roundup_len,
                                              bool refit_tail,
                                              uint64_t& aligned_useful_len) {
  uint64_t aligned_useful_offset_in_buf = 0;
  bool copy_data_to_new_buffer = false;
  // Check if requested bytes are in the existing buffer_.
  // If only a few bytes exist -- reuse them & read only what is really needed.
  //     This is typically the case of incremental reading of data.
  // If no bytes exist in buffer -- full pread.
  if (buf->DoesBufferContainData() && buf->IsOffsetInBuffer(offset)) {
    // Only a few requested bytes are in the buffer. memmove those chunk of
    // bytes to the beginning, and memcpy them back into the new buffer if a
    // new buffer is created.
    aligned_useful_offset_in_buf =
        Rounddown(static_cast<size_t>(offset - buf->offset_), alignment);
    aligned_useful_len = static_cast<uint64_t>(buf->CurrentSize()) -
                         aligned_useful_offset_in_buf;
    assert(aligned_useful_offset_in_buf % alignment == 0);
    assert(aligned_useful_len % alignment == 0);
    assert(aligned_useful_offset_in_buf + aligned_useful_len <=
           buf->offset_ + buf->CurrentSize());
    if (aligned_useful_len > 0) {
      copy_data_to_new_buffer = true;
    } else {
      // this reset is not necessary, but just to be safe.
      aligned_useful_offset_in_buf = 0;
    }
  }

  // Create a new buffer only if current capacity is not sufficient, and memcopy
  // bytes from old buffer if needed (i.e., if aligned_useful_len is greater
  // than 0).
  if (buf->buffer_.Capacity() < roundup_len) {
    buf->buffer_.Alignment(alignment);
    buf->buffer_.AllocateNewBuffer(
        static_cast<size_t>(roundup_len), copy_data_to_new_buffer,
        aligned_useful_offset_in_buf, static_cast<size_t>(aligned_useful_len));
  } else if (aligned_useful_len > 0 && refit_tail) {
    // New buffer not needed. But memmove bytes from tail to the beginning since
    // aligned_useful_len is greater than 0.
    buf->buffer_.RefitTail(static_cast<size_t>(aligned_useful_offset_in_buf),
                           static_cast<size_t>(aligned_useful_len));
  } else if (aligned_useful_len > 0) {
    // For async prefetching, it doesn't call RefitTail with aligned_useful_len
    // > 0. Allocate new buffer if needed because aligned buffer calculate
    // remaining buffer as capacity - cursize which might not be the case in
    // this as it's not refitting.
    // TODO: Use refit_tail for async prefetching too.
    buf->buffer_.Alignment(alignment);
    buf->buffer_.AllocateNewBuffer(
        static_cast<size_t>(roundup_len), copy_data_to_new_buffer,
        aligned_useful_offset_in_buf, static_cast<size_t>(aligned_useful_len));
  }
}

Status FilePrefetchBuffer::Read(BufferInfo* buf, const IOOptions& opts,
                                RandomAccessFileReader* reader,
                                uint64_t read_len, uint64_t aligned_useful_len,
                                uint64_t start_offset) {
  Slice result;
  char* to_buf = buf->buffer_.BufferStart() + aligned_useful_len;
  Status s = reader->Read(opts, start_offset + aligned_useful_len, read_len,
                          &result, to_buf, /*aligned_buf=*/nullptr);
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
  // Update the buffer size.
  buf->buffer_.Size(static_cast<size_t>(aligned_useful_len) + result.size());
  return s;
}

Status FilePrefetchBuffer::ReadAsync(BufferInfo* buf, const IOOptions& opts,
                                     RandomAccessFileReader* reader,
                                     uint64_t read_len, uint64_t start_offset) {
  TEST_SYNC_POINT("FilePrefetchBuffer::ReadAsync");
  // callback for async read request.
  auto fp = std::bind(&FilePrefetchBuffer::PrefetchAsyncCallback, this,
                      std::placeholders::_1, std::placeholders::_2);
  FSReadRequest req;
  Slice result;
  req.len = read_len;
  req.offset = start_offset;
  req.result = result;
  req.scratch = buf->buffer_.BufferStart();
  buf->async_req_len_ = req.len;

  Status s = reader->ReadAsync(req, opts, fp, buf, &(buf->io_handle_),
                               &(buf->del_fn_), /*aligned_buf =*/nullptr);
  req.status.PermitUncheckedError();
  if (s.ok()) {
    RecordTick(stats_, PREFETCH_BYTES, read_len);
    buf->async_read_in_progress_ = true;
  }
  return s;
}

Status FilePrefetchBuffer::Prefetch(const IOOptions& opts,
                                    RandomAccessFileReader* reader,
                                    uint64_t offset, size_t n) {
  if (!enable_ || reader == nullptr) {
    return Status::OK();
  }

  assert(num_buffers_ == 1);

  AllocateBufferIfEmpty();
  BufferInfo* buf = GetFirstBuffer();

  TEST_SYNC_POINT("FilePrefetchBuffer::Prefetch:Start");

  if (offset + n <= buf->offset_ + buf->CurrentSize()) {
    // All requested bytes are already in the buffer. So no need to Read again.
    return Status::OK();
  }

  size_t alignment = reader->file()->GetRequiredBufferAlignment();
  uint64_t rounddown_offset = offset, roundup_end = 0, aligned_useful_len = 0;
  size_t read_len = 0;

  ReadAheadSizeTuning(buf, /*read_curr_block=*/true,
                      /*refit_tail=*/true, rounddown_offset, alignment, 0, n,
                      rounddown_offset, roundup_end, read_len,
                      aligned_useful_len);

  Status s;
  if (read_len > 0) {
    s = Read(buf, opts, reader, read_len, aligned_useful_len, rounddown_offset);
  }

  if (usage_ == FilePrefetchBufferUsage::kTableOpenPrefetchTail && s.ok()) {
    RecordInHistogram(stats_, TABLE_OPEN_PREFETCH_TAIL_READ_BYTES, read_len);
  }
  return s;
}

// Copy data from src to overlap_buf_.
void FilePrefetchBuffer::CopyDataToBuffer(BufferInfo* src, uint64_t& offset,
                                          size_t& length) {
  if (length == 0) {
    return;
  }

  uint64_t copy_offset = (offset - src->offset_);
  size_t copy_len = 0;
  if (src->IsDataBlockInBuffer(offset, length)) {
    // All the bytes are in src.
    copy_len = length;
  } else {
    copy_len = src->CurrentSize() - copy_offset;
  }

  BufferInfo* dst = overlap_buf_;
  memcpy(dst->buffer_.BufferStart() + dst->CurrentSize(),
         src->buffer_.BufferStart() + copy_offset, copy_len);

  dst->buffer_.Size(dst->CurrentSize() + copy_len);

  // Update offset and length.
  offset += copy_len;
  length -= copy_len;

  // length > 0 indicates it has consumed all data from the src buffer and it
  // still needs to read more other buffer.
  if (length > 0) {
    FreeFrontBuffer();
  }
}

// Clear the buffers if it contains outdated data. Outdated data can be because
// previous sequential reads were read from the cache instead of these buffer.
// In that case outdated IOs should be aborted.
void FilePrefetchBuffer::AbortOutdatedIO(uint64_t offset) {
  std::vector<void*> handles;
  std::vector<BufferInfo*> tmp_buf;
  for (auto& buf : bufs_) {
    if (buf->IsBufferOutdatedWithAsyncProgress(offset)) {
      handles.emplace_back(buf->io_handle_);
      tmp_buf.emplace_back(buf);
    }
  }

  if (!handles.empty()) {
    StopWatch sw(clock_, stats_, ASYNC_PREFETCH_ABORT_MICROS);
    Status s = fs_->AbortIO(handles);
    assert(s.ok());
  }

  for (auto& buf : tmp_buf) {
    if (buf->async_read_in_progress_) {
      DestroyAndClearIOHandle(buf);
      buf->async_read_in_progress_ = false;
    }
    buf->ClearBuffer();
  }
}

void FilePrefetchBuffer::AbortAllIOs() {
  std::vector<void*> handles;
  for (auto& buf : bufs_) {
    if (buf->async_read_in_progress_ && buf->io_handle_ != nullptr) {
      handles.emplace_back(buf->io_handle_);
    }
  }
  if (!handles.empty()) {
    StopWatch sw(clock_, stats_, ASYNC_PREFETCH_ABORT_MICROS);
    Status s = fs_->AbortIO(handles);
    assert(s.ok());
  }

  for (auto& buf : bufs_) {
    if (buf->io_handle_ != nullptr && buf->del_fn_ != nullptr) {
      DestroyAndClearIOHandle(buf);
    }
    buf->async_read_in_progress_ = false;
  }
}

// Clear the buffers if it contains outdated data wrt offset. Outdated data can
// be because previous sequential reads were read from the cache instead of
// these buffer or there is IOError while filling the buffers.
//
// offset - the offset requested to be read. This API makes sure that the
// front/first buffer in bufs_ should contain this offset, otherwise, all
// buffers will be freed.
void FilePrefetchBuffer::ClearOutdatedData(uint64_t offset, size_t length) {
  while (!IsBufferQueueEmpty()) {
    BufferInfo* buf = GetFirstBuffer();
    // Offset is greater than this buffer's end offset.
    if (buf->IsBufferOutdated(offset)) {
      FreeFrontBuffer();
    } else {
      break;
    }
  }

  if (IsBufferQueueEmpty() || NumBuffersAllocated() == 1) {
    return;
  }

  BufferInfo* buf = GetFirstBuffer();

  if (buf->async_read_in_progress_) {
    FreeEmptyBuffers();
    return;
  }

  // Below handles the case for Overlapping buffers (NumBuffersAllocated > 1).
  bool abort_io = false;

  if (buf->DoesBufferContainData() && buf->IsOffsetInBuffer(offset)) {
    BufferInfo* next_buf = bufs_[1];
    if (/* next buffer doesn't align with first buffer and requested data
       overlaps with next buffer */
        ((buf->offset_ + buf->CurrentSize() != next_buf->offset_) &&
         (offset + length > buf->offset_ + buf->CurrentSize()))) {
      abort_io = true;
    }
  } else {
    // buffer with offset doesn't contain data or offset doesn't lie in this
    // buffer.
    buf->ClearBuffer();
    abort_io = true;
  }

  if (abort_io) {
    AbortAllIOs();
    // Clear all buffers after first.
    for (size_t i = 1; i < bufs_.size(); ++i) {
      bufs_[i]->ClearBuffer();
    }
  }
  FreeEmptyBuffers();
  assert(IsBufferQueueEmpty() || buf->IsOffsetInBuffer(offset));
}

void FilePrefetchBuffer::PollIfNeeded(uint64_t offset, size_t length) {
  BufferInfo* buf = GetFirstBuffer();

  if (buf->async_read_in_progress_ && fs_ != nullptr) {
    if (buf->io_handle_ != nullptr) {
      // Wait for prefetch data to complete.
      // No mutex is needed as async_read_in_progress behaves as mutex and is
      // updated by main thread only.
      std::vector<void*> handles;
      handles.emplace_back(buf->io_handle_);
      StopWatch sw(clock_, stats_, POLL_WAIT_MICROS);
      fs_->Poll(handles, 1).PermitUncheckedError();
    }

    // Reset and Release io_handle after the Poll API as request has been
    // completed.
    DestroyAndClearIOHandle(buf);
  }

  // Always call outdated data after Poll as Buffers might be out of sync w.r.t
  // offset and length.
  ClearOutdatedData(offset, length);
}

// ReadAheadSizeTuning API calls readaheadsize_cb_
// (BlockBasedTableIterator::BlockCacheLookupForReadAheadSize) to lookup in the
// cache and tune the start and end offsets based on cache hits/misses.
//
// Arguments -
// read_curr_block   :   True if this call was due to miss in the cache and
//                       FilePrefetchBuffer wants to read that block
//                       synchronously.
//                       False if current call is to prefetch additional data in
//                       extra buffers through ReadAsync API.
// prev_buf_end_offset : End offset of the previous buffer. It's used in case
//                       of ReadAsync to make sure it doesn't read anything from
//                       previous buffer which is already prefetched.
void FilePrefetchBuffer::ReadAheadSizeTuning(
    BufferInfo* buf, bool read_curr_block, bool refit_tail,
    uint64_t prev_buf_end_offset, size_t alignment, size_t length,
    size_t readahead_size, uint64_t& start_offset, uint64_t& end_offset,
    size_t& read_len, uint64_t& aligned_useful_len) {
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
    start_offset = end_offset = updated_start_offset;
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

  PrepareBufferForRead(buf, alignment, start_offset, roundup_len, refit_tail,
                       aligned_useful_len);
  assert(roundup_len >= aligned_useful_len);

  // Update the buffer offset.
  buf->offset_ = start_offset;
  // Update the initial end offset of this buffer which will be the starting
  // offset of next prefetch.
  buf->initial_end_offset_ = initial_end_offset;
  read_len = static_cast<size_t>(roundup_len - aligned_useful_len);

  UpdateReadAheadTrimmedStat((initial_end_offset - initial_start_offset),
                             (end_offset - start_offset));
}

// If data is overlapping between two buffers then during this call:
//   - data from first buffer is copied into overlapping buffer,
//   - first is removed from bufs_ and freed so that it can be used for async
//     prefetching of further data.
Status FilePrefetchBuffer::HandleOverlappingData(
    const IOOptions& opts, RandomAccessFileReader* reader, uint64_t offset,
    size_t length, size_t readahead_size, bool& copy_to_overlap_buffer,
    uint64_t& tmp_offset, size_t& tmp_length) {
  // No Overlapping of data between 2 buffers.
  if (IsBufferQueueEmpty() || NumBuffersAllocated() == 1) {
    return Status::OK();
  }

  Status s;
  size_t alignment = reader->file()->GetRequiredBufferAlignment();

  BufferInfo* buf = GetFirstBuffer();

  // Check if the first buffer has the required offset and the async read is
  // still in progress. This should only happen if a prefetch was initiated
  // by Seek, but the next access is at another offset.
  if (buf->async_read_in_progress_ &&
      buf->IsOffsetInBufferWithAsyncProgress(offset)) {
    PollIfNeeded(offset, length);
  }

  if (IsBufferQueueEmpty() || NumBuffersAllocated() == 1) {
    return Status::OK();
  }

  BufferInfo* next_buf = bufs_[1];

  // If data is overlapping over two buffers, copy the data from front and
  // call ReadAsync on freed buffer.
  if (!buf->async_read_in_progress_ && buf->DoesBufferContainData() &&
      buf->IsOffsetInBuffer(offset) &&
      (/*Data extends over two buffers and second buffer either has data or in
         process of population=*/
       (offset + length > next_buf->offset_) &&
       (next_buf->async_read_in_progress_ ||
        next_buf->DoesBufferContainData()))) {
    // Allocate new buffer to overlap_buf_.
    overlap_buf_->ClearBuffer();
    overlap_buf_->buffer_.Alignment(alignment);
    overlap_buf_->buffer_.AllocateNewBuffer(length);
    overlap_buf_->offset_ = offset;
    copy_to_overlap_buffer = true;

    CopyDataToBuffer(buf, tmp_offset, tmp_length);
    UpdateStats(/*found_in_buffer=*/false, overlap_buf_->CurrentSize());

    // Call async prefetching on freed buffer since data has been consumed
    // only if requested data lies within next buffer.
    size_t second_size = next_buf->async_read_in_progress_
                             ? next_buf->async_req_len_
                             : next_buf->CurrentSize();
    uint64_t start_offset = next_buf->initial_end_offset_;

    // If requested bytes - tmp_offset + tmp_length are in next buffer, freed
    // buffer can go for further prefetching.
    // If requested bytes are not in next buffer, next buffer has to go for sync
    // call to get remaining requested bytes. In that case it shouldn't go for
    // async prefetching as async prefetching calculates offset based on
    // previous buffer end offset and previous buffer has to go for sync
    // prefetching.

    if (tmp_offset + tmp_length <= next_buf->offset_ + second_size) {
      AllocateBuffer();
      BufferInfo* new_buf = GetLastBuffer();
      size_t read_len = 0;
      uint64_t end_offset = start_offset, aligned_useful_len = 0;

      ReadAheadSizeTuning(new_buf, /*read_curr_block=*/false,
                          /*refit_tail=*/false, next_buf->offset_ + second_size,
                          alignment,
                          /*length=*/0, readahead_size, start_offset,
                          end_offset, read_len, aligned_useful_len);
      if (read_len > 0) {
        s = ReadAsync(new_buf, opts, reader, read_len, start_offset);
        if (!s.ok()) {
          DestroyAndClearIOHandle(new_buf);
          FreeLastBuffer();
          return s;
        }
      }
    }
  }
  return s;
}

// When data is outdated, we clear the first buffer and free it as the
// data has been consumed because of sequential reads.
//
// Scenarios for prefetching asynchronously:
// Case1: If all buffers are in free_bufs_, prefetch n + readahead_size_/2 bytes
//        synchronously in first buffer and prefetch readahead_size_/2 async in
//        remaining buffers (num_buffers_ -1 ).
// Case2: If first buffer has partial data, prefetch readahead_size_/2 async in
//        remaining buffers. In case of partial data, prefetch remaining bytes
//        from size n synchronously to fulfill the requested bytes request.
// Case5: (Special case) If data is overlapping in two buffers, copy requested
//        data from first, free that buffer to send for async request, wait for
//        poll to fill next buffer (if any), and copy remaining data from that
//        buffer to overlap buffer.
Status FilePrefetchBuffer::PrefetchInternal(const IOOptions& opts,
                                            RandomAccessFileReader* reader,
                                            uint64_t offset, size_t length,
                                            size_t readahead_size,
                                            bool& copy_to_overlap_buffer) {
  if (!enable_) {
    return Status::OK();
  }

  TEST_SYNC_POINT("FilePrefetchBuffer::Prefetch:Start");

  size_t alignment = reader->file()->GetRequiredBufferAlignment();
  Status s;
  uint64_t tmp_offset = offset;
  size_t tmp_length = length;
  size_t original_length = length;

  // Abort outdated IO.
  if (!explicit_prefetch_submitted_) {
    AbortOutdatedIO(offset);
    FreeEmptyBuffers();
  }
  ClearOutdatedData(offset, length);

  // Handle overlapping data over two buffers.
  s = HandleOverlappingData(opts, reader, offset, length, readahead_size,
                            copy_to_overlap_buffer, tmp_offset, tmp_length);
  if (!s.ok()) {
    return s;
  }

  AllocateBufferIfEmpty();
  BufferInfo* buf = GetFirstBuffer();

  // Call Poll only if data is needed for the second buffer.
  //    - Return if whole data is in first and second buffer is in progress or
  //      already full.
  //    - If second buffer is empty, it will go for ReadAsync for second buffer.
  if (!buf->async_read_in_progress_ && buf->DoesBufferContainData() &&
      buf->IsDataBlockInBuffer(offset, length)) {
    // Whole data is in buffer.
    if (!IsEligibleForFurtherPrefetching()) {
      UpdateStats(/*found_in_buffer=*/true, original_length);
      return s;
    }
  } else {
    PollIfNeeded(tmp_offset, tmp_length);
  }

  AllocateBufferIfEmpty();
  buf = GetFirstBuffer();
  offset = tmp_offset;
  length = tmp_length;

  // After polling, if all the requested bytes are in first buffer, it will only
  // go for async prefetching.
  if (buf->DoesBufferContainData()) {
    if (copy_to_overlap_buffer) {
      // Data is overlapping i.e. some of the data has been copied to overlap
      // buffer and remaining will be updated below.
      size_t initial_buf_size = overlap_buf_->CurrentSize();
      CopyDataToBuffer(buf, offset, length);
      UpdateStats(
          /*found_in_buffer=*/false,
          overlap_buf_->CurrentSize() - initial_buf_size);

      // Length == 0: All the requested data has been copied to overlap buffer
      // and it has already gone for async prefetching. It can return without
      // doing anything further.
      // Length > 0: More data needs to be consumed so it will continue async
      // and sync prefetching and copy the remaining data to overlap buffer in
      // the end.
      if (length == 0) {
        UpdateStats(/*found_in_buffer=*/true, length);
        return s;
      }
    } else {
      if (buf->IsDataBlockInBuffer(offset, length)) {
        offset += length;
        length = 0;
        // Since async request was submitted directly by calling PrefetchAsync
        // in last call, we don't need to prefetch further as this call is to
        // poll the data submitted in previous call.
        if (explicit_prefetch_submitted_) {
          return s;
        }
        if (!IsEligibleForFurtherPrefetching()) {
          UpdateStats(/*found_in_buffer=*/true, original_length);
          return s;
        }
      }
    }
  }

  AllocateBufferIfEmpty();
  buf = GetFirstBuffer();

  assert(!buf->async_read_in_progress_);

  // Go for ReadAsync and Read (if needed).
  // offset and size alignment for first buffer with synchronous prefetching
  uint64_t start_offset1 = offset, end_offset1 = 0, aligned_useful_len1 = 0;
  size_t read_len1 = 0;

  // For length == 0, skip the synchronous prefetching. read_len1 will be 0.
  if (length > 0) {
    if (buf->IsOffsetInBuffer(offset)) {
      UpdateStats(/*found_in_buffer=*/false,
                  (buf->offset_ + buf->CurrentSize() - offset));
    }
    ReadAheadSizeTuning(buf, /*read_curr_block=*/true, /*refit_tail*/
                        true, start_offset1, alignment, length, readahead_size,
                        start_offset1, end_offset1, read_len1,
                        aligned_useful_len1);
  } else {
    UpdateStats(/*found_in_buffer=*/true, original_length);
  }

  // Prefetch in remaining buffer only if readahead_size > 0.
  if (readahead_size > 0) {
    s = PrefetchRemBuffers(opts, reader, end_offset1, alignment,
                           readahead_size);
    if (!s.ok()) {
      return s;
    }
  }

  if (read_len1 > 0) {
    s = Read(buf, opts, reader, read_len1, aligned_useful_len1, start_offset1);
    if (!s.ok()) {
      AbortAllIOs();
      FreeAllBuffers();
      return s;
    }
  }

  // Copy remaining requested bytes to overlap_buffer. No need to update stats
  // as data is prefetched during this call.
  if (copy_to_overlap_buffer && length > 0) {
    CopyDataToBuffer(buf, offset, length);
  }
  return s;
}

bool FilePrefetchBuffer::TryReadFromCache(const IOOptions& opts,
                                          RandomAccessFileReader* reader,
                                          uint64_t offset, size_t n,
                                          Slice* result, Status* status,
                                          bool for_compaction) {
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
    size_t n, Slice* result, Status* status, bool for_compaction) {
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
      FreeAllBuffers();
      explicit_prefetch_submitted_ = false;
      return false;
    }
  }

  AllocateBufferIfEmpty();
  BufferInfo* buf = GetFirstBuffer();

  if (!explicit_prefetch_submitted_ && offset < buf->offset_) {
    return false;
  }

  bool prefetched = false;
  bool copy_to_overlap_buffer = false;
  // If the buffer contains only a few of the requested bytes:
  //    If readahead is enabled: prefetch the remaining bytes + readahead
  //    bytes
  //        and satisfy the request.
  //    If readahead is not enabled: return false.
  TEST_SYNC_POINT_CALLBACK("FilePrefetchBuffer::TryReadFromCache",
                           &readahead_size_);

  if (explicit_prefetch_submitted_ ||
      (buf->async_read_in_progress_ ||
       offset + n > buf->offset_ + buf->CurrentSize())) {
    // In case readahead_size is trimmed (=0), we still want to poll the data
    // submitted with explicit_prefetch_submitted_=true.
    if (readahead_size_ > 0 || explicit_prefetch_submitted_) {
      Status s;
      assert(reader != nullptr);
      assert(max_readahead_size_ >= readahead_size_);

      if (for_compaction) {
        s = Prefetch(opts, reader, offset, std::max(n, readahead_size_));
      } else {
        if (implicit_auto_readahead_) {
          if (!IsEligibleForPrefetch(offset, n)) {
            // Ignore status as Prefetch is not called.
            s.PermitUncheckedError();
            return false;
          }
        }

        // Prefetch n + readahead_size_/2 synchronously as remaining
        // readahead_size_/2 will be prefetched asynchronously if num_buffers_
        // > 1.
        s = PrefetchInternal(
            opts, reader, offset, n,
            (num_buffers_ > 1 ? readahead_size_ / 2 : readahead_size_),
            copy_to_overlap_buffer);
        explicit_prefetch_submitted_ = false;
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
      prefetched = explicit_prefetch_submitted_ ? false : true;
    } else {
      return false;
    }
  } else if (!for_compaction) {
    UpdateStats(/*found_in_buffer=*/true, n);
  }

  UpdateReadPattern(offset, n, /*decrease_readaheadsize=*/false);

  buf = GetFirstBuffer();
  if (copy_to_overlap_buffer) {
    buf = overlap_buf_;
  }
  uint64_t offset_in_buffer = offset - buf->offset_;
  *result = Slice(buf->buffer_.BufferStart() + offset_in_buffer, n);
  if (prefetched) {
    readahead_size_ = std::min(max_readahead_size_, readahead_size_ * 2);
  }
  return true;
}

void FilePrefetchBuffer::PrefetchAsyncCallback(FSReadRequest& req,
                                               void* cb_arg) {
  BufferInfo* buf = static_cast<BufferInfo*>(cb_arg);

#ifndef NDEBUG
  if (req.result.size() < req.len) {
    // Fake an IO error to force db_stress fault injection to ignore
    // truncated read errors
    IGNORE_STATUS_IF_ERROR(Status::IOError());
  }
  IGNORE_STATUS_IF_ERROR(req.status);
#endif

  if (req.status.ok()) {
    if (req.offset + req.result.size() <= buf->offset_ + buf->CurrentSize()) {
      // All requested bytes are already in the buffer or no data is read
      // because of EOF. So no need to update.
      return;
    }
    if (req.offset < buf->offset_) {
      // Next block to be read has changed (Recent read was not a sequential
      // read). So ignore this read.
      return;
    }
    size_t current_size = buf->CurrentSize();
    buf->buffer_.Size(current_size + req.result.size());
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

  // Cancel any pending async read to make code simpler as buffers can be out
  // of sync.
  AbortAllIOs();
  // Free empty buffers after aborting IOs.
  FreeEmptyBuffers();
  ClearOutdatedData(offset, n);

  // - Since PrefetchAsync can be called on non sequential reads. So offset can
  //   be less than first buffers' offset. In that case it clears all
  //   buffers.
  // - In case of tuning of readahead_size, on Reseek, we have to clear all
  //   buffers otherwise, we may end up with inconsistent BlockHandles in queue
  //   and data in buffer.
  if (!IsBufferQueueEmpty()) {
    BufferInfo* buf = GetFirstBuffer();
    if (readaheadsize_cb_ != nullptr || !buf->IsOffsetInBuffer(offset)) {
      FreeAllBuffers();
    }
  }

  UpdateReadPattern(offset, n, /*decrease_readaheadsize=*/false);

  bool data_found = false;

  // If first buffer has full data.
  if (!IsBufferQueueEmpty()) {
    BufferInfo* buf = GetFirstBuffer();
    if (buf->DoesBufferContainData() && buf->IsDataBlockInBuffer(offset, n)) {
      uint64_t offset_in_buffer = offset - buf->offset_;
      *result = Slice(buf->buffer_.BufferStart() + offset_in_buffer, n);
      data_found = true;
      UpdateStats(/*found_in_buffer=*/true, n);

      // Update num_file_reads_ as TryReadFromCacheAsync won't be called for
      // poll and update num_file_reads_ if data is found.
      num_file_reads_++;

      // If next buffer contains some data or is not eligible for prefetching,
      // return.
      if (!is_eligible_for_prefetching || NumBuffersAllocated() > 1) {
        return Status::OK();
      }
    } else {
      // Partial data in first buffer. Clear it to return continous data in one
      // buffer.
      FreeAllBuffers();
    }
  }

  std::string msg;

  Status s;
  size_t alignment = reader->file()->GetRequiredBufferAlignment();
  size_t readahead_size = is_eligible_for_prefetching ? readahead_size_ / 2 : 0;
  size_t offset_to_read = static_cast<size_t>(offset);
  uint64_t start_offset1 = offset, end_offset1 = 0, aligned_useful_len1 = 0;
  size_t read_len1 = 0;

  AllocateBufferIfEmpty();
  BufferInfo* buf = GetFirstBuffer();

  // - If first buffer is empty.
  //   - Call async read for full data + readahead_size on first buffer.
  //   - Call async read for readahead_size on all remaining buffers if
  //     eligible.
  // - If first buffer contains data,
  //   - Call async read for readahead_size on all remaining buffers if
  //     eligible.

  // Calculate length and offsets for reading.
  if (!buf->DoesBufferContainData()) {
    uint64_t roundup_len1;
    // Prefetch full data + readahead_size in the first buffer.
    if (is_eligible_for_prefetching || reader->use_direct_io()) {
      ReadAheadSizeTuning(buf, /*read_curr_block=*/true, /*refit_tail=*/false,
                          /*prev_buf_end_offset=*/start_offset1, alignment, n,
                          readahead_size, start_offset1, end_offset1, read_len1,
                          aligned_useful_len1);
    } else {
      // No alignment or extra prefetching.
      start_offset1 = offset_to_read;
      end_offset1 = offset_to_read + n;
      roundup_len1 = end_offset1 - start_offset1;
      PrepareBufferForRead(buf, alignment, start_offset1, roundup_len1, false,
                           aligned_useful_len1);
      assert(aligned_useful_len1 == 0);
      assert(roundup_len1 >= aligned_useful_len1);
      read_len1 = static_cast<size_t>(roundup_len1);
      buf->offset_ = start_offset1;
    }

    if (read_len1 > 0) {
      s = ReadAsync(buf, opts, reader, read_len1, start_offset1);
      if (!s.ok()) {
        DestroyAndClearIOHandle(buf);
        FreeLastBuffer();
        return s;
      }
      explicit_prefetch_submitted_ = true;
      prev_len_ = 0;
    }
  }

  if (is_eligible_for_prefetching) {
    s = PrefetchRemBuffers(opts, reader, end_offset1, alignment,
                           readahead_size);
    if (!s.ok()) {
      return s;
    }
    readahead_size_ = std::min(max_readahead_size_, readahead_size_ * 2);
  }
  return (data_found ? Status::OK() : Status::TryAgain());
}

Status FilePrefetchBuffer::PrefetchRemBuffers(const IOOptions& opts,
                                              RandomAccessFileReader* reader,
                                              uint64_t end_offset1,
                                              size_t alignment,
                                              size_t readahead_size) {
  Status s;
  while (NumBuffersAllocated() < num_buffers_) {
    BufferInfo* prev_buf = GetLastBuffer();
    uint64_t start_offset2 = prev_buf->initial_end_offset_;

    AllocateBuffer();
    BufferInfo* new_buf = GetLastBuffer();

    uint64_t end_offset2 = start_offset2, aligned_useful_len2 = 0;
    size_t read_len2 = 0;
    ReadAheadSizeTuning(new_buf, /*read_curr_block=*/false,
                        /*refit_tail=*/false,
                        /*prev_buf_end_offset=*/end_offset1, alignment,
                        /*length=*/0, readahead_size, start_offset2,
                        end_offset2, read_len2, aligned_useful_len2);

    if (read_len2 > 0) {
      TEST_SYNC_POINT("FilePrefetchBuffer::PrefetchAsync:ExtraPrefetching");
      s = ReadAsync(new_buf, opts, reader, read_len2, start_offset2);
      if (!s.ok()) {
        DestroyAndClearIOHandle(new_buf);
        FreeLastBuffer();
        return s;
      }
    }
    end_offset1 = end_offset2;
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
