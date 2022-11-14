//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "file/random_access_file_reader.h"

#include <algorithm>
#include <mutex>

#include "file/file_util.h"
#include "monitoring/histogram.h"
#include "monitoring/iostats_context_imp.h"
#include "port/port.h"
#include "table/format.h"
#include "test_util/sync_point.h"
#include "util/random.h"
#include "util/rate_limiter.h"

namespace ROCKSDB_NAMESPACE {

inline void RecordIOStats(Statistics* stats, Temperature file_temperature,
                          bool is_last_level, size_t size) {
  IOSTATS_ADD(bytes_read, size);
  // record for last/non-last level
  if (is_last_level) {
    RecordTick(stats, LAST_LEVEL_READ_BYTES, size);
    RecordTick(stats, LAST_LEVEL_READ_COUNT, 1);
  } else {
    RecordTick(stats, NON_LAST_LEVEL_READ_BYTES, size);
    RecordTick(stats, NON_LAST_LEVEL_READ_COUNT, 1);
  }

  // record for temperature file
  if (file_temperature != Temperature::kUnknown) {
    switch (file_temperature) {
      case Temperature::kHot:
        IOSTATS_ADD(file_io_stats_by_temperature.hot_file_bytes_read, size);
        IOSTATS_ADD(file_io_stats_by_temperature.hot_file_read_count, 1);
        RecordTick(stats, HOT_FILE_READ_BYTES, size);
        RecordTick(stats, HOT_FILE_READ_COUNT, 1);
        break;
      case Temperature::kWarm:
        IOSTATS_ADD(file_io_stats_by_temperature.warm_file_bytes_read, size);
        IOSTATS_ADD(file_io_stats_by_temperature.warm_file_read_count, 1);
        RecordTick(stats, WARM_FILE_READ_BYTES, size);
        RecordTick(stats, WARM_FILE_READ_COUNT, 1);
        break;
      case Temperature::kCold:
        IOSTATS_ADD(file_io_stats_by_temperature.cold_file_bytes_read, size);
        IOSTATS_ADD(file_io_stats_by_temperature.cold_file_read_count, 1);
        RecordTick(stats, COLD_FILE_READ_BYTES, size);
        RecordTick(stats, COLD_FILE_READ_COUNT, 1);
        break;
      default:
        break;
    }
  }
}

IOStatus RandomAccessFileReader::Create(
    const std::shared_ptr<FileSystem>& fs, const std::string& fname,
    const FileOptions& file_opts,
    std::unique_ptr<RandomAccessFileReader>* reader, IODebugContext* dbg) {
  std::unique_ptr<FSRandomAccessFile> file;
  IOStatus io_s = fs->NewRandomAccessFile(fname, file_opts, &file, dbg);
  if (io_s.ok()) {
    reader->reset(new RandomAccessFileReader(std::move(file), fname));
  }
  return io_s;
}

IOStatus RandomAccessFileReader::Read(
    const IOOptions& opts, uint64_t offset, size_t n, Slice* result,
    char* scratch, AlignedBuf* aligned_buf,
    Env::IOPriority rate_limiter_priority) const {
  (void)aligned_buf;

  TEST_SYNC_POINT_CALLBACK("RandomAccessFileReader::Read", nullptr);

  // To be paranoid: modify scratch a little bit, so in case underlying
  // FileSystem doesn't fill the buffer but return success and `scratch` returns
  // contains a previous block, returned value will not pass checksum.
  if (n > 0 && scratch != nullptr) {
    // This byte might not change anything for direct I/O case, but it's OK.
    scratch[0]++;
  }

  IOStatus io_s;
  uint64_t elapsed = 0;
  {
    StopWatch sw(clock_, stats_, hist_type_,
                 (stats_ != nullptr) ? &elapsed : nullptr, true /*overwrite*/,
                 true /*delay_enabled*/);
    auto prev_perf_level = GetPerfLevel();
    IOSTATS_TIMER_GUARD(read_nanos);
    if (use_direct_io()) {
#ifndef ROCKSDB_LITE
      size_t alignment = file_->GetRequiredBufferAlignment();
      size_t aligned_offset =
          TruncateToPageBoundary(alignment, static_cast<size_t>(offset));
      size_t offset_advance = static_cast<size_t>(offset) - aligned_offset;
      size_t read_size =
          Roundup(static_cast<size_t>(offset + n), alignment) - aligned_offset;
      AlignedBuffer buf;
      buf.Alignment(alignment);
      buf.AllocateNewBuffer(read_size);
      while (buf.CurrentSize() < read_size) {
        size_t allowed;
        if (rate_limiter_priority != Env::IO_TOTAL &&
            rate_limiter_ != nullptr) {
          allowed = rate_limiter_->RequestToken(
              buf.Capacity() - buf.CurrentSize(), buf.Alignment(),
              rate_limiter_priority, stats_, RateLimiter::OpType::kRead);
        } else {
          assert(buf.CurrentSize() == 0);
          allowed = read_size;
        }
        Slice tmp;

        FileOperationInfo::StartTimePoint start_ts;
        uint64_t orig_offset = 0;
        if (ShouldNotifyListeners()) {
          start_ts = FileOperationInfo::StartNow();
          orig_offset = aligned_offset + buf.CurrentSize();
        }

        {
          IOSTATS_CPU_TIMER_GUARD(cpu_read_nanos, clock_);
          // Only user reads are expected to specify a timeout. And user reads
          // are not subjected to rate_limiter and should go through only
          // one iteration of this loop, so we don't need to check and adjust
          // the opts.timeout before calling file_->Read
          assert(!opts.timeout.count() || allowed == read_size);
          io_s = file_->Read(aligned_offset + buf.CurrentSize(), allowed, opts,
                             &tmp, buf.Destination(), nullptr);
        }
        if (ShouldNotifyListeners()) {
          auto finish_ts = FileOperationInfo::FinishNow();
          NotifyOnFileReadFinish(orig_offset, tmp.size(), start_ts, finish_ts,
                                 io_s);
          if (!io_s.ok()) {
            NotifyOnIOError(io_s, FileOperationType::kRead, file_name(),
                            tmp.size(), orig_offset);
          }
        }

        buf.Size(buf.CurrentSize() + tmp.size());
        if (!io_s.ok() || tmp.size() < allowed) {
          break;
        }
      }
      size_t res_len = 0;
      if (io_s.ok() && offset_advance < buf.CurrentSize()) {
        res_len = std::min(buf.CurrentSize() - offset_advance, n);
        if (aligned_buf == nullptr) {
          buf.Read(scratch, offset_advance, res_len);
        } else {
          scratch = buf.BufferStart() + offset_advance;
          aligned_buf->reset(buf.Release());
        }
      }
      *result = Slice(scratch, res_len);
#endif  // !ROCKSDB_LITE
    } else {
      size_t pos = 0;
      const char* res_scratch = nullptr;
      while (pos < n) {
        size_t allowed;
        if (rate_limiter_priority != Env::IO_TOTAL &&
            rate_limiter_ != nullptr) {
          if (rate_limiter_->IsRateLimited(RateLimiter::OpType::kRead)) {
            sw.DelayStart();
          }
          allowed = rate_limiter_->RequestToken(n - pos, 0 /* alignment */,
                                                rate_limiter_priority, stats_,
                                                RateLimiter::OpType::kRead);
          if (rate_limiter_->IsRateLimited(RateLimiter::OpType::kRead)) {
            sw.DelayStop();
          }
        } else {
          allowed = n;
        }
        Slice tmp_result;

#ifndef ROCKSDB_LITE
        FileOperationInfo::StartTimePoint start_ts;
        if (ShouldNotifyListeners()) {
          start_ts = FileOperationInfo::StartNow();
        }
#endif

        {
          IOSTATS_CPU_TIMER_GUARD(cpu_read_nanos, clock_);
          // Only user reads are expected to specify a timeout. And user reads
          // are not subjected to rate_limiter and should go through only
          // one iteration of this loop, so we don't need to check and adjust
          // the opts.timeout before calling file_->Read
          assert(!opts.timeout.count() || allowed == n);
          io_s = file_->Read(offset + pos, allowed, opts, &tmp_result,
                             scratch + pos, nullptr);
        }
#ifndef ROCKSDB_LITE
        if (ShouldNotifyListeners()) {
          auto finish_ts = FileOperationInfo::FinishNow();
          NotifyOnFileReadFinish(offset + pos, tmp_result.size(), start_ts,
                                 finish_ts, io_s);

          if (!io_s.ok()) {
            NotifyOnIOError(io_s, FileOperationType::kRead, file_name(),
                            tmp_result.size(), offset + pos);
          }
        }
#endif
        if (res_scratch == nullptr) {
          // we can't simply use `scratch` because reads of mmap'd files return
          // data in a different buffer.
          res_scratch = tmp_result.data();
        } else {
          // make sure chunks are inserted contiguously into `res_scratch`.
          assert(tmp_result.data() == res_scratch + pos);
        }
        pos += tmp_result.size();
        if (!io_s.ok() || tmp_result.size() < allowed) {
          break;
        }
      }
      *result = Slice(res_scratch, io_s.ok() ? pos : 0);
    }
    RecordIOStats(stats_, file_temperature_, is_last_level_, result->size());
    SetPerfLevel(prev_perf_level);
  }
  if (stats_ != nullptr && file_read_hist_ != nullptr) {
    file_read_hist_->Add(elapsed);
  }

  return io_s;
}

size_t End(const FSReadRequest& r) {
  return static_cast<size_t>(r.offset) + r.len;
}

FSReadRequest Align(const FSReadRequest& r, size_t alignment) {
  FSReadRequest req;
  req.offset = static_cast<uint64_t>(
      TruncateToPageBoundary(alignment, static_cast<size_t>(r.offset)));
  req.len = Roundup(End(r), alignment) - req.offset;
  req.scratch = nullptr;
  return req;
}

bool TryMerge(FSReadRequest* dest, const FSReadRequest& src) {
  size_t dest_offset = static_cast<size_t>(dest->offset);
  size_t src_offset = static_cast<size_t>(src.offset);
  size_t dest_end = End(*dest);
  size_t src_end = End(src);
  if (std::max(dest_offset, src_offset) > std::min(dest_end, src_end)) {
    return false;
  }
  dest->offset = static_cast<uint64_t>(std::min(dest_offset, src_offset));
  dest->len = std::max(dest_end, src_end) - dest->offset;
  return true;
}

IOStatus RandomAccessFileReader::MultiRead(
    const IOOptions& opts, FSReadRequest* read_reqs, size_t num_reqs,
    AlignedBuf* aligned_buf, Env::IOPriority rate_limiter_priority) const {
  (void)aligned_buf;  // suppress warning of unused variable in LITE mode
  assert(num_reqs > 0);

#ifndef NDEBUG
  for (size_t i = 0; i < num_reqs - 1; ++i) {
    assert(read_reqs[i].offset <= read_reqs[i + 1].offset);
  }
#endif  // !NDEBUG

  // To be paranoid modify scratch a little bit, so in case underlying
  // FileSystem doesn't fill the buffer but return success and `scratch` returns
  // contains a previous block, returned value will not pass checksum.
  // This byte might not change anything for direct I/O case, but it's OK.
  for (size_t i = 0; i < num_reqs; i++) {
    FSReadRequest& r = read_reqs[i];
    if (r.len > 0 && r.scratch != nullptr) {
      r.scratch[0]++;
    }
  }

  IOStatus io_s;
  uint64_t elapsed = 0;
  {
    StopWatch sw(clock_, stats_, hist_type_,
                 (stats_ != nullptr) ? &elapsed : nullptr, true /*overwrite*/,
                 true /*delay_enabled*/);
    auto prev_perf_level = GetPerfLevel();
    IOSTATS_TIMER_GUARD(read_nanos);

    FSReadRequest* fs_reqs = read_reqs;
    size_t num_fs_reqs = num_reqs;
#ifndef ROCKSDB_LITE
    std::vector<FSReadRequest> aligned_reqs;
    if (use_direct_io()) {
      // num_reqs is the max possible size,
      // this can reduce std::vecector's internal resize operations.
      aligned_reqs.reserve(num_reqs);
      // Align and merge the read requests.
      size_t alignment = file_->GetRequiredBufferAlignment();
      for (size_t i = 0; i < num_reqs; i++) {
        const auto& r = Align(read_reqs[i], alignment);
        if (i == 0) {
          // head
          aligned_reqs.push_back(r);

        } else if (!TryMerge(&aligned_reqs.back(), r)) {
          // head + n
          aligned_reqs.push_back(r);

        } else {
          // unused
          r.status.PermitUncheckedError();
        }
      }
      TEST_SYNC_POINT_CALLBACK("RandomAccessFileReader::MultiRead:AlignedReqs",
                               &aligned_reqs);

      // Allocate aligned buffer and let scratch buffers point to it.
      size_t total_len = 0;
      for (const auto& r : aligned_reqs) {
        total_len += r.len;
      }
      AlignedBuffer buf;
      buf.Alignment(alignment);
      buf.AllocateNewBuffer(total_len);
      char* scratch = buf.BufferStart();
      for (auto& r : aligned_reqs) {
        r.scratch = scratch;
        scratch += r.len;
      }

      aligned_buf->reset(buf.Release());
      fs_reqs = aligned_reqs.data();
      num_fs_reqs = aligned_reqs.size();
    }
#endif  // ROCKSDB_LITE

#ifndef ROCKSDB_LITE
    FileOperationInfo::StartTimePoint start_ts;
    if (ShouldNotifyListeners()) {
      start_ts = FileOperationInfo::StartNow();
    }
#endif  // ROCKSDB_LITE

    {
      IOSTATS_CPU_TIMER_GUARD(cpu_read_nanos, clock_);
      if (rate_limiter_priority != Env::IO_TOTAL && rate_limiter_ != nullptr) {
        // TODO: ideally we should call `RateLimiter::RequestToken()` for
        // allowed bytes to multi-read and then consume those bytes by
        // satisfying as many requests in `MultiRead()` as possible, instead of
        // what we do here, which can cause burst when the
        // `total_multi_read_size` is big.
        size_t total_multi_read_size = 0;
        assert(fs_reqs != nullptr);
        for (size_t i = 0; i < num_fs_reqs; ++i) {
          FSReadRequest& req = fs_reqs[i];
          total_multi_read_size += req.len;
        }
        size_t remaining_bytes = total_multi_read_size;
        size_t request_bytes = 0;
        while (remaining_bytes > 0) {
          request_bytes = std::min(
              static_cast<size_t>(rate_limiter_->GetSingleBurstBytes()),
              remaining_bytes);
          rate_limiter_->Request(request_bytes, rate_limiter_priority,
                                 nullptr /* stats */,
                                 RateLimiter::OpType::kRead);
          remaining_bytes -= request_bytes;
        }
      }
      io_s = file_->MultiRead(fs_reqs, num_fs_reqs, opts, nullptr);
      RecordInHistogram(stats_, MULTIGET_IO_BATCH_SIZE, num_fs_reqs);
    }

#ifndef ROCKSDB_LITE
    if (use_direct_io()) {
      // Populate results in the unaligned read requests.
      size_t aligned_i = 0;
      for (size_t i = 0; i < num_reqs; i++) {
        auto& r = read_reqs[i];
        if (static_cast<size_t>(r.offset) > End(aligned_reqs[aligned_i])) {
          aligned_i++;
        }
        const auto& fs_r = fs_reqs[aligned_i];
        r.status = fs_r.status;
        if (r.status.ok()) {
          uint64_t offset = r.offset - fs_r.offset;
          if (fs_r.result.size() <= offset) {
            // No byte in the read range is returned.
            r.result = Slice();
          } else {
            size_t len = std::min(
                r.len, static_cast<size_t>(fs_r.result.size() - offset));
            r.result = Slice(fs_r.scratch + offset, len);
          }
        } else {
          r.result = Slice();
        }
      }
    }
#endif  // ROCKSDB_LITE

    for (size_t i = 0; i < num_reqs; ++i) {
#ifndef ROCKSDB_LITE
      if (ShouldNotifyListeners()) {
        auto finish_ts = FileOperationInfo::FinishNow();
        NotifyOnFileReadFinish(read_reqs[i].offset, read_reqs[i].result.size(),
                               start_ts, finish_ts, read_reqs[i].status);
      }
      if (!read_reqs[i].status.ok()) {
        NotifyOnIOError(read_reqs[i].status, FileOperationType::kRead,
                        file_name(), read_reqs[i].result.size(),
                        read_reqs[i].offset);
      }

#endif  // ROCKSDB_LITE
      RecordIOStats(stats_, file_temperature_, is_last_level_,
                    read_reqs[i].result.size());
    }
    SetPerfLevel(prev_perf_level);
  }
  if (stats_ != nullptr && file_read_hist_ != nullptr) {
    file_read_hist_->Add(elapsed);
  }

  return io_s;
}

IOStatus RandomAccessFileReader::PrepareIOOptions(const ReadOptions& ro,
                                                  IOOptions& opts) {
  if (clock_ != nullptr) {
    return PrepareIOFromReadOptions(ro, clock_, opts);
  } else {
    return PrepareIOFromReadOptions(ro, SystemClock::Default().get(), opts);
  }
}

IOStatus RandomAccessFileReader::ReadAsync(
    FSReadRequest& req, const IOOptions& opts,
    std::function<void(const FSReadRequest&, void*)> cb, void* cb_arg,
    void** io_handle, IOHandleDeleter* del_fn, AlignedBuf* aligned_buf) {
  IOStatus s;
  // Create a callback and populate info.
  auto read_async_callback =
      std::bind(&RandomAccessFileReader::ReadAsyncCallback, this,
                std::placeholders::_1, std::placeholders::_2);
  ReadAsyncInfo* read_async_info =
      new ReadAsyncInfo(cb, cb_arg, clock_->NowMicros());

#ifndef ROCKSDB_LITE
  if (ShouldNotifyListeners()) {
    read_async_info->fs_start_ts_ = FileOperationInfo::StartNow();
  }
#endif

  size_t alignment = file_->GetRequiredBufferAlignment();
  bool is_aligned = (req.offset & (alignment - 1)) == 0 &&
                    (req.len & (alignment - 1)) == 0 &&
                    (uintptr_t(req.scratch) & (alignment - 1)) == 0;
  read_async_info->is_aligned_ = is_aligned;

  uint64_t elapsed = 0;
  if (use_direct_io() && is_aligned == false) {
    FSReadRequest aligned_req = Align(req, alignment);
    aligned_req.status.PermitUncheckedError();

    // Allocate aligned buffer.
    read_async_info->buf_.Alignment(alignment);
    read_async_info->buf_.AllocateNewBuffer(aligned_req.len);

    // Set rem fields in aligned FSReadRequest.
    aligned_req.scratch = read_async_info->buf_.BufferStart();

    // Set user provided fields to populate back in callback.
    read_async_info->user_scratch_ = req.scratch;
    read_async_info->user_aligned_buf_ = aligned_buf;
    read_async_info->user_len_ = req.len;
    read_async_info->user_offset_ = req.offset;
    read_async_info->user_result_ = req.result;

    assert(read_async_info->buf_.CurrentSize() == 0);

    StopWatch sw(clock_, nullptr /*stats*/, 0 /*hist_type*/, &elapsed,
                 true /*overwrite*/, true /*delay_enabled*/);
    s = file_->ReadAsync(aligned_req, opts, read_async_callback,
                         read_async_info, io_handle, del_fn, nullptr /*dbg*/);
  } else {
    StopWatch sw(clock_, nullptr /*stats*/, 0 /*hist_type*/, &elapsed,
                 true /*overwrite*/, true /*delay_enabled*/);
    s = file_->ReadAsync(req, opts, read_async_callback, read_async_info,
                         io_handle, del_fn, nullptr /*dbg*/);
  }
  RecordTick(stats_, READ_ASYNC_MICROS, elapsed);

// Suppress false positive clang analyzer warnings.
// Memory is not released if file_->ReadAsync returns !s.ok(), because
// ReadAsyncCallback is never called in that case. If ReadAsyncCallback is
// called then ReadAsync should always return IOStatus::OK().
#ifndef __clang_analyzer__
  if (!s.ok()) {
    delete read_async_info;
  }
#endif  // __clang_analyzer__

  return s;
}

void RandomAccessFileReader::ReadAsyncCallback(const FSReadRequest& req,
                                               void* cb_arg) {
  ReadAsyncInfo* read_async_info = static_cast<ReadAsyncInfo*>(cb_arg);
  assert(read_async_info);
  assert(read_async_info->cb_);

  if (use_direct_io() && read_async_info->is_aligned_ == false) {
    // Create FSReadRequest with user provided fields.
    FSReadRequest user_req;
    user_req.scratch = read_async_info->user_scratch_;
    user_req.offset = read_async_info->user_offset_;
    user_req.len = read_async_info->user_len_;

    // Update results in user_req.
    user_req.result = req.result;
    user_req.status = req.status;

    read_async_info->buf_.Size(read_async_info->buf_.CurrentSize() +
                               req.result.size());

    size_t offset_advance_len = static_cast<size_t>(
        /*offset_passed_by_user=*/read_async_info->user_offset_ -
        /*aligned_offset=*/req.offset);

    size_t res_len = 0;
    if (req.status.ok() &&
        offset_advance_len < read_async_info->buf_.CurrentSize()) {
      res_len =
          std::min(read_async_info->buf_.CurrentSize() - offset_advance_len,
                   read_async_info->user_len_);
      if (read_async_info->user_aligned_buf_ == nullptr) {
        // Copy the data into user's scratch.
// Clang analyzer assumes that it will take use_direct_io() == false in
// ReadAsync and use_direct_io() == true in Callback which cannot be true.
#ifndef __clang_analyzer__
        read_async_info->buf_.Read(user_req.scratch, offset_advance_len,
                                   res_len);
#endif  // __clang_analyzer__
      } else {
        // Set aligned_buf provided by user without additional copy.
        user_req.scratch =
            read_async_info->buf_.BufferStart() + offset_advance_len;
        read_async_info->user_aligned_buf_->reset(
            read_async_info->buf_.Release());
      }
      user_req.result = Slice(user_req.scratch, res_len);
    } else {
      // Either req.status is not ok or data was not read.
      user_req.result = Slice();
    }
    read_async_info->cb_(user_req, read_async_info->cb_arg_);
  } else {
    read_async_info->cb_(req, read_async_info->cb_arg_);
  }

  // Update stats and notify listeners.
  if (stats_ != nullptr && file_read_hist_ != nullptr) {
    // elapsed doesn't take into account delay and overwrite as StopWatch does
    // in Read.
    uint64_t elapsed = clock_->NowMicros() - read_async_info->start_time_;
    file_read_hist_->Add(elapsed);
  }
  if (req.status.ok()) {
    RecordInHistogram(stats_, ASYNC_READ_BYTES, req.result.size());
  } else if (!req.status.IsAborted()) {
    RecordTick(stats_, ASYNC_READ_ERROR_COUNT, 1);
  }
#ifndef ROCKSDB_LITE
  if (ShouldNotifyListeners()) {
    auto finish_ts = FileOperationInfo::FinishNow();
    NotifyOnFileReadFinish(req.offset, req.result.size(),
                           read_async_info->fs_start_ts_, finish_ts,
                           req.status);
  }
  if (!req.status.ok()) {
    NotifyOnIOError(req.status, FileOperationType::kRead, file_name(),
                    req.result.size(), req.offset);
  }
#endif
  RecordIOStats(stats_, file_temperature_, is_last_level_, req.result.size());
  delete read_async_info;
}
}  // namespace ROCKSDB_NAMESPACE
