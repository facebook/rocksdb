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
#include <string>

#include "db/version_edit.h"
#include "env/file_system_tracer.h"
#include "port/port.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/listener.h"
#include "rocksdb/rate_limiter.h"
#include "test_util/sync_point.h"
#include "util/aligned_buffer.h"

namespace ROCKSDB_NAMESPACE {
class Statistics;
class SystemClock;

// WritableFileWriter is a wrapper on top of Env::WritableFile. It provides
// facilities to:
// - Handle Buffered and Direct writes.
// - Rate limit writes.
// - Flush and Sync the data to the underlying filesystem.
// - Notify any interested listeners on the completion of a write.
// - Update IO stats.
class WritableFileWriter {
 private:
#ifndef ROCKSDB_LITE
  void NotifyOnFileWriteFinish(
      uint64_t offset, size_t length,
      const FileOperationInfo::StartTimePoint& start_ts,
      const FileOperationInfo::FinishTimePoint& finish_ts,
      const IOStatus& io_status) {
    FileOperationInfo info(FileOperationType::kWrite, file_name_, start_ts,
                           finish_ts, io_status, temperature_);
    info.offset = offset;
    info.length = length;

    for (auto& listener : listeners_) {
      listener->OnFileWriteFinish(info);
    }
    info.status.PermitUncheckedError();
  }
  void NotifyOnFileFlushFinish(
      const FileOperationInfo::StartTimePoint& start_ts,
      const FileOperationInfo::FinishTimePoint& finish_ts,
      const IOStatus& io_status) {
    FileOperationInfo info(FileOperationType::kFlush, file_name_, start_ts,
                           finish_ts, io_status, temperature_);

    for (auto& listener : listeners_) {
      listener->OnFileFlushFinish(info);
    }
    info.status.PermitUncheckedError();
  }
  void NotifyOnFileSyncFinish(
      const FileOperationInfo::StartTimePoint& start_ts,
      const FileOperationInfo::FinishTimePoint& finish_ts,
      const IOStatus& io_status,
      FileOperationType type = FileOperationType::kSync) {
    FileOperationInfo info(type, file_name_, start_ts, finish_ts, io_status,
                           temperature_);

    for (auto& listener : listeners_) {
      listener->OnFileSyncFinish(info);
    }
    info.status.PermitUncheckedError();
  }
  void NotifyOnFileRangeSyncFinish(
      uint64_t offset, size_t length,
      const FileOperationInfo::StartTimePoint& start_ts,
      const FileOperationInfo::FinishTimePoint& finish_ts,
      const IOStatus& io_status) {
    FileOperationInfo info(FileOperationType::kRangeSync, file_name_, start_ts,
                           finish_ts, io_status, temperature_);
    info.offset = offset;
    info.length = length;

    for (auto& listener : listeners_) {
      listener->OnFileRangeSyncFinish(info);
    }
    info.status.PermitUncheckedError();
  }
  void NotifyOnFileTruncateFinish(
      const FileOperationInfo::StartTimePoint& start_ts,
      const FileOperationInfo::FinishTimePoint& finish_ts,
      const IOStatus& io_status) {
    FileOperationInfo info(FileOperationType::kTruncate, file_name_, start_ts,
                           finish_ts, io_status, temperature_);

    for (auto& listener : listeners_) {
      listener->OnFileTruncateFinish(info);
    }
    info.status.PermitUncheckedError();
  }
  void NotifyOnFileCloseFinish(
      const FileOperationInfo::StartTimePoint& start_ts,
      const FileOperationInfo::FinishTimePoint& finish_ts,
      const IOStatus& io_status) {
    FileOperationInfo info(FileOperationType::kClose, file_name_, start_ts,
                           finish_ts, io_status, temperature_);

    for (auto& listener : listeners_) {
      listener->OnFileCloseFinish(info);
    }
    info.status.PermitUncheckedError();
  }

  void NotifyOnIOError(const IOStatus& io_status, FileOperationType operation,
                       const std::string& file_path, size_t length = 0,
                       uint64_t offset = 0) {
    if (listeners_.empty()) {
      return;
    }
    IOErrorInfo io_error_info(io_status, operation, file_path, length, offset);
    for (auto& listener : listeners_) {
      listener->OnIOError(io_error_info);
    }
    io_error_info.io_status.PermitUncheckedError();
  }
#endif  // ROCKSDB_LITE

  bool ShouldNotifyListeners() const { return !listeners_.empty(); }
  void UpdateFileChecksum(const Slice& data);
  void Crc32cHandoffChecksumCalculation(const char* data, size_t size,
                                        char* buf);

  std::string file_name_;
  FSWritableFilePtr writable_file_;
  SystemClock* clock_;
  AlignedBuffer buf_;
  size_t max_buffer_size_;
  // Actually written data size can be used for truncate
  // not counting padding data
  std::atomic<uint64_t> filesize_;
  std::atomic<uint64_t> flushed_size_;
#ifndef ROCKSDB_LITE
  // This is necessary when we use unbuffered access
  // and writes must happen on aligned offsets
  // so we need to go back and write that page again
  uint64_t next_write_offset_;
#endif  // ROCKSDB_LITE
  bool pending_sync_;
  std::atomic<bool> seen_error_;
#ifndef NDEBUG
  // SyncWithoutFlush() is the function that is allowed to be called
  // concurrently with other function. One of the concurrent call
  // could set seen_error_, and the other one would hit assertion
  // in debug mode.
  std::atomic<bool> sync_without_flush_called_ = false;
#endif  // NDEBUG
  uint64_t last_sync_size_;
  uint64_t bytes_per_sync_;
  RateLimiter* rate_limiter_;
  Statistics* stats_;
  std::vector<std::shared_ptr<EventListener>> listeners_;
  std::unique_ptr<FileChecksumGenerator> checksum_generator_;
  bool checksum_finalized_;
  bool perform_data_verification_;
  uint32_t buffered_data_crc32c_checksum_;
  bool buffered_data_with_checksum_;
#ifndef ROCKSDB_LITE
  Temperature temperature_;
#endif  // ROCKSDB_LITE

 public:
  WritableFileWriter(
      std::unique_ptr<FSWritableFile>&& file, const std::string& _file_name,
      const FileOptions& options, SystemClock* clock = nullptr,
      const std::shared_ptr<IOTracer>& io_tracer = nullptr,
      Statistics* stats = nullptr,
      const std::vector<std::shared_ptr<EventListener>>& listeners = {},
      FileChecksumGenFactory* file_checksum_gen_factory = nullptr,
      bool perform_data_verification = false,
      bool buffered_data_with_checksum = false)
      : file_name_(_file_name),
        writable_file_(std::move(file), io_tracer, _file_name),
        clock_(clock),
        buf_(),
        max_buffer_size_(options.writable_file_max_buffer_size),
        filesize_(0),
        flushed_size_(0),
#ifndef ROCKSDB_LITE
        next_write_offset_(0),
#endif  // ROCKSDB_LITE
        pending_sync_(false),
        seen_error_(false),
        last_sync_size_(0),
        bytes_per_sync_(options.bytes_per_sync),
        rate_limiter_(options.rate_limiter),
        stats_(stats),
        listeners_(),
        checksum_generator_(nullptr),
        checksum_finalized_(false),
        perform_data_verification_(perform_data_verification),
        buffered_data_crc32c_checksum_(0),
        buffered_data_with_checksum_(buffered_data_with_checksum) {
#ifndef ROCKSDB_LITE
    temperature_ = options.temperature;
#endif  // ROCKSDB_LITE
    assert(!use_direct_io() || max_buffer_size_ > 0);
    TEST_SYNC_POINT_CALLBACK("WritableFileWriter::WritableFileWriter:0",
                             reinterpret_cast<void*>(max_buffer_size_));
    buf_.Alignment(writable_file_->GetRequiredBufferAlignment());
    buf_.AllocateNewBuffer(std::min((size_t)65536, max_buffer_size_));
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
    if (file_checksum_gen_factory != nullptr) {
      FileChecksumGenContext checksum_gen_context;
      checksum_gen_context.file_name = _file_name;
      checksum_generator_ =
          file_checksum_gen_factory->CreateFileChecksumGenerator(
              checksum_gen_context);
    }
  }

  static IOStatus Create(const std::shared_ptr<FileSystem>& fs,
                         const std::string& fname, const FileOptions& file_opts,
                         std::unique_ptr<WritableFileWriter>* writer,
                         IODebugContext* dbg);
  WritableFileWriter(const WritableFileWriter&) = delete;

  WritableFileWriter& operator=(const WritableFileWriter&) = delete;

  ~WritableFileWriter() {
    auto s = Close();
    s.PermitUncheckedError();
  }

  std::string file_name() const { return file_name_; }

  // When this Append API is called, if the crc32c_checksum is not provided, we
  // will calculate the checksum internally.
  IOStatus Append(const Slice& data, uint32_t crc32c_checksum = 0,
                  Env::IOPriority op_rate_limiter_priority = Env::IO_TOTAL);

  IOStatus Pad(const size_t pad_bytes,
               Env::IOPriority op_rate_limiter_priority = Env::IO_TOTAL);

  IOStatus Flush(Env::IOPriority op_rate_limiter_priority = Env::IO_TOTAL);

  IOStatus Close();

  IOStatus Sync(bool use_fsync);

  // Sync only the data that was already Flush()ed. Safe to call concurrently
  // with Append() and Flush(). If !writable_file_->IsSyncThreadSafe(),
  // returns NotSupported status.
  IOStatus SyncWithoutFlush(bool use_fsync);

  uint64_t GetFileSize() const {
    return filesize_.load(std::memory_order_acquire);
  }

  // Returns the size of data flushed to the underlying `FSWritableFile`.
  // Expected to match `writable_file()->GetFileSize()`.
  // The return value can serve as a lower-bound for the amount of data synced
  // by a future call to `SyncWithoutFlush()`.
  uint64_t GetFlushedSize() const {
    return flushed_size_.load(std::memory_order_acquire);
  }

  IOStatus InvalidateCache(size_t offset, size_t length) {
    return writable_file_->InvalidateCache(offset, length);
  }

  FSWritableFile* writable_file() const { return writable_file_.get(); }

  bool use_direct_io() { return writable_file_->use_direct_io(); }

  bool BufferIsEmpty() { return buf_.CurrentSize() == 0; }

  void TEST_SetFileChecksumGenerator(
      FileChecksumGenerator* checksum_generator) {
    checksum_generator_.reset(checksum_generator);
  }

  std::string GetFileChecksum();

  const char* GetFileChecksumFuncName() const;

  bool seen_error() const {
    return seen_error_.load(std::memory_order_relaxed);
  }
  // For options of relaxed consistency, users might hope to continue
  // operating on the file after an error happens.
  void reset_seen_error() {
    seen_error_.store(false, std::memory_order_relaxed);
  }
  void set_seen_error() { seen_error_.store(true, std::memory_order_relaxed); }

  IOStatus AssertFalseAndGetStatusForPrevError() {
    // This should only happen if SyncWithoutFlush() was called.
    assert(sync_without_flush_called_);
    return IOStatus::IOError("Writer has previous error.");
  }

 private:
  // Decide the Rate Limiter priority.
  static Env::IOPriority DecideRateLimiterPriority(
      Env::IOPriority writable_file_io_priority,
      Env::IOPriority op_rate_limiter_priority);

  // Used when os buffering is OFF and we are writing
  // DMA such as in Direct I/O mode
#ifndef ROCKSDB_LITE
  IOStatus WriteDirect(Env::IOPriority op_rate_limiter_priority);
  IOStatus WriteDirectWithChecksum(Env::IOPriority op_rate_limiter_priority);
#endif  // !ROCKSDB_LITE
  // Normal write.
  IOStatus WriteBuffered(const char* data, size_t size,
                         Env::IOPriority op_rate_limiter_priority);
  IOStatus WriteBufferedWithChecksum(const char* data, size_t size,
                                     Env::IOPriority op_rate_limiter_priority);
  IOStatus RangeSync(uint64_t offset, uint64_t nbytes);
  IOStatus SyncInternal(bool use_fsync);
};
}  // namespace ROCKSDB_NAMESPACE
