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
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/rate_limiter.h"
#include "util/aligned_buffer.h"
#include "util/sync_point.h"

namespace rocksdb {

class Statistics;
class HistogramImpl;

std::unique_ptr<RandomAccessFile> NewReadaheadRandomAccessFile(
  std::unique_ptr<RandomAccessFile>&& file, size_t readahead_size);

class SequentialFileReader {
 private:
  std::unique_ptr<SequentialFile> file_;
  std::atomic<size_t> offset_;  // read offset

 public:
  explicit SequentialFileReader(std::unique_ptr<SequentialFile>&& _file)
      : file_(std::move(_file)), offset_(0) {}

  SequentialFileReader(SequentialFileReader&& o) ROCKSDB_NOEXCEPT {
    *this = std::move(o);
  }

  SequentialFileReader& operator=(SequentialFileReader&& o) ROCKSDB_NOEXCEPT {
    file_ = std::move(o.file_);
    return *this;
  }

  SequentialFileReader(const SequentialFileReader&) = delete;
  SequentialFileReader& operator=(const SequentialFileReader&) = delete;

  Status Read(size_t n, Slice* result, char* scratch);

  Status Skip(uint64_t n);

  void Rewind();

  SequentialFile* file() { return file_.get(); }

  bool use_direct_io() const { return file_->use_direct_io(); }
};

class RandomAccessFileReader {
 private:
  std::unique_ptr<RandomAccessFile> file_;
  std::string     file_name_;
  Env*            env_;
  Statistics*     stats_;
  uint32_t        hist_type_;
  HistogramImpl*  file_read_hist_;
  RateLimiter* rate_limiter_;
  bool for_compaction_;

 public:
  explicit RandomAccessFileReader(std::unique_ptr<RandomAccessFile>&& raf,
                                  std::string _file_name,
                                  Env* env = nullptr,
                                  Statistics* stats = nullptr,
                                  uint32_t hist_type = 0,
                                  HistogramImpl* file_read_hist = nullptr,
                                  RateLimiter* rate_limiter = nullptr,
                                  bool for_compaction = false)
      : file_(std::move(raf)),
        file_name_(std::move(_file_name)),
        env_(env),
        stats_(stats),
        hist_type_(hist_type),
        file_read_hist_(file_read_hist),
        rate_limiter_(rate_limiter),
        for_compaction_(for_compaction) {}

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
    for_compaction_ = std::move(o.for_compaction_);
    return *this;
  }

  RandomAccessFileReader(const RandomAccessFileReader&) = delete;
  RandomAccessFileReader& operator=(const RandomAccessFileReader&) = delete;

  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const;

  Status Prefetch(uint64_t offset, size_t n) const {
    return file_->Prefetch(offset, n);
  }

  RandomAccessFile* file() { return file_.get(); }

  std::string file_name() const { return file_name_; }

  bool use_direct_io() const { return file_->use_direct_io(); }
};

// Use posix write to write data to a file.
class WritableFileWriter {
 private:
  std::unique_ptr<WritableFile> writable_file_;
  AlignedBuffer           buf_;
  size_t                  max_buffer_size_;
  // Actually written data size can be used for truncate
  // not counting padding data
  uint64_t                filesize_;
#ifndef ROCKSDB_LITE
  // This is necessary when we use unbuffered access
  // and writes must happen on aligned offsets
  // so we need to go back and write that page again
  uint64_t                next_write_offset_;
#endif  // ROCKSDB_LITE
  bool                    pending_sync_;
  uint64_t                last_sync_size_;
  uint64_t                bytes_per_sync_;
  RateLimiter*            rate_limiter_;
  Statistics* stats_;

 public:
  WritableFileWriter(std::unique_ptr<WritableFile>&& file,
                     const EnvOptions& options, Statistics* stats = nullptr)
      : writable_file_(std::move(file)),
        buf_(),
        max_buffer_size_(options.writable_file_max_buffer_size),
        filesize_(0),
#ifndef ROCKSDB_LITE
        next_write_offset_(0),
#endif  // ROCKSDB_LITE
        pending_sync_(false),
        last_sync_size_(0),
        bytes_per_sync_(options.bytes_per_sync),
        rate_limiter_(options.rate_limiter),
        stats_(stats) {
    TEST_SYNC_POINT_CALLBACK("WritableFileWriter::WritableFileWriter:0",
                             reinterpret_cast<void*>(max_buffer_size_));
    buf_.Alignment(writable_file_->GetRequiredBufferAlignment());
    buf_.AllocateNewBuffer(std::min((size_t)65536, max_buffer_size_));
  }

  WritableFileWriter(const WritableFileWriter&) = delete;

  WritableFileWriter& operator=(const WritableFileWriter&) = delete;

  ~WritableFileWriter() { Close(); }

  Status Append(const Slice& data);

  Status Pad(const size_t pad_bytes);

  Status Flush();

  Status Close();

  Status Sync(bool use_fsync);

  // Sync only the data that was already Flush()ed. Safe to call concurrently
  // with Append() and Flush(). If !writable_file_->IsSyncThreadSafe(),
  // returns NotSupported status.
  Status SyncWithoutFlush(bool use_fsync);

  uint64_t GetFileSize() { return filesize_; }

  Status InvalidateCache(size_t offset, size_t length) {
    return writable_file_->InvalidateCache(offset, length);
  }

  WritableFile* writable_file() const { return writable_file_.get(); }

  bool use_direct_io() { return writable_file_->use_direct_io(); }

  bool TEST_BufferIsEmpty() { return buf_.CurrentSize() == 0; }

 private:
  // Used when os buffering is OFF and we are writing
  // DMA such as in Direct I/O mode
#ifndef ROCKSDB_LITE
  Status WriteDirect();
#endif  // !ROCKSDB_LITE
  // Normal write
  Status WriteBuffered(const char* data, size_t size);
  Status RangeSync(uint64_t offset, uint64_t nbytes);
  Status SyncInternal(bool use_fsync);
};

class FilePrefetchBuffer {
 public:
  FilePrefetchBuffer() : buffer_offset_(0), buffer_len_(0) {}
  Status Prefetch(RandomAccessFileReader* reader, uint64_t offset, size_t n);
  bool TryReadFromCache(uint64_t offset, size_t n, Slice* result) const;

 private:
  AlignedBuffer buffer_;
  uint64_t buffer_offset_;
  size_t buffer_len_;
};

extern Status NewWritableFile(Env* env, const std::string& fname,
                              unique_ptr<WritableFile>* result,
                              const EnvOptions& options);
}  // namespace rocksdb
