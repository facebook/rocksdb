//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/file_reader_writer.h"

#include <algorithm>
#include <mutex>

#include "port/port.h"
#include "util/histogram.h"
#include "util/iostats_context_imp.h"
#include "util/random.h"
#include "util/rate_limiter.h"
#include "util/sync_point.h"

namespace rocksdb {

Status SequentialFileReader::Read(size_t n, Slice* result, char* scratch) {
  Status s = file_->Read(n, result, scratch);
  IOSTATS_ADD(bytes_read, result->size());
  return s;
}

Status SequentialFileReader::Skip(uint64_t n) { return file_->Skip(n); }

Status RandomAccessFileReader::Read(uint64_t offset, size_t n, Slice* result,
                                    char* scratch) const {
  Status s;
  uint64_t elapsed = 0;
  {
    StopWatch sw(env_, stats_, hist_type_,
                 (stats_ != nullptr) ? &elapsed : nullptr);
    IOSTATS_TIMER_GUARD(read_nanos);
    s = file_->Read(offset, n, result, scratch);
    IOSTATS_ADD_IF_POSITIVE(bytes_read, result->size());
  }
  if (stats_ != nullptr && file_read_hist_ != nullptr) {
    file_read_hist_->Add(elapsed);
  }
  return s;
}

Status WritableFileWriter::Append(const Slice& data) {
  const char* src = data.data();
  size_t left = data.size();
  Status s;
  pending_sync_ = true;
  pending_fsync_ = true;

  TEST_KILL_RANDOM("WritableFileWriter::Append:0",
                   rocksdb_kill_odds * REDUCE_ODDS2);

  {
    IOSTATS_TIMER_GUARD(prepare_write_nanos);
    TEST_SYNC_POINT("WritableFileWriter::Append:BeforePrepareWrite");
    writable_file_->PrepareWrite(static_cast<size_t>(GetFileSize()), left);
  }

  // Flush only when I/O is buffered
  if (use_os_buffer_ &&
    (buf_.Capacity() - buf_.CurrentSize()) < left) {
    if (buf_.CurrentSize() > 0) {
      s = Flush();
      if (!s.ok()) {
        return s;
      }
    }

    if (buf_.Capacity() < max_buffer_size_) {
      size_t desiredCapacity = buf_.Capacity() * 2;
      desiredCapacity = std::min(desiredCapacity, max_buffer_size_);
      buf_.AllocateNewBuffer(desiredCapacity);
    }
    assert(buf_.CurrentSize() == 0);
  }

  // We never write directly to disk with unbuffered I/O on.
  // or we simply use it for its original purpose to accumulate many small
  // chunks
  if (!use_os_buffer_ || (buf_.Capacity() >= left)) {
    while (left > 0) {
      size_t appended = buf_.Append(src, left);
      left -= appended;
      src += appended;

      if (left > 0) {
        s = Flush();
        if (!s.ok()) {
          break;
        }

        // We double the buffer here because
        // Flush calls do not keep up with the incoming bytes
        // This is the only place when buffer is changed with unbuffered I/O
        if (buf_.Capacity() < max_buffer_size_) {
          size_t desiredCapacity = buf_.Capacity() * 2;
          desiredCapacity = std::min(desiredCapacity, max_buffer_size_);
          buf_.AllocateNewBuffer(desiredCapacity);
        }
      }
    }
  } else {
    // Writing directly to file bypassing the buffer
    assert(buf_.CurrentSize() == 0);
    s = WriteBuffered(src, left);
  }

  TEST_KILL_RANDOM("WritableFileWriter::Append:1", rocksdb_kill_odds);
  if (s.ok()) {
    filesize_ += data.size();
  }
  return s;
}

Status WritableFileWriter::Close() {

  // Do not quit immediately on failure the file MUST be closed
  Status s;

  // Possible to close it twice now as we MUST close
  // in __dtor, simply flushing is not enough
  // Windows when pre-allocating does not fill with zeros
  // also with unbuffered access we also set the end of data.
  if (!writable_file_) {
    return s;
  }

  s = Flush();  // flush cache to OS

  // In unbuffered mode we write whole pages so
  // we need to let the file know where data ends.
  Status interim = writable_file_->Truncate(filesize_);
  if (!interim.ok() && s.ok()) {
    s = interim;
  }

  TEST_KILL_RANDOM("WritableFileWriter::Close:0", rocksdb_kill_odds);
  interim = writable_file_->Close();
  if (!interim.ok() && s.ok()) {
    s = interim;
  }

  writable_file_.reset();
  TEST_KILL_RANDOM("WritableFileWriter::Close:1", rocksdb_kill_odds);

  return s;
}

// write out the cached data to the OS cache
Status WritableFileWriter::Flush() {
  Status s;
  TEST_KILL_RANDOM("WritableFileWriter::Flush:0",
                   rocksdb_kill_odds * REDUCE_ODDS2);

  if (buf_.CurrentSize() > 0) {
    if (use_os_buffer_) {
      s = WriteBuffered(buf_.BufferStart(), buf_.CurrentSize());
    } else {
      s = WriteUnbuffered();
    }
    if (!s.ok()) {
      return s;
    }
  }

  s = writable_file_->Flush();

  if (!s.ok()) {
    return s;
  }

  // sync OS cache to disk for every bytes_per_sync_
  // TODO: give log file and sst file different options (log
  // files could be potentially cached in OS for their whole
  // life time, thus we might not want to flush at all).

  // We try to avoid sync to the last 1MB of data. For two reasons:
  // (1) avoid rewrite the same page that is modified later.
  // (2) for older version of OS, write can block while writing out
  //     the page.
  // Xfs does neighbor page flushing outside of the specified ranges. We
  // need to make sure sync range is far from the write offset.
  if (!direct_io_ && bytes_per_sync_) {
    const uint64_t kBytesNotSyncRange = 1024 * 1024;  // recent 1MB is not synced.
    const uint64_t kBytesAlignWhenSync = 4 * 1024;    // Align 4KB.
    if (filesize_ > kBytesNotSyncRange) {
      uint64_t offset_sync_to = filesize_ - kBytesNotSyncRange;
      offset_sync_to -= offset_sync_to % kBytesAlignWhenSync;
      assert(offset_sync_to >= last_sync_size_);
      if (offset_sync_to > 0 &&
          offset_sync_to - last_sync_size_ >= bytes_per_sync_) {
        s = RangeSync(last_sync_size_, offset_sync_to - last_sync_size_);
        last_sync_size_ = offset_sync_to;
      }
    }
  }

  return s;
}

Status WritableFileWriter::Sync(bool use_fsync) {
  Status s = Flush();
  if (!s.ok()) {
    return s;
  }
  TEST_KILL_RANDOM("WritableFileWriter::Sync:0", rocksdb_kill_odds);
  if (!direct_io_ && pending_sync_) {
    s = SyncInternal(use_fsync);
    if (!s.ok()) {
      return s;
    }
  }
  TEST_KILL_RANDOM("WritableFileWriter::Sync:1", rocksdb_kill_odds);
  pending_sync_ = false;
  if (use_fsync) {
    pending_fsync_ = false;
  }
  return Status::OK();
}

Status WritableFileWriter::SyncWithoutFlush(bool use_fsync) {
  if (!writable_file_->IsSyncThreadSafe()) {
    return Status::NotSupported(
      "Can't WritableFileWriter::SyncWithoutFlush() because "
      "WritableFile::IsSyncThreadSafe() is false");
  }
  TEST_SYNC_POINT("WritableFileWriter::SyncWithoutFlush:1");
  Status s = SyncInternal(use_fsync);
  TEST_SYNC_POINT("WritableFileWriter::SyncWithoutFlush:2");
  return s;
}

Status WritableFileWriter::SyncInternal(bool use_fsync) {
  Status s;
  IOSTATS_TIMER_GUARD(fsync_nanos);
  TEST_SYNC_POINT("WritableFileWriter::SyncInternal:0");
  if (use_fsync) {
    s = writable_file_->Fsync();
  } else {
    s = writable_file_->Sync();
  }
  return s;
}

Status WritableFileWriter::RangeSync(off_t offset, off_t nbytes) {
  IOSTATS_TIMER_GUARD(range_sync_nanos);
  TEST_SYNC_POINT("WritableFileWriter::RangeSync:0");
  return writable_file_->RangeSync(offset, nbytes);
}

size_t WritableFileWriter::RequestToken(size_t bytes, bool align) {
  Env::IOPriority io_priority;
  if (rate_limiter_ && (io_priority = writable_file_->GetIOPriority()) <
      Env::IO_TOTAL) {
    bytes = std::min(
      bytes, static_cast<size_t>(rate_limiter_->GetSingleBurstBytes()));

    if (align) {
      // Here we may actually require more than burst and block
      // but we can not write less than one page at a time on unbuffered
      // thus we may want not to use ratelimiter s
      size_t alignment = buf_.Alignment();
      bytes = std::max(alignment, TruncateToPageBoundary(alignment, bytes));
    }
    rate_limiter_->Request(bytes, io_priority);
  }
  return bytes;
}

// This method writes to disk the specified data and makes use of the rate
// limiter if available
Status WritableFileWriter::WriteBuffered(const char* data, size_t size) {
  Status s;
  assert(use_os_buffer_);
  const char* src = data;
  size_t left = size;

  while (left > 0) {
    size_t allowed = RequestToken(left, false);

    {
      IOSTATS_TIMER_GUARD(write_nanos);
      TEST_SYNC_POINT("WritableFileWriter::Flush:BeforeAppend");
      s = writable_file_->Append(Slice(src, allowed));
      if (!s.ok()) {
        return s;
      }
    }

    IOSTATS_ADD(bytes_written, allowed);
    TEST_KILL_RANDOM("WritableFileWriter::WriteBuffered:0", rocksdb_kill_odds);

    left -= allowed;
    src += allowed;
  }
  buf_.Size(0);
  return s;
}


// This flushes the accumulated data in the buffer. We pad data with zeros if
// necessary to the whole page.
// However, during automatic flushes padding would not be necessary.
// We always use RateLimiter if available. We move (Refit) any buffer bytes
// that are left over the
// whole number of pages to be written again on the next flush because we can
// only write on aligned
// offsets.
Status WritableFileWriter::WriteUnbuffered() {
  Status s;

  assert(!use_os_buffer_);
  const size_t alignment = buf_.Alignment();
  assert((next_write_offset_ % alignment) == 0);

  // Calculate whole page final file advance if all writes succeed
  size_t file_advance =
    TruncateToPageBoundary(alignment, buf_.CurrentSize());

  // Calculate the leftover tail, we write it here padded with zeros BUT we
  // will write
  // it again in the future either on Close() OR when the current whole page
  // fills out
  size_t leftover_tail = buf_.CurrentSize() - file_advance;

  // Round up and pad
  buf_.PadToAlignmentWith(0);

  const char* src = buf_.BufferStart();
  uint64_t write_offset = next_write_offset_;
  size_t left = buf_.CurrentSize();

  while (left > 0) {
    // Check how much is allowed
    size_t size = RequestToken(left, true);

    {
      IOSTATS_TIMER_GUARD(write_nanos);
      TEST_SYNC_POINT("WritableFileWriter::Flush:BeforeAppend");
      // Unbuffered writes must be positional
      s = writable_file_->PositionedAppend(Slice(src, size), write_offset);
      if (!s.ok()) {
        buf_.Size(file_advance + leftover_tail);
        return s;
      }
    }

    IOSTATS_ADD(bytes_written, size);
    left -= size;
    src += size;
    write_offset += size;
    assert((next_write_offset_ % alignment) == 0);
  }

  if (s.ok()) {
    // Move the tail to the beginning of the buffer
    // This never happens during normal Append but rather during
    // explicit call to Flush()/Sync() or Close()
    buf_.RefitTail(file_advance, leftover_tail);
    // This is where we start writing next time which may or not be
    // the actual file size on disk. They match if the buffer size
    // is a multiple of whole pages otherwise filesize_ is leftover_tail
    // behind
    next_write_offset_ += file_advance;
  }
  return s;
}


namespace {
class ReadaheadRandomAccessFile : public RandomAccessFile {
 public:
  ReadaheadRandomAccessFile(std::unique_ptr<RandomAccessFile>&& file,
                            size_t readahead_size)
      : file_(std::move(file)),
        readahead_size_(readahead_size),
        forward_calls_(file_->ShouldForwardRawRequest()),
        buffer_(),
        buffer_offset_(0),
        buffer_len_(0) {
    if (!forward_calls_) {
      buffer_.reset(new char[readahead_size_]);
    } else if (readahead_size_ > 0) {
      file_->EnableReadAhead();
    }
  }

 ReadaheadRandomAccessFile(const ReadaheadRandomAccessFile&) = delete;

 ReadaheadRandomAccessFile& operator=(const ReadaheadRandomAccessFile&) = delete;

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const override {
    if (n >= readahead_size_) {
      return file_->Read(offset, n, result, scratch);
    }

    // On Windows in unbuffered mode this will lead to double buffering
    // and double locking so we avoid that.
    // In normal mode Windows caches so much data from disk that we do
    // not need readahead.
    if (forward_calls_) {
      return file_->Read(offset, n, result, scratch);
    }

    std::unique_lock<std::mutex> lk(lock_);

    size_t copied = 0;
    // if offset between [buffer_offset_, buffer_offset_ + buffer_len>
    if (offset >= buffer_offset_ && offset < buffer_len_ + buffer_offset_) {
      uint64_t offset_in_buffer = offset - buffer_offset_;
      copied = std::min(buffer_len_ - static_cast<size_t>(offset_in_buffer), n);
      memcpy(scratch, buffer_.get() + offset_in_buffer, copied);
      if (copied == n) {
        // fully cached
        *result = Slice(scratch, n);
        return Status::OK();
      }
    }
    Slice readahead_result;
    Status s = file_->Read(offset + copied, readahead_size_, &readahead_result,
      buffer_.get());
    if (!s.ok()) {
      return s;
    }

    auto left_to_copy = std::min(readahead_result.size(), n - copied);
    memcpy(scratch + copied, readahead_result.data(), left_to_copy);
    *result = Slice(scratch, copied + left_to_copy);

    if (readahead_result.data() == buffer_.get()) {
      buffer_offset_ = offset + copied;
      buffer_len_ = readahead_result.size();
    } else {
      buffer_len_ = 0;
    }

    return Status::OK();
  }

  virtual size_t GetUniqueId(char* id, size_t max_size) const override {
    return file_->GetUniqueId(id, max_size);
  }

  virtual void Hint(AccessPattern pattern) override { file_->Hint(pattern); }

  virtual Status InvalidateCache(size_t offset, size_t length) override {
    return file_->InvalidateCache(offset, length);
  }

 private:
  std::unique_ptr<RandomAccessFile> file_;
  size_t               readahead_size_;
  const bool           forward_calls_;

  mutable std::mutex   lock_;
  mutable std::unique_ptr<char[]> buffer_;
  mutable uint64_t     buffer_offset_;
  mutable size_t       buffer_len_;
};
}  // namespace

std::unique_ptr<RandomAccessFile> NewReadaheadRandomAccessFile(
    std::unique_ptr<RandomAccessFile>&& file, size_t readahead_size) {
  std::unique_ptr<RandomAccessFile> result(
    new ReadaheadRandomAccessFile(std::move(file), readahead_size));
  return result;
}

Status NewWritableFile(Env* env, const std::string& fname,
                       unique_ptr<WritableFile>* result,
                       const EnvOptions& options) {
  Status s = env->NewWritableFile(fname, result, options);
  TEST_KILL_RANDOM("NewWritableFile:0", rocksdb_kill_odds * REDUCE_ODDS2);
  return s;
}

}  // namespace rocksdb
