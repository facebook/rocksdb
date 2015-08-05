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
#include "port/port.h"
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
  StopWatch sw(env_, stats_, hist_type_);
  IOSTATS_TIMER_GUARD(read_nanos);
  Status s = file_->Read(offset, n, result, scratch);
  IOSTATS_ADD_IF_POSITIVE(bytes_read, result->size());
  return s;
}

Status WritableFileWriter::Append(const Slice& data) {
  const char* src = data.data();
  size_t left = data.size();
  Status s;
  pending_sync_ = true;
  pending_fsync_ = true;

  TEST_KILL_RANDOM(rocksdb_kill_odds * REDUCE_ODDS2);

  writable_file_->PrepareWrite(static_cast<size_t>(GetFileSize()), left);
  // if there is no space in the cache, then flush
  if (cursize_ + left > capacity_) {
    s = Flush();
    if (!s.ok()) {
      return s;
    }
    // Increase the buffer size, but capped at 1MB
    if (capacity_ < (1 << 20)) {
      capacity_ *= 2;
      buf_.reset(new char[capacity_]);
    }
    assert(cursize_ == 0);
  }

  // if the write fits into the cache, then write to cache
  // otherwise do a write() syscall to write to OS buffers.
  if (cursize_ + left <= capacity_) {
    memcpy(buf_.get() + cursize_, src, left);
    cursize_ += left;
  } else {
    while (left != 0) {
      size_t size = RequestToken(left);
      {
        IOSTATS_TIMER_GUARD(write_nanos);
        s = writable_file_->Append(Slice(src, size));
        if (!s.ok()) {
          return s;
        }
      }
      IOSTATS_ADD(bytes_written, size);
      TEST_KILL_RANDOM(rocksdb_kill_odds);

      left -= size;
      src += size;
    }
  }
  TEST_KILL_RANDOM(rocksdb_kill_odds);
  filesize_ += data.size();
  return Status::OK();
}

Status WritableFileWriter::Close() {
  Status s;
  s = Flush();  // flush cache to OS
  if (!s.ok()) {
    return s;
  }

  TEST_KILL_RANDOM(rocksdb_kill_odds);
  return writable_file_->Close();
}

// write out the cached data to the OS cache
Status WritableFileWriter::Flush() {
  TEST_KILL_RANDOM(rocksdb_kill_odds * REDUCE_ODDS2);
  size_t left = cursize_;
  char* src = buf_.get();
  while (left != 0) {
    size_t size = RequestToken(left);
    {
      IOSTATS_TIMER_GUARD(write_nanos);
      Status s = writable_file_->Append(Slice(src, size));
      if (!s.ok()) {
        return s;
      }
    }
    IOSTATS_ADD(bytes_written, size);
    TEST_KILL_RANDOM(rocksdb_kill_odds * REDUCE_ODDS2);
    left -= size;
    src += size;
  }
  cursize_ = 0;

  writable_file_->Flush();

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
  if (bytes_per_sync_) {
    uint64_t kBytesNotSyncRange = 1024 * 1024;  // recent 1MB is not synced.
    uint64_t kBytesAlignWhenSync = 4 * 1024;    // Align 4KB.
    if (filesize_ > kBytesNotSyncRange) {
      uint64_t offset_sync_to = filesize_ - kBytesNotSyncRange;
      offset_sync_to -= offset_sync_to % kBytesAlignWhenSync;
      assert(offset_sync_to >= last_sync_size_);
      if (offset_sync_to > 0 &&
          offset_sync_to - last_sync_size_ >= bytes_per_sync_) {
        RangeSync(last_sync_size_, offset_sync_to - last_sync_size_);
        last_sync_size_ = offset_sync_to;
      }
    }
  }

  return Status::OK();
}

Status WritableFileWriter::Sync(bool use_fsync) {
  Status s = Flush();
  if (!s.ok()) {
    return s;
  }
  TEST_KILL_RANDOM(rocksdb_kill_odds);
  if (pending_sync_) {
    s = SyncInternal(use_fsync);
    if (!s.ok()) {
      return s;
    }
  }
  TEST_KILL_RANDOM(rocksdb_kill_odds);
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
  if (use_fsync) {
    s = writable_file_->Fsync();
  } else {
    s = writable_file_->Sync();
  }
  return s;
}

Status WritableFileWriter::RangeSync(off_t offset, off_t nbytes) {
  IOSTATS_TIMER_GUARD(range_sync_nanos);
  return writable_file_->RangeSync(offset, nbytes);
}

size_t WritableFileWriter::RequestToken(size_t bytes) {
  Env::IOPriority io_priority;
  if (rate_limiter_&&(io_priority = writable_file_->GetIOPriority()) <
      Env::IO_TOTAL) {
    bytes = std::min(bytes,
                     static_cast<size_t>(rate_limiter_->GetSingleBurstBytes()));
    rate_limiter_->Request(bytes, io_priority);
  }
  return bytes;
}

Status RandomRWFileAccessor::Write(uint64_t offset, const Slice& data) {
  Status s;
  pending_sync_ = true;
  pending_fsync_ = true;

  {
    IOSTATS_TIMER_GUARD(write_nanos);
    s = random_rw_file_->Write(offset, data);
    if (!s.ok()) {
      return s;
    }
  }
  IOSTATS_ADD(bytes_written, data.size());

  return s;
}

Status RandomRWFileAccessor::Read(uint64_t offset, size_t n, Slice* result,
                                  char* scratch) const {
  Status s;
  {
    IOSTATS_TIMER_GUARD(read_nanos);
    s = random_rw_file_->Read(offset, n, result, scratch);
    if (!s.ok()) {
      return s;
    }
  }
  IOSTATS_ADD_IF_POSITIVE(bytes_read, result->size());
  return s;
}

Status RandomRWFileAccessor::Close() { return random_rw_file_->Close(); }

Status RandomRWFileAccessor::Sync(bool use_fsync) {
  Status s;
  if (pending_sync_) {
    if (use_fsync) {
      s = random_rw_file_->Fsync();
    } else {
      s = random_rw_file_->Sync();
    }
    if (!s.ok()) {
      return s;
    }
  }
  if (use_fsync) {
    pending_fsync_ = false;
  }
  pending_sync_ = false;

  return s;
}
}  // namespace rocksdb
