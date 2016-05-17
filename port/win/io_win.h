//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <rocksdb/Status.h>
#include <rocksdb/env.h>

#include "util/aligned_buffer.h"

#include <string>
#include <stdint.h>

#include <Windows.h>

#include <mutex>

namespace rocksdb {
namespace port {

std::string GetWindowsErrSz(DWORD err);

inline Status IOErrorFromWindowsError(const std::string& context, DWORD err) {
  return Status::IOError(context, GetWindowsErrSz(err));
}

inline Status IOErrorFromLastWindowsError(const std::string& context) {
  return IOErrorFromWindowsError(context, GetLastError());
}

inline Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number));
}

// Note the below two do not set errno because they are used only here in this
// file
// on a Windows handle and, therefore, not necessary. Translating GetLastError()
// to errno
// is a sad business
inline int fsync(HANDLE hFile) {
  if (!FlushFileBuffers(hFile)) {
    return -1;
  }

  return 0;
}

SSIZE_T pwrite(HANDLE hFile, const char* src, size_t numBytes,
  uint64_t offset);

SSIZE_T pread(HANDLE hFile, char* src, size_t numBytes, uint64_t offset);

Status fallocate(const std::string& filename, HANDLE hFile,
  uint64_t to_size);

Status ftruncate(const std::string& filename, HANDLE hFile,
  uint64_t toSize);


size_t GetUniqueIdFromFile(HANDLE hFile, char* id, size_t max_size);

// mmap() based random-access
class WinMmapReadableFile : public RandomAccessFile {
  const std::string fileName_;
  HANDLE hFile_;
  HANDLE hMap_;

  const void* mapped_region_;
  const size_t length_;

public:
  // mapped_region_[0,length-1] contains the mmapped contents of the file.
  WinMmapReadableFile(const std::string& fileName, HANDLE hFile, HANDLE hMap,
    const void* mapped_region, size_t length);

  ~WinMmapReadableFile();

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
    char* scratch) const override;

  virtual Status InvalidateCache(size_t offset, size_t length) override;

  virtual size_t GetUniqueId(char* id, size_t max_size) const override;
};

// We preallocate and use memcpy to append new
// data to the file.  This is safe since we either properly close the
// file before reading from it, or for log files, the reading code
// knows enough to skip zero suffixes.
class WinMmapFile : public WritableFile {
private:
  const std::string filename_;
  HANDLE hFile_;
  HANDLE hMap_;

  const size_t page_size_;  // We flush the mapping view in page_size
  // increments. We may decide if this is a memory
  // page size or SSD page size
  const size_t
    allocation_granularity_;  // View must start at such a granularity

  size_t reserved_size_;      // Preallocated size

  size_t mapping_size_;         // The max size of the mapping object
  // we want to guess the final file size to minimize the remapping
  size_t view_size_;            // How much memory to map into a view at a time

  char* mapped_begin_;  // Must begin at the file offset that is aligned with
  // allocation_granularity_
  char* mapped_end_;
  char* dst_;  // Where to write next  (in range [mapped_begin_,mapped_end_])
  char* last_sync_;  // Where have we synced up to

  uint64_t file_offset_;  // Offset of mapped_begin_ in file

  // Do we have unsynced writes?
  bool pending_sync_;

  // Can only truncate or reserve to a sector size aligned if
  // used on files that are opened with Unbuffered I/O
  Status TruncateFile(uint64_t toSize);

  Status UnmapCurrentRegion();

  Status MapNewRegion();

  virtual Status PreallocateInternal(uint64_t spaceToReserve);

public:

  WinMmapFile(const std::string& fname, HANDLE hFile, size_t page_size,
    size_t allocation_granularity, const EnvOptions& options);

  ~WinMmapFile();

  virtual Status Append(const Slice& data) override;

  // Means Close() will properly take care of truncate
  // and it does not need any additional information
  virtual Status Truncate(uint64_t size) override;

  virtual Status Close() override;

  virtual Status Flush() override;

  // Flush only data
  virtual Status Sync() override;

  /**
  * Flush data as well as metadata to stable storage.
  */
  virtual Status Fsync() override;

  /**
  * Get the size of valid data in the file. This will not match the
  * size that is returned from the filesystem because we use mmap
  * to extend file by map_size every time.
  */
  virtual uint64_t GetFileSize() override;

  virtual Status InvalidateCache(size_t offset, size_t length) override;

  virtual Status Allocate(uint64_t offset, uint64_t len) override;

  virtual size_t GetUniqueId(char* id, size_t max_size) const override;
};

class WinSequentialFile : public SequentialFile {
private:
  const std::string filename_;
  HANDLE file_;

  // There is no equivalent of advising away buffered pages as in posix.
  // To implement this flag we would need to do unbuffered reads which
  // will need to be aligned (not sure there is a guarantee that the buffer
  // passed in is aligned).
  // Hence we currently ignore this flag. It is used only in a few cases
  // which should not be perf critical.
  // If perf evaluation finds this to be a problem, we can look into
  // implementing this.
  bool use_os_buffer_;

public:
  WinSequentialFile(const std::string& fname, HANDLE f,
    const EnvOptions& options);

  ~WinSequentialFile();

  virtual Status Read(size_t n, Slice* result, char* scratch) override;

  virtual Status Skip(uint64_t n) override;

  virtual Status InvalidateCache(size_t offset, size_t length) override;
};

// pread() based random-access
class WinRandomAccessFile : public RandomAccessFile {
  const std::string filename_;
  HANDLE hFile_;
  const bool use_os_buffer_;
  bool read_ahead_;
  const size_t compaction_readahead_size_;
  const size_t random_access_max_buffer_size_;
  mutable std::mutex buffer_mut_;
  mutable AlignedBuffer buffer_;
  mutable uint64_t
    buffered_start_;  // file offset set that is currently buffered

  /*
  * The function reads a requested amount of bytes into the specified aligned
  * buffer Upon success the function sets the length of the buffer to the
  * amount of bytes actually read even though it might be less than actually
  * requested. It then copies the amount of bytes requested by the user (left)
  * to the user supplied buffer (dest) and reduces left by the amount of bytes
  * copied to the user buffer
  *
  * @user_offset [in] - offset on disk where the read was requested by the user
  * @first_page_start [in] - actual page aligned disk offset that we want to
  *                          read from
  * @bytes_to_read [in] - total amount of bytes that will be read from disk
  *                       which is generally greater or equal to the amount
  *                       that the user has requested due to the
  *                       either alignment requirements or read_ahead in
  *                       effect.
  * @left [in/out] total amount of bytes that needs to be copied to the user
  *                buffer. It is reduced by the amount of bytes that actually
  *                copied
  * @buffer - buffer to use
  * @dest - user supplied buffer
  */
  SSIZE_T ReadIntoBuffer(uint64_t user_offset, uint64_t first_page_start,
    size_t bytes_to_read, size_t& left,
    AlignedBuffer& buffer, char* dest) const;
  
  SSIZE_T ReadIntoOneShotBuffer(uint64_t user_offset, uint64_t first_page_start,
    size_t bytes_to_read, size_t& left,
    char* dest) const;

  SSIZE_T ReadIntoInstanceBuffer(uint64_t user_offset,
    uint64_t first_page_start,
    size_t bytes_to_read, size_t& left,
    char* dest) const;

  void CalculateReadParameters(uint64_t offset, size_t bytes_requested,
    size_t& actual_bytes_toread,
    uint64_t& first_page_start) const;

  // Override for behavior change
  virtual SSIZE_T PositionedReadInternal(char* src, size_t numBytes,
     uint64_t offset) const;

public:
  WinRandomAccessFile(const std::string& fname, HANDLE hFile, size_t alignment,
    const EnvOptions& options);

  ~WinRandomAccessFile();

  virtual void EnableReadAhead() override;

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
    char* scratch) const override;

  virtual bool ShouldForwardRawRequest() const override;

  virtual void Hint(AccessPattern pattern) override;

  virtual Status InvalidateCache(size_t offset, size_t length) override;

  virtual size_t GetUniqueId(char* id, size_t max_size) const override;
};


// This is a sequential write class. It has been mimicked (as others) after
// the original Posix class. We add support for unbuffered I/O on windows as
// well
// we utilize the original buffer as an alignment buffer to write directly to
// file with no buffering.
// No buffering requires that the provided buffer is aligned to the physical
// sector size (SSD page size) and
// that all SetFilePointer() operations to occur with such an alignment.
// We thus always write in sector/page size increments to the drive and leave
// the tail for the next write OR for Close() at which point we pad with zeros.
// No padding is required for
// buffered access.
class WinWritableFile : public WritableFile {
private:
  const std::string filename_;
  HANDLE            hFile_;
  const bool        use_os_buffer_;  // Used to indicate unbuffered access, the file
  const uint64_t    alignment_;
  // must be opened as unbuffered if false
  uint64_t          filesize_;      // How much data is actually written disk
  uint64_t          reservedsize_;  // how far we have reserved space

  virtual Status PreallocateInternal(uint64_t spaceToReserve);

public:
  WinWritableFile(const std::string& fname, HANDLE hFile, size_t alignment,
    size_t capacity, const EnvOptions& options);

  ~WinWritableFile();

  // Indicates if the class makes use of unbuffered I/O
  virtual bool UseOSBuffer() const override;

  virtual size_t GetRequiredBufferAlignment() const override;

  virtual Status Append(const Slice& data) override;

  virtual Status PositionedAppend(const Slice& data, uint64_t offset) override;

  // Need to implement this so the file is truncated correctly
  // when buffered and unbuffered mode
  virtual Status Truncate(uint64_t size) override;

  virtual Status Close() override;

  // write out the cached data to the OS cache
  // This is now taken care of the WritableFileWriter
  virtual Status Flush() override;

  virtual Status Sync() override;

  virtual Status Fsync() override;

  virtual uint64_t GetFileSize() override;

  virtual Status Allocate(uint64_t offset, uint64_t len) override;

  virtual size_t GetUniqueId(char* id, size_t max_size) const override;
};

class WinDirectory : public Directory {
public:
  WinDirectory() {}

  virtual Status Fsync() override;
};

class WinFileLock : public FileLock {
public:
  explicit WinFileLock(HANDLE hFile) : hFile_(hFile) {
    assert(hFile != NULL);
    assert(hFile != INVALID_HANDLE_VALUE);
  }

  ~WinFileLock();

private:
  HANDLE hFile_;
};

}
}
