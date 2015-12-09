//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <deque>
#include <thread>
#include <ctime>

#include <errno.h>
#include <process.h>
#include <io.h>
#include <direct.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "rocksdb/env.h"
#include "rocksdb/slice.h"

#include "port/port.h"
#include "port/dirent.h"
#include "port/win/win_logger.h"

#include "util/random.h"
#include "util/iostats_context_imp.h"
#include "util/rate_limiter.h"
#include "util/sync_point.h"
#include "util/aligned_buffer.h"

#include "util/thread_status_updater.h"
#include "util/thread_status_util.h"

#include <Rpc.h>  // For UUID generation
#include <Windows.h>

namespace rocksdb {

std::string GetWindowsErrSz(DWORD err) {
  LPSTR lpMsgBuf;
  FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM |
                     FORMAT_MESSAGE_IGNORE_INSERTS,
                 NULL, err,
                 0,  // Default language
                 reinterpret_cast<LPSTR>(&lpMsgBuf), 0, NULL);

  std::string Err = lpMsgBuf;
  LocalFree(lpMsgBuf);
  return Err;
}

namespace {

const size_t c_OneMB = (1 << 20);

ThreadStatusUpdater* CreateThreadStatusUpdater() {
  return new ThreadStatusUpdater();
}

inline Status IOErrorFromWindowsError(const std::string& context, DWORD err) {
  return Status::IOError(context, GetWindowsErrSz(err));
}

inline Status IOErrorFromLastWindowsError(const std::string& context) {
  return IOErrorFromWindowsError(context, GetLastError());
}

inline Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number));
}

// TODO(sdong): temp logging. Need to help debugging. Remove it when
// the feature is proved to be stable.
inline void PrintThreadInfo(size_t thread_id, size_t terminatingId) {
  fprintf(stdout, "Bg thread %Iu terminates %Iu\n", thread_id, terminatingId);
}

// returns the ID of the current process
inline int current_process_id() { return _getpid(); }

// RAII helpers for HANDLEs
const auto CloseHandleFunc = [](HANDLE h) { ::CloseHandle(h); };
typedef std::unique_ptr<void, decltype(CloseHandleFunc)> UniqueCloseHandlePtr;

// We preserve the original name of this interface to denote the original idea
// behind it.
// All reads happen by a specified offset and pwrite interface does not change
// the position of the file pointer. Judging from the man page and errno it does
// execute
// lseek atomically to return the position of the file back where it was.
// WriteFile() does not
// have this capability. Therefore, for both pread and pwrite the pointer is
// advanced to the next position
// which is fine for writes because they are (should be) sequential.
// Because all the reads/writes happen by the specified offset, the caller in
// theory should not
// rely on the current file offset.
SSIZE_T pwrite(HANDLE hFile, const char* src, size_t numBytes,
               uint64_t offset) {
  assert(numBytes <= std::numeric_limits<DWORD>::max());
  OVERLAPPED overlapped = {0};
  ULARGE_INTEGER offsetUnion;
  offsetUnion.QuadPart = offset;

  overlapped.Offset = offsetUnion.LowPart;
  overlapped.OffsetHigh = offsetUnion.HighPart;

  SSIZE_T result = 0;

  unsigned long bytesWritten = 0;

  if (FALSE == WriteFile(hFile, src, static_cast<DWORD>(numBytes), &bytesWritten,
    &overlapped)) {
    result = -1;
  } else {
    result = bytesWritten;
  }

  return result;
}

// See comments for pwrite above
SSIZE_T pread(HANDLE hFile, char* src, size_t numBytes, uint64_t offset) {
  assert(numBytes <= std::numeric_limits<DWORD>::max());
  OVERLAPPED overlapped = {0};
  ULARGE_INTEGER offsetUnion;
  offsetUnion.QuadPart = offset;

  overlapped.Offset = offsetUnion.LowPart;
  overlapped.OffsetHigh = offsetUnion.HighPart;

  SSIZE_T result = 0;

  unsigned long bytesRead = 0;

  if (FALSE == ReadFile(hFile, src, static_cast<DWORD>(numBytes), &bytesRead,
    &overlapped)) {
    return -1;
  } else {
    result = bytesRead;
  }

  return result;
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

// SetFileInformationByHandle() is capable of fast pre-allocates.
// However, this does not change the file end position unless the file is
// truncated and the pre-allocated space is not considered filled with zeros.
inline Status fallocate(const std::string& filename, HANDLE hFile,
                        uint64_t to_size) {
  Status status;

  FILE_ALLOCATION_INFO alloc_info;
  alloc_info.AllocationSize.QuadPart = to_size;

  if (!SetFileInformationByHandle(hFile, FileAllocationInfo, &alloc_info,
                                  sizeof(FILE_ALLOCATION_INFO))) {
    auto lastError = GetLastError();
    status = IOErrorFromWindowsError(
        "Failed to pre-allocate space: " + filename, lastError);
  }

  return status;
}

inline Status ftruncate(const std::string& filename, HANDLE hFile,
                        uint64_t toSize) {
  Status status;

  FILE_END_OF_FILE_INFO end_of_file;
  end_of_file.EndOfFile.QuadPart = toSize;

  if (!SetFileInformationByHandle(hFile, FileEndOfFileInfo, &end_of_file,
                                  sizeof(FILE_END_OF_FILE_INFO))) {
    auto lastError = GetLastError();
    status = IOErrorFromWindowsError("Failed to Set end of file: " + filename,
                                     lastError);
  }

  return status;
}

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
                      const void* mapped_region, size_t length)
      : fileName_(fileName),
        hFile_(hFile),
        hMap_(hMap),
        mapped_region_(mapped_region),
        length_(length) {}

  ~WinMmapReadableFile() {
    BOOL ret = ::UnmapViewOfFile(mapped_region_);
    assert(ret);

    ret = ::CloseHandle(hMap_);
    assert(ret);

    ret = ::CloseHandle(hFile_);
    assert(ret);
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const override {
    Status s;

    if (offset > length_) {
      *result = Slice();
      return IOError(fileName_, EINVAL);
    } else if (offset + n > length_) {
      n = length_ - offset;
    }
    *result =
        Slice(reinterpret_cast<const char*>(mapped_region_) + offset, n);
    return s;
  }

  virtual Status InvalidateCache(size_t offset, size_t length) override {
    return Status::OK();
  }
};

// We preallocate up to an extra megabyte and use memcpy to append new
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
  size_t mapping_size_;         // We want file mapping to be of a specific size
                                // because then the file is expandable
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
  Status TruncateFile(uint64_t toSize) {
    return ftruncate(filename_, hFile_, toSize);
  }

  // Can only truncate or reserve to a sector size aligned if
  // used on files that are opened with Unbuffered I/O
  // Normally it does not present a problem since in memory mapped files
  // we do not disable buffering
  Status ReserveFileSpace(uint64_t toSize) {
    IOSTATS_TIMER_GUARD(allocate_nanos);
    return fallocate(filename_, hFile_, toSize);
  }

  Status UnmapCurrentRegion() {
    Status status;

    if (mapped_begin_ != nullptr) {
      if (!::UnmapViewOfFile(mapped_begin_)) {
        status = IOErrorFromWindowsError(
            "Failed to unmap file view: " + filename_, GetLastError());
      }

      // UnmapView automatically sends data to disk but not the metadata
      // which is good and provides some equivalent of fdatasync() on Linux
      // therefore, we donot need separate flag for metadata
      pending_sync_ = false;
      mapped_begin_ = nullptr;
      mapped_end_ = nullptr;
      dst_ = nullptr;
      last_sync_ = nullptr;

      // Move on to the next portion of the file
      file_offset_ += view_size_;

      // Increase the amount we map the next time, but capped at 1MB
      view_size_ *= 2;
      view_size_ = std::min(view_size_, c_OneMB);
    }

    return status;
  }

  Status MapNewRegion() {
    Status status;

    assert(mapped_begin_ == nullptr);

    size_t minMappingSize = file_offset_ + view_size_;

    // Check if we need to create a new mapping since we want to write beyond
    // the current one
    // If the mapping view is now too short
    // CreateFileMapping will extend the size of the file automatically if the
    // mapping size is greater than
    // the current length of the file, which reserves the space and makes
    // writing faster, except, windows can not map an empty file.
    // Thus the first time around we must actually extend the file ourselves
    if (hMap_ == NULL || minMappingSize > mapping_size_) {
      if (NULL == hMap_) {
        // Creating mapping for the first time so reserve the space on disk
        status = ReserveFileSpace(minMappingSize);
        if (!status.ok()) {
          return status;
        }
      }

      if (hMap_) {
        // Unmap the previous one
        BOOL ret = ::CloseHandle(hMap_);
        assert(ret);
        hMap_ = NULL;
      }

      // Calculate the new mapping size which will hopefully reserve space for
      // several consecutive sliding views
      // Query preallocation block size if set
      size_t preallocationBlockSize = 0;
      size_t lastAllocatedBlockSize = 0;  // Not used
      GetPreallocationStatus(&preallocationBlockSize, &lastAllocatedBlockSize);

      if (preallocationBlockSize) {
        preallocationBlockSize =
            Roundup(preallocationBlockSize, allocation_granularity_);
      } else {
        preallocationBlockSize = 2 * view_size_;
      }

      mapping_size_ += preallocationBlockSize;

      ULARGE_INTEGER mappingSize;
      mappingSize.QuadPart = mapping_size_;

      hMap_ = CreateFileMappingA(
          hFile_,
          NULL,                  // Security attributes
          PAGE_READWRITE,        // There is not a write only mode for mapping
          mappingSize.HighPart,  // Enable mapping the whole file but the actual
                                 // amount mapped is determined by MapViewOfFile
          mappingSize.LowPart,
          NULL);  // Mapping name

      if (NULL == hMap_) {
        return IOErrorFromWindowsError(
            "WindowsMmapFile failed to create file mapping for: " + filename_,
            GetLastError());
      }
    }

    ULARGE_INTEGER offset;
    offset.QuadPart = file_offset_;

    // View must begin at the granularity aligned offset
    mapped_begin_ = reinterpret_cast<char*>(
        MapViewOfFileEx(hMap_, FILE_MAP_WRITE, offset.HighPart, offset.LowPart,
                        view_size_, NULL));

    if (!mapped_begin_) {
      status = IOErrorFromWindowsError(
          "WindowsMmapFile failed to map file view: " + filename_,
          GetLastError());
    } else {
      mapped_end_ = mapped_begin_ + view_size_;
      dst_ = mapped_begin_;
      last_sync_ = mapped_begin_;
      pending_sync_ = false;
    }
    return status;
  }

 public:
  WinMmapFile(const std::string& fname, HANDLE hFile, size_t page_size,
              size_t allocation_granularity, const EnvOptions& options)
      : filename_(fname),
        hFile_(hFile),
        hMap_(NULL),
        page_size_(page_size),
        allocation_granularity_(allocation_granularity),
        mapping_size_(0),
        view_size_(0),
        mapped_begin_(nullptr),
        mapped_end_(nullptr),
        dst_(nullptr),
        last_sync_(nullptr),
        file_offset_(0),
        pending_sync_(false) {
    // Allocation granularity must be obtained from GetSystemInfo() and must be
    // a power of two.
    assert(allocation_granularity > 0);
    assert((allocation_granularity & (allocation_granularity - 1)) == 0);

    assert(page_size > 0);
    assert((page_size & (page_size - 1)) == 0);

    // Only for memory mapped writes
    assert(options.use_mmap_writes);

    // Make sure buffering is not disabled. It is ignored for mapping
    // purposes but also imposes restriction on moving file position
    // it is not a problem so much with reserving space since it is probably a
    // factor
    // of allocation_granularity but we also want to truncate the file in
    // Close() at
    // arbitrary position so we do not have to feel this with zeros.
    assert(options.use_os_buffer);

    // View size must be both the multiple of allocation_granularity AND the
    // page size
    if ((allocation_granularity_ % page_size_) == 0) {
      view_size_ = 2 * allocation_granularity;
    } else if ((page_size_ % allocation_granularity_) == 0) {
      view_size_ = 2 * page_size_;
    } else {
      // we can multiply them together
      assert(false);
    }
  }

  ~WinMmapFile() {
    if (hFile_) {
      this->Close();
    }
  }

  virtual Status Append(const Slice& data) override {
    const char* src = data.data();
    size_t left = data.size();

    while (left > 0) {
      assert(mapped_begin_ <= dst_);
      size_t avail = mapped_end_ - dst_;

      if (avail == 0) {
        Status s = UnmapCurrentRegion();
        if (s.ok()) {
          s = MapNewRegion();
        }

        if (!s.ok()) {
          return s;
        }
      }

      size_t n = std::min(left, avail);
      memcpy(dst_, src, n);
      dst_ += n;
      src += n;
      left -= n;
      pending_sync_ = true;
    }

    return Status::OK();
  }

  // Means Close() will properly take care of truncate
  // and it does not need any additional information
  virtual Status Truncate(uint64_t size) override {
    return Status::OK();
  }

  virtual Status Close() override {
    Status s;

    assert(NULL != hFile_);

    // We truncate to the precise size so no
    // uninitialized data at the end. SetEndOfFile
    // which we use does not write zeros and it is good.
    uint64_t targetSize = GetFileSize();

    s = UnmapCurrentRegion();

    if (NULL != hMap_) {
      BOOL ret = ::CloseHandle(hMap_);
      if (!ret && s.ok()) {
        auto lastError = GetLastError();
        s = IOErrorFromWindowsError(
            "Failed to Close mapping for file: " + filename_, lastError);
      }

      hMap_ = NULL;
    }

    TruncateFile(targetSize);

    BOOL ret = ::CloseHandle(hFile_);
    hFile_ = NULL;

    if (!ret && s.ok()) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError(
          "Failed to close file map handle: " + filename_, lastError);
    }

    return s;
  }

  virtual Status Flush() override { return Status::OK(); }

  // Flush only data
  virtual Status Sync() override {
    Status s;

    // Some writes occurred since last sync
    if (pending_sync_) {
      assert(mapped_begin_);
      assert(dst_);
      assert(dst_ > mapped_begin_);
      assert(dst_ < mapped_end_);

      size_t page_begin =
          TruncateToPageBoundary(page_size_, last_sync_ - mapped_begin_);
      size_t page_end =
          TruncateToPageBoundary(page_size_, dst_ - mapped_begin_ - 1);
      last_sync_ = dst_;

      // Flush only the amount of that is a multiple of pages
      if (!::FlushViewOfFile(mapped_begin_ + page_begin,
                             (page_end - page_begin) + page_size_)) {
        s = IOErrorFromWindowsError("Failed to FlushViewOfFile: " + filename_,
                                    GetLastError());
      }

      pending_sync_ = false;
    }

    return s;
  }

  /**
  * Flush data as well as metadata to stable storage.
  */
  virtual Status Fsync() override {
    Status s;

    // Flush metadata if pending
    const bool pending = pending_sync_;

    s = Sync();

    // Flush metadata
    if (s.ok() && pending) {
      if (!::FlushFileBuffers(hFile_)) {
        s = IOErrorFromWindowsError("Failed to FlushFileBuffers: " + filename_,
                                    GetLastError());
      }
    }

    return s;
  }

  /**
  * Get the size of valid data in the file. This will not match the
  * size that is returned from the filesystem because we use mmap
  * to extend file by map_size every time.
  */
  virtual uint64_t GetFileSize() override {
    size_t used = dst_ - mapped_begin_;
    return file_offset_ + used;
  }

  virtual Status InvalidateCache(size_t offset, size_t length) override {
    return Status::OK();
  }

  virtual Status Allocate(uint64_t offset, uint64_t len) override {
    return Status::OK();
  }
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
                    const EnvOptions& options)
      : filename_(fname),
        file_(f),
        use_os_buffer_(options.use_os_buffer) {}

  virtual ~WinSequentialFile() {
    assert(file_ != INVALID_HANDLE_VALUE);
    CloseHandle(file_);
  }

  virtual Status Read(size_t n, Slice* result, char* scratch) override {
    Status s;
    size_t r = 0;

    // Windows ReadFile API accepts a DWORD.
    // While it is possible to read in a loop if n is > UINT_MAX
    // it is a highly unlikely case.
    if (n > UINT_MAX) {
      return IOErrorFromWindowsError(filename_, ERROR_INVALID_PARAMETER);
    }

    DWORD bytesToRead = static_cast<DWORD>(n); //cast is safe due to the check above
    DWORD bytesRead = 0;
    BOOL ret = ReadFile(file_, scratch, bytesToRead, &bytesRead, NULL);
    if (ret == TRUE) {
      r = bytesRead;
    } else {
      return IOErrorFromWindowsError(filename_, GetLastError());
    }

    *result = Slice(scratch, r);

    return s;
  }

  virtual Status Skip(uint64_t n) override {
    // Can't handle more than signed max as SetFilePointerEx accepts a signed 64-bit
    // integer. As such it is a highly unlikley case to have n so large.
    if (n > _I64_MAX) {
      return IOErrorFromWindowsError(filename_, ERROR_INVALID_PARAMETER);
    }

    LARGE_INTEGER li;
    li.QuadPart = static_cast<int64_t>(n); //cast is safe due to the check above
    BOOL ret = SetFilePointerEx(file_, li, NULL, FILE_CURRENT);
    if (ret == FALSE) {
      return IOErrorFromWindowsError(filename_, GetLastError());
    }
    return Status::OK();
  }

  virtual Status InvalidateCache(size_t offset, size_t length) override {
    return Status::OK();
  }
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
                         AlignedBuffer& buffer, char* dest) const {
    assert(buffer.CurrentSize() == 0);
    assert(buffer.Capacity() >= bytes_to_read);

    SSIZE_T read =
        pread(hFile_, buffer.Destination(), bytes_to_read, first_page_start);

    if (read > 0) {
      buffer.Size(read);

      // Let's figure out how much we read from the users standpoint
      if ((first_page_start + buffer.CurrentSize()) > user_offset) {
        assert(first_page_start <= user_offset);
        size_t buffer_offset = user_offset - first_page_start;
        read = buffer.Read(dest, buffer_offset, left);
      } else {
        read = 0;
      }
      left -= read;
    }
    return read;
  }

  SSIZE_T ReadIntoOneShotBuffer(uint64_t user_offset, uint64_t first_page_start,
                                size_t bytes_to_read, size_t& left,
                                char* dest) const {
    AlignedBuffer bigBuffer;
    bigBuffer.Alignment(buffer_.Alignment());
    bigBuffer.AllocateNewBuffer(bytes_to_read);

    return ReadIntoBuffer(user_offset, first_page_start, bytes_to_read, left,
                          bigBuffer, dest);
  }

  SSIZE_T ReadIntoInstanceBuffer(uint64_t user_offset,
                                 uint64_t first_page_start,
                                 size_t bytes_to_read, size_t& left,
                                 char* dest) const {
    SSIZE_T read = ReadIntoBuffer(user_offset, first_page_start, bytes_to_read,
                                  left, buffer_, dest);

    if (read > 0) {
      buffered_start_ = first_page_start;
    }

    return read;
  }

 public:
  WinRandomAccessFile(const std::string& fname, HANDLE hFile, size_t alignment,
                      const EnvOptions& options)
      : filename_(fname),
        hFile_(hFile),
        use_os_buffer_(options.use_os_buffer),
        read_ahead_(false),
        compaction_readahead_size_(options.compaction_readahead_size),
        random_access_max_buffer_size_(options.random_access_max_buffer_size),
        buffer_(),
        buffered_start_(0) {
    assert(!options.use_mmap_reads);

    // Unbuffered access, use internal buffer for reads
    if (!use_os_buffer_) {
      // Do not allocate the buffer either until the first request or
      // until there is a call to allocate a read-ahead buffer
      buffer_.Alignment(alignment);
    }
  }

  virtual ~WinRandomAccessFile() {
    if (hFile_ != NULL && hFile_ != INVALID_HANDLE_VALUE) {
      ::CloseHandle(hFile_);
    }
  }

  virtual void EnableReadAhead() override { this->Hint(SEQUENTIAL); }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const override {
    Status s;
    SSIZE_T r = -1;
    size_t left = n;
    char* dest = scratch;

    // When in unbuffered mode we need to do the following changes:
    // - use our own aligned buffer
    // - always read at the offset of that is a multiple of alignment
    if (!use_os_buffer_) {
      std::unique_lock<std::mutex> lock(buffer_mut_);

      // Let's see if at least some of the requested data is already
      // in the buffer
      if (offset >= buffered_start_ &&
          offset < (buffered_start_ + buffer_.CurrentSize())) {
        size_t buffer_offset = offset - buffered_start_;
        r = buffer_.Read(dest, buffer_offset, left);
        assert(r >= 0);

        left -= size_t(r);
        offset += r;
        dest += r;
      }

      // Still some left or none was buffered
      if (left > 0) {
        // Figure out the start/end offset for reading and amount to read
        const size_t alignment = buffer_.Alignment();
        const size_t first_page_start =
            TruncateToPageBoundary(alignment, offset);

        size_t bytes_requested = left;
        if (read_ahead_ && bytes_requested < compaction_readahead_size_) {
          bytes_requested = compaction_readahead_size_;
        }

        const size_t last_page_start =
            TruncateToPageBoundary(alignment, offset + bytes_requested - 1);
        const size_t actual_bytes_toread =
            (last_page_start - first_page_start) + alignment;

        if (buffer_.Capacity() < actual_bytes_toread) {
          // If we are in read-ahead mode or the requested size
          // exceeds max buffer size then use one-shot
          // big buffer otherwise reallocate main buffer
          if (read_ahead_ ||
              (actual_bytes_toread > random_access_max_buffer_size_)) {
            // Unlock the mutex since we are not using instance buffer
            lock.unlock();
            r = ReadIntoOneShotBuffer(offset, first_page_start,
                                      actual_bytes_toread, left, dest);
          } else {
            buffer_.AllocateNewBuffer(actual_bytes_toread);
            r = ReadIntoInstanceBuffer(offset, first_page_start,
                                       actual_bytes_toread, left, dest);
          }
        } else {
          buffer_.Clear();
          r = ReadIntoInstanceBuffer(offset, first_page_start,
                                     actual_bytes_toread, left, dest);
        }
      }
    } else {
      r = pread(hFile_, scratch, left, offset);
      if (r > 0) {
        left -= r;
      }
    }

    *result = Slice(scratch, (r < 0) ? 0 : n - left);

    if (r < 0) {
      s = IOErrorFromLastWindowsError(filename_);
    }
    return s;
  }

  virtual bool ShouldForwardRawRequest() const override {
    return true;
  }

  virtual void Hint(AccessPattern pattern) override {
    if (pattern == SEQUENTIAL && !use_os_buffer_ &&
        compaction_readahead_size_ > 0) {
      std::lock_guard<std::mutex> lg(buffer_mut_);
      if (!read_ahead_) {
        read_ahead_ = true;
        // This would allocate read-ahead size + 2 alignments
        // - one for memory alignment which added implicitly by AlignedBuffer
        // - We add one more alignment because we will read one alignment more
        // from disk
        buffer_.AllocateNewBuffer(compaction_readahead_size_ +
                                  buffer_.Alignment());
      }
    }
  }

  virtual Status InvalidateCache(size_t offset, size_t length) override {
    return Status::OK();
  }
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

 public:
  WinWritableFile(const std::string& fname, HANDLE hFile, size_t alignment,
                  size_t capacity, const EnvOptions& options)
      : filename_(fname),
        hFile_(hFile),
        use_os_buffer_(options.use_os_buffer),
        alignment_(alignment),
        filesize_(0),
        reservedsize_(0) {
    assert(!options.use_mmap_writes);
  }

  ~WinWritableFile() {
    if (NULL != hFile_ && INVALID_HANDLE_VALUE != hFile_) {
      WinWritableFile::Close();
    }
  }

  // Indicates if the class makes use of unbuffered I/O
  virtual bool UseOSBuffer() const override {
    return use_os_buffer_;
  }

  virtual size_t GetRequiredBufferAlignment() const override {
    return alignment_;
  }

  virtual Status Append(const Slice& data) override {

    // Used for buffered access ONLY
    assert(use_os_buffer_);
    assert(data.size() < std::numeric_limits<DWORD>::max());

    Status s;

    DWORD bytesWritten = 0;
    if (!WriteFile(hFile_, data.data(),
        static_cast<DWORD>(data.size()), &bytesWritten, NULL)) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError(
        "Failed to WriteFile: " + filename_,
        lastError);
    } else {
      assert(size_t(bytesWritten) == data.size());
      filesize_ += data.size();
    }

    return s;
  }

  virtual Status PositionedAppend(const Slice& data, uint64_t offset) override {
    Status s;

    SSIZE_T ret = pwrite(hFile_, data.data(), data.size(), offset);

    // Error break
    if (ret < 0) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError(
        "Failed to pwrite for: " + filename_, lastError);
    } else {
      // With positional write it is not clear at all
      // if this actually extends the filesize
      assert(size_t(ret) == data.size());
      filesize_ += data.size();
    }
    return s;
  }

  // Need to implement this so the file is truncated correctly
  // when buffered and unbuffered mode
  virtual Status Truncate(uint64_t size) override {
    Status s =  ftruncate(filename_, hFile_, size);
    if (s.ok()) {
      filesize_ = size;
    }
    return s;
  }

  virtual Status Close() override {

    Status s;

    assert(INVALID_HANDLE_VALUE != hFile_);

    if (fsync(hFile_) < 0) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError("fsync failed at Close() for: " + filename_,
        lastError);
    }

    if (FALSE == ::CloseHandle(hFile_)) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError("CloseHandle failed for: " + filename_,
                                  lastError);
    }

    hFile_ = INVALID_HANDLE_VALUE;
    return s;
  }

  // write out the cached data to the OS cache
  // This is now taken care of the WritableFileWriter
  virtual Status Flush() override {
    return Status::OK();
  }

  virtual Status Sync() override {
    Status s;
    // Calls flush buffers
    if (fsync(hFile_) < 0) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError("fsync failed at Sync() for: " + filename_,
                                  lastError);
    }
    return s;
  }

  virtual Status Fsync() override { return Sync(); }

  virtual uint64_t GetFileSize() override {
    // Double accounting now here with WritableFileWriter
    // and this size will be wrong when unbuffered access is used
    // but tests implement their own writable files and do not use WritableFileWrapper
    // so we need to squeeze a square peg through
    // a round hole here.
    return filesize_;
  }

  virtual Status Allocate(uint64_t offset, uint64_t len) override {
    Status status;
    TEST_KILL_RANDOM("WinWritableFile::Allocate", rocksdb_kill_odds);

    // Make sure that we reserve an aligned amount of space
    // since the reservation block size is driven outside so we want
    // to check if we are ok with reservation here
    size_t spaceToReserve = Roundup(offset + len, alignment_);
    // Nothing to do
    if (spaceToReserve <= reservedsize_) {
      return status;
    }

    IOSTATS_TIMER_GUARD(allocate_nanos);
    status = fallocate(filename_, hFile_, spaceToReserve);
    if (status.ok()) {
      reservedsize_ = spaceToReserve;
    }
    return status;
  }
};

class WinDirectory : public Directory {
 public:
  WinDirectory() {}

  virtual Status Fsync() override { return Status::OK(); }
};

class WinFileLock : public FileLock {
 public:
  explicit WinFileLock(HANDLE hFile) : hFile_(hFile) {
    assert(hFile != NULL);
    assert(hFile != INVALID_HANDLE_VALUE);
  }

  ~WinFileLock() {
    BOOL ret = ::CloseHandle(hFile_);
    assert(ret);
  }

 private:
  HANDLE hFile_;
};

namespace {

void WinthreadCall(const char* label, std::error_code result) {
  if (0 != result.value()) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result.value()));
    abort();
  }
}
}

class WinEnv : public Env {
 public:
  WinEnv();

  virtual ~WinEnv() {
    for (auto& th : threads_to_join_) {
      th.join();
    }

    threads_to_join_.clear();

    for (auto& thpool : thread_pools_) {
      thpool.JoinAllThreads();
    }
    // All threads must be joined before the deletion of
    // thread_status_updater_.
    delete thread_status_updater_;
  }

  virtual Status DeleteFile(const std::string& fname) override {
    Status result;

    if (_unlink(fname.c_str())) {
      result = IOError("Failed to delete: " + fname, errno);
    }

    return result;
  }

  Status GetCurrentTime(int64_t* unix_time) override {
    time_t time = std::time(nullptr);
    if (time == (time_t)(-1)) {
      return Status::NotSupported("Failed to get time");
    }

    *unix_time = time;
    return Status::OK();
  }

  virtual Status NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) override {
    Status s;

    result->reset();

    // Corruption test needs to rename and delete files of these kind
    // while they are still open with another handle. For that reason we
    // allow share_write and delete(allows rename).
    HANDLE hFile = INVALID_HANDLE_VALUE;
    {
      IOSTATS_TIMER_GUARD(open_nanos);
      hFile = CreateFileA(
          fname.c_str(), GENERIC_READ,
          FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
          OPEN_EXISTING,  // Original fopen mode is "rb"
          FILE_ATTRIBUTE_NORMAL, NULL);
    }

    if (INVALID_HANDLE_VALUE == hFile) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError("Failed to open NewSequentialFile" + fname,
                                  lastError);
    } else {
      result->reset(new WinSequentialFile(fname, hFile, options));
    }
    return s;
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     std::unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& options) override {
    result->reset();
    Status s;

    // Open the file for read-only random access
    // Random access is to disable read-ahead as the system reads too much data
    DWORD fileFlags = FILE_ATTRIBUTE_READONLY;

    if (!options.use_os_buffer && !options.use_mmap_reads) {
      fileFlags |= FILE_FLAG_NO_BUFFERING;
    } else {
      fileFlags |= FILE_FLAG_RANDOM_ACCESS;
    }

    /// Shared access is necessary for corruption test to pass
    // almost all tests would work with a possible exception of fault_injection
    HANDLE hFile = 0;
    {
      IOSTATS_TIMER_GUARD(open_nanos);
      hFile =
          CreateFileA(fname.c_str(), GENERIC_READ,
                      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                      NULL, OPEN_EXISTING, fileFlags, NULL);
    }

    if (INVALID_HANDLE_VALUE == hFile) {
      auto lastError = GetLastError();
      return IOErrorFromWindowsError(
          "NewRandomAccessFile failed to Create/Open: " + fname, lastError);
    }

    UniqueCloseHandlePtr fileGuard(hFile, CloseHandleFunc);

    // CAUTION! This will map the entire file into the process address space
    if (options.use_mmap_reads && sizeof(void*) >= 8) {
      // Use mmap when virtual address-space is plentiful.
      uint64_t fileSize;

      s = GetFileSize(fname, &fileSize);

      if (s.ok()) {
        // Will not map empty files
        if (fileSize == 0) {
          return IOError(
              "NewRandomAccessFile failed to map empty file: " + fname, EINVAL);
        }

        HANDLE hMap = CreateFileMappingA(hFile, NULL, PAGE_READONLY,
                                         0,  // Whole file at its present length
                                         0,
                                         NULL);  // Mapping name

        if (!hMap) {
          auto lastError = GetLastError();
          return IOErrorFromWindowsError(
              "Failed to create file mapping for NewRandomAccessFile: " + fname,
              lastError);
        }

        UniqueCloseHandlePtr mapGuard(hMap, CloseHandleFunc);

        const void* mapped_region =
            MapViewOfFileEx(hMap, FILE_MAP_READ,
                            0,  // High DWORD of access start
                            0,  // Low DWORD
                            fileSize,
                            NULL);  // Let the OS choose the mapping

        if (!mapped_region) {
          auto lastError = GetLastError();
          return IOErrorFromWindowsError(
              "Failed to MapViewOfFile for NewRandomAccessFile: " + fname,
              lastError);
        }

        result->reset(new WinMmapReadableFile(fname, hFile, hMap, mapped_region,
                                              fileSize));

        mapGuard.release();
        fileGuard.release();
      }
    } else {
      result->reset(new WinRandomAccessFile(fname, hFile, page_size_, options));
      fileGuard.release();
    }
    return s;
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 std::unique_ptr<WritableFile>* result,
                                 const EnvOptions& options) override {
    const size_t c_BufferCapacity = 64 * 1024;

    EnvOptions local_options(options);

    result->reset();
    Status s;

    DWORD fileFlags = FILE_ATTRIBUTE_NORMAL;

    if (!local_options.use_os_buffer && !local_options.use_mmap_writes) {
      fileFlags = FILE_FLAG_NO_BUFFERING;
    }

    // Desired access. We are want to write only here but if we want to memory
    // map
    // the file then there is no write only mode so we have to create it
    // Read/Write
    // However, MapViewOfFile specifies only Write only
    DWORD desired_access = GENERIC_WRITE;
    DWORD shared_mode = FILE_SHARE_READ;

    if (local_options.use_mmap_writes) {
      desired_access |= GENERIC_READ;
    } else {
      // Adding this solely for tests to pass (fault_injection_test,
      // wal_manager_test).
      shared_mode |= (FILE_SHARE_WRITE | FILE_SHARE_DELETE);
    }

    HANDLE hFile = 0;
    {
      IOSTATS_TIMER_GUARD(open_nanos);
      hFile = CreateFileA(
          fname.c_str(),
          desired_access,  // Access desired
          shared_mode,
          NULL,           // Security attributes
          CREATE_ALWAYS,  // Posix env says O_CREAT | O_RDWR | O_TRUNC
          fileFlags,      // Flags
          NULL);          // Template File
    }

    if (INVALID_HANDLE_VALUE == hFile) {
      auto lastError = GetLastError();
      return IOErrorFromWindowsError(
          "Failed to create a NewWriteableFile: " + fname, lastError);
    }

    if (options.use_mmap_writes) {
      // We usually do not use mmmapping on SSD and thus we pass memory
      // page_size
      result->reset(new WinMmapFile(fname, hFile, page_size_,
                                    allocation_granularity_, local_options));
    } else {
      // Here we want the buffer allocation to be aligned by the SSD page size
      // and to be a multiple of it
      result->reset(new WinWritableFile(fname, hFile, page_size_,
                                        c_BufferCapacity, local_options));
    }
    return s;
  }

  virtual Status NewDirectory(const std::string& name,
                              std::unique_ptr<Directory>* result) override {
    Status s;
    // Must be nullptr on failure
    result->reset();
    // Must fail if directory does not exist
    if (!DirExists(name)) {
      s = IOError("Directory does not exist: " + name, EEXIST);
    } else {
      IOSTATS_TIMER_GUARD(open_nanos);
      result->reset(new WinDirectory);
    }
    return s;
  }

  virtual Status FileExists(const std::string& fname) override {
    // F_OK == 0
    const int F_OK_ = 0;
    return _access(fname.c_str(), F_OK_) == 0 ? Status::OK()
                                              : Status::NotFound();
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) override {
    std::vector<std::string> output;

    Status status;

    auto CloseDir = [](DIR* p) { closedir(p); };
    std::unique_ptr<DIR, decltype(CloseDir)> dirp(opendir(dir.c_str()),
                                                  CloseDir);

    if (!dirp) {
      status = IOError(dir, errno);
    } else {
      if (result->capacity() > 0) {
        output.reserve(result->capacity());
      }

      struct dirent* ent = readdir(dirp.get());
      while (ent) {
        output.push_back(ent->d_name);
        ent = readdir(dirp.get());
      }
    }

    output.swap(*result);

    return status;
  }

  virtual Status CreateDir(const std::string& name) override {
    Status result;

    if (_mkdir(name.c_str()) != 0) {
      auto code = errno;
      result = IOError("Failed to create dir: " + name, code);
    }

    return result;
  }

  virtual Status CreateDirIfMissing(const std::string& name) override {
    Status result;

    if (DirExists(name)) {
      return result;
    }

    if (_mkdir(name.c_str()) != 0) {
      if (errno == EEXIST) {
        result =
            Status::IOError("`" + name + "' exists but is not a directory");
      } else {
        auto code = errno;
        result = IOError("Failed to create dir: " + name, code);
      }
    }

    return result;
  }

  virtual Status DeleteDir(const std::string& name) override {
    Status result;
    if (_rmdir(name.c_str()) != 0) {
      auto code = errno;
      result = IOError("Failed to remove dir: " + name, code);
    }
    return result;
  }

  virtual Status GetFileSize(const std::string& fname,
                             uint64_t* size) override {
    Status s;

    WIN32_FILE_ATTRIBUTE_DATA attrs;
    if (GetFileAttributesExA(fname.c_str(), GetFileExInfoStandard, &attrs)) {
      ULARGE_INTEGER file_size;
      file_size.HighPart = attrs.nFileSizeHigh;
      file_size.LowPart = attrs.nFileSizeLow;
      *size = file_size.QuadPart;
    } else {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError("Can not get size for: " + fname, lastError);
    }
    return s;
  }

  static inline uint64_t FileTimeToUnixTime(const FILETIME& ftTime) {
    const uint64_t c_FileTimePerSecond = 10000000U;
    // UNIX epoch starts on 1970-01-01T00:00:00Z
    // Windows FILETIME starts on 1601-01-01T00:00:00Z
    // Therefore, we need to subtract the below number of seconds from
    // the seconds that we obtain from FILETIME with an obvious loss of
    // precision
    const uint64_t c_SecondBeforeUnixEpoch = 11644473600U;

    ULARGE_INTEGER li;
    li.HighPart = ftTime.dwHighDateTime;
    li.LowPart = ftTime.dwLowDateTime;

    uint64_t result =
        (li.QuadPart / c_FileTimePerSecond) - c_SecondBeforeUnixEpoch;
    return result;
  }

  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* file_mtime) override {
    Status s;

    WIN32_FILE_ATTRIBUTE_DATA attrs;
    if (GetFileAttributesExA(fname.c_str(), GetFileExInfoStandard, &attrs)) {
      *file_mtime = FileTimeToUnixTime(attrs.ftLastWriteTime);
    } else {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError(
          "Can not get file modification time for: " + fname, lastError);
      *file_mtime = 0;
    }

    return s;
  }

  virtual Status RenameFile(const std::string& src,
                            const std::string& target) override {
    Status result;

    // rename() is not capable of replacing the existing file as on Linux
    // so use OS API directly
    if (!MoveFileExA(src.c_str(), target.c_str(), MOVEFILE_REPLACE_EXISTING)) {
      DWORD lastError = GetLastError();

      std::string text("Failed to rename: ");
      text.append(src).append(" to: ").append(target);

      result = IOErrorFromWindowsError(text, lastError);
    }

    return result;
  }

  virtual Status LinkFile(const std::string& src,
                          const std::string& target) override {
    Status result;

    if (!CreateHardLinkA(target.c_str(), src.c_str(), NULL)) {
      DWORD lastError = GetLastError();

      std::string text("Failed to link: ");
      text.append(src).append(" to: ").append(target);

      result = IOErrorFromWindowsError(text, lastError);
    }

    return result;
  }

  virtual Status LockFile(const std::string& lockFname,
                          FileLock** lock) override {
    assert(lock != nullptr);

    *lock = NULL;
    Status result;

    // No-sharing, this is a LOCK file
    const DWORD ExclusiveAccessON = 0;

    // Obtain exclusive access to the LOCK file
    // Previously, instead of NORMAL attr we set DELETE on close and that worked
    // well except with fault_injection test that insists on deleting it.
    HANDLE hFile = 0;
    {
      IOSTATS_TIMER_GUARD(open_nanos);
      hFile = CreateFileA(lockFname.c_str(), (GENERIC_READ | GENERIC_WRITE),
                          ExclusiveAccessON, NULL, CREATE_ALWAYS,
                          FILE_ATTRIBUTE_NORMAL, NULL);
    }

    if (INVALID_HANDLE_VALUE == hFile) {
      auto lastError = GetLastError();
      result = IOErrorFromWindowsError(
          "Failed to create lock file: " + lockFname, lastError);
    } else {
      *lock = new WinFileLock(hFile);
    }

    return result;
  }

  virtual Status UnlockFile(FileLock* lock) override {
    Status result;

    assert(lock != nullptr);

    delete lock;

    return result;
  }

  virtual void Schedule(void (*function)(void*), void* arg, Priority pri = LOW,
                        void* tag = nullptr) override;

  virtual int UnSchedule(void* arg, Priority pri) override;

  virtual void StartThread(void (*function)(void* arg), void* arg) override;

  virtual void WaitForJoin() override;

  virtual unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override;

  virtual Status GetTestDirectory(std::string* result) override {
    std::string output;

    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      output = env;
      CreateDir(output);
    } else {
      env = getenv("TMP");

      if (env && env[0] != '\0') {
        output = env;
      } else {
        output = "c:\\tmp";
      }

      CreateDir(output);
    }

    output.append("\\testrocksdb-");
    output.append(std::to_string(_getpid()));

    CreateDir(output);

    output.swap(*result);

    return Status::OK();
  }

  virtual Status GetThreadList(
      std::vector<ThreadStatus>* thread_list) override {
    assert(thread_status_updater_);
    return thread_status_updater_->GetThreadList(thread_list);
  }

  static uint64_t gettid() {
    uint64_t thread_id = GetCurrentThreadId();
    return thread_id;
  }

  virtual uint64_t GetThreadID() const override { return gettid(); }

  virtual Status NewLogger(const std::string& fname,
                           std::shared_ptr<Logger>* result) override {
    Status s;

    result->reset();

    HANDLE hFile = 0;
    {
      IOSTATS_TIMER_GUARD(open_nanos);
      hFile = CreateFileA(
          fname.c_str(), GENERIC_WRITE,
          FILE_SHARE_READ | FILE_SHARE_DELETE,  // In RocksDb log files are
                                                // renamed and deleted before
                                                // they are closed. This enables
                                                // doing so.
          NULL,
          CREATE_ALWAYS,  // Original fopen mode is "w"
          FILE_ATTRIBUTE_NORMAL, NULL);
    }

    if (INVALID_HANDLE_VALUE == hFile) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError("Failed to open LogFile" + fname, lastError);
    } else {
      {
        // With log files we want to set the true creation time as of now
        // because the system
        // for some reason caches the attributes of the previous file that just
        // been renamed from
        // this name so auto_roll_logger_test fails
        FILETIME ft;
        GetSystemTimeAsFileTime(&ft);
        // Set creation, last access and last write time to the same value
        SetFileTime(hFile, &ft, &ft, &ft);
      }
      result->reset(new WinLogger(&WinEnv::gettid, this, hFile));
    }
    return s;
  }

  virtual uint64_t NowMicros() override {
    // all std::chrono clocks on windows proved to return
    // values that may repeat that is not good enough for some uses.
    const int64_t c_UnixEpochStartTicks = 116444736000000000i64;
    const int64_t c_FtToMicroSec = 10;

    // This interface needs to return system time and not
    // just any microseconds because it is often used as an argument
    // to TimedWait() on condition variable
    FILETIME ftSystemTime;
    GetSystemTimePreciseAsFileTime(&ftSystemTime);

    LARGE_INTEGER li;
    li.LowPart = ftSystemTime.dwLowDateTime;
    li.HighPart = ftSystemTime.dwHighDateTime;
    // Subtract unix epoch start
    li.QuadPart -= c_UnixEpochStartTicks;
    // Convert to microsecs
    li.QuadPart /= c_FtToMicroSec;
    return li.QuadPart;
  }

  virtual uint64_t NowNanos() override {
    // all std::chrono clocks on windows have the same resolution that is only
    // good enough for microseconds but not nanoseconds
    // On Windows 8 and Windows 2012 Server
    // GetSystemTimePreciseAsFileTime(&current_time) can be used
    LARGE_INTEGER li;
    QueryPerformanceCounter(&li);
    // Convert to nanoseconds first to avoid loss of precision
    // and divide by frequency
    li.QuadPart *= std::nano::den;
    li.QuadPart /= perf_counter_frequency_;
    return li.QuadPart;
  }

  virtual void SleepForMicroseconds(int micros) override {
    std::this_thread::sleep_for(std::chrono::microseconds(micros));
  }

  virtual Status GetHostName(char* name, uint64_t len) override {
    Status s;
    DWORD nSize = len;

    if (!::GetComputerNameA(name, &nSize)) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError("GetHostName", lastError);
    } else {
      name[nSize] = 0;
    }

    return s;
  }

  virtual Status GetCurrTime(int64_t* unix_time) {
    Status s;

    time_t ret = time(nullptr);
    if (ret == (time_t)-1) {
      *unix_time = 0;
      s = IOError("GetCurrTime", errno);
    } else {
      *unix_time = (int64_t)ret;
    }

    return s;
  }

  virtual Status GetAbsolutePath(const std::string& db_path,
                                 std::string* output_path) override {
    // Check if we already have an absolute path
    // that starts with non dot and has a semicolon in it
    if ((!db_path.empty() && (db_path[0] == '/' || db_path[0] == '\\')) ||
        (db_path.size() > 2 && db_path[0] != '.' &&
         ((db_path[1] == ':' && db_path[2] == '\\') ||
          (db_path[1] == ':' && db_path[2] == '/')))) {
      *output_path = db_path;
      return Status::OK();
    }

    std::string result;
    result.resize(_MAX_PATH);

    char* ret = _getcwd(&result[0], _MAX_PATH);
    if (ret == nullptr) {
      return Status::IOError("Failed to get current working directory",
                             strerror(errno));
    }

    result.resize(strlen(result.data()));

    result.swap(*output_path);
    return Status::OK();
  }

  // Allow increasing the number of worker threads.
  virtual void SetBackgroundThreads(int num, Priority pri) override {
    assert(pri >= Priority::LOW && pri <= Priority::HIGH);
    thread_pools_[pri].SetBackgroundThreads(num);
  }

  virtual void IncBackgroundThreadsIfNeeded(int num, Priority pri) override {
    assert(pri >= Priority::LOW && pri <= Priority::HIGH);
    thread_pools_[pri].IncBackgroundThreadsIfNeeded(num);
  }

  virtual std::string TimeToString(uint64_t secondsSince1970) override {
    std::string result;

    const time_t seconds = secondsSince1970;
    const int maxsize = 64;

    struct tm t;
    errno_t ret = localtime_s(&t, &seconds);

    if (ret) {
      result = std::to_string(seconds);
    } else {
      result.resize(maxsize);
      char* p = &result[0];

      int len = snprintf(p, maxsize, "%04d/%02d/%02d-%02d:%02d:%02d ",
                         t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour,
                         t.tm_min, t.tm_sec);
      assert(len > 0);

      result.resize(len);
    }

    return result;
  }

  EnvOptions OptimizeForLogWrite(const EnvOptions& env_options,
                                 const DBOptions& db_options) const override {
    EnvOptions optimized = env_options;
    optimized.use_mmap_writes = false;
    optimized.bytes_per_sync = db_options.wal_bytes_per_sync;
    optimized.use_os_buffer =
        true;  // This is because we flush only whole pages on unbuffered io and
               // the last records are not guaranteed to be flushed.
    // TODO(icanadi) it's faster if fallocate_with_keep_size is false, but it
    // breaks TransactionLogIteratorStallAtLastRecord unit test. Fix the unit
    // test and make this false
    optimized.fallocate_with_keep_size = true;
    return optimized;
  }

  EnvOptions OptimizeForManifestWrite(
      const EnvOptions& env_options) const override {
    EnvOptions optimized = env_options;
    optimized.use_mmap_writes = false;
    optimized.use_os_buffer = true;
    optimized.fallocate_with_keep_size = true;
    return optimized;
  }

 private:
  // Returns true iff the named directory exists and is a directory.
  virtual bool DirExists(const std::string& dname) {
    WIN32_FILE_ATTRIBUTE_DATA attrs;
    if (GetFileAttributesExA(dname.c_str(), GetFileExInfoStandard, &attrs)) {
      return 0 != (attrs.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY);
    }
    return false;
  }

  bool SupportsFastAllocate(const std::string& /* path */) { return false; }

  class ThreadPool {
   public:
    ThreadPool()
        : total_threads_limit_(1),
          bgthreads_(0),
          queue_(),
          queue_len_(0U),
          exit_all_threads_(false),
          low_io_priority_(false),
          env_(nullptr) {}

    ~ThreadPool() { assert(bgthreads_.size() == 0U); }

    void JoinAllThreads() {
      {
        std::lock_guard<std::mutex> lock(mu_);
        assert(!exit_all_threads_);
        exit_all_threads_ = true;
        bgsignal_.notify_all();
      }

      for (std::thread& th : bgthreads_) {
        th.join();
      }

      // Subject to assert in the __dtor
      bgthreads_.clear();
    }

    void SetHostEnv(Env* env) { env_ = env; }

    // Return true if there is at least one thread needs to terminate.
    bool HasExcessiveThread() const {
      return bgthreads_.size() > total_threads_limit_;
    }

    // Return true iff the current thread is the excessive thread to terminate.
    // Always terminate the running thread that is added last, even if there are
    // more than one thread to terminate.
    bool IsLastExcessiveThread(size_t thread_id) const {
      return HasExcessiveThread() && thread_id == bgthreads_.size() - 1;
    }

    // Is one of the threads to terminate.
    bool IsExcessiveThread(size_t thread_id) const {
      return thread_id >= total_threads_limit_;
    }

    // Return the thread priority.
    // This would allow its member-thread to know its priority.
    Env::Priority GetThreadPriority() { return priority_; }

    // Set the thread priority.
    void SetThreadPriority(Env::Priority priority) { priority_ = priority; }

    void BGThread(size_t thread_id) {
      while (true) {
        // Wait until there is an item that is ready to run
        std::unique_lock<std::mutex> uniqueLock(mu_);

        // Stop waiting if the thread needs to do work or needs to terminate.
        while (!exit_all_threads_ && !IsLastExcessiveThread(thread_id) &&
               (queue_.empty() || IsExcessiveThread(thread_id))) {
          bgsignal_.wait(uniqueLock);
        }

        if (exit_all_threads_) {
          // mechanism to let BG threads exit safely
          uniqueLock.unlock();
          break;
        }

        if (IsLastExcessiveThread(thread_id)) {
          // Current thread is the last generated one and is excessive.
          // We always terminate excessive thread in the reverse order of
          // generation time.
          std::thread& terminating_thread = bgthreads_.back();
          auto tid = terminating_thread.get_id();
          // Ensure that that this thread is ours
          assert(tid == std::this_thread::get_id());
          terminating_thread.detach();
          bgthreads_.pop_back();

          if (HasExcessiveThread()) {
            // There is still at least more excessive thread to terminate.
            WakeUpAllThreads();
          }

          uniqueLock.unlock();

          PrintThreadInfo(thread_id, gettid());
          break;
        }

        void (*function)(void*) = queue_.front().function;
        void* arg = queue_.front().arg;
        queue_.pop_front();
        queue_len_.store(queue_.size(), std::memory_order_relaxed);

        uniqueLock.unlock();
        (*function)(arg);
      }
    }

    // Helper struct for passing arguments when creating threads.
    struct BGThreadMetadata {
      ThreadPool* thread_pool_;
      size_t thread_id_;  // Thread count in the thread.

      BGThreadMetadata(ThreadPool* thread_pool, size_t thread_id)
          : thread_pool_(thread_pool), thread_id_(thread_id) {}
    };

    static void* BGThreadWrapper(void* arg) {
      std::unique_ptr<BGThreadMetadata> meta(
          reinterpret_cast<BGThreadMetadata*>(arg));

      size_t thread_id = meta->thread_id_;
      ThreadPool* tp = meta->thread_pool_;

#if ROCKSDB_USING_THREAD_STATUS
      // for thread-status
      ThreadStatusUtil::RegisterThread(
          tp->env_, (tp->GetThreadPriority() == Env::Priority::HIGH
                         ? ThreadStatus::HIGH_PRIORITY
                         : ThreadStatus::LOW_PRIORITY));
#endif
      tp->BGThread(thread_id);
#if ROCKSDB_USING_THREAD_STATUS
      ThreadStatusUtil::UnregisterThread();
#endif
      return nullptr;
    }

    void WakeUpAllThreads() { bgsignal_.notify_all(); }

    void SetBackgroundThreadsInternal(size_t num, bool allow_reduce) {
      std::lock_guard<std::mutex> lg(mu_);

      if (exit_all_threads_) {
        return;
      }

      if (num > total_threads_limit_ ||
          (num < total_threads_limit_ && allow_reduce)) {
        total_threads_limit_ = std::max(size_t(1), num);
        WakeUpAllThreads();
        StartBGThreads();
      }
      assert(total_threads_limit_ > 0);
    }

    void IncBackgroundThreadsIfNeeded(int num) {
      SetBackgroundThreadsInternal(num, false);
    }

    void SetBackgroundThreads(int num) {
      SetBackgroundThreadsInternal(num, true);
    }

    void StartBGThreads() {
      // Start background thread if necessary
      while (bgthreads_.size() < total_threads_limit_) {
        std::thread p_t(&ThreadPool::BGThreadWrapper,
                        new BGThreadMetadata(this, bgthreads_.size()));
        bgthreads_.push_back(std::move(p_t));
      }
    }

    void Schedule(void (*function)(void* arg1), void* arg, void* tag) {
      std::lock_guard<std::mutex> lg(mu_);

      if (exit_all_threads_) {
        return;
      }

      StartBGThreads();

      // Add to priority queue
      queue_.push_back(BGItem());
      queue_.back().function = function;
      queue_.back().arg = arg;
      queue_.back().tag = tag;
      queue_len_.store(queue_.size(), std::memory_order_relaxed);

      if (!HasExcessiveThread()) {
        // Wake up at least one waiting thread.
        bgsignal_.notify_one();
      } else {
        // Need to wake up all threads to make sure the one woken
        // up is not the one to terminate.
        WakeUpAllThreads();
      }
    }

    int UnSchedule(void* arg) {
      int count = 0;

      std::lock_guard<std::mutex> lg(mu_);

      // Remove from priority queue
      BGQueue::iterator it = queue_.begin();
      while (it != queue_.end()) {
        if (arg == (*it).tag) {
          it = queue_.erase(it);
          count++;
        } else {
          ++it;
        }
      }

      queue_len_.store(queue_.size(), std::memory_order_relaxed);

      return count;
    }

    unsigned int GetQueueLen() const {
      return static_cast<unsigned int>(
          queue_len_.load(std::memory_order_relaxed));
    }

   private:
    // Entry per Schedule() call
    struct BGItem {
      void* arg;
      void (*function)(void*);
      void* tag;
    };

    typedef std::deque<BGItem> BGQueue;

    std::mutex mu_;
    std::condition_variable bgsignal_;
    size_t total_threads_limit_;
    std::vector<std::thread> bgthreads_;
    BGQueue queue_;
    std::atomic_size_t queue_len_;  // Queue length. Used for stats reporting
    bool exit_all_threads_;
    bool low_io_priority_;
    Env::Priority priority_;
    Env* env_;
  };

  bool checkedDiskForMmap_;
  bool forceMmapOff;  // do we override Env options?
  size_t page_size_;
  size_t allocation_granularity_;
  uint64_t perf_counter_frequency_;
  std::vector<ThreadPool> thread_pools_;
  mutable std::mutex mu_;
  std::vector<std::thread> threads_to_join_;
};

WinEnv::WinEnv()
    : checkedDiskForMmap_(false),
      forceMmapOff(false),
      page_size_(4 * 1012),
      allocation_granularity_(page_size_),
      perf_counter_frequency_(0),
      thread_pools_(Priority::TOTAL) {
  SYSTEM_INFO sinfo;
  GetSystemInfo(&sinfo);

  page_size_ = sinfo.dwPageSize;
  allocation_granularity_ = sinfo.dwAllocationGranularity;

  {
    LARGE_INTEGER qpf;
    BOOL ret = QueryPerformanceFrequency(&qpf);
    assert(ret == TRUE);
    perf_counter_frequency_ = qpf.QuadPart;
  }

  for (int pool_id = 0; pool_id < Env::Priority::TOTAL; ++pool_id) {
    thread_pools_[pool_id].SetThreadPriority(
        static_cast<Env::Priority>(pool_id));
    // This allows later initializing the thread-local-env of each thread.
    thread_pools_[pool_id].SetHostEnv(this);
  }

  // Protected member of the base class
  thread_status_updater_ = CreateThreadStatusUpdater();
}

void WinEnv::Schedule(void (*function)(void*), void* arg, Priority pri,
                      void* tag) {
  assert(pri >= Priority::LOW && pri <= Priority::HIGH);
  thread_pools_[pri].Schedule(function, arg, tag);
}

int WinEnv::UnSchedule(void* arg, Priority pri) {
  return thread_pools_[pri].UnSchedule(arg);
}

unsigned int WinEnv::GetThreadPoolQueueLen(Priority pri) const {
  assert(pri >= Priority::LOW && pri <= Priority::HIGH);
  return thread_pools_[pri].GetQueueLen();
}

namespace {
struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};
}

static void* StartThreadWrapper(void* arg) {
  std::unique_ptr<StartThreadState> state(
      reinterpret_cast<StartThreadState*>(arg));
  state->user_function(state->arg);
  return nullptr;
}

void WinEnv::StartThread(void (*function)(void* arg), void* arg) {
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  try {
    std::thread th(&StartThreadWrapper, state);

    std::lock_guard<std::mutex> lg(mu_);
    threads_to_join_.push_back(std::move(th));

  } catch (const std::system_error& ex) {
    WinthreadCall("start thread", ex.code());
  }
}

void WinEnv::WaitForJoin() {
  for (auto& th : threads_to_join_) {
    th.join();
  }

  threads_to_join_.clear();
}

}  // namespace

std::string Env::GenerateUniqueId() {
  std::string result;

  UUID uuid;
  UuidCreateSequential(&uuid);

  RPC_CSTR rpc_str;
  auto status = UuidToStringA(&uuid, &rpc_str);
  assert(status == RPC_S_OK);

  result = reinterpret_cast<char*>(rpc_str);

  status = RpcStringFreeA(&rpc_str);
  assert(status == RPC_S_OK);

  return result;
}

// We choose to create this on the heap and using std::once for the following
// reasons
// 1) Currently available MS compiler does not implement atomic C++11
// initialization of
//    function local statics
// 2) We choose not to destroy the env because joining the threads from the
// system loader
//    which destroys the statics (same as from DLLMain) creates a system loader
//    dead-lock.
//    in this manner any remaining threads are terminated OK.
namespace {
std::once_flag winenv_once_flag;
Env* envptr;
};

Env* Env::Default() {
  std::call_once(winenv_once_flag, []() { envptr = new WinEnv(); });
  return envptr;
}

}  // namespace rocksdb
