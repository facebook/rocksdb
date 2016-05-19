//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "port/win/io_win.h"

#include "util/sync_point.h"
#include "util/coding.h"
#include "util/iostats_context_imp.h"
#include "util/sync_point.h"
#include "util/aligned_buffer.h"


namespace rocksdb {
namespace port {

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
  OVERLAPPED overlapped = { 0 };
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
  OVERLAPPED overlapped = { 0 };
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

// SetFileInformationByHandle() is capable of fast pre-allocates.
// However, this does not change the file end position unless the file is
// truncated and the pre-allocated space is not considered filled with zeros.
Status fallocate(const std::string& filename, HANDLE hFile,
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

Status ftruncate(const std::string& filename, HANDLE hFile,
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

size_t GetUniqueIdFromFile(HANDLE hFile, char* id, size_t max_size) {

  if (max_size < kMaxVarint64Length * 3) {
    return 0;
  }

  BY_HANDLE_FILE_INFORMATION FileInfo;

  BOOL result = GetFileInformationByHandle(hFile, &FileInfo);

  TEST_SYNC_POINT_CALLBACK("GetUniqueIdFromFile:FS_IOC_GETVERSION", &result);

  if (!result) {
    return 0;
  }

  char* rid = id;
  rid = EncodeVarint64(rid, uint64_t(FileInfo.dwVolumeSerialNumber));
  rid = EncodeVarint64(rid, uint64_t(FileInfo.nFileIndexHigh));
  rid = EncodeVarint64(rid, uint64_t(FileInfo.nFileIndexLow));

  assert(rid >= id);
  return static_cast<size_t>(rid - id);
}

WinMmapReadableFile::WinMmapReadableFile(const std::string& fileName, HANDLE hFile, HANDLE hMap,
  const void* mapped_region, size_t length)
  : fileName_(fileName),
  hFile_(hFile),
  hMap_(hMap),
  mapped_region_(mapped_region),
  length_(length) {}

WinMmapReadableFile::~WinMmapReadableFile() {
  BOOL ret = ::UnmapViewOfFile(mapped_region_);
  assert(ret);

  ret = ::CloseHandle(hMap_);
  assert(ret);

  ret = ::CloseHandle(hFile_);
  assert(ret);
}

Status WinMmapReadableFile::Read(uint64_t offset, size_t n, Slice* result,
  char* scratch) const {
  Status s;

  if (offset > length_) {
    *result = Slice();
    return IOError(fileName_, EINVAL);
  } else if (offset + n > length_) {
    n = length_ - offset;
  }
  *result =
    Slice(reinterpret_cast<const char*>(mapped_region_)+offset, n);
  return s;
}

Status WinMmapReadableFile::InvalidateCache(size_t offset, size_t length) {
  return Status::OK();
}

size_t WinMmapReadableFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(hFile_, id, max_size);
}

// Can only truncate or reserve to a sector size aligned if
// used on files that are opened with Unbuffered I/O
Status WinMmapFile::TruncateFile(uint64_t toSize) {
  return ftruncate(filename_, hFile_, toSize);
}

Status WinMmapFile::UnmapCurrentRegion() {
  Status status;

  if (mapped_begin_ != nullptr) {
    if (!::UnmapViewOfFile(mapped_begin_)) {
      status = IOErrorFromWindowsError(
        "Failed to unmap file view: " + filename_, GetLastError());
    }

    // Move on to the next portion of the file
    file_offset_ += view_size_;

    // UnmapView automatically sends data to disk but not the metadata
    // which is good and provides some equivalent of fdatasync() on Linux
    // therefore, we donot need separate flag for metadata
    mapped_begin_ = nullptr;
    mapped_end_ = nullptr;
    dst_ = nullptr;

    last_sync_ = nullptr;
    pending_sync_ = false;
  }

  return status;
}

Status WinMmapFile::MapNewRegion() {

  Status status;

  assert(mapped_begin_ == nullptr);

  size_t minDiskSize = file_offset_ + view_size_;

  if (minDiskSize > reserved_size_) {
    status = Allocate(file_offset_, view_size_);
    if (!status.ok()) {
      return status;
    }
  }

  // Need to remap
  if (hMap_ == NULL || reserved_size_ > mapping_size_) {

    if (hMap_ != NULL) {
      // Unmap the previous one
      BOOL ret = ::CloseHandle(hMap_);
      assert(ret);
      hMap_ = NULL;
    }

    ULARGE_INTEGER mappingSize;
    mappingSize.QuadPart = reserved_size_;

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

    mapping_size_ = reserved_size_;
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

Status WinMmapFile::PreallocateInternal(uint64_t spaceToReserve) {
  return fallocate(filename_, hFile_, spaceToReserve);
}

WinMmapFile::WinMmapFile(const std::string& fname, HANDLE hFile, size_t page_size,
  size_t allocation_granularity, const EnvOptions& options)
  : filename_(fname),
  hFile_(hFile),
  hMap_(NULL),
  page_size_(page_size),
  allocation_granularity_(allocation_granularity),
  reserved_size_(0),
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

  // View size must be both the multiple of allocation_granularity AND the
  // page size and the granularity is usually a multiple of a page size.
  const size_t viewSize = 32 * 1024; // 32Kb similar to the Windows File Cache in buffered mode
  view_size_ = Roundup(viewSize, allocation_granularity_);
}

WinMmapFile::~WinMmapFile() {
  if (hFile_) {
    this->Close();
  }
}

Status WinMmapFile::Append(const Slice& data) {
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
    } else {
      size_t n = std::min(left, avail);
      memcpy(dst_, src, n);
      dst_ += n;
      src += n;
      left -= n;
      pending_sync_ = true;
    }
  }

  // Now make sure that the last partial page is padded with zeros if needed
  size_t bytesToPad = Roundup(size_t(dst_), page_size_) - size_t(dst_);
  if (bytesToPad > 0) {
    memset(dst_, 0, bytesToPad);
  }

  return Status::OK();
}

// Means Close() will properly take care of truncate
// and it does not need any additional information
Status WinMmapFile::Truncate(uint64_t size) {
  return Status::OK();
}

Status WinMmapFile::Close() {
  Status s;

  assert(NULL != hFile_);

  // We truncate to the precise size so no
  // uninitialized data at the end. SetEndOfFile
  // which we use does not write zeros and it is good.
  uint64_t targetSize = GetFileSize();

  if (mapped_begin_ != nullptr) {
    // Sync before unmapping to make sure everything
    // is on disk and there is not a lazy writing
    // so we are deterministic with the tests
    Sync();
    s = UnmapCurrentRegion();
  }

  if (NULL != hMap_) {
    BOOL ret = ::CloseHandle(hMap_);
    if (!ret && s.ok()) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError(
        "Failed to Close mapping for file: " + filename_, lastError);
    }

    hMap_ = NULL;
  }

  if (hFile_ != NULL) {

    TruncateFile(targetSize);

    BOOL ret = ::CloseHandle(hFile_);
    hFile_ = NULL;

    if (!ret && s.ok()) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError(
        "Failed to close file map handle: " + filename_, lastError);
    }
  }

  return s;
}

Status WinMmapFile::Flush() { return Status::OK(); }

// Flush only data
Status WinMmapFile::Sync() {
  Status s;

  // Some writes occurred since last sync
  if (dst_ > last_sync_) {
    assert(mapped_begin_);
    assert(dst_);
    assert(dst_ > mapped_begin_);
    assert(dst_ < mapped_end_);

    size_t page_begin =
      TruncateToPageBoundary(page_size_, last_sync_ - mapped_begin_);
    size_t page_end =
      TruncateToPageBoundary(page_size_, dst_ - mapped_begin_ - 1);

    // Flush only the amount of that is a multiple of pages
    if (!::FlushViewOfFile(mapped_begin_ + page_begin,
      (page_end - page_begin) + page_size_)) {
      s = IOErrorFromWindowsError("Failed to FlushViewOfFile: " + filename_,
        GetLastError());
    } else {
      last_sync_ = dst_;
    }
  }

  return s;
}

/**
* Flush data as well as metadata to stable storage.
*/
Status WinMmapFile::Fsync() {
  Status s = Sync();

  // Flush metadata
  if (s.ok() && pending_sync_) {
    if (!::FlushFileBuffers(hFile_)) {
      s = IOErrorFromWindowsError("Failed to FlushFileBuffers: " + filename_,
        GetLastError());
    }
    pending_sync_ = false;
  }

  return s;
}

/**
* Get the size of valid data in the file. This will not match the
* size that is returned from the filesystem because we use mmap
* to extend file by map_size every time.
*/
uint64_t WinMmapFile::GetFileSize() {
  size_t used = dst_ - mapped_begin_;
  return file_offset_ + used;
}

Status WinMmapFile::InvalidateCache(size_t offset, size_t length) {
  return Status::OK();
}

Status WinMmapFile::Allocate(uint64_t offset, uint64_t len) {
  Status status;
  TEST_KILL_RANDOM("WinMmapFile::Allocate", rocksdb_kill_odds);

  // Make sure that we reserve an aligned amount of space
  // since the reservation block size is driven outside so we want
  // to check if we are ok with reservation here
  size_t spaceToReserve = Roundup(offset + len, view_size_);
  // Nothing to do
  if (spaceToReserve <= reserved_size_) {
    return status;
  }

  IOSTATS_TIMER_GUARD(allocate_nanos);
  status = PreallocateInternal(spaceToReserve);
  if (status.ok()) {
    reserved_size_ = spaceToReserve;
  }
  return status;
}

size_t WinMmapFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(hFile_, id, max_size);
}

WinSequentialFile::WinSequentialFile(const std::string& fname, HANDLE f,
  const EnvOptions& options)
  : filename_(fname),
  file_(f),
  use_os_buffer_(options.use_os_buffer)
{}

WinSequentialFile::~WinSequentialFile() {
  assert(file_ != INVALID_HANDLE_VALUE);
  CloseHandle(file_);
}

Status WinSequentialFile::Read(size_t n, Slice* result, char* scratch) {
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

Status WinSequentialFile::Skip(uint64_t n) {
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

Status WinSequentialFile::InvalidateCache(size_t offset, size_t length) {
  return Status::OK();
}

SSIZE_T WinRandomAccessFile::ReadIntoBuffer(uint64_t user_offset, uint64_t first_page_start,
  size_t bytes_to_read, size_t& left,
  AlignedBuffer& buffer, char* dest) const {
  assert(buffer.CurrentSize() == 0);
  assert(buffer.Capacity() >= bytes_to_read);

  SSIZE_T read =
    PositionedReadInternal(buffer.Destination(), bytes_to_read, first_page_start);

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

SSIZE_T WinRandomAccessFile::ReadIntoOneShotBuffer(uint64_t user_offset, uint64_t first_page_start,
  size_t bytes_to_read, size_t& left,
  char* dest) const {
  AlignedBuffer bigBuffer;
  bigBuffer.Alignment(buffer_.Alignment());
  bigBuffer.AllocateNewBuffer(bytes_to_read);

  return ReadIntoBuffer(user_offset, first_page_start, bytes_to_read, left,
    bigBuffer, dest);
}

SSIZE_T WinRandomAccessFile::ReadIntoInstanceBuffer(uint64_t user_offset,
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

void WinRandomAccessFile::CalculateReadParameters(uint64_t offset, size_t bytes_requested,
  size_t& actual_bytes_toread,
  uint64_t& first_page_start) const {

  const size_t alignment = buffer_.Alignment();

  first_page_start = TruncateToPageBoundary(alignment, offset);
  const uint64_t last_page_start =
    TruncateToPageBoundary(alignment, offset + bytes_requested - 1);
  actual_bytes_toread = (last_page_start - first_page_start) + alignment;
}

SSIZE_T WinRandomAccessFile::PositionedReadInternal(char* src, size_t numBytes,
  uint64_t offset) const {
  return pread(hFile_, src, numBytes, offset);
}

WinRandomAccessFile::WinRandomAccessFile(const std::string& fname, HANDLE hFile, size_t alignment,
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

WinRandomAccessFile::~WinRandomAccessFile() {
  if (hFile_ != NULL && hFile_ != INVALID_HANDLE_VALUE) {
    ::CloseHandle(hFile_);
  }
}

void WinRandomAccessFile::EnableReadAhead() { this->Hint(SEQUENTIAL); }

Status WinRandomAccessFile::Read(uint64_t offset, size_t n, Slice* result,
  char* scratch) const {

  Status s;
  SSIZE_T r = -1;
  size_t left = n;
  char* dest = scratch;

  if (n == 0) {
    *result = Slice(scratch, 0);
    return s;
  }

  // When in unbuffered mode we need to do the following changes:
  // - use our own aligned buffer
  // - always read at the offset of that is a multiple of alignment
  if (!use_os_buffer_) {

    uint64_t first_page_start = 0;
    size_t actual_bytes_toread = 0;
    size_t bytes_requested = left;

    if (!read_ahead_ && random_access_max_buffer_size_ == 0) {
      CalculateReadParameters(offset, bytes_requested, actual_bytes_toread,
        first_page_start);

      assert(actual_bytes_toread > 0);

      r = ReadIntoOneShotBuffer(offset, first_page_start,
        actual_bytes_toread, left, dest);
    } else {

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
        bytes_requested = left;

        if (read_ahead_ && bytes_requested < compaction_readahead_size_) {
          bytes_requested = compaction_readahead_size_;
        }

        CalculateReadParameters(offset, bytes_requested, actual_bytes_toread,
          first_page_start);

        assert(actual_bytes_toread > 0);

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
    }
  } else {
    r = PositionedReadInternal(scratch, left, offset);
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

bool WinRandomAccessFile::ShouldForwardRawRequest() const {
  return true;
}

void WinRandomAccessFile::Hint(AccessPattern pattern) {
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

Status WinRandomAccessFile::InvalidateCache(size_t offset, size_t length) {
  return Status::OK();
}

size_t WinRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(hFile_, id, max_size);
}

Status WinWritableFile::PreallocateInternal(uint64_t spaceToReserve) {
  return fallocate(filename_, hFile_, spaceToReserve);
}

WinWritableFile::WinWritableFile(const std::string& fname, HANDLE hFile, size_t alignment,
    size_t capacity, const EnvOptions& options)
    : filename_(fname),
    hFile_(hFile),
    use_os_buffer_(options.use_os_buffer),
    alignment_(alignment),
    filesize_(0),
    reservedsize_(0) {
  assert(!options.use_mmap_writes);
}

WinWritableFile::~WinWritableFile() {
  if (NULL != hFile_ && INVALID_HANDLE_VALUE != hFile_) {
    WinWritableFile::Close();
  }
}

  // Indicates if the class makes use of unbuffered I/O
bool WinWritableFile::UseOSBuffer() const {
  return use_os_buffer_;
}

size_t WinWritableFile::GetRequiredBufferAlignment() const {
  return alignment_;
}

Status WinWritableFile::Append(const Slice& data) {

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

Status WinWritableFile::PositionedAppend(const Slice& data, uint64_t offset) {
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
Status WinWritableFile::Truncate(uint64_t size) {
  Status s = ftruncate(filename_, hFile_, size);
  if (s.ok()) {
    filesize_ = size;
  }
  return s;
}

Status WinWritableFile::Close() {

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
Status WinWritableFile::Flush() {
  return Status::OK();
}

Status WinWritableFile::Sync() {
  Status s;
  // Calls flush buffers
  if (fsync(hFile_) < 0) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError("fsync failed at Sync() for: " + filename_,
      lastError);
  }
  return s;
}

Status WinWritableFile::Fsync() { return Sync(); }

uint64_t WinWritableFile::GetFileSize() {
  // Double accounting now here with WritableFileWriter
  // and this size will be wrong when unbuffered access is used
  // but tests implement their own writable files and do not use WritableFileWrapper
  // so we need to squeeze a square peg through
  // a round hole here.
  return filesize_;
}

Status WinWritableFile::Allocate(uint64_t offset, uint64_t len) {
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
  status = PreallocateInternal(spaceToReserve);
  if (status.ok()) {
    reservedsize_ = spaceToReserve;
  }
  return status;
}

size_t WinWritableFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(hFile_, id, max_size);
}

Status WinDirectory::Fsync() { return Status::OK(); }

WinFileLock::~WinFileLock() {
  BOOL ret = ::CloseHandle(hFile_);
  assert(ret);
}


}
}

