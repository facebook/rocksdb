//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "port/win/io_win.h"

#include "monitoring/iostats_context_imp.h"
#include "util/aligned_buffer.h"
#include "util/coding.h"
#include "util/sync_point.h"

namespace rocksdb {
namespace port {

/*
* DirectIOHelper
*/
namespace {

const size_t kSectorSize = 512;

inline
bool IsPowerOfTwo(const size_t alignment) {
  return ((alignment) & (alignment - 1)) == 0;
}

inline
bool IsSectorAligned(const size_t off) { 
  return (off & (kSectorSize - 1)) == 0;
}

inline
bool IsAligned(size_t alignment, const void* ptr) {
  return ((uintptr_t(ptr)) & (alignment - 1)) == 0;
}
}


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

  // This function has to be re-worked for cases when
  // ReFS file system introduced on Windows Server 2012 is used
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

////////////////////////////////////////////////////////////////////////////////////////////////////
// WinMmapReadableFile

WinMmapReadableFile::WinMmapReadableFile(const std::string& fileName,
                                         HANDLE hFile, HANDLE hMap,
                                         const void* mapped_region,
                                         size_t length)
    : WinFileData(fileName, hFile, false /* use_direct_io */),
      hMap_(hMap),
      mapped_region_(mapped_region),
      length_(length) {}

WinMmapReadableFile::~WinMmapReadableFile() {
  BOOL ret = ::UnmapViewOfFile(mapped_region_);
  (void)ret;
  assert(ret);

  ret = ::CloseHandle(hMap_);
  assert(ret);
}

Status WinMmapReadableFile::Read(uint64_t offset, size_t n, Slice* result,
  char* scratch) const {
  Status s;

  if (offset > length_) {
    *result = Slice();
    return IOError(filename_, EINVAL);
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

///////////////////////////////////////////////////////////////////////////////
/// WinMmapFile


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
  : WinFileData(fname, hFile, false),
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

//////////////////////////////////////////////////////////////////////////////////
// WinSequentialFile

WinSequentialFile::WinSequentialFile(const std::string& fname, HANDLE f,
                                     const EnvOptions& options)
    : WinFileData(fname, f, options.use_direct_reads) {}

WinSequentialFile::~WinSequentialFile() {
  assert(hFile_ != INVALID_HANDLE_VALUE);
}

Status WinSequentialFile::Read(size_t n, Slice* result, char* scratch) {
  assert(result != nullptr && !WinFileData::use_direct_io());
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
  BOOL ret = ReadFile(hFile_, scratch, bytesToRead, &bytesRead, NULL);
  if (ret == TRUE) {
    r = bytesRead;
  } else {
    return IOErrorFromWindowsError(filename_, GetLastError());
  }

  *result = Slice(scratch, r);

  return s;
}

SSIZE_T WinSequentialFile::PositionedReadInternal(char* src, size_t numBytes,
  uint64_t offset) const {
  return pread(GetFileHandle(), src, numBytes, offset);
}

Status WinSequentialFile::PositionedRead(uint64_t offset, size_t n, Slice* result,
  char* scratch) {

  Status s;

  assert(WinFileData::use_direct_io());

  // Windows ReadFile API accepts a DWORD.
  // While it is possible to read in a loop if n is > UINT_MAX
  // it is a highly unlikely case.
  if (n > UINT_MAX) {
    return IOErrorFromWindowsError(GetName(), ERROR_INVALID_PARAMETER);
  }

  auto r = PositionedReadInternal(scratch, n, offset);

  if (r < 0) {
    auto lastError = GetLastError();
    // Posix impl wants to treat reads from beyond
    // of the file as OK.
    if (lastError != ERROR_HANDLE_EOF) {
      s = IOErrorFromWindowsError(GetName(), lastError);
    }
  }

  *result = Slice(scratch, (r < 0) ? 0 : size_t(r));
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
  BOOL ret = SetFilePointerEx(hFile_, li, NULL, FILE_CURRENT);
  if (ret == FALSE) {
    return IOErrorFromWindowsError(filename_, GetLastError());
  }
  return Status::OK();
}

Status WinSequentialFile::InvalidateCache(size_t offset, size_t length) {
  return Status::OK();
}

//////////////////////////////////////////////////////////////////////////////////////////////////
/// WinRandomAccessBase

SSIZE_T WinRandomAccessImpl::PositionedReadInternal(char* src,
  size_t numBytes,
  uint64_t offset) const {
  return pread(file_base_->GetFileHandle(), src, numBytes, offset);
}

inline
WinRandomAccessImpl::WinRandomAccessImpl(WinFileData* file_base,
  size_t alignment,
  const EnvOptions& options) :
    file_base_(file_base),
    alignment_(alignment) {

  assert(!options.use_mmap_reads);
}

inline
Status WinRandomAccessImpl::ReadImpl(uint64_t offset, size_t n, Slice* result,
  char* scratch) const {

  Status s;

  // Check buffer alignment
  if (file_base_->use_direct_io()) {
    if (!IsAligned(alignment_, scratch)) {
      return Status::InvalidArgument("WinRandomAccessImpl::ReadImpl: scratch is not properly aligned");
    }
  }

  if (n == 0) {
    *result = Slice(scratch, 0);
    return s;
  }

  size_t left = n;
  char* dest = scratch;

  SSIZE_T r = PositionedReadInternal(scratch, left, offset);
  if (r > 0) {
    left -= r;
  } else if (r < 0) {
    auto lastError = GetLastError();
    // Posix impl wants to treat reads from beyond
    // of the file as OK.
    if(lastError != ERROR_HANDLE_EOF) {
      s = IOErrorFromWindowsError(file_base_->GetName(), lastError);
    }
  }

  *result = Slice(scratch, (r < 0) ? 0 : n - left);

  return s;
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// WinRandomAccessFile

WinRandomAccessFile::WinRandomAccessFile(const std::string& fname, HANDLE hFile,
                                         size_t alignment,
                                         const EnvOptions& options)
    : WinFileData(fname, hFile, options.use_direct_reads),
      WinRandomAccessImpl(this, alignment, options) {}

WinRandomAccessFile::~WinRandomAccessFile() {
}

Status WinRandomAccessFile::Read(uint64_t offset, size_t n, Slice* result,
  char* scratch) const {
  return ReadImpl(offset, n, result, scratch);
}

Status WinRandomAccessFile::InvalidateCache(size_t offset, size_t length) {
  return Status::OK();
}

size_t WinRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(GetFileHandle(), id, max_size);
}

size_t WinRandomAccessFile::GetRequiredBufferAlignment() const {
  return GetAlignment();
}

/////////////////////////////////////////////////////////////////////////////
// WinWritableImpl
//

inline
Status WinWritableImpl::PreallocateInternal(uint64_t spaceToReserve) {
  return fallocate(file_data_->GetName(), file_data_->GetFileHandle(), spaceToReserve);
}

WinWritableImpl::WinWritableImpl(WinFileData* file_data, size_t alignment)
  : file_data_(file_data),
  alignment_(alignment),
  filesize_(0),
  reservedsize_(0) {
}

Status WinWritableImpl::AppendImpl(const Slice& data) {

  Status s;

  assert(data.size() < std::numeric_limits<DWORD>::max());

  uint64_t written = 0;
  (void)written;

  if (file_data_->use_direct_io()) {

    // With no offset specified we are appending
    // to the end of the file

    assert(IsSectorAligned(filesize_));
    assert(IsSectorAligned(data.size()));
    assert(IsAligned(GetAlignement(), data.data()));

    SSIZE_T ret = pwrite(file_data_->GetFileHandle(), data.data(),
     data.size(), filesize_);

    if (ret < 0) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError(
        "Failed to pwrite for: " + file_data_->GetName(), lastError);
    }
    else {
      written = ret;
    }

  } else {

    DWORD bytesWritten = 0;
    if (!WriteFile(file_data_->GetFileHandle(), data.data(),
      static_cast<DWORD>(data.size()), &bytesWritten, NULL)) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError(
        "Failed to WriteFile: " + file_data_->GetName(),
        lastError);
    }
    else {
      written = bytesWritten;
    }
  }

  if(s.ok()) {
    assert(written == data.size());
    filesize_ += data.size();
  }

  return s;
}

Status WinWritableImpl::PositionedAppendImpl(const Slice& data, uint64_t offset) {

  if(file_data_->use_direct_io()) {
    assert(IsSectorAligned(offset));
    assert(IsSectorAligned(data.size()));
    assert(IsAligned(GetAlignement(), data.data()));
  }

  Status s;

  SSIZE_T ret = pwrite(file_data_->GetFileHandle(), data.data(), data.size(), offset);

  // Error break
  if (ret < 0) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError(
      "Failed to pwrite for: " + file_data_->GetName(), lastError);
  }
  else {
    assert(size_t(ret) == data.size());
    // For sequential write this would be simple
    // size extension by data.size()
    uint64_t write_end = offset + data.size();
    if (write_end >= filesize_) {
      filesize_ = write_end;
    }
  }
  return s;
}

// Need to implement this so the file is truncated correctly
// when buffered and unbuffered mode
inline
Status WinWritableImpl::TruncateImpl(uint64_t size) {
  Status s = ftruncate(file_data_->GetName(), file_data_->GetFileHandle(),
    size);
  if (s.ok()) {
    filesize_ = size;
  }
  return s;
}

Status WinWritableImpl::CloseImpl() {

  Status s;

  auto hFile = file_data_->GetFileHandle();
  assert(INVALID_HANDLE_VALUE != hFile);

  if (fsync(hFile) < 0) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError("fsync failed at Close() for: " +
      file_data_->GetName(),
      lastError);
  }

  if(!file_data_->CloseFile()) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError("CloseHandle failed for: " + file_data_->GetName(),
      lastError);
  }
  return s;
}

Status WinWritableImpl::SyncImpl() {
  Status s;
  // Calls flush buffers
  if (fsync(file_data_->GetFileHandle()) < 0) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError(
        "fsync failed at Sync() for: " + file_data_->GetName(), lastError);
  }
  return s;
}


Status WinWritableImpl::AllocateImpl(uint64_t offset, uint64_t len) {
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


////////////////////////////////////////////////////////////////////////////////
/// WinWritableFile

WinWritableFile::WinWritableFile(const std::string& fname, HANDLE hFile,
                                 size_t alignment, size_t /* capacity */,
                                 const EnvOptions& options)
    : WinFileData(fname, hFile, options.use_direct_writes),
      WinWritableImpl(this, alignment) {
  assert(!options.use_mmap_writes);
}

WinWritableFile::~WinWritableFile() {
}

// Indicates if the class makes use of direct I/O
bool WinWritableFile::use_direct_io() const { return WinFileData::use_direct_io(); }

size_t WinWritableFile::GetRequiredBufferAlignment() const {
  return GetAlignement();
}

Status WinWritableFile::Append(const Slice& data) {
  return AppendImpl(data);
}

Status WinWritableFile::PositionedAppend(const Slice& data, uint64_t offset) {
  return PositionedAppendImpl(data, offset);
}

// Need to implement this so the file is truncated correctly
// when buffered and unbuffered mode
Status WinWritableFile::Truncate(uint64_t size) {
  return TruncateImpl(size);
}

Status WinWritableFile::Close() {
  return CloseImpl();
}

  // write out the cached data to the OS cache
  // This is now taken care of the WritableFileWriter
Status WinWritableFile::Flush() {
  return Status::OK();
}

Status WinWritableFile::Sync() {
  return SyncImpl();
}

Status WinWritableFile::Fsync() { return SyncImpl(); }

uint64_t WinWritableFile::GetFileSize() {
  return GetFileSizeImpl();
}

Status WinWritableFile::Allocate(uint64_t offset, uint64_t len) {
  return AllocateImpl(offset, len);
}

size_t WinWritableFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(GetFileHandle(), id, max_size);
}

/////////////////////////////////////////////////////////////////////////
/// WinRandomRWFile

WinRandomRWFile::WinRandomRWFile(const std::string& fname, HANDLE hFile,
                                 size_t alignment, const EnvOptions& options)
    : WinFileData(fname, hFile,
                  options.use_direct_reads && options.use_direct_writes),
      WinRandomAccessImpl(this, alignment, options),
      WinWritableImpl(this, alignment) {}

bool WinRandomRWFile::use_direct_io() const { return WinFileData::use_direct_io(); }

size_t WinRandomRWFile::GetRequiredBufferAlignment() const {
  return GetAlignement();
}

Status WinRandomRWFile::Write(uint64_t offset, const Slice & data) {
  return PositionedAppendImpl(data, offset);
}

Status WinRandomRWFile::Read(uint64_t offset, size_t n, Slice* result,
                             char* scratch) const {
  return ReadImpl(offset, n, result, scratch);
}

Status WinRandomRWFile::Flush() {
  return Status::OK();
}

Status WinRandomRWFile::Sync() {
  return SyncImpl();
}

Status WinRandomRWFile::Close() {
  return CloseImpl();
}

//////////////////////////////////////////////////////////////////////////
/// WinDirectory

Status WinDirectory::Fsync() { return Status::OK(); }

//////////////////////////////////////////////////////////////////////////
/// WinFileLock

WinFileLock::~WinFileLock() {
  BOOL ret = ::CloseHandle(hFile_);
  assert(ret);
}

}
}
