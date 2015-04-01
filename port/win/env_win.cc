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


#include <Windows.h>
#include <Rpc.h> // For UUID generation


// For non linux platform, the following macros are used only as place
// holder.
#ifndef OS_LINUX
#define POSIX_FADV_NORMAL 0 /* [MC1] no further special treatment */
#define POSIX_FADV_RANDOM 1 /* [MC1] expect random page refs */
#define POSIX_FADV_SEQUENTIAL 2 /* [MC1] expect sequential page refs */
#define POSIX_FADV_WILLNEED 3 /* [MC1] will need these pages */
#define POSIX_FADV_DONTNEED 4 /* [MC1] dont need these pages */
#endif

// This is only set from db_stress.cc and for testing only.
// If non-zero, kill at various points in source code with probability 1/this
int rocksdb_kill_odds = 0;

namespace rocksdb 
{

namespace 
{

const size_t c_OneMB = (1 << 20);

std::string GetWindowsErrSz(DWORD err)
{
    LPSTR lpMsgBuf;
    FormatMessageA(
        FORMAT_MESSAGE_ALLOCATE_BUFFER |
        FORMAT_MESSAGE_FROM_SYSTEM |
        FORMAT_MESSAGE_IGNORE_INSERTS,
        NULL,
        err,
        0, // Default language
        reinterpret_cast<LPSTR>(&lpMsgBuf),
        0,
        NULL
        );

    std::string Err = lpMsgBuf;
    LocalFree(lpMsgBuf);
    return Err;
}


// A wrapper for fadvise, if the platform doesn't support fadvise,
// it will simply return Status::NotSupport.
int Fadvise(int fd, off_t offset, size_t len, int advice) 
{
    return 0;  // simply do nothing.
}

inline
Status IOErrorFromWindowsError(const std::string& context, DWORD err) {
    return Status::IOError(context, GetWindowsErrSz(err));
}

inline
Status IOErrorFromLastWindowsError(const std::string& context) {
    return IOErrorFromWindowsError(context, GetLastError());
}

inline
Status IOError(const std::string& context, int err_number) {
    return Status::IOError(context, strerror(err_number));
}

// TODO(sdong): temp logging. Need to help debugging. Remove it when
// the feature is proved to be stable.
inline void PrintThreadInfo(size_t thread_id, const std::thread& th) {

    fprintf(stdout, "Bg thread %Iu terminates %Iu\n", thread_id,
        th.get_id());
}

// returns the ID of the current process
inline
int current_process_id() {
    return _getpid();
}

#ifdef NDEBUG
    // empty in release build
    #define TEST_KILL_RANDOM(rocksdb_kill_odds)
#else

// Kill the process with probablity 1/odds for testing.
void TestKillRandom(int odds, const std::string& srcfile, int srcline) 
{
    time_t curtime = time(nullptr);
    Random r((uint32_t)curtime);

    assert(odds > 0);
    bool crash = r.OneIn(odds);
    if (crash) 
    {
        fprintf(stdout, "Crashing at %s:%d\n", srcfile.c_str(), srcline);
        fflush(stdout);
        std::string* p_str = nullptr;
        p_str->c_str();
    }
}

// To avoid crashing always at some frequently executed codepaths (during
// kill random test), use this factor to reduce odds
#define REDUCE_ODDS 2
#define REDUCE_ODDS2 4

#define TEST_KILL_RANDOM(rocksdb_kill_odds) {   \
  if (rocksdb_kill_odds > 0) { \
    TestKillRandom(rocksdb_kill_odds, __FILE__, __LINE__);     \
  } \
}

#endif

// RAII helpers for HANDLEs
const auto CloseHandleFunc = [](HANDLE h) { ::CloseHandle(h); };
typedef std::unique_ptr<void, decltype(CloseHandleFunc)> UniqueCloseHandlePtr;


// We preserve the original name of this interface to denote the original idea behind it.
// All reads happen by a specified offset and pwrite interface does not change
// the position of the file pointer. Judging from the man page and errno it does execute
// lseek atomically to return the position of the file back where it was. WriteFile() does not
// have this capability. Therefore, for both pread and pwrite the pointer is advanced to the next position
// which is fine for writes because they are (should be) sequential.
// Because all the reads/writes happen by the specified offset, the caller in theory should not
// rely on the current file offset.
SSIZE_T pwrite(HANDLE hFile, const char * src, size_t numBytes, uint64_t offset) {

    OVERLAPPED overlapped = { 0 };
    ULARGE_INTEGER offsetUnion;
    offsetUnion.QuadPart = offset;

    overlapped.Offset = offsetUnion.LowPart;
    overlapped.OffsetHigh = offsetUnion.HighPart;

    SSIZE_T result = 0;

    unsigned long bytesWritten = 0;

    if (FALSE == WriteFile(hFile, src, numBytes, &bytesWritten, &overlapped)) {
        result = -1;
    }
    else {
        result = bytesWritten;
    }

    return result;
}

// See comments for pwrite above
SSIZE_T pread(HANDLE hFile, char * src, size_t numBytes, uint64_t offset)
{
    OVERLAPPED overlapped = { 0 };
    ULARGE_INTEGER offsetUnion;
    offsetUnion.QuadPart = offset;

    overlapped.Offset = offsetUnion.LowPart;
    overlapped.OffsetHigh = offsetUnion.HighPart;

    SSIZE_T result = 0;

    unsigned long bytesRead = 0;

    if (FALSE == ReadFile(hFile, src, numBytes, &bytesRead, &overlapped)) {
        return -1;
    }
    else  {
        result = bytesRead;
    }

    return result;
}

// Note the below two do not set errno because they are used only here in this file
// on a Windows handle and, therefore, not necessary. Translating GetLastError() to errno
// is a sad business
inline
int fsync(HANDLE hFile) {

    if (!FlushFileBuffers(hFile)) {
        return -1;
    }

    return 0;
}

inline
int fdatasync(HANDLE hFile)
{
    // Windows doesn't have an equivalent of fdatasync (which syncs data only and not metadata), so we do full sync
    return fsync(hFile);
}

inline
size_t TruncateToPageBoundary(size_t page_size, size_t s) {

    s -= (s & (page_size - 1));
    assert((s % page_size) == 0);
    return s;
}

// Roundup x to a multiple of y
inline
size_t Roundup(size_t x, size_t y) {
    return ((x + y - 1) / y) * y;
}


// Can only truncate or reserve to a sector size aligned if
// used on files that are opened with Unbuffered I/O
// Normally it does not present a problem since in memory mapped files
// we do not disable buffering
Status fallocate(const std::string& filename, HANDLE hFile, uint64_t toSize) {

    Status status;

    LARGE_INTEGER length = { 0 };
    LARGE_INTEGER current = { 0 };

    // Get current pointer. We do not move here.
    if (!::SetFilePointerEx(hFile, length, &current, FILE_CURRENT)) {
        return IOErrorFromWindowsError("Failed to read current file pointer: " + filename, GetLastError());
    }

    // Extend the file size
    length.QuadPart = toSize;
    if (!::SetFilePointerEx(hFile, length, NULL, FILE_BEGIN)) {
        return IOErrorFromWindowsError("Failed to seek to new length file: " + filename, GetLastError());
    }

    if (!::SetEndOfFile(hFile)) {
        status = IOErrorFromWindowsError("Failed to set end of file: " + filename, GetLastError());
    }

    // Rewind back the current position in all cases but report the original error
    // although with the mapped file it may not be that important
    if (!::SetFilePointerEx(hFile, current, NULL, FILE_BEGIN)) {
        if (status.ok()) {
            status = IOErrorFromWindowsError("Failed to restore original position in ReserveFileSpace(): " + filename, GetLastError());
        }
    }

    return status;
}

// Can only truncate or reserve to a sector size aligned if
// used on files that are opened with Unbuffered I/O
Status ftruncate(const std::string& filename, HANDLE hFile, uint64_t toSize) {

    Status status;

    LARGE_INTEGER length;
    length.QuadPart = toSize;

    if (!::SetFilePointerEx(hFile, length, NULL, FILE_BEGIN)) {
        status = IOErrorFromWindowsError("Failed to set pointer for file: " + filename, GetLastError());
    } else if (!::SetEndOfFile(hFile)) { // End of file at the current file ptr
        status = IOErrorFromWindowsError("Failed to set end of file: " + filename, GetLastError());
    }

    return status;
}

class WinRandomRWFile : public RandomRWFile {

    const std::string filename_;
    HANDLE            hFile_;
    bool              pending_sync_;
    bool              pending_fsync_;

public:

    WinRandomRWFile(const std::string& fname, HANDLE hFile, const EnvOptions& options)
        : filename_(fname),
        hFile_(hFile),
        pending_sync_(false),
        pending_fsync_(false) {

        assert(!options.use_mmap_writes && !options.use_mmap_reads);
    }

    ~WinRandomRWFile() {

        if (hFile_ != INVALID_HANDLE_VALUE && hFile_ != NULL) {
            ::CloseHandle(hFile_);
        }
    }

    virtual Status Write(uint64_t offset, const Slice& data) override {

        const char* src = data.data();
        size_t left = data.size();


        pending_sync_ = true;
        pending_fsync_ = true;

        SSIZE_T done = pwrite(hFile_, src, left, offset);

        if (done < 0) {
            return IOErrorFromWindowsError("pwrite failed to: " + filename_, GetLastError());
        }

        IOSTATS_ADD(bytes_written, done);

        left -= done;
        src += done;
        offset += done;

        return Status::OK();
    }

    virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {

        Status s;

        SSIZE_T r = -1;
        char* ptr = scratch;
        size_t left = n;

        while (left > 0) {

            r = pread(hFile_, ptr, n, offset);

            if (r <= 0) {
                break;
            }

            ptr += r;
            offset += r;
            left -= r;
        }

        IOSTATS_ADD_IF_POSITIVE(bytes_read, n - left);

        *result = Slice(scratch, (r < 0) ? 0 : r);

        if (r < 0) {
            s = IOErrorFromWindowsError("pread failed from: " + filename_, GetLastError());
        }
        return s;
    }

    virtual Status Close() override {

        Status s = Status::OK();
        if (hFile_ != INVALID_HANDLE_VALUE && ::CloseHandle(hFile_) == FALSE) {
            s = IOErrorFromWindowsError("Failed to close file: " + filename_, GetLastError());
        }
        hFile_ = INVALID_HANDLE_VALUE;
        return s;
    }

    virtual Status Sync() override {

        if (pending_sync_ && fdatasync(hFile_) < 0) {
            return IOErrorFromWindowsError("Failed to Sync() buffers for: " + filename_, GetLastError());
        }
        pending_sync_ = false;
        return Status::OK();
    }

    virtual Status Fsync() override {
        if (pending_fsync_ && fsync(hFile_) < 0) {
            return IOErrorFromWindowsError("Failed to Fsync() for: " + filename_, GetLastError());
        }
        pending_fsync_ = false;
        pending_sync_ = false;
        return Status::OK();
    }
};


// mmap() based random-access
class WinMmapReadableFile : public RandomAccessFile {

    const std::string                    fileName_;
    HANDLE                               hFile_;
    HANDLE                               hMap_;

    const void*                          mapped_region_;
    const size_t                         length_;

public:
    // base[0,length-1] contains the mmapped contents of the file.
    WinMmapReadableFile(const std::string &fileName, HANDLE hFile, HANDLE hMap, const void* mapped_region, size_t length)
        : fileName_(fileName), hFile_(hFile), hMap_(hMap), mapped_region_(mapped_region), length_(length) {

    }

    ~WinMmapReadableFile() {

        BOOL ret = ::UnmapViewOfFile(mapped_region_);
        assert(ret);

        ret = ::CloseHandle(hMap_);
        assert(ret);

        ret = ::CloseHandle(hFile_);
        assert(ret);
    }

    virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {

        Status s;

        if (offset + n > length_) {
            *result = Slice();
            s = IOError(fileName_, EINVAL);
        }
        else {
            *result = Slice(reinterpret_cast<const char*>(mapped_region_) + offset, n);
        }
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
    HANDLE            hFile_;
    HANDLE            hMap_;

    const size_t      page_size_;              // We flush the mapping view in page_size increments. We may decide if this is a memory page size or SSD page size
    const size_t      allocation_granularity_; // View must start at such a granularity
    size_t            mapping_size_;           // We want file mapping to be of a specific size because then the file is expandable
    size_t            view_size_;              // How much memory to map into a view at a time

    char*             mapped_begin_;           // Must begin at the file offset that is aligned with allocation_granularity_
    char*             mapped_end_;
    char*             dst_;                    // Where to write next  (in range [mapped_begin_,mapped_end_])
    char*             last_sync_;              // Where have we synced up to

    uint64_t          file_offset_;            // Offset of mapped_begin_ in file

    // Do we have unsynced writes?
    bool              pending_sync_;

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
        return fallocate(filename_, hFile_, toSize);
    }


    Status UnmapCurrentRegion() {

        Status status;

        if (mapped_begin_ != nullptr) {

            if (!::UnmapViewOfFile(mapped_begin_)) {
                status = IOErrorFromWindowsError("Failed to unmap file view: " + filename_, GetLastError());
            }

            // UnmapView automatically sends data to disk but not the metadata
            // which is good and provides some equivalent of fdatasync() on Linux
            // therefore, we donot need separate flag for metadata
            pending_sync_ = false;
            mapped_begin_ = nullptr;
            mapped_end_   = nullptr;
            dst_          = nullptr;
            last_sync_    = nullptr;

            // Move on to the next portion of the file
            file_offset_ += view_size_;

            // Increase the amount we map the next time, but capped at 1MB
            view_size_    *= 2;
            view_size_ = std::min(view_size_, c_OneMB);
        }

        return status;
    }

    Status MapNewRegion() {

        Status status;

        assert(mapped_begin_ == nullptr);

        size_t minMappingSize = file_offset_ + view_size_;

        // Check if we need to create a new mapping since we want to write beyond the current one
        // If the mapping view is now too short
        // CreateFileMapping will extend the size of the file automatically if the mapping size is greater than
        // the current length of the file, which reserves the space and makes writing faster, except, windows can not map an empty file.
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

            // Calculate the new mapping size which will hopefully reserve space for several consecutive sliding views
            // Query preallocation block size if set
            size_t preallocationBlockSize = 0;
            size_t lastAllocatedBlockSize = 0; // Not used
            GetPreallocationStatus(&preallocationBlockSize, &lastAllocatedBlockSize);

            if (preallocationBlockSize) {
                preallocationBlockSize = Roundup(preallocationBlockSize, allocation_granularity_);
            } else {
                preallocationBlockSize = 2 * view_size_;
            }

            mapping_size_ += preallocationBlockSize;

            ULARGE_INTEGER mappingSize;
            mappingSize.QuadPart = mapping_size_;

            hMap_ = CreateFileMappingA(
                hFile_,
                NULL,                    // Security attributes
                PAGE_READWRITE,         // There is not a write only mode for mapping
                mappingSize.HighPart,  // Enable mapping the whole file but the actual amount mapped is determined by MapViewOfFile
                mappingSize.LowPart,
                NULL);  // Mapping name

             if (NULL == hMap_) {
                 return IOErrorFromWindowsError("WindowsMmapFile failed to create file mapping for: " + filename_, GetLastError());
             }
        }

        ULARGE_INTEGER offset;
        offset.QuadPart = file_offset_;

        // View must begin at the granularity aligned offset
        mapped_begin_ = reinterpret_cast<char*>(MapViewOfFileEx(hMap_, FILE_MAP_WRITE, offset.HighPart, offset.LowPart, view_size_, NULL));

        if (!mapped_begin_) {
            status = IOErrorFromWindowsError("WindowsMmapFile failed to map file view: " + filename_, GetLastError());
        } else {
            mapped_end_   = mapped_begin_ + view_size_;
            dst_          = mapped_begin_;
            last_sync_    = mapped_begin_;
            pending_sync_ = false;
        }

        return status;
    }

public:

    WinMmapFile(const std::string& fname, HANDLE hFile, size_t page_size, size_t allocation_granularity, const EnvOptions& options)
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

        // Allocation granularity must be obtained from GetSystemInfo() and must be a power of two.
        assert(allocation_granularity > 0);
        assert((allocation_granularity & (allocation_granularity - 1)) == 0);

        assert(page_size > 0);
        assert((page_size & (page_size - 1)) == 0);

        // Only for memory mapped writes
        assert(options.use_mmap_writes);

        // Make sure buffering is not disabled. It is ignored for mapping
        // purposes but also imposes restriction on moving file position
        // it is not a problem so much with reserving space since it is probably a factor
        // of allocation_granularity but we also want to truncate the file in Close() at
        // arbitrary position so we do not have to feel this with zeros.
        assert(options.use_os_buffer);

        // View size must be both the multiple of allocation_granularity AND the page size
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
            IOSTATS_ADD(bytes_written, n);
            dst_ += n;
            src += n;
            left -= n;
            pending_sync_ = true;
        }

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

        if (NULL != hMap_ ) {

            BOOL ret = ::CloseHandle(hMap_);
            if (!ret && s.ok()) {
                auto lastError = GetLastError();
                s = IOErrorFromWindowsError("Failed to Close mapping for file: " + filename_, lastError);
            }

            hMap_ = NULL;
        }

        TruncateFile(targetSize);

        BOOL ret = ::CloseHandle(hFile_);
        hFile_ = NULL;

        if (!ret && s.ok()) {
            auto lastError = GetLastError();
            s = IOErrorFromWindowsError("Failed to close file map handle: " + filename_, lastError);
        }

        return s;
    }

    virtual Status Flush() override {
        return Status::OK();
    }

    // Flush only data
    virtual Status Sync() override {

        Status s;

        // Some writes occurred since last sync
        if (pending_sync_) {

            assert(mapped_begin_);
            assert(dst_);
            assert(dst_ > mapped_begin_);
            assert(dst_ < mapped_end_);

            size_t page_begin = TruncateToPageBoundary(page_size_, last_sync_ - mapped_begin_);
            size_t page_end = TruncateToPageBoundary(page_size_, dst_ - mapped_begin_ - 1);
            last_sync_ = dst_;

            // Flush only the amount of that is a multiple of pages
            if(!::FlushViewOfFile(mapped_begin_ + page_begin, (page_end - page_begin) + page_size_)) {
                s = IOErrorFromWindowsError("Failed to FlushViewOfFile: " + filename_, GetLastError());
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
                s = IOErrorFromWindowsError("Failed to FlushFileBuffers: " + filename_, GetLastError());
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

    virtual Status Allocate(off_t offset, off_t len) override {

        return Status::OK();
    }
};


class WinSequentialFile: public SequentialFile 
{
private:
    const std::string filename_;
    FILE*             file_;
    int               fd_;
    bool              use_os_buffer_;

public:
    WinSequentialFile(const std::string& fname, FILE* f, const EnvOptions& options) : 
        filename_(fname), file_(f), fd_(fileno(f)), use_os_buffer_(options.use_os_buffer) {
    }
  
    virtual ~WinSequentialFile() { 
        assert(file_ != nullptr);
        fclose(file_); 
    }

    virtual Status Read(size_t n, Slice* result, char* scratch) 
    {
        Status s;
        size_t r = 0;

        // read() and fread() as well as write/fwrite do not guarantee
        // to fullfil the entire request in one call thus the loop.
        do {
            r = fread(scratch, 1, n, file_);
        } while (r == 0 && ferror(file_));

        IOSTATS_ADD(bytes_read, r);

        *result = Slice(scratch, r);

        if (r < n) {

            if (feof(file_)) {
                // We leave status as ok if we hit the end of the file
                // We also clear the error so that the reads can continue
                // if a new data is written to the file
                clearerr(file_);
            } else  {
                // A partial read with an error: return a non-ok status
                s = Status::IOError(filename_, strerror(errno));
            }
        }

        if (!use_os_buffer_) {

            // we need to fadvise away the entire range of pages because
            // we do not want readahead pages to be cached.
            Fadvise(fd_, 0, 0, POSIX_FADV_DONTNEED); // free OS pages
        }

        return s;
    }

    virtual Status Skip(uint64_t n) override {
        if (fseek(file_, n, SEEK_CUR)) {
            return IOError(filename_, errno);
        }
        return Status::OK();
    }

    virtual Status InvalidateCache(size_t offset, size_t length) override {
        return Status::OK();
    }
};

// pread() based random-access
class WinRandomAccessFile: public RandomAccessFile 
{

    const std::string  filename_;
    HANDLE             hFile_;
    bool               use_os_buffer_;

public:

    WinRandomAccessFile(const std::string& fname, HANDLE hFile, const EnvOptions& options) :
        filename_(fname), hFile_(hFile), use_os_buffer_(options.use_os_buffer) {

        assert(!options.use_mmap_reads);
    }
  
    virtual ~WinRandomAccessFile() { 
        if (hFile_ != NULL && hFile_ != INVALID_HANDLE_VALUE) {
            ::CloseHandle(hFile_);
        }
    }

    virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {

        Status s;
        SSIZE_T r = -1;
        size_t left = n;
        char* ptr = scratch;

        while (left > 0) {
            r = pread(hFile_, ptr, left, offset);
            if (r <= 0) {
                break;
            }
            ptr += r;
            offset += r;
            left -= r;
        }

        IOSTATS_ADD_IF_POSITIVE(bytes_read, n - left);

        *result = Slice(scratch, (r < 0) ? 0 : n - left);

        if (r < 0) {

            s = IOErrorFromLastWindowsError(filename_);
        }

        return s;
    }

    virtual void Hint(AccessPattern pattern) override {
    }

    virtual Status InvalidateCache(size_t offset, size_t length) override {
        return Status::OK();
    }
};

// This is a sequential write class. It has been mimicked (as others) after
// the original Posix class. We add support for unbuffered I/O on windows as well
// we utilize the original buffer as an alignment buffer to write directly to file with no buffering.
// No buffering requires that the provided buffer is aligned to the physical sector size (SSD page size) and
// that all SetFilePointer() operations to occur with such an alignment.
// We thus always write in sector/page size increments to the drive and leave
// the tail for the next write OR for Close() at which point we pad with zeros. No padding is required for
// buffered access.
class WinWritableFile : public WritableFile {
private:

    const std::string   filename_;

    HANDLE              hFile_;

    size_t              allignment_;        // Really alignment value for the buffer either physical sector or SSD page size. Used for unbuffered access.

    size_t              cursize_;           // current size of cached data in buf_
    size_t              capacity_;          // max size of buf_
    std::unique_ptr<char[]>  buf_;           // a buffer to cache writes
    char*               bufstart_;          // aligned start of the buffer

    uint64_t            filesize_;          // How much data is actually written disk
    uint64_t            reservedsize_;      // how far we have reserved space

    bool                pending_sync_;
    bool                pending_fsync_;

    uint64_t            last_sync_size_;
    uint64_t            bytes_per_sync_;

    const bool          use_os_buffer_;        // Used to indicate unbuffered access, the file must be opened as unbuffered if false

    // Allocates a new buffer and sets bufstart_ to the aligned first byte
    // Returns ptr to the fist aligned byte
    void AllocateNewBuffer(size_t requestedCapacity) {

        size_t size = Roundup(requestedCapacity, allignment_);

        buf_.reset(new char[size + allignment_]);

        char* p = buf_.get();

        capacity_ = 0;
        cursize_ = 0;
        bufstart_ = nullptr;

        bufstart_ = reinterpret_cast<char*>((reinterpret_cast<uintptr_t>(p) + (allignment_ - 1)) &  ~static_cast<uintptr_t>(allignment_ - 1));
        capacity_ = size;
    }

public:

    WinWritableFile(const std::string& fname,
            HANDLE hFile,
            size_t allignment,
            size_t capacity,
            const EnvOptions& options) :
        filename_(fname),
        hFile_(hFile),
        allignment_(allignment),
        cursize_(0),
        capacity_(0),
        buf_(),
        bufstart_(nullptr),
        filesize_(0),
        reservedsize_(0),
        pending_sync_(false),
        pending_fsync_(false),
        last_sync_size_(0),
        bytes_per_sync_(options.bytes_per_sync),
        use_os_buffer_(options.use_os_buffer) {

        assert(!options.use_mmap_writes);

        assert(allignment_ > 0);
        assert((allignment_ & (allignment_ - 1)) == 0);

        AllocateNewBuffer(capacity);
    }

    ~WinWritableFile() {

        if (NULL != hFile_ && INVALID_HANDLE_VALUE != hFile_) {
            WinWritableFile::Close();
        }
    }

    virtual Status Append(const Slice& data) override {

        const char* src = data.data();

        assert(data.size() < INT_MAX);

        size_t left = data.size();
        Status s;
        pending_sync_ = true;
        pending_fsync_ = true;

        // This would call Alloc() if we are out of blocks
        PrepareWrite(GetFileSize(), left);

        while (left > 0) {

            assert(capacity_ >= cursize_);

            // Fill it to the limit so we can flush if necessary whole pages
            size_t bufferRemaining = capacity_ - cursize_;
            size_t toCopy = std::min(left, bufferRemaining);

            if (toCopy > 0) {
                memcpy(bufstart_ + cursize_, src, toCopy);
                cursize_ += toCopy;
                left -= toCopy;
                src += toCopy;
            }

            if (left > 0) {

                Flush();

                // May increase the buffer here
                if (cursize_ == 0 && capacity_ < c_OneMB) {

                    size_t desiredCapacity = capacity_ * 2;
                    desiredCapacity = std::min(desiredCapacity, c_OneMB);

                    AllocateNewBuffer(desiredCapacity);
                }
            }

        }

        return s;
    }

    virtual Status Close() override {

        Status s;

        // If there is any data in the cache not written we need to deal with it
        if (cursize_ > 0) {

            // If OS buffering is on, we just flush the remainder, otherwise need 
            if (!use_os_buffer_) {

                size_t totalSize = Roundup(cursize_, allignment_);

                // Pad with zeros because table readers should be able to handle this
                // but for CURRENT, MANIFEST files buffering is enabled via Optimize or using
                // default options
                if (totalSize > cursize_) {

                    size_t padSize = totalSize - cursize_;
                    assert((padSize + cursize_) <= capacity_);

                    memset(bufstart_ + cursize_, 0, padSize);
                    cursize_ = totalSize;
                }

                SSIZE_T written = pwrite(hFile_, bufstart_, cursize_, filesize_);

                if (written < 0) {
                    auto lastError = GetLastError();
                    s = IOErrorFromWindowsError("Failed to pwrite on Close() to file: " + filename_, lastError);
                }
            } else {

                DWORD done = 0;

                if (!WriteFile(hFile_, bufstart_, cursize_, &done, NULL) || done < cursize_) {
                    auto lastError = GetLastError();
                    s = IOErrorFromWindowsError("Failed to write on Close() to file: " + filename_, lastError);
                }
            }

        }

        // Truncate the file as the file position is just right. Note, we can not
        // use ftruncate as if we are unbuffered then we can not SetFilePointer() arbitrary
        if (s.ok()) {
            if (!::SetEndOfFile(hFile_)) {
                auto lastError = GetLastError();
                s = IOErrorFromWindowsError("Failed to truncate file: " + filename_, lastError);
            }
        }

        if (FALSE == ::CloseHandle(hFile_)) {
            if (s.ok()) {
                auto lastError = GetLastError();
                s = IOErrorFromWindowsError("CloseHandle failed for: " + filename_, lastError);
            }
        }

        hFile_ = INVALID_HANDLE_VALUE;
        return s;
    }

    // write out the cached data to the OS cache
    virtual Status Flush() override {

        Status status;

        SSIZE_T done = 0;

        size_t remains = 0;

        if (!use_os_buffer_) {
            // Flush only a multiple of alignment
            assert((filesize_ % allignment_) == 0);

            size_t left = TruncateToPageBoundary(allignment_, cursize_);

            // We have not accumulated one whole page. will flush next time
            if (left == 0) {
                return status;
            }

            remains = cursize_ - left;

            // I am under impression that if WriteFile succeeds then everything must be written
            done = pwrite(hFile_, bufstart_, left, filesize_);

            if (done < (SSIZE_T)left) {
                auto lastError = GetLastError();
                status = IOErrorFromWindowsError("Failed to pwrite while flushing failed for: " + filename_, lastError);
                done = (done < 0) ? 0 : done;
            }

        } else {

            DWORD bytesWritten = 0;

            if (!WriteFile(hFile_, bufstart_, cursize_, &bytesWritten, NULL)) {
                auto lastError = GetLastError();
                status = IOErrorFromWindowsError("Failed to write while flushing for: " + filename_, lastError);
            }

            done = bytesWritten;
            remains = cursize_ - done;

        }

        IOSTATS_ADD_IF_POSITIVE(bytes_written, done);
        filesize_ += done;

        // Move the tail to the beginning of the buffer
        // This never happens during normal Append but rather during
        // explicit call to Flush()/Sync() or Close()
        if (remains > 0) {
            memmove(bufstart_, bufstart_ + done, remains);
        }

        // Remains from what we have written
        cursize_ = remains;

        return status;
    }

    virtual Status Sync() override {

        Status s = Flush();

        if (!s.ok()) {
            return s;
        }

        // On unbuffered IO everything is flushed to disk on each write
        // so need for FlushBuffers
        if (use_os_buffer_) {
            // Calls flush buffers
            if (pending_sync_ && fdatasync(hFile_) < 0) {
                auto lastError = GetLastError();
                s = IOErrorFromWindowsError("fdatasync failed at Sync() for: " + filename_, lastError);
            } else {
                pending_fsync_ = false;
                pending_sync_ = false;
            }
        } else {
            pending_fsync_ = false;
            pending_sync_ = false;
        }


        return s;
    }

    virtual Status Fsync() override {

        return Sync();
    }

    virtual uint64_t GetFileSize() override {
        return filesize_ + cursize_;
    }

    virtual Status Allocate(off_t offset, off_t len) override {

        Status status;

        TEST_KILL_RANDOM(rocksdb_kill_odds);

        // Make sure that we reserve an aligned amount of space
        // since the reservation block size is driven outside so we want
        // to check if we are ok with reservation here
        size_t spaceToReserve = Roundup(offset + len, allignment_);

        // Nothing to do
        if (spaceToReserve <= reservedsize_) {
            return status;
        }

        status = fallocate(filename_, hFile_, spaceToReserve);

        if (status.ok()) {
            reservedsize_ = spaceToReserve;
        }

        return status;
    }
};

class WinDirectory : public Directory 
{
public:
    WinDirectory() {
    }

    virtual Status Fsync() override {
        return Status::OK();
    }
};

class WinFileLock : public FileLock 
{
public:

    explicit WinFileLock(HANDLE hFile) :
        hFile_(hFile) {

        assert(hFile != NULL);
        assert(hFile != INVALID_HANDLE_VALUE);
    }

    ~WinFileLock()  {

        BOOL ret = ::CloseHandle(hFile_);
        assert(ret);
    }

private:
    HANDLE hFile_;
};

namespace 
{

void WinthreadCall(const char* label, std::error_code result) {

    if (0 != result.value()) {

        fprintf(stderr, "pthread %s: %s\n", label, strerror(result.value()));
        exit(1);
    }
}

}

class WinEnv : public Env
{
public:
    WinEnv();

    virtual ~WinEnv() {

        for (int i = 0; i<threads_to_join_.size(); ++i)
        {
            threads_to_join_[i]->join();
        }
    }

    virtual Status DeleteFile(const std::string& fname) override {

        Status result;

        if (_unlink(fname.c_str())) {

            result = IOError("Failed to delete: " + fname, errno);
        }

        return result;
    }

    Status GetCurrentTime(int64_t* unix_time) {

        time_t time = std::time(nullptr);
        if (time == (time_t)(-1)) {
            return Status::NotSupported("Failed to get time");
        }

        *unix_time = time;
        return Status::OK();
    }

    virtual Status NewSequentialFile(const std::string& fname, std::unique_ptr<SequentialFile>* result, const EnvOptions& options) override {

        result->reset();
        FILE* f = fopen(fname.c_str(), "rb");
        if (f == NULL) {
            *result = NULL;
            return Status::IOError("Failed to open NewSequentialFile: " + fname, strerror(errno));
        }  else  {
            result->reset(new WinSequentialFile(fname, f, options));
            return Status::OK();
        }
    }

    virtual Status NewRandomAccessFile(const std::string& fname, std::unique_ptr<RandomAccessFile>* result, const EnvOptions& options) override {

        result->reset();
        Status s;

        // We ignore no buffering flag here

        // Open the file for read-only random access
        // Random access is to disable read-ahead as the system reads too much data
        const DWORD fileFlags = FILE_ATTRIBUTE_READONLY | FILE_FLAG_RANDOM_ACCESS;


        HANDLE hFile = CreateFileA(
            fname.c_str(),
            GENERIC_READ,
            FILE_SHARE_READ,
            NULL,
            OPEN_EXISTING,
            fileFlags,
            NULL);

        if (INVALID_HANDLE_VALUE == hFile) {
            return IOErrorFromLastWindowsError("NewRandomAccessFile failed to Create/Open: " + fname);
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
                    return IOError("NewRandomAccessFile failed to map empty file: " + fname, EINVAL);
                }

                HANDLE hMap = CreateFileMappingA(hFile,
                                                 NULL,
                                                 PAGE_READONLY,
                                                 0, // Whole file at its present length
                                                 0,
                                                 NULL);  // Mapping name

                if (!hMap) {
                    return IOErrorFromLastWindowsError("Failed to create file mapping for NewRandomAccessFile: " + fname);
                }

                UniqueCloseHandlePtr mapGuard (hMap, CloseHandleFunc);

                const void* mapped_region = MapViewOfFileEx(hMap,
                                            FILE_MAP_READ,
                                            0, // High DWORD of access start
                                            0,  // Low DWORD
                                            fileSize,
                                            NULL); // Let the OS choose the mapping

                if (!mapped_region) {
                    return IOErrorFromLastWindowsError("Failed to MapViewOfFile for NewRandomAccessFile: " + fname);
                }

                result->reset(new WinMmapReadableFile(fname, hFile, hMap, mapped_region, fileSize));

                mapGuard.release();
                fileGuard.release();
            }
        }
        else {

            result->reset(new WinRandomAccessFile(fname, hFile, options));
            fileGuard.release();
        }

        return s;
    }

    
    virtual Status NewWritableFile(const std::string& fname, std::unique_ptr<WritableFile>* result, const EnvOptions& options) override {

        const size_t c_BufferCapacity = 64 * 1024;

        result->reset();
        Status s;

        DWORD fileFlags = FILE_ATTRIBUTE_NORMAL;

        if (!options.use_os_buffer && !options.use_mmap_writes) {
            fileFlags = FILE_FLAG_NO_BUFFERING;
        }

        // Desired access. We are want to write only here but if we want to memory map
        // the file then there is no write only mode so we have to create it Read/Write
        // However, MapViewOfFile specifies only Write only
        DWORD desiredAccess = GENERIC_WRITE;

        if (options.use_mmap_writes) {
            desiredAccess |= GENERIC_READ;
        }

        HANDLE hFile = CreateFileA(fname.c_str(),
            desiredAccess,    // Access desired
            FILE_SHARE_READ,  // Reads can proceed in parallel 
            NULL,             // Security attributes
            CREATE_ALWAYS,    // Posix env says O_CREAT | O_RDWR | O_TRUNC
            fileFlags,       // Flags
            NULL);            // Template File

        if (INVALID_HANDLE_VALUE == hFile) {
            return IOErrorFromLastWindowsError("Failed to create a NewWriteableFile: " + fname);
        }

        if (options.use_mmap_writes) {

            // We usually do not use mmmapping on SSD and thus we pass memory page_size
            result->reset(new WinMmapFile(fname, hFile, page_size_, allocation_granularity_, options));

        } else {
            // Here we want the buffer allocation to be alligned by the SSD page size and to be a multiple of it
            result->reset(new WinWritableFile(fname, hFile, page_size_, c_BufferCapacity, options));
        }

        return s;
    }
    
    virtual Status NewRandomRWFile(const std::string& fname, std::unique_ptr<RandomRWFile>* result,
                                    const EnvOptions& options) override {
        result->reset();

        // no support for mmap yet (same as POSIX env)
        if (options.use_mmap_writes || options.use_mmap_reads) {
            return Status::NotSupported("No support for mmap read/write yet");
        }

        Status s;

        HANDLE hFile = CreateFileA(fname.c_str(),
                                   GENERIC_READ | GENERIC_WRITE,
                                   FILE_SHARE_READ,
                                   NULL,
                                   OPEN_ALWAYS,         // Posix env specifies O_CREAT, it will open existing file or create new
                                   FILE_ATTRIBUTE_NORMAL,
                                   NULL);

        if (hFile == INVALID_HANDLE_VALUE) {
            s = IOErrorFromLastWindowsError("Failed to Open/Create NewRandomRWFile" + fname);
        }
        else {

            result->reset(new WinRandomRWFile(fname, hFile, options));
        }
        return s;
    }

    virtual Status NewDirectory(const std::string& name, std::unique_ptr<Directory>* result) override {
        result->reset(new WinDirectory);
        return Status::OK();
    }

    virtual bool FileExists(const std::string& fname) override {
        // F_OK == 0
        const int F_OK_ = 0;
        return _access(fname.c_str(), F_OK_) == 0;
    }

    virtual Status GetChildren(const std::string& dir, std::vector<std::string>* result) override {

        std::vector<std::string> output;

        Status status;

        auto CloseDir = [](DIR* p) { closedir(p); };
        std::unique_ptr<DIR, decltype(CloseDir)> dirp(opendir(dir.c_str()), CloseDir);

        if (!dirp) {
            status = IOError(dir, errno);
        }
        else {

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
    };

    virtual Status CreateDirIfMissing(const std::string& name) override {

        Status result;

        if (DirExists(name)) {
            return result;
        }

        if (_mkdir(name.c_str()) != 0) {

            if (errno == EEXIST) {
                result = Status::IOError("`" + name + "' exists but is not a directory");
            }  else {
                auto code = errno;
                result = IOError("Failed to create dir: " + name, code);
            }
        }

        return result;
    };

    virtual Status DeleteDir(const std::string& name) override {

        Status result;
        if (_rmdir(name.c_str()) != 0) {
            auto code = errno;
            result = IOError("Failed to remove dir: " + name, code);
        }
        return result;
    };

    virtual Status GetFileSize(const std::string& fname, uint64_t* size) override {

        Status s;

        struct _stat64 sbuf;

        if (_stat64(fname.c_str(), &sbuf) != 0) {
            *size = 0;
            auto code = errno;
            s = IOError("Failed to stat: " + fname, code);
        }
        else {
            *size = sbuf.st_size;
        }
        return s;
    }

    virtual Status GetFileModificationTime(const std::string& fname, uint64_t* file_mtime) override {

        Status s;

        struct _stat64 st;
        if (_stat64(fname.c_str(), &st) != 0) {
            *file_mtime = 0;
            auto code = errno;
            s = IOError("Failed to stat: " + fname, code);
        } else {
            *file_mtime = st.st_mtime;
        }

        return s;
    }

    virtual Status RenameFile(const std::string& src, const std::string& target) override {

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

    virtual Status LockFile(const std::string& lockFname, FileLock** lock) override {
        assert(lock != nullptr);

        *lock = NULL;
        Status result;

        // No-sharing, this is a LOCK file
        const DWORD ExclusiveAccessON = 0;

        // Obtain exclusive access to the LOCK file
        HANDLE hFile = CreateFileA(lockFname.c_str(), (GENERIC_READ | GENERIC_WRITE),
            ExclusiveAccessON, NULL, CREATE_ALWAYS, FILE_FLAG_DELETE_ON_CLOSE, NULL);


        if (INVALID_HANDLE_VALUE == hFile) {
            result = IOErrorFromLastWindowsError("Failed to create lock file: " + lockFname);
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

    virtual void Schedule(void (*function)(void*), void* arg, Priority pri = LOW) override;

    virtual void StartThread(void (*function)(void* arg), void* arg) override;

    virtual void WaitForJoin();

    virtual unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override;

    virtual Status GetTestDirectory(std::string* result) override {

        std::string output;

        const char* env = getenv("TEST_TMPDIR");
        if (env && env[0] != '\0') {
            output = env;
            CreateDir(output);
        } else  {
            env = getenv("TMP");

            if (env && env[0] != '\0') {
                output = env;
            } else  {
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

    static uint64_t gettid() {

        uint64_t thread_id = GetCurrentThreadId();
        return thread_id;
    }

    virtual Status NewLogger(const std::string& fname, std::shared_ptr<Logger>* result) override {

        FILE * file = fopen(fname.c_str(), "w");
        if (file == nullptr) {
            auto code = errno;
            result->reset();
            return IOError("Failed to fopen: " + fname, code);
        }

        result->reset(new WinLogger(&WinEnv::gettid, this, file));
        return Status::OK();
    }

    virtual uint64_t NowMicros() override {
        using namespace std::chrono;
        return duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
    }

    virtual uint64_t NowNanos() override {
        using namespace std::chrono;
        return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
    }

    virtual void SleepForMicroseconds(int micros) override  {
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
        if (ret == (time_t) -1) {
            *unix_time = 0;
            s = IOError("GetCurrTime", errno);
        } else {
            *unix_time = (int64_t)ret;
        }

        return s;
    }

    virtual Status GetAbsolutePath(const std::string& db_path, std::string* output_path) override {

        // Check if we already have an absolute path
        // that starts with non dot and has a semicolon in it
        if ((!db_path.empty() &&
            (db_path[0] == '/' || db_path[0] == '\\')) ||
            (
                db_path.size() > 2 &&
                db_path[0] != '.' &&
                ((db_path[1] == ':' && db_path[2] == '\\') ||
                (db_path[1] == ':' && db_path[2] == '/'))
             )
           ) {

            *output_path = db_path;
            return Status::OK();
        }

        std::string result;
        result.resize(_MAX_PATH);

        char* ret = _getcwd(&result[0], _MAX_PATH);
        if (ret == nullptr) {
            return Status::IOError("Failed to get current working directory", strerror(errno));
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

            int len = snprintf(p, maxsize, "%04d/%02d/%02d-%02d:%02d:%02d ", t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec);
            assert(len > 0);

            result.resize(len);
        }

        return result;
    }

    EnvOptions OptimizeForLogWrite(const EnvOptions& env_options) const  {
        EnvOptions optimized = env_options;
        optimized.use_mmap_writes = false;
        optimized.use_os_buffer = true; // This is because we flush only whole pages on unbuffered io and the last records are not guaranteed to be flushed.
        // TODO(icanadi) it's faster if fallocate_with_keep_size is false, but it
        // breaks TransactionLogIteratorStallAtLastRecord unit test. Fix the unit
        // test and make this false
        optimized.fallocate_with_keep_size = true;
        return optimized;
    }

    EnvOptions OptimizeForManifestWrite(const EnvOptions& env_options) const {
        EnvOptions optimized = env_options;
        optimized.use_mmap_writes = false;
        optimized.use_os_buffer = true;
        optimized.fallocate_with_keep_size = true;
        return optimized;
    }

 private:

    // Returns true iff the named directory exists and is a directory.
    virtual bool DirExists(const std::string& dname) {

        struct _stat64 statbuf;

        if (_stat64(dname.c_str(), &statbuf) == 0) {
            return 0 != (_S_IFDIR & statbuf.st_mode);
        }

        return false; // stat() failed return false
    }

    bool SupportsFastAllocate(const std::string& /* path */) {
        return false;
    }


    class ThreadPool 
    {
    public:
        ThreadPool() : total_threads_limit_(1), bgthreads_(0), queue_(), exit_all_threads_(false)
        {
            std::atomic_init(&queue_len_, 0);
        }
    
        ~ThreadPool() 
        {
            mu_.lock();
            assert(!exit_all_threads_);
            exit_all_threads_ = true;
            bgsignal_.notify_all();
            mu_.unlock();
            for (int i = 0; i < bgthreads_.size(); ++i)
            {
                bgthreads_[i]->join();
            }
            for (int i = 0; i < bgthreads_.size(); ++i)
            {
                delete bgthreads_[i];
            }
            bgthreads_.clear();
        }

        // Return true if there is at least one thread needs to terminate.
        bool HasExcessiveThread() const {
            return static_cast<int>(bgthreads_.size()) > total_threads_limit_;
        }

        // Return true iff the current thread is the excessive thread to terminate.
        // Always terminate the running thread that is added last, even if there are
        // more than one thread to terminate.
        bool IsLastExcessiveThread(size_t thread_id) const  {
            return HasExcessiveThread() && thread_id == bgthreads_.size() - 1;
        }

        // Is one of the threads to terminate.
        bool IsExcessiveThread(size_t thread_id) const
        {
            return static_cast<int>(thread_id) >= total_threads_limit_;
        }

        void BGThread(size_t thread_id) {

            while (true) 
            {
                // Wait until there is an item that is ready to run
                std::unique_lock<std::mutex> uniqueLock(mu_);
                
                // Stop waiting if the thread needs to do work or needs to terminate.
                while (!exit_all_threads_ && !IsLastExcessiveThread(thread_id) && (queue_.empty() || IsExcessiveThread(thread_id))) 
                {
                    bgsignal_.wait(uniqueLock);
                }

                if (exit_all_threads_) 
                { 
                    // mechanism to let BG threads exit safely
                    uniqueLock.unlock();
                    break;
                }

                else if (IsLastExcessiveThread(thread_id)) 
                {
                    // Current thread is the last generated one and is excessive.
                    // We always terminate excessive thread in the reverse order of
                    // generation time.
                    std::thread* terminating_thread = bgthreads_.back();
                    terminating_thread->detach();
                    bgthreads_.pop_back();
                    if (HasExcessiveThread()) 
                    {
                        // There is still at least more excessive thread to terminate.
                        WakeUpAllThreads();
                    }
                    uniqueLock.unlock();
                
                    // TODO(sdong): temp logging. Need to help debugging. Remove it when
                    // the feature is proved to be stable.
                    PrintThreadInfo(thread_id, *terminating_thread);
                    break;
                }
                else
                {
                    void(*function)(void*) = queue_.front().function;
                    void* arg = queue_.front().arg;
                    queue_.pop_front();
                    queue_len_.store(queue_.size(), std::memory_order_relaxed);

                    uniqueLock.unlock();
                    (*function)(arg);
                }
            }
        }

        // Helper struct for passing arguments when creating threads.
        struct BGThreadMetadata 
        {
            ThreadPool* thread_pool_;
            size_t thread_id_;  // Thread count in the thread.
            
            explicit BGThreadMetadata(ThreadPool* thread_pool, size_t thread_id) : thread_pool_(thread_pool), thread_id_(thread_id) {}
        };

        static void* BGThreadWrapper(void* arg) {

            std::unique_ptr<BGThreadMetadata> meta(reinterpret_cast<BGThreadMetadata*>(arg));

            size_t thread_id = meta->thread_id_;
            ThreadPool* tp = meta->thread_pool_;

            tp->BGThread(thread_id);
            return nullptr;
        }

        void WakeUpAllThreads() {
            bgsignal_.notify_all();
        }

        void SetBackgroundThreads(int num) {
            std::lock_guard<std::mutex> lg(mu_);

            if (exit_all_threads_) {
                return;
            }

            if (num != total_threads_limit_) {
                total_threads_limit_ = num;
                WakeUpAllThreads();
                StartBGThreads();
            }

            assert(total_threads_limit_ > 0);
        }

        void StartBGThreads() {

            // Start background thread if necessary
            while ((int)bgthreads_.size() < total_threads_limit_) 
            {
                std::thread* p_t = new std::thread(&ThreadPool::BGThreadWrapper, new BGThreadMetadata(this, bgthreads_.size()));

                // Set the thread name to aid debugging
                #if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
                #if __GLIBC_PREREQ(2, 12)
                    char name_buf[16];
                    snprintf(name_buf, sizeof name_buf, "rocksdb:bg%Iu", bgthreads_.size());
                    name_buf[sizeof name_buf - 1] = '\0';
                    pthread_setname_np(t, name_buf);
                #endif
                #endif

                bgthreads_.push_back(p_t);
            }
        }

        void Schedule(void (*function)(void*), void* arg) {

            std::lock_guard<std::mutex> lg(mu_);

            if (exit_all_threads_) 
            {
                return;
            }

            StartBGThreads();

            // Add to priority queue
            queue_.push_back(BGItem());
            queue_.back().function = function;
            queue_.back().arg = arg;
            queue_len_.store(queue_.size(), std::memory_order_relaxed);

            if (!HasExcessiveThread()) 
            {
                // Wake up at least one waiting thread.
                bgsignal_.notify_one();
            } 
            else 
            {
                // Need to wake up all threads to make sure the one woken
                // up is not the one to terminate.
                WakeUpAllThreads();
            }
        }

        unsigned int GetQueueLen() const  {
            return queue_len_.load(std::memory_order_relaxed);
        }

    private:
        // Entry per Schedule() call
        struct BGItem  { 
            void* arg; void (*function)(void*); 
        };
        
        typedef std::deque<BGItem> BGQueue;

        std::mutex mu_;
        std::condition_variable bgsignal_;
        int total_threads_limit_;
        std::vector<std::thread*> bgthreads_;
        BGQueue queue_;
        std::atomic_uint queue_len_;  // Queue length. Used for stats reporting
        bool exit_all_threads_;
    };

    bool                      checkedDiskForMmap_;
    bool                      forceMmapOff; // do we override Env options?
    size_t                    page_size_;
    size_t                    allocation_granularity_;
    std::vector<ThreadPool>   thread_pools_;
    mutable std::mutex        mu_;
    std::vector<std::thread*> threads_to_join_;
};

WinEnv::WinEnv() : 
    checkedDiskForMmap_(false),
    forceMmapOff(false),
    page_size_(4 * 1012),
    allocation_granularity_(page_size_),
    thread_pools_(Priority::TOTAL) {

    SYSTEM_INFO sinfo;
    GetSystemInfo(&sinfo);

    page_size_ = sinfo.dwPageSize;
    allocation_granularity_ = sinfo.dwAllocationGranularity;
}

void WinEnv::Schedule(void (*function)(void*), void* arg, Priority pri) 
{
    assert(pri >= Priority::LOW && pri <= Priority::HIGH);
    thread_pools_[pri].Schedule(function, arg);
}

unsigned int WinEnv::GetThreadPoolQueueLen(Priority pri) const 
{
    assert(pri >= Priority::LOW && pri <= Priority::HIGH);
    return thread_pools_[pri].GetQueueLen();
}

namespace 
{
struct StartThreadState 
{
    void (*user_function)(void*);
    void* arg;
};
}

static void* StartThreadWrapper(void* arg) 
{
    std::unique_ptr<StartThreadState> state(reinterpret_cast<StartThreadState*>(arg));
    state->user_function(state->arg);
    return nullptr;
}

void WinEnv::StartThread(void (*function)(void* arg), void* arg) 
{
    StartThreadState* state = new StartThreadState;
    state->user_function = function;
    state->arg = arg;
    try
    {
        std::thread* p_t = new  std::thread(&StartThreadWrapper, state);

        std::lock_guard<std::mutex> lg(mu_);
        threads_to_join_.push_back(p_t);
    }
    catch (const std::system_error& ex)
    {
        WinthreadCall("start thread", ex.code());
    }
}

void WinEnv::WaitForJoin() 
{
    for (int i = 0; i < threads_to_join_.size(); ++i)
    {
        threads_to_join_[i]->join();
    }
    for (int i = 0; i < threads_to_join_.size(); ++i)
    {
        delete threads_to_join_[i];
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

Env* Env::Default() 
{
    static WinEnv default_env;
    return &default_env;
}

}  // namespace rocksdb
