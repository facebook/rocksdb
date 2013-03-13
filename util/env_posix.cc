// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <deque>
#include <set>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/vfs.h>
#include <time.h>
#include <unistd.h>
#if defined(OS_LINUX)
#include <linux/fs.h>
#endif
#if defined(LEVELDB_PLATFORM_ANDROID)
#include <sys/stat.h>
#endif
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/posix_logger.h"

#if !defined(TMPFS_MAGIC)
#define TMPFS_MAGIC 0x01021994
#endif
#if !defined(XFS_SUPER_MAGIC)
#define XFS_SUPER_MAGIC 0x58465342
#endif
#if !defined(EXT4_SUPER_MAGIC)
#define EXT4_SUPER_MAGIC 0xEF53
#endif

bool useOsBuffer = 1;     // cache data in OS buffers
bool useFsReadAhead = 1;  // allow filesystem to do readaheads
bool useMmapRead = 0;     // do not use mmaps for reading files
bool useMmapWrite = 1;    // use mmaps for appending to files

namespace leveldb {

namespace {

// list of pathnames that are locked
static std::set<std::string> lockedFiles;
static port::Mutex mutex_lockedFiles;

static Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number));
}

class PosixSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  FILE* file_;

 public:
  PosixSequentialFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }
  virtual ~PosixSequentialFile() { fclose(file_); }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    size_t r = fread_unlocked(scratch, 1, n, file_);
    *result = Slice(scratch, r);
    if (r < n) {
      if (feof(file_)) {
        // We leave status as ok if we hit the end of the file
      } else {
        // A partial read with an error: return a non-ok status
        s = IOError(filename_, errno);
      }
    }
    return s;
  }

  virtual Status Skip(uint64_t n) {
    if (fseek(file_, n, SEEK_CUR)) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }
};

// pread() based random-access
class PosixRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  int fd_;

 public:
  PosixRandomAccessFile(const std::string& fname, int fd)
      : filename_(fname), fd_(fd) {
    if (!useFsReadAhead) {
      // disable read-aheads
      posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM);
    }
  }
  virtual ~PosixRandomAccessFile() { close(fd_); }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));
    *result = Slice(scratch, (r < 0) ? 0 : r);
    if (r < 0) {
      // An error: return a non-ok status
      s = IOError(filename_, errno);
    }
    if (!useOsBuffer) {
      // we need to fadvise away the entire range of pages because
      // we do not want readahead pages to be cached.
      posix_fadvise(fd_, 0, 0, POSIX_FADV_DONTNEED); // free OS pages
    }
    return s;
  }

#if defined(OS_LINUX)
  virtual size_t GetUniqueId(char* id, size_t max_size) const {
    // TODO: possibly allow this function to handle tighter bounds.
    if (max_size < kMaxVarint64Length*3) {
      return 0;
    }

    struct stat buf;
    int result = fstat(fd_, &buf);
    if (result == -1) {
      return 0;
    }

    long version = 0;
    result = ioctl(fd_, FS_IOC_GETVERSION, &version);
    if (result == -1) {
      return 0;
    }
    uint64_t uversion = (uint64_t)version;

    char* rid = id;
    rid = EncodeVarint64(rid, buf.st_dev);
    rid = EncodeVarint64(rid, buf.st_ino);
    rid = EncodeVarint64(rid, uversion);
    assert(rid >= id);
    return static_cast<size_t>(rid-id);
  }
#endif
};

// mmap() based random-access
class PosixMmapReadableFile: public RandomAccessFile {
 private:
  std::string filename_;
  void* mmapped_region_;
  size_t length_;

 public:
  // base[0,length-1] contains the mmapped contents of the file.
  PosixMmapReadableFile(const std::string& fname, void* base, size_t length)
      : filename_(fname), mmapped_region_(base), length_(length) { }
  virtual ~PosixMmapReadableFile() { munmap(mmapped_region_, length_); }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    if (offset + n > length_) {
      *result = Slice();
      s = IOError(filename_, EINVAL);
    } else {
      *result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n);
    }
    return s;
  }
};

// We preallocate up to an extra megabyte and use memcpy to append new
// data to the file.  This is safe since we either properly close the
// file before reading from it, or for log files, the reading code
// knows enough to skip zero suffixes.
class PosixMmapFile : public WritableFile {
 private:
  std::string filename_;
  int fd_;
  size_t page_size_;
  size_t map_size_;       // How much extra memory to map at a time
  char* base_;            // The mapped region
  char* limit_;           // Limit of the mapped region
  char* dst_;             // Where to write next  (in range [base_,limit_])
  char* last_sync_;       // Where have we synced up to
  uint64_t file_offset_;  // Offset of base_ in file

  // Have we done an munmap of unsynced data?
  bool pending_sync_;

  // Roundup x to a multiple of y
  static size_t Roundup(size_t x, size_t y) {
    return ((x + y - 1) / y) * y;
  }

  size_t TruncateToPageBoundary(size_t s) {
    s -= (s & (page_size_ - 1));
    assert((s % page_size_) == 0);
    return s;
  }

  bool UnmapCurrentRegion() {
    bool result = true;
    if (base_ != nullptr) {
      if (last_sync_ < limit_) {
        // Defer syncing this data until next Sync() call, if any
        pending_sync_ = true;
      }
      if (munmap(base_, limit_ - base_) != 0) {
        result = false;
      }
      file_offset_ += limit_ - base_;
      base_ = nullptr;
      limit_ = nullptr;
      last_sync_ = nullptr;
      dst_ = nullptr;

      // Increase the amount we map the next time, but capped at 1MB
      if (map_size_ < (1<<20)) {
        map_size_ *= 2;
      }
    }
    return result;
  }

  Status MapNewRegion() {
    assert(base_ == nullptr);

    int alloc_status = posix_fallocate(fd_, file_offset_, map_size_);
    if (alloc_status != 0) {
      return Status::IOError("Error allocating space to file : " + filename_ +
        "Error : " + strerror(alloc_status));
    }


    void* ptr = mmap(nullptr, map_size_, PROT_READ | PROT_WRITE, MAP_SHARED,
                     fd_, file_offset_);
    if (ptr == MAP_FAILED) {
      return Status::IOError("MMap failed on " + filename_);
    }
    base_ = reinterpret_cast<char*>(ptr);
    limit_ = base_ + map_size_;
    dst_ = base_;
    last_sync_ = base_;
    return Status::OK();
  }

 public:
  PosixMmapFile(const std::string& fname, int fd, size_t page_size)
      : filename_(fname),
        fd_(fd),
        page_size_(page_size),
        map_size_(Roundup(65536, page_size)),
        base_(nullptr),
        limit_(nullptr),
        dst_(nullptr),
        last_sync_(nullptr),
        file_offset_(0),
        pending_sync_(false) {
    assert((page_size & (page_size - 1)) == 0);
  }


  ~PosixMmapFile() {
    if (fd_ >= 0) {
      PosixMmapFile::Close();
    }
  }

  virtual Status Append(const Slice& data) {
    const char* src = data.data();
    size_t left = data.size();
    PrepareWrite(GetFileSize(), left);
    while (left > 0) {
      assert(base_ <= dst_);
      assert(dst_ <= limit_);
      size_t avail = limit_ - dst_;
      if (avail == 0) {
        if (UnmapCurrentRegion()) {
          Status s = MapNewRegion();
          if (!s.ok()) {
            return s;
          }
        }
      }

      size_t n = (left <= avail) ? left : avail;
      memcpy(dst_, src, n);
      dst_ += n;
      src += n;
      left -= n;
    }
    return Status::OK();
  }

  virtual Status Close() {
    Status s;
    size_t unused = limit_ - dst_;
    if (!UnmapCurrentRegion()) {
      s = IOError(filename_, errno);
    } else if (unused > 0) {
      // Trim the extra space at the end of the file
      if (ftruncate(fd_, file_offset_ - unused) < 0) {
        s = IOError(filename_, errno);
      }
    }

    if (close(fd_) < 0) {
      if (s.ok()) {
        s = IOError(filename_, errno);
      }
    }

    fd_ = -1;
    base_ = nullptr;
    limit_ = nullptr;
    return s;
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    Status s;

    if (pending_sync_) {
      // Some unmapped data was not synced
      pending_sync_ = false;
      if (fdatasync(fd_) < 0) {
        s = IOError(filename_, errno);
      }
    }

    if (dst_ > last_sync_) {
      // Find the beginnings of the pages that contain the first and last
      // bytes to be synced.
      size_t p1 = TruncateToPageBoundary(last_sync_ - base_);
      size_t p2 = TruncateToPageBoundary(dst_ - base_ - 1);
      last_sync_ = dst_;
      if (msync(base_ + p1, p2 - p1 + page_size_, MS_SYNC) < 0) {
        s = IOError(filename_, errno);
      }
    }

    return s;
  }

  /**
   * Flush data as well as metadata to stable storage.
   */
  virtual Status Fsync() {
    if (pending_sync_) {
      // Some unmapped data was not synced
      pending_sync_ = false;
      if (fsync(fd_) < 0) {
        return IOError(filename_, errno);
      }
    }
    // This invocation to Sync will not issue the call to
    // fdatasync because pending_sync_ has already been cleared.
    return Sync();
  }

  /**
   * Get the size of valid data in the file. This will not match the
   * size that is returned from the filesystem because we use mmap
   * to extend file by map_size every time.
   */
  virtual uint64_t GetFileSize() {
    size_t used = dst_ - base_;
    return file_offset_ + used;
  }

#ifdef OS_LINUX
  virtual Status Allocate(off_t offset, off_t len) {
    if (!fallocate(fd_, FALLOC_FL_KEEP_SIZE, offset, len)) {
      return Status::OK();
    } else {
      return IOError(filename_, errno);
    }
  }
#endif
};

// Use posix write to write data to a file.
class PosixWritableFile : public WritableFile {
 private:
  const std::string filename_;
  int fd_;
  size_t cursize_;      // current size of cached data in buf_
  size_t capacity_;     // max size of buf_
  char* buf_;           // a buffer to cache writes
  uint64_t filesize_;
  bool pending_sync_;
  bool pending_fsync_;

 public:
  PosixWritableFile(const std::string& fname, int fd, size_t capacity) :
    filename_(fname),
    fd_(fd),
    cursize_(0),
    capacity_(capacity),
    buf_(new char[capacity]),
    filesize_(0),
    pending_sync_(false),
    pending_fsync_(false) {
  }

  ~PosixWritableFile() {
    if (fd_ >= 0) {
      PosixWritableFile::Close();
    }
    delete buf_;
    buf_ = 0;
  }

  virtual Status Append(const Slice& data) {
    char* src = (char *)data.data();
    size_t left = data.size();
    Status s;
    pending_sync_ = true;
    pending_fsync_ = true;

    PrepareWrite(GetFileSize(), left);
    // if there is no space in the cache, then flush
    if (cursize_ + left > capacity_) {
      s = Flush();
      if (!s.ok()) {
        return s;
      }
      // Increase the buffer size, but capped at 1MB
      if (capacity_ < (1<<20)) {
        delete buf_;
        capacity_ *= 2;
        buf_ = new char[capacity_];
      }
      assert(cursize_ == 0);
    }

    // if the write fits into the cache, then write to cache
    // otherwise do a write() syscall to write to OS buffers.
    if (cursize_ + left <= capacity_) {
      memcpy(buf_+cursize_, src, left);
      cursize_ += left;
    } else {
      while (left != 0) {
        ssize_t done = write(fd_, src, left);
        if (done < 0) {
          return IOError(filename_, errno);
        }
        left -= done;
        src += done;
      }
    }
    filesize_ += data.size();
    return Status::OK();
  }

  virtual Status Close() {
    Status s;
    s = Flush(); // flush cache to OS
    if (!s.ok()) {
    }
    if (close(fd_) < 0) {
      if (s.ok()) {
        s = IOError(filename_, errno);
      }
    }
    fd_ = -1;
    return s;
  }

  // write out the cached data to the OS cache
  virtual Status Flush() {
    size_t left = cursize_;
    char* src = buf_;
    while (left != 0) {
      ssize_t done = write(fd_, src, left);
      if (done < 0) {
        return IOError(filename_, errno);
      }
      left -= done;
      src += done;
    }
    cursize_ = 0;
    return Status::OK();
  }

  virtual Status Sync() {
    if (pending_sync_ && fdatasync(fd_) < 0) {
      return IOError(filename_, errno);
    }
    pending_sync_ = false;
    return Status::OK();
  }

  virtual Status Fsync() {
    if (pending_fsync_ && fsync(fd_) < 0) {
      return IOError(filename_, errno);
    }
    pending_fsync_ = false;
    pending_sync_ = false;
    return Status::OK();
  }

  virtual uint64_t GetFileSize() {
    return filesize_;
  }

#ifdef OS_LINUX
  virtual Status Allocate(off_t offset, off_t len) {
    if (!fallocate(fd_, FALLOC_FL_KEEP_SIZE, offset, len)) {
      return Status::OK();
    } else {
      return IOError(filename_, errno);
    }
  }
#endif
};

static int LockOrUnlock(const std::string& fname, int fd, bool lock) {
  mutex_lockedFiles.Lock();
  if (lock) {
    // If it already exists in the lockedFiles set, then it is already locked,
    // and fail this lock attempt. Otherwise, insert it into lockedFiles.
    // This check is needed because fcntl() does not detect lock conflict
    // if the fcntl is issued by the same thread that earlier acquired
    // this lock.
    if (lockedFiles.insert(fname).second == false) {
      mutex_lockedFiles.Unlock();
      errno = ENOLCK;
      return -1;
    }
  } else {
    // If we are unlocking, then verify that we had locked it earlier,
    // it should already exist in lockedFiles. Remove it from lockedFiles.
    if (lockedFiles.erase(fname) != 1) {
      mutex_lockedFiles.Unlock();
      errno = ENOLCK;
      return -1;
    }
  }
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;        // Lock/unlock entire file
  int value = fcntl(fd, F_SETLK, &f);
  if (value == -1 && lock) {
    // if there is an error in locking, then remove the pathname from lockedfiles
    lockedFiles.erase(fname);
  }
  mutex_lockedFiles.Unlock();
  return value;
}

class PosixFileLock : public FileLock {
 public:
  int fd_;
  std::string filename;
};

class PosixEnv : public Env {
 public:
  PosixEnv();
  virtual ~PosixEnv() {
    fprintf(stderr, "Destroying Env::Default()\n");
    exit(1);
  }

  virtual Status NewSequentialFile(const std::string& fname,
                                   unique_ptr<SequentialFile>* result) {
    result->reset();
    FILE* f = fopen(fname.c_str(), "r");
    if (f == nullptr) {
      *result = nullptr;
      return IOError(fname, errno);
    } else {
      result->reset(new PosixSequentialFile(fname, f));
      return Status::OK();
    }
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     unique_ptr<RandomAccessFile>* result) {
    result->reset();
    Status s;
    int fd = open(fname.c_str(), O_RDONLY);
    if (fd < 0) {
      s = IOError(fname, errno);
    } else if (useMmapRead && sizeof(void*) >= 8) {
      // Use of mmap for random reads has been removed because it
      // kills performance when storage is fast.
      // Use mmap when virtual address-space is plentiful.
      uint64_t size;
      s = GetFileSize(fname, &size);
      if (s.ok()) {
        void* base = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (base != MAP_FAILED) {
          result->reset(new PosixMmapReadableFile(fname, base, size));
        } else {
          s = IOError(fname, errno);
        }
      }
      close(fd);
    } else {
      result->reset(new PosixRandomAccessFile(fname, fd));
    }
    return s;
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 unique_ptr<WritableFile>* result) {
    result->reset();
    Status s;
    const int fd = open(fname.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
      s = IOError(fname, errno);
    } else {
      if (!checkedDiskForMmap_) {
        // this will be executed once in the program's lifetime.
        if (useMmapWrite) {
          // do not use mmapWrite on non ext-3/xfs/tmpfs systems.
          useMmapWrite = SupportsFastAllocate(fname);
        }
        checkedDiskForMmap_ = true;
      }

      if (useMmapWrite) {
        result->reset(new PosixMmapFile(fname, fd, page_size_));
      } else {
        result->reset(new PosixWritableFile(fname, fd, 65536));
      }
    }
    return s;
  }

  virtual bool FileExists(const std::string& fname) {
    return access(fname.c_str(), F_OK) == 0;
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    result->clear();
    DIR* d = opendir(dir.c_str());
    if (d == nullptr) {
      return IOError(dir, errno);
    }
    struct dirent* entry;
    while ((entry = readdir(d)) != nullptr) {
      result->push_back(entry->d_name);
    }
    closedir(d);
    return Status::OK();
  }

  virtual Status DeleteFile(const std::string& fname) {
    Status result;
    if (unlink(fname.c_str()) != 0) {
      result = IOError(fname, errno);
    }
    return result;
  };

  virtual Status CreateDir(const std::string& name) {
    Status result;
    if (mkdir(name.c_str(), 0755) != 0) {
      result = IOError(name, errno);
    }
    return result;
  };

  virtual Status CreateDirIfMissing(const std::string& name) {
    Status result;
    if (mkdir(name.c_str(), 0755) != 0) {
      if (errno != EEXIST) {
        result = IOError(name, errno);
      } else if (!DirExists(name)) { // Check that name is actually a
                                     // directory.
        // Message is taken from mkdir
        result = Status::IOError("`"+name+"' exists but is not a directory");
      }
    }
    return result;
  };

  virtual Status DeleteDir(const std::string& name) {
    Status result;
    if (rmdir(name.c_str()) != 0) {
      result = IOError(name, errno);
    }
    return result;
  };

  virtual Status GetFileSize(const std::string& fname, uint64_t* size) {
    Status s;
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
      *size = 0;
      s = IOError(fname, errno);
    } else {
      *size = sbuf.st_size;
    }
    return s;
  }

  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* file_mtime) {
    struct stat s;
    if (stat(fname.c_str(), &s) !=0) {
      return IOError(fname, errno);
    }
    *file_mtime = static_cast<uint64_t>(s.st_mtime);
    return Status::OK();
  }
  virtual Status RenameFile(const std::string& src, const std::string& target) {
    Status result;
    if (rename(src.c_str(), target.c_str()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    *lock = nullptr;
    Status result;
    int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
      result = IOError(fname, errno);
    } else if (LockOrUnlock(fname, fd, true) == -1) {
      result = IOError("lock " + fname, errno);
      close(fd);
    } else {
      PosixFileLock* my_lock = new PosixFileLock;
      my_lock->fd_ = fd;
      my_lock->filename = fname;
      *lock = my_lock;
    }
    return result;
  }

  virtual Status UnlockFile(FileLock* lock) {
    PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
    Status result;
    if (LockOrUnlock(my_lock->filename, my_lock->fd_, false) == -1) {
      result = IOError("unlock", errno);
    }
    close(my_lock->fd_);
    delete my_lock;
    return result;
  }

  virtual void Schedule(void (*function)(void*), void* arg);

  virtual void StartThread(void (*function)(void* arg), void* arg);

  virtual Status GetTestDirectory(std::string* result) {
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d", int(geteuid()));
      *result = buf;
    }
    // Directory may already exist
    CreateDir(*result);
    return Status::OK();
  }

  static uint64_t gettid() {
    pthread_t tid = pthread_self();
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
  }

  virtual Status NewLogger(const std::string& fname,
                           shared_ptr<Logger>* result) {
    FILE* f = fopen(fname.c_str(), "w");
    if (f == nullptr) {
      result->reset();
      return IOError(fname, errno);
    } else {
      result->reset(new PosixLogger(f, &PosixEnv::gettid));
      return Status::OK();
    }
  }

  virtual uint64_t NowMicros() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  virtual void SleepForMicroseconds(int micros) {
    usleep(micros);
  }

  virtual Status GetHostName(char* name, uint64_t len) {
    int ret = gethostname(name, len);
    if (ret < 0) {
      if (errno == EFAULT || errno == EINVAL)
        return Status::InvalidArgument(strerror(errno));
      else
        return IOError("GetHostName", errno);
    }
    return Status::OK();
  }

  virtual Status GetCurrentTime(int64_t* unix_time) {
    time_t ret = time(nullptr);
    if (ret == (time_t) -1) {
      return IOError("GetCurrentTime", errno);
    }
    *unix_time = (int64_t) ret;
    return Status::OK();
  }

  virtual Status GetAbsolutePath(const std::string& db_path,
      std::string* output_path) {
    if (db_path.find('/') == 0) {
      *output_path = db_path;
      return Status::OK();
    }

    char the_path[256];
    char* ret = getcwd(the_path, 256);
    if (ret == nullptr) {
      return Status::IOError(strerror(errno));
    }

    *output_path = ret;
    return Status::OK();
  }

  // Allow increasing the number of worker threads.
  virtual void SetBackgroundThreads(int num) {
    if (num > num_threads_) {
      num_threads_ = num;
      bgthread_.resize(num_threads_);
    }
  }

  virtual std::string TimeToString(uint64_t secondsSince1970) {
    const time_t seconds = (time_t)secondsSince1970;
    struct tm t;
    int maxsize = 64;
    std::string dummy;
    dummy.reserve(maxsize);
    dummy.resize(maxsize);
    char* p = &dummy[0];
    localtime_r(&seconds, &t);
    snprintf(p, maxsize,
             "%04d/%02d/%02d-%02d:%02d:%02d ",
             t.tm_year + 1900,
             t.tm_mon + 1,
             t.tm_mday,
             t.tm_hour,
             t.tm_min,
             t.tm_sec);
    return dummy;
  }

 private:
  bool checkedDiskForMmap_ = false;

  void PthreadCall(const char* label, int result) {
    if (result != 0) {
      fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
      exit(1);
    }
  }

  // Returns true iff the named directory exists and is a directory.
  virtual bool DirExists(const std::string& dname) {
    struct stat statbuf;
    if (stat(dname.c_str(), &statbuf) == 0) {
      return S_ISDIR(statbuf.st_mode);
    }
    return false; // stat() failed return false
  }


  // BGThread() is the body of the background thread
  void BGThread();
  static void* BGThreadWrapper(void* arg) {
    reinterpret_cast<PosixEnv*>(arg)->BGThread();
    return nullptr;
  }

  bool SupportsFastAllocate(const std::string& path) {
    struct statfs s;
    if (statfs(path.c_str(), &s)){
      return false;
    }
    switch (s.f_type) {
      case EXT4_SUPER_MAGIC:
        return true;
      case XFS_SUPER_MAGIC:
        return true;
      case TMPFS_MAGIC:
        return true;
      default:
        return false;
    }
  }

  size_t page_size_;
  pthread_mutex_t mu_;
  pthread_cond_t bgsignal_;
  std::vector<pthread_t> bgthread_;
  int started_bgthread_;
  int num_threads_;

  // Entry per Schedule() call
  struct BGItem { void* arg; void (*function)(void*); };
  typedef std::deque<BGItem> BGQueue;
  int queue_size_; // number of items in BGQueue
  BGQueue queue_;
};

PosixEnv::PosixEnv() : page_size_(getpagesize()),
                       started_bgthread_(0),
                       num_threads_(1),
                       queue_size_(0) {
  PthreadCall("mutex_init", pthread_mutex_init(&mu_, nullptr));
  PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, nullptr));
  bgthread_.resize(num_threads_);
}

void PosixEnv::Schedule(void (*function)(void*), void* arg) {
  PthreadCall("lock", pthread_mutex_lock(&mu_));

  // Start background thread if necessary
  for (; started_bgthread_ < num_threads_; started_bgthread_++) {
    PthreadCall(
        "create thread",
        pthread_create(&bgthread_[started_bgthread_],
                       nullptr,
                       &PosixEnv::BGThreadWrapper,
                       this));
    fprintf(stdout, "Created bg thread 0x%lx\n", bgthread_[started_bgthread_]);
  }

  // always wake up at least one waiting thread.
  PthreadCall("signal", pthread_cond_signal(&bgsignal_));

  // Add to priority queue
  queue_.push_back(BGItem());
  queue_.back().function = function;
  queue_.back().arg = arg;
  queue_size_++;

  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

void PosixEnv::BGThread() {
  while (true) {
    // Wait until there is an item that is ready to run
    PthreadCall("lock", pthread_mutex_lock(&mu_));
    while (queue_.empty()) {
      PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_));
    }

    void (*function)(void*) = queue_.front().function;
    void* arg = queue_.front().arg;
    queue_.pop_front();
    queue_size_--;

    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
    (*function)(arg);
  }
}

namespace {
struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};
}
static void* StartThreadWrapper(void* arg) {
  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
  state->user_function(state->arg);
  delete state;
  return nullptr;
}

void PosixEnv::StartThread(void (*function)(void* arg), void* arg) {
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  PthreadCall("start thread",
              pthread_create(&t, nullptr,  &StartThreadWrapper, state));
}

}  // namespace

static pthread_once_t once = PTHREAD_ONCE_INIT;
static Env* default_env;
static void InitDefaultEnv() { default_env = new PosixEnv; }

Env* Env::Default() {
  pthread_once(&once, InitDefaultEnv);
  return default_env;
}

}  // namespace leveldb
