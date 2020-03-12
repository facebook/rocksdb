// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An Env is an interface used by the rocksdb implementation to access
// operating system functionality like the filesystem etc.  Callers
// may wish to provide a custom Env object when opening a database to
// get fine gain control; e.g., to rate limit file system operations.
//
// All Env implementations are safe for concurrent access from
// multiple threads without any external synchronization.

#pragma once

#include <stdint.h>
#include <cstdarg>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <vector>
#include "rocksdb/status.h"
#include "rocksdb/thread_status.h"

#ifdef _WIN32
// Windows API macro interference
#undef DeleteFile
#undef GetCurrentTime
#endif

#if defined(__GNUC__) || defined(__clang__)
#define ROCKSDB_PRINTF_FORMAT_ATTR(format_param, dots_param) \
    __attribute__((__format__(__printf__, format_param, dots_param)))
#else
#define ROCKSDB_PRINTF_FORMAT_ATTR(format_param, dots_param)
#endif

namespace ROCKSDB_NAMESPACE {

class DynamicLibrary;
class FileLock;
class Logger;
class RandomAccessFile;
class SequentialFile;
class Slice;
class WritableFile;
class RandomRWFile;
class MemoryMappedFileBuffer;
class Directory;
struct DBOptions;
struct ImmutableDBOptions;
struct MutableDBOptions;
class RateLimiter;
class ThreadStatusUpdater;
struct ThreadStatus;

const size_t kDefaultPageSize = 4 * 1024;

// Options while opening a file to read/write
struct EnvOptions {
  // Construct with default Options
  EnvOptions();

  // Construct from Options
  explicit EnvOptions(const DBOptions& options);

  // If true, then use mmap to read data
  bool use_mmap_reads = false;

  // If true, then use mmap to write data
  bool use_mmap_writes = true;

  // If true, then use O_DIRECT for reading data
  bool use_direct_reads = false;

  // If true, then use O_DIRECT for writing data
  bool use_direct_writes = false;

  // If false, fallocate() calls are bypassed
  bool allow_fallocate = true;

  // If true, set the FD_CLOEXEC on open fd.
  bool set_fd_cloexec = true;

  // Allows OS to incrementally sync files to disk while they are being
  // written, in the background. Issue one request for every bytes_per_sync
  // written. 0 turns it off.
  // Default: 0
  uint64_t bytes_per_sync = 0;

  // When true, guarantees the file has at most `bytes_per_sync` bytes submitted
  // for writeback at any given time.
  //
  //  - If `sync_file_range` is supported it achieves this by waiting for any
  //    prior `sync_file_range`s to finish before proceeding. In this way,
  //    processing (compression, etc.) can proceed uninhibited in the gap
  //    between `sync_file_range`s, and we block only when I/O falls behind.
  //  - Otherwise the `WritableFile::Sync` method is used. Note this mechanism
  //    always blocks, thus preventing the interleaving of I/O and processing.
  //
  // Note: Enabling this option does not provide any additional persistence
  // guarantees, as it may use `sync_file_range`, which does not write out
  // metadata.
  //
  // Default: false
  bool strict_bytes_per_sync = false;

  // If true, we will preallocate the file with FALLOC_FL_KEEP_SIZE flag, which
  // means that file size won't change as part of preallocation.
  // If false, preallocation will also change the file size. This option will
  // improve the performance in workloads where you sync the data on every
  // write. By default, we set it to true for MANIFEST writes and false for
  // WAL writes
  bool fallocate_with_keep_size = true;

  // See DBOptions doc
  size_t compaction_readahead_size = 0;

  // See DBOptions doc
  size_t random_access_max_buffer_size = 0;

  // See DBOptions doc
  size_t writable_file_max_buffer_size = 1024 * 1024;

  // If not nullptr, write rate limiting is enabled for flush and compaction
  RateLimiter* rate_limiter = nullptr;
};

class Env {
 public:
  struct FileAttributes {
    // File name
    std::string name;

    // Size of file in bytes
    uint64_t size_bytes;
  };

  Env() : thread_status_updater_(nullptr) {}
  // No copying allowed
  Env(const Env&) = delete;
  void operator=(const Env&) = delete;

  virtual ~Env();

  static const char* Type() { return "Environment"; }

  // Loads the environment specified by the input value into the result
  static Status LoadEnv(const std::string& value, Env** result);

  // Loads the environment specified by the input value into the result
  static Status LoadEnv(const std::string& value, Env** result,
                        std::shared_ptr<Env>* guard);

  // Return a default environment suitable for the current operating
  // system.  Sophisticated users may wish to provide their own Env
  // implementation instead of relying on this default environment.
  //
  // The result of Default() belongs to rocksdb and must never be deleted.
  static Env* Default();

  // See FileSystem::RegisterDbPaths.
  virtual Status RegisterDbPaths(const std::vector<std::string>& /*paths*/) {
    return Status::OK();
  }
  // See FileSystem::UnregisterDbPaths.
  virtual Status UnregisterDbPaths(const std::vector<std::string>& /*paths*/) {
    return Status::OK();
  }

  // Create a brand new sequentially-readable file with the specified name.
  // On success, stores a pointer to the new file in *result and returns OK.
  // On failure stores nullptr in *result and returns non-OK.  If the file does
  // not exist, returns a non-OK status.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual Status NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) = 0;

  // Create a brand new random access read-only file with the
  // specified name.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.  If the file does not exist, returns a non-OK
  // status.
  //
  // The returned file may be concurrently accessed by multiple threads.
  virtual Status NewRandomAccessFile(const std::string& fname,
                                     std::unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& options) = 0;
  // These values match Linux definition
  // https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/include/uapi/linux/fcntl.h#n56
  enum WriteLifeTimeHint {
    WLTH_NOT_SET = 0,  // No hint information set
    WLTH_NONE,         // No hints about write life time
    WLTH_SHORT,        // Data written has a short life time
    WLTH_MEDIUM,       // Data written has a medium life time
    WLTH_LONG,         // Data written has a long life time
    WLTH_EXTREME,      // Data written has an extremely long life time
  };

  // Create an object that writes to a new file with the specified
  // name.  Deletes any existing file with the same name and creates a
  // new file.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual Status NewWritableFile(const std::string& fname,
                                 std::unique_ptr<WritableFile>* result,
                                 const EnvOptions& options) = 0;

  // Create an object that writes to a new file with the specified
  // name.  Deletes any existing file with the same name and creates a
  // new file.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual Status ReopenWritableFile(const std::string& /*fname*/,
                                    std::unique_ptr<WritableFile>* /*result*/,
                                    const EnvOptions& /*options*/) {
    return Status::NotSupported();
  }

  // Reuse an existing file by renaming it and opening it as writable.
  virtual Status ReuseWritableFile(const std::string& fname,
                                   const std::string& old_fname,
                                   std::unique_ptr<WritableFile>* result,
                                   const EnvOptions& options);

  // Open `fname` for random read and write, if file doesn't exist the file
  // will be created.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual Status NewRandomRWFile(const std::string& /*fname*/,
                                 std::unique_ptr<RandomRWFile>* /*result*/,
                                 const EnvOptions& /*options*/) {
    return Status::NotSupported("RandomRWFile is not implemented in this Env");
  }

  // Opens `fname` as a memory-mapped file for read and write (in-place updates
  // only, i.e., no appends). On success, stores a raw buffer covering the whole
  // file in `*result`. The file must exist prior to this call.
  virtual Status NewMemoryMappedFileBuffer(
      const std::string& /*fname*/,
      std::unique_ptr<MemoryMappedFileBuffer>* /*result*/) {
    return Status::NotSupported(
        "MemoryMappedFileBuffer is not implemented in this Env");
  }

  // Create an object that represents a directory. Will fail if directory
  // doesn't exist. If the directory exists, it will open the directory
  // and create a new Directory object.
  //
  // On success, stores a pointer to the new Directory in
  // *result and returns OK. On failure stores nullptr in *result and
  // returns non-OK.
  virtual Status NewDirectory(const std::string& name,
                              std::unique_ptr<Directory>* result) = 0;

  // Returns OK if the named file exists.
  //         NotFound if the named file does not exist,
  //                  the calling process does not have permission to determine
  //                  whether this file exists, or if the path is invalid.
  //         IOError if an IO Error was encountered
  virtual Status FileExists(const std::string& fname) = 0;

  // Store in *result the names of the children of the specified directory.
  // The names are relative to "dir".
  // Original contents of *results are dropped.
  // Returns OK if "dir" exists and "*result" contains its children.
  //         NotFound if "dir" does not exist, the calling process does not have
  //                  permission to access "dir", or if "dir" is invalid.
  //         IOError if an IO Error was encountered
  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) = 0;

  // Store in *result the attributes of the children of the specified directory.
  // In case the implementation lists the directory prior to iterating the files
  // and files are concurrently deleted, the deleted files will be omitted from
  // result.
  // The name attributes are relative to "dir".
  // Original contents of *results are dropped.
  // Returns OK if "dir" exists and "*result" contains its children.
  //         NotFound if "dir" does not exist, the calling process does not have
  //                  permission to access "dir", or if "dir" is invalid.
  //         IOError if an IO Error was encountered
  virtual Status GetChildrenFileAttributes(const std::string& dir,
                                           std::vector<FileAttributes>* result);

  // Delete the named file.
  virtual Status DeleteFile(const std::string& fname) = 0;

  // Truncate the named file to the specified size.
  virtual Status Truncate(const std::string& /*fname*/, size_t /*size*/) {
    return Status::NotSupported("Truncate is not supported for this Env");
  }

  // Create the specified directory. Returns error if directory exists.
  virtual Status CreateDir(const std::string& dirname) = 0;

  // Creates directory if missing. Return Ok if it exists, or successful in
  // Creating.
  virtual Status CreateDirIfMissing(const std::string& dirname) = 0;

  // Delete the specified directory.
  virtual Status DeleteDir(const std::string& dirname) = 0;

  // Store the size of fname in *file_size.
  virtual Status GetFileSize(const std::string& fname, uint64_t* file_size) = 0;

  // Store the last modification time of fname in *file_mtime.
  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* file_mtime) = 0;
  // Rename file src to target.
  virtual Status RenameFile(const std::string& src,
                            const std::string& target) = 0;

  // Hard Link file src to target.
  virtual Status LinkFile(const std::string& /*src*/,
                          const std::string& /*target*/) {
    return Status::NotSupported("LinkFile is not supported for this Env");
  }

  virtual Status NumFileLinks(const std::string& /*fname*/,
                              uint64_t* /*count*/) {
    return Status::NotSupported(
        "Getting number of file links is not supported for this Env");
  }

  virtual Status AreFilesSame(const std::string& /*first*/,
                              const std::string& /*second*/, bool* /*res*/) {
    return Status::NotSupported("AreFilesSame is not supported for this Env");
  }

  // Lock the specified file.  Used to prevent concurrent access to
  // the same db by multiple processes.  On failure, stores nullptr in
  // *lock and returns non-OK.
  //
  // On success, stores a pointer to the object that represents the
  // acquired lock in *lock and returns OK.  The caller should call
  // UnlockFile(*lock) to release the lock.  If the process exits,
  // the lock will be automatically released.
  //
  // If somebody else already holds the lock, finishes immediately
  // with a failure.  I.e., this call does not wait for existing locks
  // to go away.
  //
  // May create the named file if it does not already exist.
  virtual Status LockFile(const std::string& fname, FileLock** lock) = 0;

  // Release the lock acquired by a previous successful call to LockFile.
  // REQUIRES: lock was returned by a successful LockFile() call
  // REQUIRES: lock has not already been unlocked.
  virtual Status UnlockFile(FileLock* lock) = 0;

  // Opens `lib_name` as a dynamic library.
  // If the 'search_path' is specified, breaks the path into its components
  // based on the appropriate platform separator (";" or ";") and looks for the
  // library in those directories.  If 'search path is not specified, uses the
  // default library path search mechanism (such as LD_LIBRARY_PATH). On
  // success, stores a dynamic library in `*result`.
  virtual Status LoadLibrary(const std::string& /*lib_name*/,
                             const std::string& /*search_path */,
                             std::shared_ptr<DynamicLibrary>* /*result*/) {
    return Status::NotSupported("LoadLibrary is not implemented in this Env");
  }

  // Priority for scheduling job in thread pool
  enum Priority { BOTTOM, LOW, HIGH, USER, TOTAL };

  static std::string PriorityToString(Priority priority);

  // Priority for requesting bytes in rate limiter scheduler
  enum IOPriority { IO_LOW = 0, IO_HIGH = 1, IO_TOTAL = 2 };

  // Arrange to run "(*function)(arg)" once in a background thread, in
  // the thread pool specified by pri. By default, jobs go to the 'LOW'
  // priority thread pool.

  // "function" may run in an unspecified thread.  Multiple functions
  // added to the same Env may run concurrently in different threads.
  // I.e., the caller may not assume that background work items are
  // serialized.
  // When the UnSchedule function is called, the unschedFunction
  // registered at the time of Schedule is invoked with arg as a parameter.
  virtual void Schedule(void (*function)(void* arg), void* arg,
                        Priority pri = LOW, void* tag = nullptr,
                        void (*unschedFunction)(void* arg) = nullptr) = 0;

  // Arrange to remove jobs for given arg from the queue_ if they are not
  // already scheduled. Caller is expected to have exclusive lock on arg.
  virtual int UnSchedule(void* /*arg*/, Priority /*pri*/) { return 0; }

  // Start a new thread, invoking "function(arg)" within the new thread.
  // When "function(arg)" returns, the thread will be destroyed.
  virtual void StartThread(void (*function)(void* arg), void* arg) = 0;

  // Wait for all threads started by StartThread to terminate.
  virtual void WaitForJoin() {}

  // Get thread pool queue length for specific thread pool.
  virtual unsigned int GetThreadPoolQueueLen(Priority /*pri*/ = LOW) const {
    return 0;
  }

  // *path is set to a temporary directory that can be used for testing. It may
  // or many not have just been created. The directory may or may not differ
  // between runs of the same process, but subsequent calls will return the
  // same directory.
  virtual Status GetTestDirectory(std::string* path) = 0;

  // Create and returns a default logger (an instance of EnvLogger) for storing
  // informational messages. Derived classes can overide to provide custom
  // logger.
  virtual Status NewLogger(const std::string& fname,
                           std::shared_ptr<Logger>* result);

  // Returns the number of micro-seconds since some fixed point in time.
  // It is often used as system time such as in GenericRateLimiter
  // and other places so a port needs to return system time in order to work.
  virtual uint64_t NowMicros() = 0;

  // Returns the number of nano-seconds since some fixed point in time. Only
  // useful for computing deltas of time in one run.
  // Default implementation simply relies on NowMicros.
  // In platform-specific implementations, NowNanos() should return time points
  // that are MONOTONIC.
  virtual uint64_t NowNanos() { return NowMicros() * 1000; }

  // 0 indicates not supported.
  virtual uint64_t NowCPUNanos() { return 0; }

  // Sleep/delay the thread for the prescribed number of micro-seconds.
  virtual void SleepForMicroseconds(int micros) = 0;

  // Get the current host name.
  virtual Status GetHostName(char* name, uint64_t len) = 0;

  // Get the number of seconds since the Epoch, 1970-01-01 00:00:00 (UTC).
  // Only overwrites *unix_time on success.
  virtual Status GetCurrentTime(int64_t* unix_time) = 0;

  // Get full directory name for this db.
  virtual Status GetAbsolutePath(const std::string& db_path,
                                 std::string* output_path) = 0;

  // The number of background worker threads of a specific thread pool
  // for this environment. 'LOW' is the default pool.
  // default number: 1
  virtual void SetBackgroundThreads(int number, Priority pri = LOW) = 0;
  virtual int GetBackgroundThreads(Priority pri = LOW) = 0;

  virtual Status SetAllowNonOwnerAccess(bool /*allow_non_owner_access*/) {
    return Status::NotSupported("Not supported.");
  }

  // Enlarge number of background worker threads of a specific thread pool
  // for this environment if it is smaller than specified. 'LOW' is the default
  // pool.
  virtual void IncBackgroundThreadsIfNeeded(int number, Priority pri) = 0;

  // Lower IO priority for threads from the specified pool.
  virtual void LowerThreadPoolIOPriority(Priority /*pool*/ = LOW) {}

  // Lower CPU priority for threads from the specified pool.
  virtual void LowerThreadPoolCPUPriority(Priority /*pool*/ = LOW) {}

  // Converts seconds-since-Jan-01-1970 to a printable string
  virtual std::string TimeToString(uint64_t time) = 0;

  // Generates a unique id that can be used to identify a db
  virtual std::string GenerateUniqueId();

  // OptimizeForLogWrite will create a new EnvOptions object that is a copy of
  // the EnvOptions in the parameters, but is optimized for reading log files.
  virtual EnvOptions OptimizeForLogRead(const EnvOptions& env_options) const;

  // OptimizeForManifestRead will create a new EnvOptions object that is a copy
  // of the EnvOptions in the parameters, but is optimized for reading manifest
  // files.
  virtual EnvOptions OptimizeForManifestRead(
      const EnvOptions& env_options) const;

  // OptimizeForLogWrite will create a new EnvOptions object that is a copy of
  // the EnvOptions in the parameters, but is optimized for writing log files.
  // Default implementation returns the copy of the same object.
  virtual EnvOptions OptimizeForLogWrite(const EnvOptions& env_options,
                                         const DBOptions& db_options) const;
  // OptimizeForManifestWrite will create a new EnvOptions object that is a copy
  // of the EnvOptions in the parameters, but is optimized for writing manifest
  // files. Default implementation returns the copy of the same object.
  virtual EnvOptions OptimizeForManifestWrite(
      const EnvOptions& env_options) const;

  // OptimizeForCompactionTableWrite will create a new EnvOptions object that is
  // a copy of the EnvOptions in the parameters, but is optimized for writing
  // table files.
  virtual EnvOptions OptimizeForCompactionTableWrite(
      const EnvOptions& env_options,
      const ImmutableDBOptions& immutable_ops) const;

  // OptimizeForCompactionTableWrite will create a new EnvOptions object that
  // is a copy of the EnvOptions in the parameters, but is optimized for reading
  // table files.
  virtual EnvOptions OptimizeForCompactionTableRead(
      const EnvOptions& env_options,
      const ImmutableDBOptions& db_options) const;

  // Returns the status of all threads that belong to the current Env.
  virtual Status GetThreadList(std::vector<ThreadStatus>* /*thread_list*/) {
    return Status::NotSupported("Not supported.");
  }

  // Returns the pointer to ThreadStatusUpdater.  This function will be
  // used in RocksDB internally to update thread status and supports
  // GetThreadList().
  virtual ThreadStatusUpdater* GetThreadStatusUpdater() const {
    return thread_status_updater_;
  }

  // Returns the ID of the current thread.
  virtual uint64_t GetThreadID() const;

// This seems to clash with a macro on Windows, so #undef it here
#undef GetFreeSpace

  // Get the amount of free disk space
  virtual Status GetFreeSpace(const std::string& /*path*/,
                              uint64_t* /*diskfree*/) {
    return Status::NotSupported();
  }

  virtual void SanitizeEnvOptions(EnvOptions* /*env_opts*/) const {}

  // If you're adding methods here, remember to add them to EnvWrapper too.

 protected:
  // The pointer to an internal structure that will update the
  // status of each thread.
  ThreadStatusUpdater* thread_status_updater_;
};

// The factory function to construct a ThreadStatusUpdater.  Any Env
// that supports GetThreadList() feature should call this function in its
// constructor to initialize thread_status_updater_.
ThreadStatusUpdater* CreateThreadStatusUpdater();

// A file abstraction for reading sequentially through a file
class SequentialFile {
 public:
  SequentialFile() {}
  virtual ~SequentialFile();

  // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
  // written by this routine.  Sets "*result" to the data that was
  // read (including if fewer than "n" bytes were successfully read).
  // May set "*result" to point at data in "scratch[0..n-1]", so
  // "scratch[0..n-1]" must be live when "*result" is used.
  // If an error was encountered, returns a non-OK status.
  //
  // REQUIRES: External synchronization
  virtual Status Read(size_t n, Slice* result, char* scratch) = 0;

  // Skip "n" bytes from the file. This is guaranteed to be no
  // slower that reading the same data, but may be faster.
  //
  // If end of file is reached, skipping will stop at the end of the
  // file, and Skip will return OK.
  //
  // REQUIRES: External synchronization
  virtual Status Skip(uint64_t n) = 0;

  // Indicates the upper layers if the current SequentialFile implementation
  // uses direct IO.
  virtual bool use_direct_io() const { return false; }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const { return kDefaultPageSize; }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  virtual Status InvalidateCache(size_t /*offset*/, size_t /*length*/) {
    return Status::NotSupported("InvalidateCache not supported.");
  }

  // Positioned Read for direct I/O
  // If Direct I/O enabled, offset, n, and scratch should be properly aligned
  virtual Status PositionedRead(uint64_t /*offset*/, size_t /*n*/,
                                Slice* /*result*/, char* /*scratch*/) {
    return Status::NotSupported();
  }

  // If you're adding methods here, remember to add them to
  // SequentialFileWrapper too.
};

// A read IO request structure for use in MultiRead
struct ReadRequest {
  // File offset in bytes
  uint64_t offset;

  // Length to read in bytes
  size_t len;

  // A buffer that MultiRead()  can optionally place data in. It can
  // ignore this and allocate its own buffer
  char* scratch;

  // Output parameter set by MultiRead() to point to the data buffer, and
  // the number of valid bytes
  Slice result;

  // Status of read
  Status status;
};

// A file abstraction for randomly reading the contents of a file.
class RandomAccessFile {
 public:
  RandomAccessFile() {}
  virtual ~RandomAccessFile();

  // Read up to "n" bytes from the file starting at "offset".
  // "scratch[0..n-1]" may be written by this routine.  Sets "*result"
  // to the data that was read (including if fewer than "n" bytes were
  // successfully read).  May set "*result" to point at data in
  // "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
  // "*result" is used.  If an error was encountered, returns a non-OK
  // status.
  //
  // Safe for concurrent use by multiple threads.
  // If Direct I/O enabled, offset, n, and scratch should be aligned properly.
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const = 0;

  // Readahead the file starting from offset by n bytes for caching.
  virtual Status Prefetch(uint64_t /*offset*/, size_t /*n*/) {
    return Status::OK();
  }

  // Read a bunch of blocks as described by reqs. The blocks can
  // optionally be read in parallel. This is a synchronous call, i.e it
  // should return after all reads have completed. The reads will be
  // non-overlapping. If the function return Status is not ok, status of
  // individual requests will be ignored and return status will be assumed
  // for all read requests. The function return status is only meant for any
  // any errors that occur before even processing specific read requests
  virtual Status MultiRead(ReadRequest* reqs, size_t num_reqs) {
    assert(reqs != nullptr);
    for (size_t i = 0; i < num_reqs; ++i) {
      ReadRequest& req = reqs[i];
      req.status = Read(req.offset, req.len, &req.result, req.scratch);
    }
    return Status::OK();
  }

  // Tries to get an unique ID for this file that will be the same each time
  // the file is opened (and will stay the same while the file is open).
  // Furthermore, it tries to make this ID at most "max_size" bytes. If such an
  // ID can be created this function returns the length of the ID and places it
  // in "id"; otherwise, this function returns 0, in which case "id"
  // may not have been modified.
  //
  // This function guarantees, for IDs from a given environment, two unique ids
  // cannot be made equal to each other by adding arbitrary bytes to one of
  // them. That is, no unique ID is the prefix of another.
  //
  // This function guarantees that the returned ID will not be interpretable as
  // a single varint.
  //
  // Note: these IDs are only valid for the duration of the process.
  virtual size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const {
    return 0;  // Default implementation to prevent issues with backwards
               // compatibility.
  }

  enum AccessPattern { NORMAL, RANDOM, SEQUENTIAL, WILLNEED, DONTNEED };

  virtual void Hint(AccessPattern /*pattern*/) {}

  // Indicates the upper layers if the current RandomAccessFile implementation
  // uses direct IO.
  virtual bool use_direct_io() const { return false; }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const { return kDefaultPageSize; }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  virtual Status InvalidateCache(size_t /*offset*/, size_t /*length*/) {
    return Status::NotSupported("InvalidateCache not supported.");
  }

  // If you're adding methods here, remember to add them to
  // RandomAccessFileWrapper too.
};

// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
class WritableFile {
 public:
  WritableFile()
      : last_preallocated_block_(0),
        preallocation_block_size_(0),
        io_priority_(Env::IO_TOTAL),
        write_hint_(Env::WLTH_NOT_SET),
        strict_bytes_per_sync_(false) {}

  explicit WritableFile(const EnvOptions& options)
      : last_preallocated_block_(0),
        preallocation_block_size_(0),
        io_priority_(Env::IO_TOTAL),
        write_hint_(Env::WLTH_NOT_SET),
        strict_bytes_per_sync_(options.strict_bytes_per_sync) {}
  // No copying allowed
  WritableFile(const WritableFile&) = delete;
  void operator=(const WritableFile&) = delete;

  virtual ~WritableFile();

  // Append data to the end of the file
  // Note: A WriteabelFile object must support either Append or
  // PositionedAppend, so the users cannot mix the two.
  virtual Status Append(const Slice& data) = 0;

  // PositionedAppend data to the specified offset. The new EOF after append
  // must be larger than the previous EOF. This is to be used when writes are
  // not backed by OS buffers and hence has to always start from the start of
  // the sector. The implementation thus needs to also rewrite the last
  // partial sector.
  // Note: PositionAppend does not guarantee moving the file offset after the
  // write. A WritableFile object must support either Append or
  // PositionedAppend, so the users cannot mix the two.
  //
  // PositionedAppend() can only happen on the page/sector boundaries. For that
  // reason, if the last write was an incomplete sector we still need to rewind
  // back to the nearest sector/page and rewrite the portion of it with whatever
  // we need to add. We need to keep where we stop writing.
  //
  // PositionedAppend() can only write whole sectors. For that reason we have to
  // pad with zeros for the last write and trim the file when closing according
  // to the position we keep in the previous step.
  //
  // PositionedAppend() requires aligned buffer to be passed in. The alignment
  // required is queried via GetRequiredBufferAlignment()
  virtual Status PositionedAppend(const Slice& /* data */,
                                  uint64_t /* offset */) {
    return Status::NotSupported();
  }

  // Truncate is necessary to trim the file to the correct size
  // before closing. It is not always possible to keep track of the file
  // size due to whole pages writes. The behavior is undefined if called
  // with other writes to follow.
  virtual Status Truncate(uint64_t /*size*/) { return Status::OK(); }
  virtual Status Close() = 0;
  virtual Status Flush() = 0;
  virtual Status Sync() = 0;  // sync data

  /*
   * Sync data and/or metadata as well.
   * By default, sync only data.
   * Override this method for environments where we need to sync
   * metadata as well.
   */
  virtual Status Fsync() { return Sync(); }

  // true if Sync() and Fsync() are safe to call concurrently with Append()
  // and Flush().
  virtual bool IsSyncThreadSafe() const { return false; }

  // Indicates the upper layers if the current WritableFile implementation
  // uses direct IO.
  virtual bool use_direct_io() const { return false; }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const { return kDefaultPageSize; }
  /*
   * Change the priority in rate limiter if rate limiting is enabled.
   * If rate limiting is not enabled, this call has no effect.
   */
  virtual void SetIOPriority(Env::IOPriority pri) { io_priority_ = pri; }

  virtual Env::IOPriority GetIOPriority() { return io_priority_; }

  virtual void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) {
    write_hint_ = hint;
  }

  virtual Env::WriteLifeTimeHint GetWriteLifeTimeHint() { return write_hint_; }
  /*
   * Get the size of valid data in the file.
   */
  virtual uint64_t GetFileSize() { return 0; }

  /*
   * Get and set the default pre-allocation block size for writes to
   * this file.  If non-zero, then Allocate will be used to extend the
   * underlying storage of a file (generally via fallocate) if the Env
   * instance supports it.
   */
  virtual void SetPreallocationBlockSize(size_t size) {
    preallocation_block_size_ = size;
  }

  virtual void GetPreallocationStatus(size_t* block_size,
                                      size_t* last_allocated_block) {
    *last_allocated_block = last_preallocated_block_;
    *block_size = preallocation_block_size_;
  }

  // For documentation, refer to RandomAccessFile::GetUniqueId()
  virtual size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const {
    return 0;  // Default implementation to prevent issues with backwards
  }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  // This call has no effect on dirty pages in the cache.
  virtual Status InvalidateCache(size_t /*offset*/, size_t /*length*/) {
    return Status::NotSupported("InvalidateCache not supported.");
  }

  // Sync a file range with disk.
  // offset is the starting byte of the file range to be synchronized.
  // nbytes specifies the length of the range to be synchronized.
  // This asks the OS to initiate flushing the cached data to disk,
  // without waiting for completion.
  // Default implementation does nothing.
  virtual Status RangeSync(uint64_t /*offset*/, uint64_t /*nbytes*/) {
    if (strict_bytes_per_sync_) {
      return Sync();
    }
    return Status::OK();
  }

  // PrepareWrite performs any necessary preparation for a write
  // before the write actually occurs.  This allows for pre-allocation
  // of space on devices where it can result in less file
  // fragmentation and/or less waste from over-zealous filesystem
  // pre-allocation.
  virtual void PrepareWrite(size_t offset, size_t len) {
    if (preallocation_block_size_ == 0) {
      return;
    }
    // If this write would cross one or more preallocation blocks,
    // determine what the last preallocation block necessary to
    // cover this write would be and Allocate to that point.
    const auto block_size = preallocation_block_size_;
    size_t new_last_preallocated_block =
        (offset + len + block_size - 1) / block_size;
    if (new_last_preallocated_block > last_preallocated_block_) {
      size_t num_spanned_blocks =
          new_last_preallocated_block - last_preallocated_block_;
      Allocate(block_size * last_preallocated_block_,
               block_size * num_spanned_blocks);
      last_preallocated_block_ = new_last_preallocated_block;
    }
  }

  // Pre-allocates space for a file.
  virtual Status Allocate(uint64_t /*offset*/, uint64_t /*len*/) {
    return Status::OK();
  }

  // If you're adding methods here, remember to add them to
  // WritableFileWrapper too.

 protected:
  size_t preallocation_block_size() { return preallocation_block_size_; }

 private:
  size_t last_preallocated_block_;
  size_t preallocation_block_size_;

 protected:
  Env::IOPriority io_priority_;
  Env::WriteLifeTimeHint write_hint_;
  const bool strict_bytes_per_sync_;
};

// A file abstraction for random reading and writing.
class RandomRWFile {
 public:
  RandomRWFile() {}
  // No copying allowed
  RandomRWFile(const RandomRWFile&) = delete;
  RandomRWFile& operator=(const RandomRWFile&) = delete;

  virtual ~RandomRWFile() {}

  // Indicates if the class makes use of direct I/O
  // If false you must pass aligned buffer to Write()
  virtual bool use_direct_io() const { return false; }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const { return kDefaultPageSize; }

  // Write bytes in `data` at  offset `offset`, Returns Status::OK() on success.
  // Pass aligned buffer when use_direct_io() returns true.
  virtual Status Write(uint64_t offset, const Slice& data) = 0;

  // Read up to `n` bytes starting from offset `offset` and store them in
  // result, provided `scratch` size should be at least `n`.
  // Returns Status::OK() on success.
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const = 0;

  virtual Status Flush() = 0;

  virtual Status Sync() = 0;

  virtual Status Fsync() { return Sync(); }

  virtual Status Close() = 0;

  // If you're adding methods here, remember to add them to
  // RandomRWFileWrapper too.
};

// MemoryMappedFileBuffer object represents a memory-mapped file's raw buffer.
// Subclasses should release the mapping upon destruction.
class MemoryMappedFileBuffer {
 public:
  MemoryMappedFileBuffer(void* _base, size_t _length)
      : base_(_base), length_(_length) {}

  virtual ~MemoryMappedFileBuffer() = 0;

  // We do not want to unmap this twice. We can make this class
  // movable if desired, however, since
  MemoryMappedFileBuffer(const MemoryMappedFileBuffer&) = delete;
  MemoryMappedFileBuffer& operator=(const MemoryMappedFileBuffer&) = delete;

  void* GetBase() const { return base_; }
  size_t GetLen() const { return length_; }

 protected:
  void* base_;
  const size_t length_;
};

// Directory object represents collection of files and implements
// filesystem operations that can be executed on directories.
class Directory {
 public:
  virtual ~Directory() {}
  // Fsync directory. Can be called concurrently from multiple threads.
  virtual Status Fsync() = 0;

  virtual size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const {
    return 0;
  }

  // If you're adding methods here, remember to add them to
  // DirectoryWrapper too.
};

enum InfoLogLevel : unsigned char {
  DEBUG_LEVEL = 0,
  INFO_LEVEL,
  WARN_LEVEL,
  ERROR_LEVEL,
  FATAL_LEVEL,
  HEADER_LEVEL,
  NUM_INFO_LOG_LEVELS,
};

// An interface for writing log messages.
class Logger {
 public:
  size_t kDoNotSupportGetLogFileSize = (std::numeric_limits<size_t>::max)();

  explicit Logger(const InfoLogLevel log_level = InfoLogLevel::INFO_LEVEL)
      : closed_(false), log_level_(log_level) {}
  // No copying allowed
  Logger(const Logger&) = delete;
  void operator=(const Logger&) = delete;

  virtual ~Logger();

  // Close the log file. Must be called before destructor. If the return
  // status is NotSupported(), it means the implementation does cleanup in
  // the destructor
  virtual Status Close();

  // Write a header to the log file with the specified format
  // It is recommended that you log all header information at the start of the
  // application. But it is not enforced.
  virtual void LogHeader(const char* format, va_list ap) {
    // Default implementation does a simple INFO level log write.
    // Please override as per the logger class requirement.
    Logv(format, ap);
  }

  // Write an entry to the log file with the specified format.
  virtual void Logv(const char* format, va_list ap) = 0;

  // Write an entry to the log file with the specified log level
  // and format.  Any log with level under the internal log level
  // of *this (see @SetInfoLogLevel and @GetInfoLogLevel) will not be
  // printed.
  virtual void Logv(const InfoLogLevel log_level, const char* format,
                    va_list ap);

  virtual size_t GetLogFileSize() const { return kDoNotSupportGetLogFileSize; }
  // Flush to the OS buffers
  virtual void Flush() {}
  virtual InfoLogLevel GetInfoLogLevel() const { return log_level_; }
  virtual void SetInfoLogLevel(const InfoLogLevel log_level) {
    log_level_ = log_level;
  }

  // If you're adding methods here, remember to add them to LoggerWrapper too.

 protected:
  virtual Status CloseImpl();
  bool closed_;

 private:
  InfoLogLevel log_level_;
};

// Identifies a locked file.
class FileLock {
 public:
  FileLock() {}
  virtual ~FileLock();

 private:
  // No copying allowed
  FileLock(const FileLock&) = delete;
  void operator=(const FileLock&) = delete;
};

class DynamicLibrary {
 public:
  virtual ~DynamicLibrary() {}

  // Returns the name of the dynamic library.
  virtual const char* Name() const = 0;

  // Loads the symbol for sym_name from the library and updates the input
  // function. Returns the loaded symbol.
  template <typename T>
  Status LoadFunction(const std::string& sym_name, std::function<T>* function) {
    assert(nullptr != function);
    void* ptr = nullptr;
    Status s = LoadSymbol(sym_name, &ptr);
    *function = reinterpret_cast<T*>(ptr);
    return s;
  }
  // Loads and returns the symbol for sym_name from the library.
  virtual Status LoadSymbol(const std::string& sym_name, void** func) = 0;
};

extern void LogFlush(const std::shared_ptr<Logger>& info_log);

extern void Log(const InfoLogLevel log_level,
                const std::shared_ptr<Logger>& info_log, const char* format,
                ...) ROCKSDB_PRINTF_FORMAT_ATTR(3, 4);

// a set of log functions with different log levels.
extern void Header(const std::shared_ptr<Logger>& info_log, const char* format,
                   ...) ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
extern void Debug(const std::shared_ptr<Logger>& info_log, const char* format,
                  ...) ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
extern void Info(const std::shared_ptr<Logger>& info_log, const char* format,
                 ...) ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
extern void Warn(const std::shared_ptr<Logger>& info_log, const char* format,
                 ...) ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
extern void Error(const std::shared_ptr<Logger>& info_log, const char* format,
                  ...) ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
extern void Fatal(const std::shared_ptr<Logger>& info_log, const char* format,
                  ...) ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);

// Log the specified data to *info_log if info_log is non-nullptr.
// The default info log level is InfoLogLevel::INFO_LEVEL.
extern void Log(const std::shared_ptr<Logger>& info_log, const char* format,
                ...) ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);

extern void LogFlush(Logger* info_log);

extern void Log(const InfoLogLevel log_level, Logger* info_log,
                const char* format, ...) ROCKSDB_PRINTF_FORMAT_ATTR(3, 4);

// The default info log level is InfoLogLevel::INFO_LEVEL.
extern void Log(Logger* info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);

// a set of log functions with different log levels.
extern void Header(Logger* info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
extern void Debug(Logger* info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
extern void Info(Logger* info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
extern void Warn(Logger* info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
extern void Error(Logger* info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
extern void Fatal(Logger* info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);

// A utility routine: write "data" to the named file.
extern Status WriteStringToFile(Env* env, const Slice& data,
                                const std::string& fname,
                                bool should_sync = false);

// A utility routine: read contents of named file into *data
extern Status ReadFileToString(Env* env, const std::string& fname,
                               std::string* data);

// Below are helpers for wrapping most of the classes in this file.
// They forward all calls to another instance of the class.
// Useful when wrapping the default implementations.
// Typical usage is to inherit your wrapper from *Wrapper, e.g.:
//
// class MySequentialFileWrapper : public
// ROCKSDB_NAMESPACE::SequentialFileWrapper {
//  public:
//   MySequentialFileWrapper(ROCKSDB_NAMESPACE::SequentialFile* target):
//     ROCKSDB_NAMESPACE::SequentialFileWrapper(target) {}
//   Status Read(size_t n, Slice* result, char* scratch) override {
//     cout << "Doing a read of size " << n << "!" << endl;
//     return ROCKSDB_NAMESPACE::SequentialFileWrapper::Read(n, result,
//     scratch);
//   }
//   // All other methods are forwarded to target_ automatically.
// };
//
// This is often more convenient than inheriting the class directly because
// (a) Don't have to override and forward all methods - the Wrapper will
//     forward everything you're not explicitly overriding.
// (b) Don't need to update the wrapper when more methods are added to the
//     rocksdb class. Unless you actually want to override the behavior.
//     (And unless rocksdb people forgot to update the *Wrapper class.)

// An implementation of Env that forwards all calls to another Env.
// May be useful to clients who wish to override just part of the
// functionality of another Env.
class EnvWrapper : public Env {
 public:
  // Initialize an EnvWrapper that delegates all calls to *t
  explicit EnvWrapper(Env* t) : target_(t) {}
  ~EnvWrapper() override;

  // Return the target to which this Env forwards all calls
  Env* target() const { return target_; }

  // The following text is boilerplate that forwards all methods to target()
  Status RegisterDbPaths(const std::vector<std::string>& paths) override {
    return target_->RegisterDbPaths(paths);
  }

  Status UnregisterDbPaths(const std::vector<std::string>& paths) override {
    return target_->UnregisterDbPaths(paths);
  }

  Status NewSequentialFile(const std::string& f,
                           std::unique_ptr<SequentialFile>* r,
                           const EnvOptions& options) override {
    return target_->NewSequentialFile(f, r, options);
  }
  Status NewRandomAccessFile(const std::string& f,
                             std::unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& options) override {
    return target_->NewRandomAccessFile(f, r, options);
  }
  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& options) override {
    return target_->NewWritableFile(f, r, options);
  }
  Status ReopenWritableFile(const std::string& fname,
                            std::unique_ptr<WritableFile>* result,
                            const EnvOptions& options) override {
    return target_->ReopenWritableFile(fname, result, options);
  }
  Status ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           std::unique_ptr<WritableFile>* r,
                           const EnvOptions& options) override {
    return target_->ReuseWritableFile(fname, old_fname, r, options);
  }
  Status NewRandomRWFile(const std::string& fname,
                         std::unique_ptr<RandomRWFile>* result,
                         const EnvOptions& options) override {
    return target_->NewRandomRWFile(fname, result, options);
  }
  Status NewMemoryMappedFileBuffer(
      const std::string& fname,
      std::unique_ptr<MemoryMappedFileBuffer>* result) override {
    return target_->NewMemoryMappedFileBuffer(fname, result);
  }
  Status NewDirectory(const std::string& name,
                      std::unique_ptr<Directory>* result) override {
    return target_->NewDirectory(name, result);
  }
  Status FileExists(const std::string& f) override {
    return target_->FileExists(f);
  }
  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* r) override {
    return target_->GetChildren(dir, r);
  }
  Status GetChildrenFileAttributes(
      const std::string& dir, std::vector<FileAttributes>* result) override {
    return target_->GetChildrenFileAttributes(dir, result);
  }
  Status DeleteFile(const std::string& f) override {
    return target_->DeleteFile(f);
  }
  Status Truncate(const std::string& fname, size_t size) override {
    return target_->Truncate(fname, size);
  }
  Status CreateDir(const std::string& d) override {
    return target_->CreateDir(d);
  }
  Status CreateDirIfMissing(const std::string& d) override {
    return target_->CreateDirIfMissing(d);
  }
  Status DeleteDir(const std::string& d) override {
    return target_->DeleteDir(d);
  }
  Status GetFileSize(const std::string& f, uint64_t* s) override {
    return target_->GetFileSize(f, s);
  }

  Status GetFileModificationTime(const std::string& fname,
                                 uint64_t* file_mtime) override {
    return target_->GetFileModificationTime(fname, file_mtime);
  }

  Status RenameFile(const std::string& s, const std::string& t) override {
    return target_->RenameFile(s, t);
  }

  Status LinkFile(const std::string& s, const std::string& t) override {
    return target_->LinkFile(s, t);
  }

  Status NumFileLinks(const std::string& fname, uint64_t* count) override {
    return target_->NumFileLinks(fname, count);
  }

  Status AreFilesSame(const std::string& first, const std::string& second,
                      bool* res) override {
    return target_->AreFilesSame(first, second, res);
  }

  Status LockFile(const std::string& f, FileLock** l) override {
    return target_->LockFile(f, l);
  }

  Status UnlockFile(FileLock* l) override { return target_->UnlockFile(l); }

  Status LoadLibrary(const std::string& lib_name,
                     const std::string& search_path,
                     std::shared_ptr<DynamicLibrary>* result) override {
    return target_->LoadLibrary(lib_name, search_path, result);
  }

  void Schedule(void (*f)(void* arg), void* a, Priority pri,
                void* tag = nullptr, void (*u)(void* arg) = nullptr) override {
    return target_->Schedule(f, a, pri, tag, u);
  }

  int UnSchedule(void* tag, Priority pri) override {
    return target_->UnSchedule(tag, pri);
  }

  void StartThread(void (*f)(void*), void* a) override {
    return target_->StartThread(f, a);
  }
  void WaitForJoin() override { return target_->WaitForJoin(); }
  unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override {
    return target_->GetThreadPoolQueueLen(pri);
  }
  Status GetTestDirectory(std::string* path) override {
    return target_->GetTestDirectory(path);
  }
  Status NewLogger(const std::string& fname,
                   std::shared_ptr<Logger>* result) override {
    return target_->NewLogger(fname, result);
  }
  uint64_t NowMicros() override { return target_->NowMicros(); }
  uint64_t NowNanos() override { return target_->NowNanos(); }
  uint64_t NowCPUNanos() override { return target_->NowCPUNanos(); }

  void SleepForMicroseconds(int micros) override {
    target_->SleepForMicroseconds(micros);
  }
  Status GetHostName(char* name, uint64_t len) override {
    return target_->GetHostName(name, len);
  }
  Status GetCurrentTime(int64_t* unix_time) override {
    return target_->GetCurrentTime(unix_time);
  }
  Status GetAbsolutePath(const std::string& db_path,
                         std::string* output_path) override {
    return target_->GetAbsolutePath(db_path, output_path);
  }
  void SetBackgroundThreads(int num, Priority pri) override {
    return target_->SetBackgroundThreads(num, pri);
  }
  int GetBackgroundThreads(Priority pri) override {
    return target_->GetBackgroundThreads(pri);
  }

  Status SetAllowNonOwnerAccess(bool allow_non_owner_access) override {
    return target_->SetAllowNonOwnerAccess(allow_non_owner_access);
  }

  void IncBackgroundThreadsIfNeeded(int num, Priority pri) override {
    return target_->IncBackgroundThreadsIfNeeded(num, pri);
  }

  void LowerThreadPoolIOPriority(Priority pool = LOW) override {
    target_->LowerThreadPoolIOPriority(pool);
  }

  void LowerThreadPoolCPUPriority(Priority pool = LOW) override {
    target_->LowerThreadPoolCPUPriority(pool);
  }

  std::string TimeToString(uint64_t time) override {
    return target_->TimeToString(time);
  }

  Status GetThreadList(std::vector<ThreadStatus>* thread_list) override {
    return target_->GetThreadList(thread_list);
  }

  ThreadStatusUpdater* GetThreadStatusUpdater() const override {
    return target_->GetThreadStatusUpdater();
  }

  uint64_t GetThreadID() const override { return target_->GetThreadID(); }

  std::string GenerateUniqueId() override {
    return target_->GenerateUniqueId();
  }

  EnvOptions OptimizeForLogRead(const EnvOptions& env_options) const override {
    return target_->OptimizeForLogRead(env_options);
  }
  EnvOptions OptimizeForManifestRead(
      const EnvOptions& env_options) const override {
    return target_->OptimizeForManifestRead(env_options);
  }
  EnvOptions OptimizeForLogWrite(const EnvOptions& env_options,
                                 const DBOptions& db_options) const override {
    return target_->OptimizeForLogWrite(env_options, db_options);
  }
  EnvOptions OptimizeForManifestWrite(
      const EnvOptions& env_options) const override {
    return target_->OptimizeForManifestWrite(env_options);
  }
  EnvOptions OptimizeForCompactionTableWrite(
      const EnvOptions& env_options,
      const ImmutableDBOptions& immutable_ops) const override {
    return target_->OptimizeForCompactionTableWrite(env_options, immutable_ops);
  }
  EnvOptions OptimizeForCompactionTableRead(
      const EnvOptions& env_options,
      const ImmutableDBOptions& db_options) const override {
    return target_->OptimizeForCompactionTableRead(env_options, db_options);
  }
  Status GetFreeSpace(const std::string& path, uint64_t* diskfree) override {
    return target_->GetFreeSpace(path, diskfree);
  }
  void SanitizeEnvOptions(EnvOptions* env_opts) const override {
    target_->SanitizeEnvOptions(env_opts);
  }

 private:
  Env* target_;
};

class SequentialFileWrapper : public SequentialFile {
 public:
  explicit SequentialFileWrapper(SequentialFile* target) : target_(target) {}

  Status Read(size_t n, Slice* result, char* scratch) override {
    return target_->Read(n, result, scratch);
  }
  Status Skip(uint64_t n) override { return target_->Skip(n); }
  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  Status InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }
  Status PositionedRead(uint64_t offset, size_t n, Slice* result,
                        char* scratch) override {
    return target_->PositionedRead(offset, n, result, scratch);
  }

 private:
  SequentialFile* target_;
};

class RandomAccessFileWrapper : public RandomAccessFile {
 public:
  explicit RandomAccessFileWrapper(RandomAccessFile* target)
      : target_(target) {}

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    return target_->Read(offset, n, result, scratch);
  }
  Status MultiRead(ReadRequest* reqs, size_t num_reqs) override {
    return target_->MultiRead(reqs, num_reqs);
  }
  Status Prefetch(uint64_t offset, size_t n) override {
    return target_->Prefetch(offset, n);
  }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }
  void Hint(AccessPattern pattern) override { target_->Hint(pattern); }
  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  Status InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }

 private:
  RandomAccessFile* target_;
};

class WritableFileWrapper : public WritableFile {
 public:
  explicit WritableFileWrapper(WritableFile* t) : target_(t) {}

  Status Append(const Slice& data) override { return target_->Append(data); }
  Status PositionedAppend(const Slice& data, uint64_t offset) override {
    return target_->PositionedAppend(data, offset);
  }
  Status Truncate(uint64_t size) override { return target_->Truncate(size); }
  Status Close() override { return target_->Close(); }
  Status Flush() override { return target_->Flush(); }
  Status Sync() override { return target_->Sync(); }
  Status Fsync() override { return target_->Fsync(); }
  bool IsSyncThreadSafe() const override { return target_->IsSyncThreadSafe(); }

  bool use_direct_io() const override { return target_->use_direct_io(); }

  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }

  void SetIOPriority(Env::IOPriority pri) override {
    target_->SetIOPriority(pri);
  }

  Env::IOPriority GetIOPriority() override { return target_->GetIOPriority(); }

  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override {
    target_->SetWriteLifeTimeHint(hint);
  }

  Env::WriteLifeTimeHint GetWriteLifeTimeHint() override {
    return target_->GetWriteLifeTimeHint();
  }

  uint64_t GetFileSize() override { return target_->GetFileSize(); }

  void SetPreallocationBlockSize(size_t size) override {
    target_->SetPreallocationBlockSize(size);
  }

  void GetPreallocationStatus(size_t* block_size,
                              size_t* last_allocated_block) override {
    target_->GetPreallocationStatus(block_size, last_allocated_block);
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }

  Status InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }

  Status RangeSync(uint64_t offset, uint64_t nbytes) override {
    return target_->RangeSync(offset, nbytes);
  }

  void PrepareWrite(size_t offset, size_t len) override {
    target_->PrepareWrite(offset, len);
  }

  Status Allocate(uint64_t offset, uint64_t len) override {
    return target_->Allocate(offset, len);
  }

 private:
  WritableFile* target_;
};

class RandomRWFileWrapper : public RandomRWFile {
 public:
  explicit RandomRWFileWrapper(RandomRWFile* target) : target_(target) {}

  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  Status Write(uint64_t offset, const Slice& data) override {
    return target_->Write(offset, data);
  }
  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    return target_->Read(offset, n, result, scratch);
  }
  Status Flush() override { return target_->Flush(); }
  Status Sync() override { return target_->Sync(); }
  Status Fsync() override { return target_->Fsync(); }
  Status Close() override { return target_->Close(); }

 private:
  RandomRWFile* target_;
};

class DirectoryWrapper : public Directory {
 public:
  explicit DirectoryWrapper(Directory* target) : target_(target) {}

  Status Fsync() override { return target_->Fsync(); }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }

 private:
  Directory* target_;
};

class LoggerWrapper : public Logger {
 public:
  explicit LoggerWrapper(Logger* target) : target_(target) {}

  Status Close() override { return target_->Close(); }
  void LogHeader(const char* format, va_list ap) override {
    return target_->LogHeader(format, ap);
  }
  void Logv(const char* format, va_list ap) override {
    return target_->Logv(format, ap);
  }
  void Logv(const InfoLogLevel log_level, const char* format,
            va_list ap) override {
    return target_->Logv(log_level, format, ap);
  }
  size_t GetLogFileSize() const override { return target_->GetLogFileSize(); }
  void Flush() override { return target_->Flush(); }
  InfoLogLevel GetInfoLogLevel() const override {
    return target_->GetInfoLogLevel();
  }
  void SetInfoLogLevel(const InfoLogLevel log_level) override {
    return target_->SetInfoLogLevel(log_level);
  }

 private:
  Logger* target_;
};

// Returns a new environment that stores its data in memory and delegates
// all non-file-storage tasks to base_env. The caller must delete the result
// when it is no longer needed.
// *base_env must remain live while the result is in use.
Env* NewMemEnv(Env* base_env);

// Returns a new environment that is used for HDFS environment.
// This is a factory method for HdfsEnv declared in hdfs/env_hdfs.h
Status NewHdfsEnv(Env** hdfs_env, const std::string& fsname);

// Returns a new environment that measures function call times for filesystem
// operations, reporting results to variables in PerfContext.
// This is a factory method for TimedEnv defined in utilities/env_timed.cc.
Env* NewTimedEnv(Env* base_env);

// Returns an instance of logger that can be used for storing informational
// messages.
// This is a factory method for EnvLogger declared in logging/env_logging.h
Status NewEnvLogger(const std::string& fname, Env* env,
                    std::shared_ptr<Logger>* result);

}  // namespace ROCKSDB_NAMESPACE
