// Copyright (c) 2019-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// A FileSystem is an interface used by the rocksdb implementation to access
// storage functionality like the filesystem etc.  Callers
// may wish to provide a custom FileSystem object when opening a database to
// get fine gain control; e.g., to rate limit file system operations.
//
// All FileSystem implementations are safe for concurrent access from
// multiple threads without any external synchronization.
//
// WARNING: Since this is a new interface, it is expected that there will be
// some changes as storage systems are ported over.

#pragma once

#include <stdint.h>
#include <chrono>
#include <cstdarg>
#include <functional>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/options.h"
#include "rocksdb/thread_status.h"

namespace ROCKSDB_NAMESPACE {

class FileLock;
class FSDirectory;
class FSRandomAccessFile;
class FSRandomRWFile;
class FSSequentialFile;
class FSWritableFile;
class Logger;
class Slice;
struct ImmutableDBOptions;
struct MutableDBOptions;
class RateLimiter;

using AccessPattern = RandomAccessFile::AccessPattern;
using FileAttributes = Env::FileAttributes;

// Priority of an IO request. This is a hint and does not guarantee any
// particular QoS.
// IO_LOW - Typically background reads/writes such as compaction/flush
// IO_HIGH - Typically user reads/synchronous WAL writes
enum class IOPriority : uint8_t {
  kIOLow,
  kIOHigh,
  kIOTotal,
};

// Type of the data begin read/written. It can be passed down as a flag
// for the FileSystem implementation to optionally handle different types in
// different ways
enum class IOType : uint8_t {
  kData,
  kFilter,
  kIndex,
  kMetadata,
  kWAL,
  kManifest,
  kLog,
  kUnknown,
  kInvalid,
};

// Per-request options that can be passed down to the FileSystem
// implementation. These are hints and are not necessarily guaranteed to be
// honored. More hints can be added here in the future to indicate things like
// storage media (HDD/SSD) to be used, replication level etc.
struct IOOptions {
  // Timeout for the operation in microseconds
  std::chrono::microseconds timeout;

  // Priority - high or low
  IOPriority prio;

  // Type of data being read/written
  IOType type;

  IOOptions() : timeout(0), prio(IOPriority::kIOLow), type(IOType::kUnknown) {}
};

// File scope options that control how a file is opened/created and accessed
// while its open. We may add more options here in the future such as
// redundancy level, media to use etc.
struct FileOptions : EnvOptions {
  // Embedded IOOptions to control the parameters for any IOs that need
  // to be issued for the file open/creation
  IOOptions io_options;

  FileOptions() : EnvOptions() {}

  FileOptions(const DBOptions& opts)
    : EnvOptions(opts) {}

  FileOptions(const EnvOptions& opts)
    : EnvOptions(opts) {}

  FileOptions(const FileOptions& opts)
    : EnvOptions(opts), io_options(opts.io_options) {}

  FileOptions& operator=(const FileOptions& opts) = default;
};

// A structure to pass back some debugging information from the FileSystem
// implementation to RocksDB in case of an IO error
struct IODebugContext {
  // file_path to be filled in by RocksDB in case of an error
  std::string file_path;

  // A map of counter names to values - set by the FileSystem implementation
  std::map<std::string, uint64_t> counters;

  // To be set by the FileSystem implementation
  std::string msg;

  IODebugContext() {}

  void AddCounter(std::string& name, uint64_t value) {
    counters.emplace(name, value);
  }

  std::string ToString() {
    std::ostringstream ss;
    ss << file_path << ", ";
    for (auto counter : counters) {
      ss << counter.first << " = " << counter.second << ",";
    }
    ss << msg;
    return ss.str();
  }
};

// The FileSystem, FSSequentialFile, FSRandomAccessFile, FSWritableFile,
// FSRandomRWFileclass, and FSDIrectory classes define the interface between
// RocksDB and storage systems, such as Posix filesystems,
// remote filesystems etc.
// The interface allows for fine grained control of individual IO operations,
// such as setting a timeout, prioritization, hints on data placement,
// different handling based on type of IO etc.
// This is accomplished by passing an instance of IOOptions to every
// API call that can potentially perform IO. Additionally, each such API is
// passed a pointer to a IODebugContext structure that can be used by the
// storage system to include troubleshooting information. The return values
// of the APIs is of type IOStatus, which can indicate an error code/sub-code,
// as well as metadata about the error such as its scope and whether its
// retryable.
class FileSystem {
 public:
  FileSystem();

  // No copying allowed
  FileSystem(const FileSystem&) = delete;

  virtual ~FileSystem();

  virtual const char* Name() const = 0;

  static const char* Type() { return "FileSystem"; }

  // Loads the FileSystem specified by the input value into the result
  static Status Load(const std::string& value,
                     std::shared_ptr<FileSystem>* result);

  // Return a default fie_system suitable for the current operating
  // system.  Sophisticated users may wish to provide their own Env
  // implementation instead of relying on this default file_system
  //
  // The result of Default() belongs to rocksdb and must never be deleted.
  static std::shared_ptr<FileSystem> Default();

  // Handles the event when a new DB or a new ColumnFamily starts using the
  // specified data paths.
  //
  // The data paths might be shared by different DBs or ColumnFamilies,
  // so RegisterDbPaths might be called with the same data paths.
  // For example, when CreateColumnFamily is called multiple times with the same
  // data path, RegisterDbPaths will also be called with the same data path.
  //
  // If the return status is ok, then the paths must be correspondingly
  // called in UnregisterDbPaths;
  // otherwise this method should have no side effect, and UnregisterDbPaths
  // do not need to be called for the paths.
  //
  // Different implementations may take different actions.
  // By default, it's a no-op and returns Status::OK.
  virtual Status RegisterDbPaths(const std::vector<std::string>& /*paths*/) {
    return Status::OK();
  }
  // Handles the event a DB or a ColumnFamily stops using the specified data
  // paths.
  //
  // It should be called corresponding to each successful RegisterDbPaths.
  //
  // Different implementations may take different actions.
  // By default, it's a no-op and returns Status::OK.
  virtual Status UnregisterDbPaths(const std::vector<std::string>& /*paths*/) {
    return Status::OK();
  }

  // Create a brand new sequentially-readable file with the specified name.
  // On success, stores a pointer to the new file in *result and returns OK.
  // On failure stores nullptr in *result and returns non-OK.  If the file does
  // not exist, returns a non-OK status.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual IOStatus NewSequentialFile(const std::string& fname,
                                     const FileOptions& file_opts,
                                     std::unique_ptr<FSSequentialFile>* result,
                                     IODebugContext* dbg) = 0;

  // Create a brand new random access read-only file with the
  // specified name.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.  If the file does not exist, returns a non-OK
  // status.
  //
  // The returned file may be concurrently accessed by multiple threads.
  virtual IOStatus NewRandomAccessFile(
      const std::string& fname, const FileOptions& file_opts,
      std::unique_ptr<FSRandomAccessFile>* result,
      IODebugContext* dbg) = 0;
  // These values match Linux definition
  // https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/include/uapi/linux/fcntl.h#n56
  enum WriteLifeTimeHint {
    kWLTHNotSet = 0,  // No hint information set
    kWLTHNone,        // No hints about write life time
    kWLTHShort,       // Data written has a short life time
    kWLTHMedium,      // Data written has a medium life time
    kWLTHLong,        // Data written has a long life time
    kWLTHExtreme,     // Data written has an extremely long life time
  };

  // Create an object that writes to a new file with the specified
  // name.  Deletes any existing file with the same name and creates a
  // new file.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual IOStatus NewWritableFile(const std::string& fname,
                                   const FileOptions& file_opts,
                                   std::unique_ptr<FSWritableFile>* result,
                                   IODebugContext* dbg) = 0;

  // Create an object that writes to a new file with the specified
  // name.  Deletes any existing file with the same name and creates a
  // new file.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual IOStatus ReopenWritableFile(
      const std::string& /*fname*/, const FileOptions& /*options*/,
      std::unique_ptr<FSWritableFile>* /*result*/, IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported();
  }

  // Reuse an existing file by renaming it and opening it as writable.
  virtual IOStatus ReuseWritableFile(const std::string& fname,
                                     const std::string& old_fname,
                                     const FileOptions& file_opts,
                                     std::unique_ptr<FSWritableFile>* result,
                                     IODebugContext* dbg);

  // Open `fname` for random read and write, if file doesn't exist the file
  // will be created.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual IOStatus NewRandomRWFile(const std::string& /*fname*/,
                                   const FileOptions& /*options*/,
                                   std::unique_ptr<FSRandomRWFile>* /*result*/,
                                   IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported(
        "RandomRWFile is not implemented in this FileSystem");
  }

  // Opens `fname` as a memory-mapped file for read and write (in-place updates
  // only, i.e., no appends). On success, stores a raw buffer covering the whole
  // file in `*result`. The file must exist prior to this call.
  virtual IOStatus NewMemoryMappedFileBuffer(
      const std::string& /*fname*/,
      std::unique_ptr<MemoryMappedFileBuffer>* /*result*/) {
    return IOStatus::NotSupported(
        "MemoryMappedFileBuffer is not implemented in this FileSystem");
  }

  // Create an object that represents a directory. Will fail if directory
  // doesn't exist. If the directory exists, it will open the directory
  // and create a new Directory object.
  //
  // On success, stores a pointer to the new Directory in
  // *result and returns OK. On failure stores nullptr in *result and
  // returns non-OK.
  virtual IOStatus NewDirectory(const std::string& name,
                                const IOOptions& io_opts,
                                std::unique_ptr<FSDirectory>* result,
                                IODebugContext* dbg) = 0;

  // Returns OK if the named file exists.
  //         NotFound if the named file does not exist,
  //                  the calling process does not have permission to determine
  //                  whether this file exists, or if the path is invalid.
  //         IOError if an IO Error was encountered
  virtual IOStatus FileExists(const std::string& fname,
                              const IOOptions& options,
                              IODebugContext* dbg) = 0;

  // Store in *result the names of the children of the specified directory.
  // The names are relative to "dir".
  // Original contents of *results are dropped.
  // Returns OK if "dir" exists and "*result" contains its children.
  //         NotFound if "dir" does not exist, the calling process does not have
  //                  permission to access "dir", or if "dir" is invalid.
  //         IOError if an IO Error was encountered
  virtual IOStatus GetChildren(const std::string& dir, const IOOptions& options,
                               std::vector<std::string>* result,
                               IODebugContext* dbg) = 0;

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
  virtual IOStatus GetChildrenFileAttributes(
      const std::string& dir, const IOOptions& options,
      std::vector<FileAttributes>* result, IODebugContext* dbg) {
    assert(result != nullptr);
    std::vector<std::string> child_fnames;
    IOStatus s = GetChildren(dir, options, &child_fnames, dbg);
    if (!s.ok()) {
      return s;
    }
    result->resize(child_fnames.size());
    size_t result_size = 0;
    for (size_t i = 0; i < child_fnames.size(); ++i) {
      const std::string path = dir + "/" + child_fnames[i];
      if (!(s = GetFileSize(path, options, &(*result)[result_size].size_bytes,
                            dbg))
               .ok()) {
        if (FileExists(path, options, dbg).IsNotFound()) {
          // The file may have been deleted since we listed the directory
          continue;
        }
        return s;
      }
      (*result)[result_size].name = std::move(child_fnames[i]);
      result_size++;
    }
    result->resize(result_size);
    return IOStatus::OK();
  }

  // Delete the named file.
  virtual IOStatus DeleteFile(const std::string& fname,
                              const IOOptions& options,
                              IODebugContext* dbg) = 0;

  // Truncate the named file to the specified size.
  virtual IOStatus Truncate(const std::string& /*fname*/, size_t /*size*/,
                            const IOOptions& /*options*/,
                            IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported("Truncate is not supported for this FileSystem");
  }

  // Create the specified directory. Returns error if directory exists.
  virtual IOStatus CreateDir(const std::string& dirname,
                             const IOOptions& options, IODebugContext* dbg) = 0;

  // Creates directory if missing. Return Ok if it exists, or successful in
  // Creating.
  virtual IOStatus CreateDirIfMissing(const std::string& dirname,
                                      const IOOptions& options,
                                      IODebugContext* dbg) = 0;

  // Delete the specified directory.
  virtual IOStatus DeleteDir(const std::string& dirname,
                             const IOOptions& options, IODebugContext* dbg) = 0;

  // Store the size of fname in *file_size.
  virtual IOStatus GetFileSize(const std::string& fname,
                               const IOOptions& options, uint64_t* file_size,
                               IODebugContext* dbg) = 0;

  // Store the last modification time of fname in *file_mtime.
  virtual IOStatus GetFileModificationTime(const std::string& fname,
                                           const IOOptions& options,
                                           uint64_t* file_mtime,
                                           IODebugContext* dbg) = 0;
  // Rename file src to target.
  virtual IOStatus RenameFile(const std::string& src, const std::string& target,
                              const IOOptions& options,
                              IODebugContext* dbg) = 0;

  // Hard Link file src to target.
  virtual IOStatus LinkFile(const std::string& /*src*/,
                            const std::string& /*target*/,
                            const IOOptions& /*options*/,
                            IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported("LinkFile is not supported for this FileSystem");
  }

  virtual IOStatus NumFileLinks(const std::string& /*fname*/,
                                const IOOptions& /*options*/,
                                uint64_t* /*count*/, IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported(
        "Getting number of file links is not supported for this FileSystem");
  }

  virtual IOStatus AreFilesSame(const std::string& /*first*/,
                                const std::string& /*second*/,
                                const IOOptions& /*options*/, bool* /*res*/,
                                IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported("AreFilesSame is not supported for this FileSystem");
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
  virtual IOStatus LockFile(const std::string& fname, const IOOptions& options,
                            FileLock** lock, IODebugContext* dbg) = 0;

  // Release the lock acquired by a previous successful call to LockFile.
  // REQUIRES: lock was returned by a successful LockFile() call
  // REQUIRES: lock has not already been unlocked.
  virtual IOStatus UnlockFile(FileLock* lock, const IOOptions& options,
                              IODebugContext* dbg) = 0;

  // *path is set to a temporary directory that can be used for testing. It may
  // or many not have just been created. The directory may or may not differ
  // between runs of the same process, but subsequent calls will return the
  // same directory.
  virtual IOStatus GetTestDirectory(const IOOptions& options, std::string* path,
                                    IODebugContext* dbg) = 0;

  // Create and returns a default logger (an instance of EnvLogger) for storing
  // informational messages. Derived classes can overide to provide custom
  // logger.
  virtual IOStatus NewLogger(const std::string& fname, const IOOptions& io_opts,
                             std::shared_ptr<Logger>* result,
                             IODebugContext* dbg) = 0;

  // Get full directory name for this db.
  virtual IOStatus GetAbsolutePath(const std::string& db_path,
                                   const IOOptions& options,
                                   std::string* output_path,
                                   IODebugContext* dbg) = 0;

  // Sanitize the FileOptions. Typically called by a FileOptions/EnvOptions
  // copy constructor
  virtual void SanitizeFileOptions(FileOptions* /*opts*/) const {}

  // OptimizeForLogRead will create a new FileOptions object that is a copy of
  // the FileOptions in the parameters, but is optimized for reading log files.
  virtual FileOptions OptimizeForLogRead(const FileOptions& file_options) const;

  // OptimizeForManifestRead will create a new FileOptions object that is a copy
  // of the FileOptions in the parameters, but is optimized for reading manifest
  // files.
  virtual FileOptions OptimizeForManifestRead(
      const FileOptions& file_options) const;

  // OptimizeForLogWrite will create a new FileOptions object that is a copy of
  // the FileOptions in the parameters, but is optimized for writing log files.
  // Default implementation returns the copy of the same object.
  virtual FileOptions OptimizeForLogWrite(const FileOptions& file_options,
                                         const DBOptions& db_options) const;

  // OptimizeForManifestWrite will create a new FileOptions object that is a
  // copy of the FileOptions in the parameters, but is optimized for writing
  // manifest files. Default implementation returns the copy of the same
  // object.
  virtual FileOptions OptimizeForManifestWrite(
      const FileOptions& file_options) const;

  // OptimizeForCompactionTableWrite will create a new FileOptions object that
  // is a copy of the FileOptions in the parameters, but is optimized for
  // writing table files.
  virtual FileOptions OptimizeForCompactionTableWrite(
      const FileOptions& file_options,
      const ImmutableDBOptions& immutable_ops) const;

  // OptimizeForCompactionTableRead will create a new FileOptions object that
  // is a copy of the FileOptions in the parameters, but is optimized for
  // reading table files.
  virtual FileOptions OptimizeForCompactionTableRead(
      const FileOptions& file_options,
      const ImmutableDBOptions& db_options) const;

// This seems to clash with a macro on Windows, so #undef it here
#ifdef GetFreeSpace
#undef GetFreeSpace
#endif

  // Get the amount of free disk space
  virtual IOStatus GetFreeSpace(const std::string& /*path*/,
                                const IOOptions& /*options*/,
                                uint64_t* /*diskfree*/,
                                IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported();
  }

  virtual IOStatus IsDirectory(const std::string& /*path*/,
                               const IOOptions& options, bool* is_dir,
                               IODebugContext* /*dgb*/) = 0;

  // If you're adding methods here, remember to add them to EnvWrapper too.

 private:
  void operator=(const FileSystem&);
};

// A file abstraction for reading sequentially through a file
class FSSequentialFile {
 public:
  FSSequentialFile() {}

  virtual ~FSSequentialFile() {}

  // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
  // written by this routine.  Sets "*result" to the data that was
  // read (including if fewer than "n" bytes were successfully read).
  // May set "*result" to point at data in "scratch[0..n-1]", so
  // "scratch[0..n-1]" must be live when "*result" is used.
  // If an error was encountered, returns a non-OK status.
  //
  // REQUIRES: External synchronization
  virtual IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                        char* scratch, IODebugContext* dbg) = 0;

  // Skip "n" bytes from the file. This is guaranteed to be no
  // slower that reading the same data, but may be faster.
  //
  // If end of file is reached, skipping will stop at the end of the
  // file, and Skip will return OK.
  //
  // REQUIRES: External synchronization
  virtual IOStatus Skip(uint64_t n) = 0;

  // Indicates the upper layers if the current SequentialFile implementation
  // uses direct IO.
  virtual bool use_direct_io() const { return false; }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const { return kDefaultPageSize; }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  virtual IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) {
    return IOStatus::NotSupported("InvalidateCache not supported.");
  }

  // Positioned Read for direct I/O
  // If Direct I/O enabled, offset, n, and scratch should be properly aligned
  virtual IOStatus PositionedRead(uint64_t /*offset*/, size_t /*n*/,
                                  const IOOptions& /*options*/,
                                  Slice* /*result*/, char* /*scratch*/,
                                  IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported();
  }

  // If you're adding methods here, remember to add them to
  // SequentialFileWrapper too.
};

// A read IO request structure for use in MultiRead
struct FSReadRequest {
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
  IOStatus status;
};

// A file abstraction for randomly reading the contents of a file.
class FSRandomAccessFile {
 public:
  FSRandomAccessFile() {}

  virtual ~FSRandomAccessFile() {}

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
  virtual IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                        Slice* result, char* scratch,
                        IODebugContext* dbg) const = 0;

  // Readahead the file starting from offset by n bytes for caching.
  virtual IOStatus Prefetch(uint64_t /*offset*/, size_t /*n*/,
                            const IOOptions& /*options*/,
                            IODebugContext* /*dbg*/) {
    return IOStatus::OK();
  }

  // Read a bunch of blocks as described by reqs. The blocks can
  // optionally be read in parallel. This is a synchronous call, i.e it
  // should return after all reads have completed. The reads will be
  // non-overlapping. If the function return Status is not ok, status of
  // individual requests will be ignored and return status will be assumed
  // for all read requests. The function return status is only meant for any
  // any errors that occur before even processing specific read requests
  virtual IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                             const IOOptions& options, IODebugContext* dbg) {
    assert(reqs != nullptr);
    for (size_t i = 0; i < num_reqs; ++i) {
      FSReadRequest& req = reqs[i];
      req.status =
          Read(req.offset, req.len, options, &req.result, req.scratch, dbg);
    }
    return IOStatus::OK();
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
  };

  enum AccessPattern { kNormal, kRandom, kSequential, kWillNeed, kWontNeed };

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
  virtual IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) {
    return IOStatus::NotSupported("InvalidateCache not supported.");
  }

  // If you're adding methods here, remember to add them to
  // RandomAccessFileWrapper too.
};

// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
class FSWritableFile {
 public:
  FSWritableFile()
      : last_preallocated_block_(0),
        preallocation_block_size_(0),
        io_priority_(Env::IO_TOTAL),
        write_hint_(Env::WLTH_NOT_SET),
        strict_bytes_per_sync_(false) {}

  explicit FSWritableFile(const FileOptions& options)
      : last_preallocated_block_(0),
        preallocation_block_size_(0),
        io_priority_(Env::IO_TOTAL),
        write_hint_(Env::WLTH_NOT_SET),
        strict_bytes_per_sync_(options.strict_bytes_per_sync) {}

  virtual ~FSWritableFile() {}

  // Append data to the end of the file
  // Note: A WriteabelFile object must support either Append or
  // PositionedAppend, so the users cannot mix the two.
  virtual IOStatus Append(const Slice& data, const IOOptions& options,
                          IODebugContext* dbg) = 0;

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
  virtual IOStatus PositionedAppend(const Slice& /* data */,
                                    uint64_t /* offset */,
                                    const IOOptions& /*options*/,
                                    IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported();
  }

  // Truncate is necessary to trim the file to the correct size
  // before closing. It is not always possible to keep track of the file
  // size due to whole pages writes. The behavior is undefined if called
  // with other writes to follow.
  virtual IOStatus Truncate(uint64_t /*size*/, const IOOptions& /*options*/,
                            IODebugContext* /*dbg*/) {
    return IOStatus::OK();
  }
  virtual IOStatus Close(const IOOptions& options, IODebugContext* dbg) = 0;
  virtual IOStatus Flush(const IOOptions& options, IODebugContext* dbg) = 0;
  virtual IOStatus Sync(const IOOptions& options,
                        IODebugContext* dbg) = 0;  // sync data

  /*
   * Sync data and/or metadata as well.
   * By default, sync only data.
   * Override this method for environments where we need to sync
   * metadata as well.
   */
  virtual IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) {
    return Sync(options, dbg);
  }

  // true if Sync() and Fsync() are safe to call concurrently with Append()
  // and Flush().
  virtual bool IsSyncThreadSafe() const { return false; }

  // Indicates the upper layers if the current WritableFile implementation
  // uses direct IO.
  virtual bool use_direct_io() const { return false; }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const { return kDefaultPageSize; }

  virtual void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) {
    write_hint_ = hint;
  }

  virtual void SetIOPriority(Env::IOPriority pri) { io_priority_ = pri; }

  virtual Env::IOPriority GetIOPriority() { return io_priority_; }

  virtual Env::WriteLifeTimeHint GetWriteLifeTimeHint() { return write_hint_; }
  /*
   * Get the size of valid data in the file.
   */
  virtual uint64_t GetFileSize(const IOOptions& /*options*/,
                               IODebugContext* /*dbg*/) {
    return 0;
  }

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
  virtual IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) {
    return IOStatus::NotSupported("InvalidateCache not supported.");
  }

  // Sync a file range with disk.
  // offset is the starting byte of the file range to be synchronized.
  // nbytes specifies the length of the range to be synchronized.
  // This asks the OS to initiate flushing the cached data to disk,
  // without waiting for completion.
  // Default implementation does nothing.
  virtual IOStatus RangeSync(uint64_t /*offset*/, uint64_t /*nbytes*/,
                             const IOOptions& options, IODebugContext* dbg) {
    if (strict_bytes_per_sync_) {
      return Sync(options, dbg);
    }
    return IOStatus::OK();
  }

  // PrepareWrite performs any necessary preparation for a write
  // before the write actually occurs.  This allows for pre-allocation
  // of space on devices where it can result in less file
  // fragmentation and/or less waste from over-zealous filesystem
  // pre-allocation.
  virtual void PrepareWrite(size_t offset, size_t len, const IOOptions& options,
                            IODebugContext* dbg) {
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
               block_size * num_spanned_blocks, options, dbg);
      last_preallocated_block_ = new_last_preallocated_block;
    }
  }

  // Pre-allocates space for a file.
  virtual IOStatus Allocate(uint64_t /*offset*/, uint64_t /*len*/,
                            const IOOptions& /*options*/,
                            IODebugContext* /*dbg*/) {
    return IOStatus::OK();
  }

  // If you're adding methods here, remember to add them to
  // WritableFileWrapper too.

 protected:
  size_t preallocation_block_size() { return preallocation_block_size_; }

 private:
  size_t last_preallocated_block_;
  size_t preallocation_block_size_;
  // No copying allowed
  FSWritableFile(const FSWritableFile&);
  void operator=(const FSWritableFile&);

 protected:
  Env::IOPriority io_priority_;
  Env::WriteLifeTimeHint write_hint_;
  const bool strict_bytes_per_sync_;
};

// A file abstraction for random reading and writing.
class FSRandomRWFile {
 public:
  FSRandomRWFile() {}

  virtual ~FSRandomRWFile() {}

  // Indicates if the class makes use of direct I/O
  // If false you must pass aligned buffer to Write()
  virtual bool use_direct_io() const { return false; }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const { return kDefaultPageSize; }

  // Write bytes in `data` at  offset `offset`, Returns Status::OK() on success.
  // Pass aligned buffer when use_direct_io() returns true.
  virtual IOStatus Write(uint64_t offset, const Slice& data,
                         const IOOptions& options, IODebugContext* dbg) = 0;

  // Read up to `n` bytes starting from offset `offset` and store them in
  // result, provided `scratch` size should be at least `n`.
  // Returns Status::OK() on success.
  virtual IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                        Slice* result, char* scratch,
                        IODebugContext* dbg) const = 0;

  virtual IOStatus Flush(const IOOptions& options, IODebugContext* dbg) = 0;

  virtual IOStatus Sync(const IOOptions& options, IODebugContext* dbg) = 0;

  virtual IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) {
    return Sync(options, dbg);
  }

  virtual IOStatus Close(const IOOptions& options, IODebugContext* dbg) = 0;

  // If you're adding methods here, remember to add them to
  // RandomRWFileWrapper too.

  // No copying allowed
  FSRandomRWFile(const RandomRWFile&) = delete;
  FSRandomRWFile& operator=(const RandomRWFile&) = delete;
};

// MemoryMappedFileBuffer object represents a memory-mapped file's raw buffer.
// Subclasses should release the mapping upon destruction.
class FSMemoryMappedFileBuffer {
 public:
  FSMemoryMappedFileBuffer(void* _base, size_t _length)
      : base_(_base), length_(_length) {}

  virtual ~FSMemoryMappedFileBuffer() = 0;

  // We do not want to unmap this twice. We can make this class
  // movable if desired, however, since
  FSMemoryMappedFileBuffer(const FSMemoryMappedFileBuffer&) = delete;
  FSMemoryMappedFileBuffer& operator=(const FSMemoryMappedFileBuffer&) = delete;

  void* GetBase() const { return base_; }
  size_t GetLen() const { return length_; }

 protected:
  void* base_;
  const size_t length_;
};

// Directory object represents collection of files and implements
// filesystem operations that can be executed on directories.
class FSDirectory {
 public:
  virtual ~FSDirectory() {}
  // Fsync directory. Can be called concurrently from multiple threads.
  virtual IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) = 0;

  virtual size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const {
    return 0;
  }

  // If you're adding methods here, remember to add them to
  // DirectoryWrapper too.
};

// Below are helpers for wrapping most of the classes in this file.
// They forward all calls to another instance of the class.
// Useful when wrapping the default implementations.
// Typical usage is to inherit your wrapper from *Wrapper, e.g.:
//
// class MySequentialFileWrapper : public
// ROCKSDB_NAMESPACE::FSSequentialFileWrapper {
//  public:
//   MySequentialFileWrapper(ROCKSDB_NAMESPACE::FSSequentialFile* target):
//     ROCKSDB_NAMESPACE::FSSequentialFileWrapper(target) {}
//   Status Read(size_t n, FileSystem::IOOptions& options, Slice* result,
//               char* scratch, FileSystem::IODebugContext* dbg) override {
//     cout << "Doing a read of size " << n << "!" << endl;
//     return ROCKSDB_NAMESPACE::FSSequentialFileWrapper::Read(n, options,
//     result,
//                                                 scratch, dbg);
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
class FileSystemWrapper : public FileSystem {
 public:
  // Initialize an EnvWrapper that delegates all calls to *t
  explicit FileSystemWrapper(std::shared_ptr<FileSystem> t) : target_(t) {}
  ~FileSystemWrapper() override {}

  const char* Name() const override { return target_->Name(); }

  // Return the target to which this Env forwards all calls
  FileSystem* target() const { return target_.get(); }

  // The following text is boilerplate that forwards all methods to target()
  IOStatus NewSequentialFile(const std::string& f,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSSequentialFile>* r,
                             IODebugContext* dbg) override {
    return target_->NewSequentialFile(f, file_opts, r, dbg);
  }
  IOStatus NewRandomAccessFile(const std::string& f,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* r,
                               IODebugContext* dbg) override {
    return target_->NewRandomAccessFile(f, file_opts, r, dbg);
  }
  IOStatus NewWritableFile(const std::string& f, const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* r,
                           IODebugContext* dbg) override {
    return target_->NewWritableFile(f, file_opts, r, dbg);
  }
  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& file_opts,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override {
    return target_->ReopenWritableFile(fname, file_opts, result, dbg);
  }
  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSWritableFile>* r,
                             IODebugContext* dbg) override {
    return target_->ReuseWritableFile(fname, old_fname, file_opts, r,
                                      dbg);
  }
  IOStatus NewRandomRWFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* dbg) override {
    return target_->NewRandomRWFile(fname, file_opts, result, dbg);
  }
  IOStatus NewMemoryMappedFileBuffer(
      const std::string& fname,
      std::unique_ptr<MemoryMappedFileBuffer>* result) override {
    return target_->NewMemoryMappedFileBuffer(fname, result);
  }
  IOStatus NewDirectory(const std::string& name, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override {
    return target_->NewDirectory(name, io_opts, result, dbg);
  }
  IOStatus FileExists(const std::string& f, const IOOptions& io_opts,
                      IODebugContext* dbg) override {
    return target_->FileExists(f, io_opts, dbg);
  }
  IOStatus GetChildren(const std::string& dir, const IOOptions& io_opts,
                       std::vector<std::string>* r,
                       IODebugContext* dbg) override {
    return target_->GetChildren(dir, io_opts, r, dbg);
  }
  IOStatus GetChildrenFileAttributes(const std::string& dir,
                                     const IOOptions& options,
                                     std::vector<FileAttributes>* result,
                                     IODebugContext* dbg) override {
    return target_->GetChildrenFileAttributes(dir, options, result, dbg);
  }
  IOStatus DeleteFile(const std::string& f, const IOOptions& options,
                      IODebugContext* dbg) override {
    return target_->DeleteFile(f, options, dbg);
  }
  IOStatus Truncate(const std::string& fname, size_t size,
                    const IOOptions& options, IODebugContext* dbg) override {
    return target_->Truncate(fname, size, options, dbg);
  }
  IOStatus CreateDir(const std::string& d, const IOOptions& options,
                     IODebugContext* dbg) override {
    return target_->CreateDir(d, options, dbg);
  }
  IOStatus CreateDirIfMissing(const std::string& d, const IOOptions& options,
                              IODebugContext* dbg) override {
    return target_->CreateDirIfMissing(d, options, dbg);
  }
  IOStatus DeleteDir(const std::string& d, const IOOptions& options,
                     IODebugContext* dbg) override {
    return target_->DeleteDir(d, options, dbg);
  }
  IOStatus GetFileSize(const std::string& f, const IOOptions& options,
                       uint64_t* s, IODebugContext* dbg) override {
    return target_->GetFileSize(f, options, s, dbg);
  }

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& options,
                                   uint64_t* file_mtime,
                                   IODebugContext* dbg) override {
    return target_->GetFileModificationTime(fname, options, file_mtime, dbg);
  }

  IOStatus GetAbsolutePath(const std::string& db_path, const IOOptions& options,
                           std::string* output_path,
                           IODebugContext* dbg) override {
    return target_->GetAbsolutePath(db_path, options, output_path, dbg);
  }

  IOStatus RenameFile(const std::string& s, const std::string& t,
                      const IOOptions& options, IODebugContext* dbg) override {
    return target_->RenameFile(s, t, options, dbg);
  }

  IOStatus LinkFile(const std::string& s, const std::string& t,
                    const IOOptions& options, IODebugContext* dbg) override {
    return target_->LinkFile(s, t, options, dbg);
  }

  IOStatus NumFileLinks(const std::string& fname, const IOOptions& options,
                        uint64_t* count, IODebugContext* dbg) override {
    return target_->NumFileLinks(fname, options, count, dbg);
  }

  IOStatus AreFilesSame(const std::string& first, const std::string& second,
                        const IOOptions& options, bool* res,
                        IODebugContext* dbg) override {
    return target_->AreFilesSame(first, second, options, res, dbg);
  }

  IOStatus LockFile(const std::string& f, const IOOptions& options,
                    FileLock** l, IODebugContext* dbg) override {
    return target_->LockFile(f, options, l, dbg);
  }

  IOStatus UnlockFile(FileLock* l, const IOOptions& options,
                      IODebugContext* dbg) override {
    return target_->UnlockFile(l, options, dbg);
  }

  IOStatus GetTestDirectory(const IOOptions& options, std::string* path,
                            IODebugContext* dbg) override {
    return target_->GetTestDirectory(options, path, dbg);
  }
  IOStatus NewLogger(const std::string& fname, const IOOptions& options,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* dbg) override {
    return target_->NewLogger(fname, options, result, dbg);
  }

  void SanitizeFileOptions(FileOptions* opts) const override {
    target_->SanitizeFileOptions(opts);
  }

  FileOptions OptimizeForLogRead(
                  const FileOptions& file_options) const override {
    return target_->OptimizeForLogRead(file_options);
  }
  FileOptions OptimizeForManifestRead(
      const FileOptions& file_options) const override {
    return target_->OptimizeForManifestRead(file_options);
  }
  FileOptions OptimizeForLogWrite(const FileOptions& file_options,
                                 const DBOptions& db_options) const override {
    return target_->OptimizeForLogWrite(file_options, db_options);
  }
  FileOptions OptimizeForManifestWrite(
      const FileOptions& file_options) const override {
    return target_->OptimizeForManifestWrite(file_options);
  }
  FileOptions OptimizeForCompactionTableWrite(
      const FileOptions& file_options,
      const ImmutableDBOptions& immutable_ops) const override {
    return target_->OptimizeForCompactionTableWrite(file_options,
                                                    immutable_ops);
  }
  FileOptions OptimizeForCompactionTableRead(
      const FileOptions& file_options,
      const ImmutableDBOptions& db_options) const override {
    return target_->OptimizeForCompactionTableRead(file_options, db_options);
  }
  IOStatus GetFreeSpace(const std::string& path, const IOOptions& options,
                        uint64_t* diskfree, IODebugContext* dbg) override {
    return target_->GetFreeSpace(path, options, diskfree, dbg);
  }
  IOStatus IsDirectory(const std::string& path, const IOOptions& options,
                       bool* is_dir, IODebugContext* dbg) override {
    return target_->IsDirectory(path, options, is_dir, dbg);
  }

 private:
  std::shared_ptr<FileSystem> target_;
};

class FSSequentialFileWrapper : public FSSequentialFile {
 public:
  explicit FSSequentialFileWrapper(FSSequentialFile* target)
      : target_(target) {}

  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override {
    return target_->Read(n, options, result, scratch, dbg);
  }
  IOStatus Skip(uint64_t n) override { return target_->Skip(n); }
  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }
  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override {
    return target_->PositionedRead(offset, n, options, result, scratch, dbg);
  }

 private:
  FSSequentialFile* target_;
};

class FSRandomAccessFileWrapper : public FSRandomAccessFile {
 public:
  explicit FSRandomAccessFileWrapper(FSRandomAccessFile* target)
      : target_(target) {}

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override {
    return target_->Read(offset, n, options, result, scratch, dbg);
  }
  IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& options, IODebugContext* dbg) override {
    return target_->MultiRead(reqs, num_reqs, options, dbg);
  }
  IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& options,
                    IODebugContext* dbg) override {
    return target_->Prefetch(offset, n, options, dbg);
  }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  };
  void Hint(AccessPattern pattern) override { target_->Hint(pattern); }
  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }

 private:
  FSRandomAccessFile* target_;
};

class FSWritableFileWrapper : public FSWritableFile {
 public:
  explicit FSWritableFileWrapper(FSWritableFile* t) : target_(t) {}

  IOStatus Append(const Slice& data, const IOOptions& options,
                  IODebugContext* dbg) override {
    return target_->Append(data, options, dbg);
  }
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override {
    return target_->PositionedAppend(data, offset, options, dbg);
  }
  IOStatus Truncate(uint64_t size, const IOOptions& options,
                    IODebugContext* dbg) override {
    return target_->Truncate(size, options, dbg);
  }
  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Close(options, dbg);
  }
  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Flush(options, dbg);
  }
  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Sync(options, dbg);
  }
  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Fsync(options, dbg);
  }
  bool IsSyncThreadSafe() const override { return target_->IsSyncThreadSafe(); }

  bool use_direct_io() const override { return target_->use_direct_io(); }

  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }

  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override {
    target_->SetWriteLifeTimeHint(hint);
  }

  Env::WriteLifeTimeHint GetWriteLifeTimeHint() override {
    return target_->GetWriteLifeTimeHint();
  }

  uint64_t GetFileSize(const IOOptions& options, IODebugContext* dbg) override {
    return target_->GetFileSize(options, dbg);
  }

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

  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }

  IOStatus RangeSync(uint64_t offset, uint64_t nbytes, const IOOptions& options,
                     IODebugContext* dbg) override {
    return target_->RangeSync(offset, nbytes, options, dbg);
  }

  void PrepareWrite(size_t offset, size_t len, const IOOptions& options,
                    IODebugContext* dbg) override {
    target_->PrepareWrite(offset, len, options, dbg);
  }

  IOStatus Allocate(uint64_t offset, uint64_t len, const IOOptions& options,
                    IODebugContext* dbg) override {
    return target_->Allocate(offset, len, options, dbg);
  }

 private:
  FSWritableFile* target_;
};

class FSRandomRWFileWrapper : public FSRandomRWFile {
 public:
  explicit FSRandomRWFileWrapper(FSRandomRWFile* target) : target_(target) {}

  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  IOStatus Write(uint64_t offset, const Slice& data, const IOOptions& options,
                 IODebugContext* dbg) override {
    return target_->Write(offset, data, options, dbg);
  }
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override {
    return target_->Read(offset, n, options, result, scratch, dbg);
  }
  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Flush(options, dbg);
  }
  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Sync(options, dbg);
  }
  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Fsync(options, dbg);
  }
  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Close(options, dbg);
  }

 private:
  FSRandomRWFile* target_;
};

class FSDirectoryWrapper : public FSDirectory {
 public:
  explicit FSDirectoryWrapper(FSDirectory* target) : target_(target) {}

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Fsync(options, dbg);
  }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }

 private:
  FSDirectory* target_;
};

// A utility routine: write "data" to the named file.
extern IOStatus WriteStringToFile(FileSystem* fs, const Slice& data,
                                  const std::string& fname,
                                  bool should_sync = false);

// A utility routine: read contents of named file into *data
extern IOStatus ReadFileToString(FileSystem* fs, const std::string& fname,
                                 std::string* data);

}  // namespace ROCKSDB_NAMESPACE
