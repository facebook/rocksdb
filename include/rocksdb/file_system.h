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
#include <unordered_map>
#include <vector>

#include "rocksdb/customizable.h"
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
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
struct ConfigOptions;

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

  // EXPERIMENTAL
  // An option map that's opaque to RocksDB. It can be used to implement a
  // custom contract between a FileSystem user and the provider. This is only
  // useful in cases where a RocksDB user directly uses the FileSystem or file
  // object for their own purposes, and wants to pass extra options to APIs
  // such as NewRandomAccessFile and NewWritableFile.
  std::unordered_map<std::string, std::string> property_bag;

  // Force directory fsync, some file systems like btrfs may skip directory
  // fsync, set this to force the fsync
  bool force_dir_fsync;

  IOOptions() : IOOptions(false) {}

  explicit IOOptions(bool force_dir_fsync_)
      : timeout(std::chrono::microseconds::zero()),
        prio(IOPriority::kIOLow),
        type(IOType::kUnknown),
        force_dir_fsync(force_dir_fsync_) {}
};

struct DirFsyncOptions {
  enum FsyncReason : uint8_t {
    kNewFileSynced,
    kFileRenamed,
    kDirRenamed,
    kFileDeleted,
    kDefault,
  } reason;

  std::string renamed_new_name;  // for kFileRenamed
  // add other options for other FsyncReason

  DirFsyncOptions();

  explicit DirFsyncOptions(std::string file_renamed_new_name);

  explicit DirFsyncOptions(FsyncReason fsync_reason);
};

// File scope options that control how a file is opened/created and accessed
// while its open. We may add more options here in the future such as
// redundancy level, media to use etc.
struct FileOptions : EnvOptions {
  // Embedded IOOptions to control the parameters for any IOs that need
  // to be issued for the file open/creation
  IOOptions io_options;

  // EXPERIMENTAL
  // The feature is in development and is subject to change.
  // When creating a new file, set the temperature of the file so that
  // underlying file systems can put it with appropriate storage media and/or
  // coding.
  Temperature temperature = Temperature::kUnknown;

  // The checksum type that is used to calculate the checksum value for
  // handoff during file writes.
  ChecksumType handoff_checksum_type;

  FileOptions() : EnvOptions(), handoff_checksum_type(ChecksumType::kCRC32c) {}

  FileOptions(const DBOptions& opts)
      : EnvOptions(opts), handoff_checksum_type(ChecksumType::kCRC32c) {}

  FileOptions(const EnvOptions& opts)
      : EnvOptions(opts), handoff_checksum_type(ChecksumType::kCRC32c) {}

  FileOptions(const FileOptions& opts)
      : EnvOptions(opts),
        io_options(opts.io_options),
        temperature(opts.temperature),
        handoff_checksum_type(opts.handoff_checksum_type) {}

  FileOptions& operator=(const FileOptions&) = default;
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

  // To be set by the underlying FileSystem implementation.
  std::string request_id;

  // In order to log required information in IO tracing for different
  // operations, Each bit in trace_data stores which corresponding info from
  // IODebugContext will be added in the trace. Foreg, if trace_data = 1, it
  // means bit at position 0 is set so TraceData::kRequestID (request_id) will
  // be logged in the trace record.
  //
  enum TraceData : char {
    // The value of each enum represents the bitwise position for
    // that information in trace_data which will be used by IOTracer for
    // tracing. Make sure to add them sequentially.
    kRequestID = 0,
  };
  uint64_t trace_data = 0;

  IODebugContext() {}

  void AddCounter(std::string& name, uint64_t value) {
    counters.emplace(name, value);
  }

  // Called by underlying file system to set request_id and log request_id in
  // IOTracing.
  void SetRequestId(const std::string& _request_id) {
    request_id = _request_id;
    trace_data |= (1 << TraceData::kRequestID);
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

// IOHandle is used by underlying file system to store any information it needs
// during Async Read requests.
using IOHandleDeleter = std::function<void(void*)>;
using IOHandle = std::unique_ptr<void, IOHandleDeleter>;

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
// NewCompositeEnv can be used to create an Env with a custom FileSystem for
// DBOptions::env.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class FileSystem : public Customizable {
 public:
  FileSystem();

  // No copying allowed
  FileSystem(const FileSystem&) = delete;

  virtual ~FileSystem();

  static const char* Type() { return "FileSystem"; }
  static const char* kDefaultName() { return "DefaultFileSystem"; }

  // Loads the FileSystem specified by the input value into the result
  // The CreateFromString alternative should be used; this method may be
  // deprecated in a future release.
  static Status Load(const std::string& value,
                     std::shared_ptr<FileSystem>* result);

  // Loads the FileSystem specified by the input value into the result
  // @see Customizable for a more detailed description of the parameters and
  // return codes
  // @param config_options Controls how the FileSystem is loaded
  // @param value The name and optional properties describing the file system
  //      to load.
  // @param result On success, returns the loaded FileSystem
  // @return OK if the FileSystem was successfully loaded.
  // @return not-OK if the load failed.
  static Status CreateFromString(const ConfigOptions& options,
                                 const std::string& value,
                                 std::shared_ptr<FileSystem>* result);

  // Return a default FileSystem suitable for the current operating
  // system.
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

  // Create an object that writes to a file with the specified name.
  // `FSWritableFile::Append()`s will append after any existing content.  If the
  // file does not already exist, creates it.
  //
  // On success, stores a pointer to the file in *result and returns OK.  On
  // failure stores nullptr in *result and returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual IOStatus ReopenWritableFile(
      const std::string& /*fname*/, const FileOptions& /*options*/,
      std::unique_ptr<FSWritableFile>* /*result*/, IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported("ReopenWritableFile");
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

// This seems to clash with a macro on Windows, so #undef it here
#ifdef DeleteFile
#undef DeleteFile
#endif
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
  // informational messages. Derived classes can override to provide custom
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

  // OptimizeForBlobFileRead will create a new FileOptions object that
  // is a copy of the FileOptions in the parameters, but is optimized for
  // reading blob files.
  virtual FileOptions OptimizeForBlobFileRead(
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
    return IOStatus::NotSupported("GetFreeSpace");
  }

  virtual IOStatus IsDirectory(const std::string& /*path*/,
                               const IOOptions& options, bool* is_dir,
                               IODebugContext* /*dgb*/) = 0;

  // EXPERIMENTAL
  // Poll for completion of read IO requests. The Poll() method should call the
  // callback functions to indicate completion of read requests. If Poll is not
  // supported it means callee should be informed of IO completions via the
  // callback on another thread.
  //
  // Default implementation is to return IOStatus::OK.

  virtual IOStatus Poll(std::vector<IOHandle*>& /*io_handles*/,
                        size_t /*min_completions*/) {
    return IOStatus::OK();
  }

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
  // After call, result->size() < n only if end of file has been
  // reached (or non-OK status). Read might fail if called again after
  // first result->size() < n.
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
    return IOStatus::NotSupported("PositionedRead");
  }

  // EXPERIMENTAL
  // When available, returns the actual temperature for the file. This is
  // useful in case some outside process moves a file from one tier to another,
  // though the temperature is generally expected not to change while a file is
  // open.
  virtual Temperature GetTemperature() const { return Temperature::kUnknown; }

  // If you're adding methods here, remember to add them to
  // SequentialFileWrapper too.
};

// A read IO request structure for use in MultiRead and asynchronous Read APIs.
struct FSReadRequest {
  // Input parameter that represents the file offset in bytes.
  uint64_t offset;

  // Input parameter that represents the length to read in bytes. `result` only
  // returns fewer bytes if end of file is hit (or `status` is not OK).
  size_t len;

  // A buffer that MultiRead()  can optionally place data in. It can
  // ignore this and allocate its own buffer.
  // The lifecycle of scratch will be until IO is completed.
  //
  // In case of asynchronous reads, its an output parameter and it will be
  // maintained until callback has been called. Scratch is allocated by RocksDB
  // and will be passed to underlying FileSystem.
  char* scratch;

  // Output parameter set by MultiRead() to point to the data buffer, and
  // the number of valid bytes
  //
  // In case of asynchronous reads, this output parameter is set by Async Read
  // APIs to point to the data buffer, and
  // the number of valid bytes.
  // Slice result should point to scratch i.e the data should
  // always be read into scratch.
  Slice result;

  // Output parameter set by underlying FileSystem that represents status of
  // read request.
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
  // After call, result->size() < n only if end of file has been
  // reached (or non-OK status). Read might fail if called again after
  // first result->size() < n.
  //
  // Safe for concurrent use by multiple threads.
  // If Direct I/O enabled, offset, n, and scratch should be aligned properly.
  virtual IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                        Slice* result, char* scratch,
                        IODebugContext* dbg) const = 0;

  // Readahead the file starting from offset by n bytes for caching.
  // If it's not implemented (default: `NotSupported`), RocksDB will create
  // internal prefetch buffer to improve read performance.
  virtual IOStatus Prefetch(uint64_t /*offset*/, size_t /*n*/,
                            const IOOptions& /*options*/,
                            IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported("Prefetch");
  }

  // Read a bunch of blocks as described by reqs. The blocks can
  // optionally be read in parallel. This is a synchronous call, i.e it
  // should return after all reads have completed. The reads will be
  // non-overlapping but can be in any order. If the function return Status
  // is not ok, status of individual requests will be ignored and return
  // status will be assumed for all read requests. The function return status
  // is only meant for errors that occur before processing individual read
  // requests.
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

  // EXPERIMENTAL
  // This API reads the requested data in FSReadRequest asynchronously. This is
  // a asynchronous call, i.e it should return after submitting the request.
  //
  // When the read request is completed, callback function specified in cb
  // should be called with arguments cb_arg and the result populated in
  // FSReadRequest with result and status fileds updated by FileSystem.
  // cb_arg should be used by the callback to track the original request
  // submitted.
  //
  // This API should also populate IOHandle which should be used by
  // underlying FileSystem to store the context in order to distinguish the read
  // requests at their side.
  //
  // req contains the request offset and size passed as input parameter of read
  // request and result and status fields are output parameter set by underlying
  // FileSystem. The data should always be read into scratch field.
  //
  // Default implementation is to read the data synchronously.
  virtual IOStatus ReadAsync(
      FSReadRequest& req, const IOOptions& opts,
      std::function<void(const FSReadRequest&, void*)> cb, void* cb_arg,
      IOHandle* /*io_handle*/, IODebugContext* dbg) {
    req.status =
        Read(req.offset, req.len, opts, &(req.result), req.scratch, dbg);
    cb(req, cb_arg);
    return IOStatus::OK();
  }

  // EXPERIMENTAL
  // When available, returns the actual temperature for the file. This is
  // useful in case some outside process moves a file from one tier to another,
  // though the temperature is generally expected not to change while a file is
  // open.
  virtual Temperature GetTemperature() const { return Temperature::kUnknown; }

  // If you're adding methods here, remember to add them to
  // RandomAccessFileWrapper too.
};

// A data structure brings the data verification information, which is
// used together with data being written to a file.
struct DataVerificationInfo {
  // checksum of the data being written.
  Slice checksum;
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
  // Note: A WriteableFile object must support either Append or
  // PositionedAppend, so the users cannot mix the two.
  virtual IOStatus Append(const Slice& data, const IOOptions& options,
                          IODebugContext* dbg) = 0;

  // Append data with verification information.
  // Note that this API change is experimental and it might be changed in
  // the future. Currently, RocksDB only generates crc32c based checksum for
  // the file writes when the checksum handoff option is set.
  // Expected behavior: if the handoff_checksum_type in FileOptions (currently,
  // ChecksumType::kCRC32C is set as default) is not supported by this
  // FSWritableFile, the information in DataVerificationInfo can be ignored
  // (i.e. does not perform checksum verification).
  virtual IOStatus Append(const Slice& data, const IOOptions& options,
                          const DataVerificationInfo& /* verification_info */,
                          IODebugContext* dbg) {
    return Append(data, options, dbg);
  }

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
    return IOStatus::NotSupported("PositionedAppend");
  }

  // PositionedAppend data with verification information.
  // Note that this API change is experimental and it might be changed in
  // the future. Currently, RocksDB only generates crc32c based checksum for
  // the file writes when the checksum handoff option is set.
  // Expected behavior: if the handoff_checksum_type in FileOptions (currently,
  // ChecksumType::kCRC32C is set as default) is not supported by this
  // FSWritableFile, the information in DataVerificationInfo can be ignored
  // (i.e. does not perform checksum verification).
  virtual IOStatus PositionedAppend(
      const Slice& /* data */, uint64_t /* offset */,
      const IOOptions& /*options*/,
      const DataVerificationInfo& /* verification_info */,
      IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported("PositionedAppend");
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
               block_size * num_spanned_blocks, options, dbg)
          .PermitUncheckedError();
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
  //
  // After call, result->size() < n only if end of file has been
  // reached (or non-OK status). Read might fail if called again after
  // first result->size() < n.
  //
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

  // EXPERIMENTAL
  // When available, returns the actual temperature for the file. This is
  // useful in case some outside process moves a file from one tier to another,
  // though the temperature is generally expected not to change while a file is
  // open.
  virtual Temperature GetTemperature() const { return Temperature::kUnknown; }

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

  // FsyncWithDirOptions after renaming a file. Depends on the filesystem, it
  // may fsync directory or just the renaming file (e.g. btrfs). By default, it
  // just calls directory fsync.
  virtual IOStatus FsyncWithDirOptions(
      const IOOptions& options, IODebugContext* dbg,
      const DirFsyncOptions& /*dir_fsync_options*/) {
    return Fsync(options, dbg);
  }

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
  explicit FileSystemWrapper(const std::shared_ptr<FileSystem>& t);
  ~FileSystemWrapper() override {}

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
  FileOptions OptimizeForBlobFileRead(
      const FileOptions& file_options,
      const ImmutableDBOptions& db_options) const override {
    return target_->OptimizeForBlobFileRead(file_options, db_options);
  }
  IOStatus GetFreeSpace(const std::string& path, const IOOptions& options,
                        uint64_t* diskfree, IODebugContext* dbg) override {
    return target_->GetFreeSpace(path, options, diskfree, dbg);
  }
  IOStatus IsDirectory(const std::string& path, const IOOptions& options,
                       bool* is_dir, IODebugContext* dbg) override {
    return target_->IsDirectory(path, options, is_dir, dbg);
  }

  const Customizable* Inner() const override { return target_.get(); }
  Status PrepareOptions(const ConfigOptions& options) override;
#ifndef ROCKSDB_LITE
  std::string SerializeOptions(const ConfigOptions& config_options,
                               const std::string& header) const override;
#endif  // ROCKSDB_LITE

  virtual IOStatus Poll(std::vector<IOHandle*>& io_handles,
                        size_t min_completions) override {
    return target_->Poll(io_handles, min_completions);
  }

 protected:
  std::shared_ptr<FileSystem> target_;
};

class FSSequentialFileWrapper : public FSSequentialFile {
 public:
  // Creates a FileWrapper around the input File object and without
  // taking ownership of the object
  explicit FSSequentialFileWrapper(FSSequentialFile* t) : target_(t) {}

  FSSequentialFile* target() const { return target_; }

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
  Temperature GetTemperature() const override {
    return target_->GetTemperature();
  }

 private:
  FSSequentialFile* target_;
};

class FSSequentialFileOwnerWrapper : public FSSequentialFileWrapper {
 public:
  // Creates a FileWrapper around the input File object and takes
  // ownership of the object
  explicit FSSequentialFileOwnerWrapper(std::unique_ptr<FSSequentialFile>&& t)
      : FSSequentialFileWrapper(t.get()), guard_(std::move(t)) {}

 private:
  std::unique_ptr<FSSequentialFile> guard_;
};

class FSRandomAccessFileWrapper : public FSRandomAccessFile {
 public:
  // Creates a FileWrapper around the input File object and without
  // taking ownership of the object
  explicit FSRandomAccessFileWrapper(FSRandomAccessFile* t) : target_(t) {}

  FSRandomAccessFile* target() const { return target_; }

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
  IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                     std::function<void(const FSReadRequest&, void*)> cb,
                     void* cb_arg, IOHandle* io_handle,
                     IODebugContext* dbg) override {
    return target()->ReadAsync(req, opts, cb, cb_arg, io_handle, dbg);
  }
  Temperature GetTemperature() const override {
    return target_->GetTemperature();
  }

 private:
  std::unique_ptr<FSRandomAccessFile> guard_;
  FSRandomAccessFile* target_;
};

class FSRandomAccessFileOwnerWrapper : public FSRandomAccessFileWrapper {
 public:
  // Creates a FileWrapper around the input File object and takes
  // ownership of the object
  explicit FSRandomAccessFileOwnerWrapper(
      std::unique_ptr<FSRandomAccessFile>&& t)
      : FSRandomAccessFileWrapper(t.get()), guard_(std::move(t)) {}

 private:
  std::unique_ptr<FSRandomAccessFile> guard_;
};

class FSWritableFileWrapper : public FSWritableFile {
 public:
  // Creates a FileWrapper around the input File object and without
  // taking ownership of the object
  explicit FSWritableFileWrapper(FSWritableFile* t) : target_(t) {}

  FSWritableFile* target() const { return target_; }

  IOStatus Append(const Slice& data, const IOOptions& options,
                  IODebugContext* dbg) override {
    return target_->Append(data, options, dbg);
  }
  IOStatus Append(const Slice& data, const IOOptions& options,
                  const DataVerificationInfo& verification_info,
                  IODebugContext* dbg) override {
    return target_->Append(data, options, verification_info, dbg);
  }
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override {
    return target_->PositionedAppend(data, offset, options, dbg);
  }
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            const DataVerificationInfo& verification_info,
                            IODebugContext* dbg) override {
    return target_->PositionedAppend(data, offset, options, verification_info,
                                     dbg);
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

class FSWritableFileOwnerWrapper : public FSWritableFileWrapper {
 public:
  // Creates a FileWrapper around the input File object and takes
  // ownership of the object
  explicit FSWritableFileOwnerWrapper(std::unique_ptr<FSWritableFile>&& t)
      : FSWritableFileWrapper(t.get()), guard_(std::move(t)) {}

 private:
  std::unique_ptr<FSWritableFile> guard_;
};

class FSRandomRWFileWrapper : public FSRandomRWFile {
 public:
  // Creates a FileWrapper around the input File object and without
  // taking ownership of the object
  explicit FSRandomRWFileWrapper(FSRandomRWFile* t) : target_(t) {}

  FSRandomRWFile* target() const { return target_; }

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
  Temperature GetTemperature() const override {
    return target_->GetTemperature();
  }

 private:
  FSRandomRWFile* target_;
};

class FSRandomRWFileOwnerWrapper : public FSRandomRWFileWrapper {
 public:
  // Creates a FileWrapper around the input File object and takes
  // ownership of the object
  explicit FSRandomRWFileOwnerWrapper(std::unique_ptr<FSRandomRWFile>&& t)
      : FSRandomRWFileWrapper(t.get()), guard_(std::move(t)) {}

 private:
  std::unique_ptr<FSRandomRWFile> guard_;
};

class FSDirectoryWrapper : public FSDirectory {
 public:
  // Creates a FileWrapper around the input File object and takes
  // ownership of the object
  explicit FSDirectoryWrapper(std::unique_ptr<FSDirectory>&& t)
      : guard_(std::move(t)) {
    target_ = guard_.get();
  }

  // Creates a FileWrapper around the input File object and without
  // taking ownership of the object
  explicit FSDirectoryWrapper(FSDirectory* t) : target_(t) {}

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Fsync(options, dbg);
  }

  IOStatus FsyncWithDirOptions(
      const IOOptions& options, IODebugContext* dbg,
      const DirFsyncOptions& dir_fsync_options) override {
    return target_->FsyncWithDirOptions(options, dbg, dir_fsync_options);
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }

 private:
  std::unique_ptr<FSDirectory> guard_;
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
