//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/counted_fs.h"

#include <sstream>

#include "rocksdb/file_system.h"
#include "rocksdb/utilities/options_type.h"

namespace ROCKSDB_NAMESPACE {

std::string FileOpCounters::PrintCounters() const {
  std::stringstream ss;
  ss << "Num files opened: " << opens.load(std::memory_order_relaxed)
     << std::endl;
  ss << "Num files deleted: " << deletes.load(std::memory_order_relaxed)
     << std::endl;
  ss << "Num files renamed: " << renames.load(std::memory_order_relaxed)
     << std::endl;
  ss << "Num Flush(): " << flushes.load(std::memory_order_relaxed) << std::endl;
  ss << "Num Sync(): " << syncs.load(std::memory_order_relaxed) << std::endl;
  ss << "Num Fsync(): " << fsyncs.load(std::memory_order_relaxed) << std::endl;
  ss << "Num Dir Fsync(): " << dsyncs.load(std::memory_order_relaxed)
     << std::endl;
  ss << "Num Close(): " << closes.load(std::memory_order_relaxed) << std::endl;
  ss << "Num Dir Open(): " << dir_opens.load(std::memory_order_relaxed)
     << std::endl;
  ss << "Num Dir Close(): " << dir_closes.load(std::memory_order_relaxed)
     << std::endl;
  ss << "Num Read(): " << reads.ops.load(std::memory_order_relaxed)
     << std::endl;
  ss << "Num Append(): " << writes.ops.load(std::memory_order_relaxed)
     << std::endl;
  ss << "Num bytes read: " << reads.bytes.load(std::memory_order_relaxed)
     << std::endl;
  ss << "Num bytes written: " << writes.bytes.load(std::memory_order_relaxed)
     << std::endl;
  return ss.str();
}

CountedFileSystem::CountedFileSystem(const std::shared_ptr<FileSystem>& base)
    : InjectionFileSystem(base) {}

IOStatus CountedFileSystem::NewSequentialFile(
    const std::string& f, const FileOptions& options,
    std::unique_ptr<FSSequentialFile>* r, IODebugContext* dbg) {
  IOStatus s = InjectionFileSystem::NewSequentialFile(f, options, r, dbg);
  if (s.ok()) {
    counters_.opens++;
  }
  return s;
}

IOStatus CountedFileSystem::NewRandomAccessFile(
    const std::string& f, const FileOptions& options,
    std::unique_ptr<FSRandomAccessFile>* r, IODebugContext* dbg) {
  IOStatus s = InjectionFileSystem::NewRandomAccessFile(f, options, r, dbg);
  if (s.ok()) {
    counters_.opens++;
  }
  return s;
}

IOStatus CountedFileSystem::NewWritableFile(const std::string& f,
                                            const FileOptions& options,
                                            std::unique_ptr<FSWritableFile>* r,
                                            IODebugContext* dbg) {
  IOStatus s = InjectionFileSystem::NewWritableFile(f, options, r, dbg);
  if (s.ok()) {
    counters_.opens++;
  }
  return s;
}

IOStatus CountedFileSystem::ReopenWritableFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  IOStatus s =
      InjectionFileSystem::ReopenWritableFile(fname, options, result, dbg);
  if (s.ok()) {
    counters_.opens++;
  }
  return s;
}

IOStatus CountedFileSystem::ReuseWritableFile(
    const std::string& fname, const std::string& old_fname,
    const FileOptions& options, std::unique_ptr<FSWritableFile>* result,
    IODebugContext* dbg) {
  IOStatus s = InjectionFileSystem::ReuseWritableFile(fname, old_fname, options,
                                                      result, dbg);
  if (s.ok()) {
    counters_.opens++;
  }
  return s;
}

IOStatus CountedFileSystem::NewRandomRWFile(
    const std::string& name, const FileOptions& options,
    std::unique_ptr<FSRandomRWFile>* result, IODebugContext* dbg) {
  IOStatus s = InjectionFileSystem::NewRandomRWFile(name, options, result, dbg);
  if (s.ok()) {
    counters_.opens++;
  }
  return s;
}

IOStatus CountedFileSystem::NewDirectory(const std::string& name,
                                         const IOOptions& options,
                                         std::unique_ptr<FSDirectory>* result,
                                         IODebugContext* dbg) {
  IOStatus s = InjectionFileSystem::NewDirectory(name, options, result, dbg);
  if (s.ok()) {
    counters_.opens++;
    counters_.dir_opens++;
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
