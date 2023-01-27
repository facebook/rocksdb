//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/injection_fs.h"

namespace ROCKSDB_NAMESPACE {
IOStatus InjectionFileSystem::NewSequentialFile(
    const std::string& f, const FileOptions& options,
    std::unique_ptr<FSSequentialFile>* r, IODebugContext* dbg) {
  std::unique_ptr<FSSequentialFile> base;
  auto rv = FileSystemWrapper::NewSequentialFile(f, options, &base, dbg);
  if (rv.ok()) {
    r->reset(new InjectionSequentialFile(std::move(base), this));
  }
  return rv;
}

IOStatus InjectionFileSystem::NewRandomAccessFile(
    const std::string& f, const FileOptions& file_opts,
    std::unique_ptr<FSRandomAccessFile>* r, IODebugContext* dbg) {
  std::unique_ptr<FSRandomAccessFile> base;
  auto rv = FileSystemWrapper::NewRandomAccessFile(f, file_opts, &base, dbg);
  if (rv.ok()) {
    r->reset(new InjectionRandomAccessFile(std::move(base), this));
  }
  return rv;
}

IOStatus InjectionFileSystem::NewWritableFile(
    const std::string& f, const FileOptions& options,
    std::unique_ptr<FSWritableFile>* r, IODebugContext* dbg) {
  std::unique_ptr<FSWritableFile> base;
  auto rv = FileSystemWrapper::NewWritableFile(f, options, &base, dbg);
  if (rv.ok()) {
    r->reset(new InjectionWritableFile(std::move(base), this));
  }
  return rv;
}

IOStatus InjectionFileSystem::ReopenWritableFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  std::unique_ptr<FSWritableFile> base;
  auto rv = FileSystemWrapper::ReopenWritableFile(fname, options, &base, dbg);
  if (rv.ok()) {
    result->reset(new InjectionWritableFile(std::move(base), this));
  }
  return rv;
}

IOStatus InjectionFileSystem::ReuseWritableFile(
    const std::string& fname, const std::string& old_fname,
    const FileOptions& file_opts, std::unique_ptr<FSWritableFile>* result,
    IODebugContext* dbg) {
  std::unique_ptr<FSWritableFile> base;
  auto rv = FileSystemWrapper::ReuseWritableFile(fname, old_fname, file_opts,
                                                 &base, dbg);
  if (rv.ok()) {
    result->reset(new InjectionWritableFile(std::move(base), this));
  }
  return rv;
}

IOStatus InjectionFileSystem::NewRandomRWFile(
    const std::string& name, const FileOptions& options,
    std::unique_ptr<FSRandomRWFile>* result, IODebugContext* dbg) {
  std::unique_ptr<FSRandomRWFile> base;
  auto rv = FileSystemWrapper::NewRandomRWFile(name, options, &base, dbg);
  if (rv.ok()) {
    result->reset(new InjectionRandomRWFile(std::move(base), this));
  }
  return rv;
}

IOStatus InjectionFileSystem::NewDirectory(const std::string& name,
                                           const IOOptions& io_opts,
                                           std::unique_ptr<FSDirectory>* result,
                                           IODebugContext* dbg) {
  std::unique_ptr<FSDirectory> base;
  auto rv = FileSystemWrapper::NewDirectory(name, io_opts, &base, dbg);
  if (rv.ok()) {
    result->reset(new InjectionDirectory(std::move(base), this));
  }
  return rv;
}

}  // namespace ROCKSDB_NAMESPACE
