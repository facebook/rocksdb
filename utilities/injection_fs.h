//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <memory>

#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
class Logger;

// A base FileSystem class that can interject into File APIs.
//
// This class creates specialized File classes (e.g. InjectionSequentialFile)
// that calls back into methods in this base class.  Implementations can
// override those base methods to inject their own code.  Example use cases
// for this class include injecting failures into file operations, counting
// or timing file operations, or skipping file operations.
//
// Derived classes should override the methods they wish to intercept.
// Additionally, derived classes must implement the Name() method.
class InjectionFileSystem : public FileSystemWrapper {
 public:
  explicit InjectionFileSystem(const std::shared_ptr<FileSystem>& base)
      : FileSystemWrapper(base) {}

  IOStatus NewSequentialFile(const std::string& f, const FileOptions& options,
                             std::unique_ptr<FSSequentialFile>* r,
                             IODebugContext* dbg) override;

  IOStatus NewRandomAccessFile(const std::string& f,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* r,
                               IODebugContext* dbg) override;

  IOStatus NewWritableFile(const std::string& f, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* r,
                           IODebugContext* dbg) override;
  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& options,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override;

  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* dbg) override;
  IOStatus NewRandomRWFile(const std::string& name, const FileOptions& options,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* dbg) override;

  IOStatus NewDirectory(const std::string& name, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override;

 protected:
  friend class InjectionSequentialFile;
  friend class InjectionRandomAccessFile;
  friend class InjectionWritableFile;
  friend class InjectionRandomRWFile;
  friend class InjectionDirectory;

  virtual IOStatus DoRead(FSSequentialFile* file, size_t n,
                          const IOOptions& options, Slice* result,
                          char* scratch, IODebugContext* dbg) {
    return file->Read(n, options, result, scratch, dbg);
  }

  virtual IOStatus DoPositionedRead(FSSequentialFile* file, uint64_t offset,
                                    size_t n, const IOOptions& options,
                                    Slice* result, char* scratch,
                                    IODebugContext* dbg) {
    return file->PositionedRead(offset, n, options, result, scratch, dbg);
  }

  virtual void DoClose(FSSequentialFile* /*file*/) {}

  virtual IOStatus DoRead(FSRandomAccessFile* file, uint64_t offset, size_t n,
                          const IOOptions& options, Slice* result,
                          char* scratch, IODebugContext* dbg) {
    return file->Read(offset, n, options, result, scratch, dbg);
  }

  virtual IOStatus DoMultiRead(FSRandomAccessFile* file, FSReadRequest* reqs,
                               size_t num_reqs, const IOOptions& options,
                               IODebugContext* dbg) {
    return file->MultiRead(reqs, num_reqs, options, dbg);
  }

  virtual IOStatus DoReadAsync(
      FSRandomAccessFile* file, FSReadRequest& req, const IOOptions& opts,
      std::function<void(const FSReadRequest&, void*)> cb, void* cb_arg,
      void** io_handle, IOHandleDeleter* del_fn, IODebugContext* dbg) {
    return file->ReadAsync(req, opts, cb, cb_arg, io_handle, del_fn, dbg);
  }

  virtual size_t DoGetUniqueId(FSRandomAccessFile* file, char* id,
                               size_t max_size) {
    return file->GetUniqueId(id, max_size);
  }

  virtual void DoClose(FSRandomAccessFile* /*file*/) {}

  virtual IOStatus DoAppend(FSWritableFile* file, const Slice& data,
                            const IOOptions& options, IODebugContext* dbg) {
    return file->Append(data, options, dbg);
  }

  virtual IOStatus DoAppend(FSWritableFile* file, const Slice& data,
                            const IOOptions& options,
                            const DataVerificationInfo& info,
                            IODebugContext* dbg) {
    return file->Append(data, options, info, dbg);
  }

  virtual IOStatus DoPositionedAppend(FSWritableFile* file, const Slice& data,
                                      uint64_t offset, const IOOptions& options,
                                      IODebugContext* dbg) {
    return file->PositionedAppend(data, offset, options, dbg);
  }

  virtual IOStatus DoPositionedAppend(FSWritableFile* file, const Slice& data,
                                      uint64_t offset, const IOOptions& options,
                                      const DataVerificationInfo& info,
                                      IODebugContext* dbg) {
    return file->PositionedAppend(data, offset, options, info, dbg);
  }

  virtual IOStatus DoTruncate(FSWritableFile* file, uint64_t size,
                              const IOOptions& options, IODebugContext* dbg) {
    return file->Truncate(size, options, dbg);
  }

  virtual IOStatus DoClose(FSWritableFile* file, const IOOptions& options,
                           IODebugContext* dbg) {
    return file->Close(options, dbg);
  }

  virtual IOStatus DoFlush(FSWritableFile* file, const IOOptions& options,
                           IODebugContext* dbg) {
    return file->Flush(options, dbg);
  }

  virtual IOStatus DoSync(FSWritableFile* file, const IOOptions& options,
                          IODebugContext* dbg) {
    return file->Sync(options, dbg);
  }

  virtual IOStatus DoFsync(FSWritableFile* file, const IOOptions& options,
                           IODebugContext* dbg) {
    return file->Fsync(options, dbg);
  }

  virtual IOStatus DoRangeSync(FSWritableFile* file, uint64_t offset,
                               uint64_t nbytes, const IOOptions& options,
                               IODebugContext* dbg) {
    return file->RangeSync(offset, nbytes, options, dbg);
  }

  virtual IOStatus DoWrite(FSRandomRWFile* file, uint64_t offset,
                           const Slice& data, const IOOptions& options,
                           IODebugContext* dbg) {
    return file->Write(offset, data, options, dbg);
  }

  virtual IOStatus DoRead(FSRandomRWFile* file, uint64_t offset, size_t n,
                          const IOOptions& options, Slice* result,
                          char* scratch, IODebugContext* dbg) {
    return file->Read(offset, n, options, result, scratch, dbg);
  }

  virtual IOStatus DoFlush(FSRandomRWFile* file, const IOOptions& options,
                           IODebugContext* dbg) {
    return file->Flush(options, dbg);
  }

  virtual IOStatus DoSync(FSRandomRWFile* file, const IOOptions& options,
                          IODebugContext* dbg) {
    return file->Sync(options, dbg);
  }

  virtual IOStatus DoFsync(FSRandomRWFile* file, const IOOptions& options,
                           IODebugContext* dbg) {
    return file->Fsync(options, dbg);
  }

  virtual IOStatus DoClose(FSRandomRWFile* file, const IOOptions& options,
                           IODebugContext* dbg) {
    return file->Close(options, dbg);
  }

  virtual IOStatus DoFsync(FSDirectory* dir, const IOOptions& options,
                           IODebugContext* dbg) {
    return dir->Fsync(options, dbg);
  }

  virtual IOStatus DoFsyncWithDirOptions(FSDirectory* dir,
                                         const IOOptions& options,
                                         IODebugContext* dbg,
                                         const DirFsyncOptions& dir_options) {
    return dir->FsyncWithDirOptions(options, dbg, dir_options);
  }

  virtual size_t DoGetUniqueId(FSDirectory* dir, char* id, size_t max_size) {
    return dir->GetUniqueId(id, max_size);
  }

  virtual IOStatus DoClose(FSDirectory* dir, const IOOptions& options,
                           IODebugContext* dbg) {
    return dir->Close(options, dbg);
  }
};

class InjectionSequentialFile : public FSSequentialFileOwnerWrapper {
 private:
  InjectionFileSystem* fs_;

 public:
  InjectionSequentialFile(std::unique_ptr<FSSequentialFile>&& f,
                          InjectionFileSystem* fs)
      : FSSequentialFileOwnerWrapper(std::move(f)), fs_(fs) {}

  ~InjectionSequentialFile() override { fs_->DoClose(target()); }

  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override {
    return fs_->DoRead(target(), n, options, result, scratch, dbg);
  }

  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override {
    return fs_->DoPositionedRead(target(), offset, n, options, result, scratch,
                                 dbg);
  }
};

class InjectionRandomAccessFile : public FSRandomAccessFileOwnerWrapper {
 private:
  InjectionFileSystem* fs_;

 public:
  InjectionRandomAccessFile(std::unique_ptr<FSRandomAccessFile>&& f,
                            InjectionFileSystem* fs)
      : FSRandomAccessFileOwnerWrapper(std::move(f)), fs_(fs) {}

  ~InjectionRandomAccessFile() override { fs_->DoClose(target()); }
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override {
    return fs_->DoRead(target(), offset, n, options, result, scratch, dbg);
  }

  IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& options, IODebugContext* dbg) override {
    return fs_->DoMultiRead(target(), reqs, num_reqs, options, dbg);
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return fs_->DoGetUniqueId(target(), id, max_size);
  }
};

class InjectionWritableFile : public FSWritableFileOwnerWrapper {
 private:
  InjectionFileSystem* fs_;

 public:
  InjectionWritableFile(std::unique_ptr<FSWritableFile>&& f,
                        InjectionFileSystem* fs)
      : FSWritableFileOwnerWrapper(std::move(f)), fs_(fs) {}

  IOStatus Append(const Slice& data, const IOOptions& options,
                  IODebugContext* dbg) override {
    return fs_->DoAppend(target(), data, options, dbg);
  }

  IOStatus Append(const Slice& data, const IOOptions& options,
                  const DataVerificationInfo& info,
                  IODebugContext* dbg) override {
    return fs_->DoAppend(target(), data, options, info, dbg);
  }

  IOStatus Truncate(uint64_t size, const IOOptions& options,
                    IODebugContext* dbg) override {
    return fs_->DoTruncate(target(), size, options, dbg);
  }

  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override {
    return fs_->DoPositionedAppend(target(), data, offset, options, dbg);
  }

  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            const DataVerificationInfo& info,
                            IODebugContext* dbg) override {
    return fs_->DoPositionedAppend(target(), data, offset, options, info, dbg);
  }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    return fs_->DoClose(target(), options, dbg);
  }

  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override {
    return fs_->DoFlush(target(), options, dbg);
  }

  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
    return fs_->DoSync(target(), options, dbg);
  }
  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    return fs_->DoFsync(target(), options, dbg);
  }
  IOStatus RangeSync(uint64_t offset, uint64_t nbytes, const IOOptions& options,
                     IODebugContext* dbg) override {
    return fs_->DoRangeSync(target(), offset, nbytes, options, dbg);
  }
};

class InjectionRandomRWFile : public FSRandomRWFileOwnerWrapper {
 private:
  mutable InjectionFileSystem* fs_;

 public:
  InjectionRandomRWFile(std::unique_ptr<FSRandomRWFile>&& f,
                        InjectionFileSystem* fs)
      : FSRandomRWFileOwnerWrapper(std::move(f)), fs_(fs) {}
  IOStatus Write(uint64_t offset, const Slice& data, const IOOptions& options,
                 IODebugContext* dbg) override {
    return fs_->DoWrite(target(), offset, data, options, dbg);
  }

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override {
    return fs_->DoRead(target(), offset, n, options, result, scratch, dbg);
  }

  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override {
    return fs_->DoFlush(target(), options, dbg);
  }

  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
    return fs_->DoSync(target(), options, dbg);
  }

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    return fs_->DoFsync(target(), options, dbg);
  }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    return fs_->DoClose(target(), options, dbg);
  }
};

class InjectionDirectory : public FSDirectoryWrapper {
 private:
  mutable InjectionFileSystem* fs_;
  bool closed_ = false;

 public:
  InjectionDirectory(std::unique_ptr<FSDirectory>&& f, InjectionFileSystem* fs)
      : FSDirectoryWrapper(std::move(f)), fs_(fs) {}

  ~InjectionDirectory() override {
    if (!closed_) {
      // TODO: fix DB+CF code to use explicit Close, not rely on destructor
      fs_->DoClose(target_, IOOptions(), nullptr).PermitUncheckedError();
    }
  }

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    return fs_->DoFsync(target_, options, dbg);
  }

  IOStatus FsyncWithDirOptions(const IOOptions& options, IODebugContext* dbg,
                               const DirFsyncOptions& dir_options) override {
    return fs_->DoFsyncWithDirOptions(target_, options, dbg, dir_options);
  }

  // Close directory
  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    auto io_s = fs_->DoClose(target_, options, dbg);
    if (io_s.ok()) {
      closed_ = true;
    }
    return io_s;
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return fs_->DoGetUniqueId(target_, id, max_size);
  }
};
}  // namespace ROCKSDB_NAMESPACE
