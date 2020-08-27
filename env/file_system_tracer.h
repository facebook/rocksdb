// Copyright (c) 2019-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/file_system.h"
#include "trace_replay/io_tracer.h"

namespace ROCKSDB_NAMESPACE {

// FileSystemTracingWrapper is a wrapper class above FileSystem that forwards
// the call to the underlying storage system. It then invokes IOTracer to record
// file operations and other contextual information in a binary format for
// tracing. It overrides methods we are interested in tracing and extends
// FileSystemWrapper, which forwards all methods that are not explicitly
// overridden.
class FileSystemTracingWrapper : public FileSystemWrapper {
 public:
  FileSystemTracingWrapper(std::shared_ptr<FileSystem> t,
                           std::shared_ptr<IOTracer> io_tracer)
      : FileSystemWrapper(t), io_tracer_(io_tracer), env_(Env::Default()) {}

  ~FileSystemTracingWrapper() override {}

  IOStatus NewWritableFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override;

  IOStatus NewDirectory(const std::string& name, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override;

  IOStatus GetChildren(const std::string& dir, const IOOptions& io_opts,
                       std::vector<std::string>* r,
                       IODebugContext* dbg) override;

  IOStatus DeleteFile(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;

  IOStatus CreateDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override;

  IOStatus CreateDirIfMissing(const std::string& dirname,
                              const IOOptions& options,
                              IODebugContext* dbg) override;

  IOStatus DeleteDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override;

  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override;

 private:
  std::shared_ptr<IOTracer> io_tracer_;
  Env* env_;
};

// The FileSystemPtr is a wrapper class that takes pointer to storage systems
// (such as posix filesystems). It overloads operator -> and returns a pointer
// of either FileSystem or FileSystemTracingWrapper based on whether tracing is
// enabled or  not. It is added to bypass FileSystemTracingWrapper when tracing
// is disabled.
class FileSystemPtr {
 public:
  FileSystemPtr(std::shared_ptr<FileSystem> fs,
                const std::shared_ptr<IOTracer>& io_tracer)
      : fs_(fs), io_tracer_(io_tracer) {
    fs_tracer_ = std::make_shared<FileSystemTracingWrapper>(fs_, io_tracer_);
  }

  std::shared_ptr<FileSystem> operator->() const {
    if (io_tracer_ && io_tracer_->is_tracing_enabled()) {
      return fs_tracer_;
    } else {
      return fs_;
    }
  }

  /* Returns the underlying File System pointer */
  FileSystem* get() const {
    if (io_tracer_ && io_tracer_->is_tracing_enabled()) {
      return fs_tracer_.get();
    } else {
      return fs_.get();
    }
  }

 private:
  std::shared_ptr<FileSystem> fs_;
  std::shared_ptr<IOTracer> io_tracer_;
  std::shared_ptr<FileSystemTracingWrapper> fs_tracer_;
};

// FSSequentialFileTracingWrapper is a wrapper class above FSSequentialFile that
// forwards the call to the underlying storage system. It then invokes IOTracer
// to record file operations and other contextual information in a binary format
// for tracing. It overrides methods we are interested in tracing and extends
// FSSequentialFileWrapper, which forwards all methods that are not explicitly
// overridden.
class FSSequentialFileTracingWrapper : public FSSequentialFileWrapper {
 public:
  FSSequentialFileTracingWrapper(FSSequentialFile* t,
                                 std::shared_ptr<IOTracer> io_tracer)
      : FSSequentialFileWrapper(t),
        io_tracer_(io_tracer),
        env_(Env::Default()) {}

  ~FSSequentialFileTracingWrapper() override {}

  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override;

  IOStatus InvalidateCache(size_t offset, size_t length) override;

  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override;

 private:
  std::shared_ptr<IOTracer> io_tracer_;
  Env* env_;
};

// The FSSequentialFilePtr is a wrapper class that takes pointer to storage
// systems (such as posix filesystems). It overloads operator -> and returns a
// pointer of either FSSequentialFile or FSSequentialFileTracingWrapper based on
// whether tracing is enabled or not. It is added to bypass
// FSSequentialFileTracingWrapper when tracing is disabled.
class FSSequentialFilePtr {
 public:
  FSSequentialFilePtr() = delete;
  FSSequentialFilePtr(std::unique_ptr<FSSequentialFile>&& fs,
                      const std::shared_ptr<IOTracer>& io_tracer)
      : fs_(std::move(fs)),
        io_tracer_(io_tracer),
        fs_tracer_(fs_.get(), io_tracer_) {}

  FSSequentialFile* operator->() const {
    if (io_tracer_ && io_tracer_->is_tracing_enabled()) {
      return const_cast<FSSequentialFileTracingWrapper*>(&fs_tracer_);
    } else {
      return fs_.get();
    }
  }

  FSSequentialFile* get() const {
    if (io_tracer_ && io_tracer_->is_tracing_enabled()) {
      return const_cast<FSSequentialFileTracingWrapper*>(&fs_tracer_);
    } else {
      return fs_.get();
    }
  }

 private:
  std::unique_ptr<FSSequentialFile> fs_;
  std::shared_ptr<IOTracer> io_tracer_;
  FSSequentialFileTracingWrapper fs_tracer_;
};

// FSRandomAccessFileTracingWrapper is a wrapper class above FSRandomAccessFile
// that forwards the call to the underlying storage system. It then invokes
// IOTracer to record file operations and other contextual information in a
// binary format for tracing. It overrides methods we are interested in tracing
// and extends FSRandomAccessFileWrapper, which forwards all methods that are
// not explicitly overridden.
class FSRandomAccessFileTracingWrapper : public FSRandomAccessFileWrapper {
 public:
  FSRandomAccessFileTracingWrapper(FSRandomAccessFile* t,
                                   std::shared_ptr<IOTracer> io_tracer)
      : FSRandomAccessFileWrapper(t),
        io_tracer_(io_tracer),
        env_(Env::Default()) {}

  ~FSRandomAccessFileTracingWrapper() override {}

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& options,
                    IODebugContext* dbg) override;

  IOStatus InvalidateCache(size_t offset, size_t length) override;

 private:
  std::shared_ptr<IOTracer> io_tracer_;
  Env* env_;
};

// The FSRandomAccessFilePtr is a wrapper class that takes pointer to storage
// systems (such as posix filesystems). It overloads operator -> and returns a
// pointer of either FSRandomAccessFile or FSRandomAccessFileTracingWrapper
// based on whether tracing is enabled or not. It is added to bypass
// FSRandomAccessFileTracingWrapper when tracing is disabled.
class FSRandomAccessFilePtr {
 public:
  FSRandomAccessFilePtr(std::unique_ptr<FSRandomAccessFile>&& fs,
                        const std::shared_ptr<IOTracer>& io_tracer)
      : fs_(std::move(fs)),
        io_tracer_(io_tracer),
        fs_tracer_(fs_.get(), io_tracer_) {}

  FSRandomAccessFile* operator->() const {
    if (io_tracer_ && io_tracer_->is_tracing_enabled()) {
      return const_cast<FSRandomAccessFileTracingWrapper*>(&fs_tracer_);
    } else {
      return fs_.get();
    }
  }

  FSRandomAccessFile* get() const {
    if (io_tracer_ && io_tracer_->is_tracing_enabled()) {
      return const_cast<FSRandomAccessFileTracingWrapper*>(&fs_tracer_);
    } else {
      return fs_.get();
    }
  }

 private:
  std::unique_ptr<FSRandomAccessFile> fs_;
  std::shared_ptr<IOTracer> io_tracer_;
  FSRandomAccessFileTracingWrapper fs_tracer_;
};

// FSWritableFileTracingWrapper is a wrapper class above FSWritableFile that
// forwards the call to the underlying storage system. It then invokes IOTracer
// to record file operations and other contextual information in a binary format
// for tracing. It overrides methods we are interested in tracing and extends
// FSWritableFileWrapper, which forwards all methods that are not explicitly
// overridden.
class FSWritableFileTracingWrapper : public FSWritableFileWrapper {
 public:
  FSWritableFileTracingWrapper(FSWritableFile* t,
                               std::shared_ptr<IOTracer> io_tracer)
      : FSWritableFileWrapper(t), io_tracer_(io_tracer), env_(Env::Default()) {}

  ~FSWritableFileTracingWrapper() override {}

  IOStatus Append(const Slice& data, const IOOptions& options,
                  IODebugContext* dbg) override;

  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override;

  IOStatus Truncate(uint64_t size, const IOOptions& options,
                    IODebugContext* dbg) override;

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override;

  uint64_t GetFileSize(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus InvalidateCache(size_t offset, size_t length) override;

 private:
  std::shared_ptr<IOTracer> io_tracer_;
  Env* env_;
};

// The FSWritableFilePtr is a wrapper class that takes pointer to storage
// systems (such as posix filesystems). It overloads operator -> and returns a
// pointer of either FSWritableFile or FSWritableFileTracingWrapper based on
// whether tracing is enabled or not. It is added to bypass
// FSWritableFileTracingWrapper when tracing is disabled.
class FSWritableFilePtr {
 public:
  FSWritableFilePtr(FSWritableFile* fs, std::shared_ptr<IOTracer> io_tracer)
      : fs_(fs),
        io_tracer_(io_tracer),
        fs_tracer_(new FSWritableFileTracingWrapper(fs_, io_tracer_)) {}

  explicit FSWritableFilePtr(FSWritableFile* fs)
      : fs_(fs), io_tracer_(nullptr), fs_tracer_(nullptr) {}

  FSWritableFile* operator->() const {
    if (io_tracer_ && io_tracer_->is_tracing_enabled()) {
      return fs_tracer_;
    } else {
      return fs_;
    }
  }

 private:
  FSWritableFile* fs_;
  std::shared_ptr<IOTracer> io_tracer_;
  FSWritableFileTracingWrapper* fs_tracer_;
};

// FSRandomRWFileTracingWrapper is a wrapper class above FSRandomRWFile that
// forwards the call to the underlying storage system. It then invokes IOTracer
// to record file operations and other contextual information in a binary format
// for tracing. It overrides methods we are interested in tracing and extends
// FSRandomRWFileWrapper, which forwards all methods that are not explicitly
// overridden.
class FSRandomRWFileTracingWrapper : public FSRandomRWFileWrapper {
 public:
  FSRandomRWFileTracingWrapper(FSRandomRWFile* t,
                               std::shared_ptr<IOTracer> io_tracer)
      : FSRandomRWFileWrapper(t), io_tracer_(io_tracer), env_(Env::Default()) {}

  ~FSRandomRWFileTracingWrapper() override {}

  IOStatus Write(uint64_t offset, const Slice& data, const IOOptions& options,
                 IODebugContext* dbg) override;

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

 private:
  std::shared_ptr<IOTracer> io_tracer_;
  Env* env_;
};

// The FSRandomRWFilePtr is a wrapper class that takes pointer to storage
// systems (such as posix filesystems). It overloads operator -> and returns a
// pointer of either FSRandomRWFile or FSRandomRWFileTracingWrapper based on
// whether tracing is enabled or not. It is added to bypass
// FSRandomRWFileTracingWrapper when tracing is disabled.
class FSRandomRWFilePtr {
 public:
  FSRandomRWFilePtr(FSRandomRWFile* fs, std::shared_ptr<IOTracer> io_tracer)
      : fs_(fs),
        io_tracer_(io_tracer),
        fs_tracer_(new FSRandomRWFileTracingWrapper(fs_, io_tracer_)) {}

  explicit FSRandomRWFilePtr(FSRandomRWFile* fs)
      : fs_(fs), io_tracer_(nullptr), fs_tracer_(nullptr) {}

  FSRandomRWFile* operator->() const {
    if (io_tracer_ && io_tracer_->is_tracing_enabled()) {
      return fs_tracer_;
    } else {
      return fs_;
    }
  }

 private:
  FSRandomRWFile* fs_;
  std::shared_ptr<IOTracer> io_tracer_;
  FSRandomRWFileTracingWrapper* fs_tracer_;
};

}  // namespace ROCKSDB_NAMESPACE
