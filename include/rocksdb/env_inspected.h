// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#pragma once
#ifndef ROCKSDB_LITE

#include <memory>
#include <string>

#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

// Interface to inspect storage requests. FileSystemInspectedEnv will consult
// FileSystemInspector before issuing actual disk IO.
class FileSystemInspector {
 public:
  virtual ~FileSystemInspector() = default;

  virtual Status Read(size_t len, size_t* allowed) = 0;
  virtual Status Write(size_t len, size_t* allowed) = 0;
};

// An Env with underlying IO requests being inspected. It holds a reference to
// an external FileSystemInspector to consult for IO inspection.
class FileSystemInspectedEnv : public EnvWrapper {
 public:
  FileSystemInspectedEnv(Env* base_env,
                         std::shared_ptr<FileSystemInspector>& inspector);

  Status NewSequentialFile(const std::string& fname,
                           std::unique_ptr<SequentialFile>* result,
                           const EnvOptions& options) override;
  Status NewRandomAccessFile(const std::string& fname,
                             std::unique_ptr<RandomAccessFile>* result,
                             const EnvOptions& options) override;
  Status NewWritableFile(const std::string& fname,
                         std::unique_ptr<WritableFile>* result,
                         const EnvOptions& options) override;
  Status ReopenWritableFile(const std::string& fname,
                            std::unique_ptr<WritableFile>* result,
                            const EnvOptions& options) override;
  Status ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           std::unique_ptr<WritableFile>* result,
                           const EnvOptions& options) override;
  Status NewRandomRWFile(const std::string& fname,
                         std::unique_ptr<RandomRWFile>* result,
                         const EnvOptions& options) override;

 private:
  const std::shared_ptr<FileSystemInspector> inspector_;
};

extern Env* NewFileSystemInspectedEnv(
    Env* base_env, std::shared_ptr<FileSystemInspector> inspector);

}  // namespace ROCKSDB_NAMESPACE

#endif  // !ROCKSDB_LITE
