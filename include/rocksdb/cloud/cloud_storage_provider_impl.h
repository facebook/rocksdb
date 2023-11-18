// Copyright (c) 2017 Rockset

#pragma once

#ifndef ROCKSDB_LITE
#include "rocksdb/cloud/cloud_storage_provider.h"
#include <optional>

namespace ROCKSDB_NAMESPACE {
class CloudStorageReadableFileImpl : public CloudStorageReadableFile {
 public:
  CloudStorageReadableFileImpl(Logger* info_log, const std::string& bucket,
                               const std::string& fname, uint64_t size);
  // sequential access, read data at current offset in file
  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override;

  // random access, read data from specified offset in file
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  IOStatus Skip(uint64_t n) override;

 protected:
  virtual IOStatus DoCloudRead(uint64_t offset, size_t n,
                               const IOOptions& options, char* scratch,
                               uint64_t* bytes_read,
                               IODebugContext* dbg) const = 0;

  Logger* info_log_;
  std::string bucket_;
  std::string fname_;
  uint64_t offset_;
  uint64_t file_size_;
};

// Appends to a file in S3.
class CloudStorageWritableFileImpl : public CloudStorageWritableFile {
 protected:
  CloudFileSystem* cfs_;
  const char* class_;
  std::string fname_;
  std::string tmp_file_;
  IOStatus status_;
  std::unique_ptr<FSWritableFile> local_file_;
  std::string bucket_;
  std::string cloud_fname_;
  bool is_manifest_;

 public:
  CloudStorageWritableFileImpl(CloudFileSystem* fs,
                               const std::string& local_fname,
                               const std::string& bucket,
                               const std::string& cloud_fname,
                               const FileOptions& file_opts);

  virtual ~CloudStorageWritableFileImpl();
  using CloudStorageWritableFile::Append;
  IOStatus Append(const Slice& data, const IOOptions& opts,
                  IODebugContext* dbg) override {
    assert(status_.ok());
    // write to temporary file
    return local_file_->Append(data, opts, dbg);
  }

  using CloudStorageWritableFile::PositionedAppend;
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& opts,
                            IODebugContext* dbg) override {
    return local_file_->PositionedAppend(data, offset, opts, dbg);
  }
  IOStatus Truncate(uint64_t size, const IOOptions& opts,
                    IODebugContext* dbg) override {
    return local_file_->Truncate(size, opts, dbg);
  }
  IOStatus Fsync(const IOOptions& opts, IODebugContext* dbg) override {
    return local_file_->Fsync(opts, dbg);
  }
  bool IsSyncThreadSafe() const override {
    return local_file_->IsSyncThreadSafe();
  }
  bool use_direct_io() const override { return local_file_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return local_file_->GetRequiredBufferAlignment();
  }
  uint64_t GetFileSize(const IOOptions& opts, IODebugContext* dbg) override {
    return local_file_->GetFileSize(opts, dbg);
  }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return local_file_->GetUniqueId(id, max_size);
  }
  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return local_file_->InvalidateCache(offset, length);
  }
  IOStatus RangeSync(uint64_t offset, uint64_t nbytes, const IOOptions& opts,
                     IODebugContext* dbg) override {
    return local_file_->RangeSync(offset, nbytes, opts, dbg);
  }
  IOStatus Allocate(uint64_t offset, uint64_t len, const IOOptions& opts,
                    IODebugContext* dbg) override {
    return local_file_->Allocate(offset, len, opts, dbg);
  }

  IOStatus Flush(const IOOptions& opts, IODebugContext* dbg) override {
    assert(status_.ok());
    return local_file_->Flush(opts, dbg);
  }
  IOStatus status() override { return status_; }
  IOStatus Sync(const IOOptions& opts, IODebugContext* dbg) override;
  IOStatus Close(const IOOptions& opts, IODebugContext* dbg) override;
};

// All writes to this DB can be configured to be persisted
// in cloud storage.
//
class Random64;
class CloudStorageProviderImpl : public CloudStorageProvider {
 public:
  static Status CreateS3Provider(std::unique_ptr<CloudStorageProvider>* result);
  static const char* kS3() { return "s3"; }

  CloudStorageProviderImpl();
  virtual ~CloudStorageProviderImpl();
  IOStatus GetCloudObject(const std::string& bucket_name,
                          const std::string& object_path,
                          const std::string& local_destination) override;
  IOStatus PutCloudObject(const std::string& local_file,
                          const std::string& bucket_name,
                          const std::string& object_path) override;
  IOStatus NewCloudReadableFile(
      const std::string& bucket, const std::string& fname,
      const FileOptions& options,
      std::unique_ptr<CloudStorageReadableFile>* result,
      IODebugContext* dbg) override;
  Status PrepareOptions(const ConfigOptions& options) override;

 protected:
  std::unique_ptr<Random64> rng_;
  virtual IOStatus DoNewCloudReadableFile(
      const std::string& bucket, const std::string& fname, uint64_t fsize,
      const std::string& content_hash, const FileOptions& options,
      std::unique_ptr<CloudStorageReadableFile>* result,
      IODebugContext* dbg) = 0;

  // Downloads object from the cloud into a local directory
  virtual IOStatus DoGetCloudObject(const std::string& bucket_name,
                                    const std::string& object_path,
                                    const std::string& local_path,
                                    uint64_t* remote_size) = 0;
  virtual IOStatus DoPutCloudObject(const std::string& local_file,
                                    const std::string& object_path,
                                    const std::string& bucket_name,
                                    uint64_t file_size) = 0;

  CloudFileSystem* cfs_;
  Status status_;
};
}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
