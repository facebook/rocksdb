// Copyright (c) 2017 Rockset

#pragma once

#ifndef ROCKSDB_LITE
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {
class CloudStorageReadableFileImpl : public CloudStorageReadableFile {
 public:
  CloudStorageReadableFileImpl(Logger* info_log, const std::string& bucket,
                               const std::string& fname, uint64_t size);
  // sequential access, read data at current offset in file
  virtual Status Read(size_t n, Slice* result, char* scratch) override;

  // random access, read data from specified offset in file
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const override;

  virtual Status Skip(uint64_t n) override;

 protected:
  virtual Status DoCloudRead(uint64_t offset, size_t n, char* scratch,
                             uint64_t* bytes_read) const = 0;

  Logger* info_log_;
  std::string bucket_;
  std::string fname_;
  uint64_t offset_;
  uint64_t file_size_;
};

// Appends to a file in S3.
class CloudStorageWritableFileImpl : public CloudStorageWritableFile {
 protected:
  CloudEnv* env_;
  const char* class_;
  std::string fname_;
  std::string tmp_file_;
  Status status_;
  std::unique_ptr<WritableFile> local_file_;
  std::string bucket_;
  std::string cloud_fname_;
  bool is_manifest_;

 public:
  CloudStorageWritableFileImpl(CloudEnv* env, const std::string& local_fname,
                               const std::string& bucket,
                               const std::string& cloud_fname,
                               const EnvOptions& options);

  virtual ~CloudStorageWritableFileImpl();
  virtual Status Append(const Slice& data) override {
    assert(status_.ok());
    // write to temporary file
    return local_file_->Append(data);
  }

  Status PositionedAppend(const Slice& data, uint64_t offset) override {
    return local_file_->PositionedAppend(data, offset);
  }
  Status Truncate(uint64_t size) override {
    return local_file_->Truncate(size);
  }
  Status Fsync() override { return local_file_->Fsync(); }
  bool IsSyncThreadSafe() const override {
    return local_file_->IsSyncThreadSafe();
  }
  bool use_direct_io() const override { return local_file_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return local_file_->GetRequiredBufferAlignment();
  }
  uint64_t GetFileSize() override { return local_file_->GetFileSize(); }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return local_file_->GetUniqueId(id, max_size);
  }
  Status InvalidateCache(size_t offset, size_t length) override {
    return local_file_->InvalidateCache(offset, length);
  }
  Status RangeSync(uint64_t offset, uint64_t nbytes) override {
    return local_file_->RangeSync(offset, nbytes);
  }
  Status Allocate(uint64_t offset, uint64_t len) override {
    return local_file_->Allocate(offset, len);
  }

  virtual Status Flush() override {
    assert(status_.ok());
    return local_file_->Flush();
  }
  virtual Status status() override { return status_; }
  virtual Status Sync() override;
  virtual Status Close() override;
};

// All writes to this DB can be configured to be persisted
// in cloud storage.
//
class CloudStorageProviderImpl : public CloudStorageProvider {
 public:
  static Status CreateS3Provider(std::unique_ptr<CloudStorageProvider>* result);
  static const char* kS3() { return "s3"; }

  CloudStorageProviderImpl();
  virtual ~CloudStorageProviderImpl();
  Status GetCloudObject(const std::string& bucket_name,
                        const std::string& object_path,
                        const std::string& local_destination) override;
  Status PutCloudObject(const std::string& local_file,
                        const std::string& bucket_name,
                        const std::string& object_path) override;
  Status NewCloudReadableFile(const std::string& bucket,
                              const std::string& fname,
                              std::unique_ptr<CloudStorageReadableFile>* result,
                              const EnvOptions& options) override;
  virtual Status PrepareOptions(const ConfigOptions& options) override;

 protected:
  Random64 rng_;
  virtual Status DoNewCloudReadableFile(
      const std::string& bucket, const std::string& fname, uint64_t fsize,
      const std::string& content_hash,
      std::unique_ptr<CloudStorageReadableFile>* result,
      const EnvOptions& options) = 0;

  // Downloads object from the cloud into a local directory
  virtual Status DoGetCloudObject(const std::string& bucket_name,
                                  const std::string& object_path,
                                  const std::string& local_path,
                                  uint64_t* remote_size) = 0;
  virtual Status DoPutCloudObject(const std::string& local_file,
                                  const std::string& object_path,
                                  const std::string& bucket_name,
                                  uint64_t file_size) = 0;

  CloudEnv* env_;
  Status status_;
};
}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
