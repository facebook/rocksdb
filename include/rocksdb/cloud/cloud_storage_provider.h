//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once

#include <unordered_map>

#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace rocksdb {
class CloudEnv;
class CloudStorageProvider;
class Logger;
struct ColumnFamilyOptions;
struct DBOptions;

class CloudStorageReadableFile : virtual public SequentialFile,
                                 virtual public RandomAccessFile {
 public:
  virtual const char* Name() const { return "cloud"; }
};

// Appends to a file in S3.
class CloudStorageWritableFile : public WritableFile {
 public:
  virtual ~CloudStorageWritableFile() {}
  virtual Status status() = 0;

  virtual const char* Name() const { return "cloud"; }
};

// A CloudStorageProvider provides the interface to the cloud object
// store.  Methods can create and empty buckets, as well as other
// standard bucket object operations get/put/list/delete

class CloudStorageProvider {
 public:
  virtual ~CloudStorageProvider();
  static const char* Type() { return "CloudStorageProvider"; }
  virtual const char* Name() const { return "cloud"; }
  virtual Status CreateBucket(const std::string& bucket_name) = 0;
  virtual Status ExistsBucket(const std::string& bucket_name) = 0;

  // Empties all contents of the associated cloud storage bucket.
  virtual Status EmptyBucket(const std::string& bucket_name,
                             const std::string& object_path) = 0;
  // Delete the specified object from the specified cloud bucket
  virtual Status DeleteObject(const std::string& bucket_name,
                              const std::string& object_path) = 0;

  // Does the specified object exist in the cloud storage
  // returns all the objects that have the specified path prefix and
  // are stored in a cloud bucket
  virtual Status ListObjects(const std::string& bucket_name,
                             const std::string& object_path,
                             std::vector<std::string>* path_names) = 0;

  // Does the specified object exist in the cloud storage
  virtual Status ExistsObject(const std::string& bucket_name,
                              const std::string& object_path) = 0;

  // Get the size of the object in cloud storage
  virtual Status GetObjectSize(const std::string& bucket_name,
                               const std::string& object_path,
                               uint64_t* filesize) = 0;

  // Get the modification time of the object in cloud storage
  virtual Status GetObjectModificationTime(const std::string& bucket_name,
                                           const std::string& object_path,
                                           uint64_t* time) = 0;

  // Get the metadata of the object in cloud storage
  virtual Status GetObjectMetadata(
      const std::string& bucket_name, const std::string& object_path,
      std::unordered_map<std::string, std::string>* metadata) = 0;

  // Copy the specified cloud object from one location in the cloud
  // storage to another location in cloud storage
  virtual Status CopyObject(const std::string& src_bucket_name,
                            const std::string& src_object_path,
                            const std::string& dest_bucket_name,
                            const std::string& dest_object_path) = 0;

  // Downloads object from the cloud into a local directory
  virtual Status GetObject(const std::string& bucket_name,
                           const std::string& object_path,
                           const std::string& local_path) = 0;

  // Uploads object to the cloud
  virtual Status PutObject(const std::string& local_path,
                           const std::string& bucket_name,
                           const std::string& object_path) = 0;

  // Updates/Sets the metadata of the object in cloud storage
  virtual Status PutObjectMetadata(
      const std::string& bucket_name, const std::string& object_path,
      const std::unordered_map<std::string, std::string>& metadata) = 0;

  // Create a new cloud file in the appropriate location from the input path.
  // Updates result with the file handle.
  virtual Status NewCloudWritableFile(
      const std::string& local_path, const std::string& bucket_name,
      const std::string& object_path,
      std::unique_ptr<CloudStorageWritableFile>* result,
      const EnvOptions& options) = 0;

  // Create a new readable cloud file, returning the file handle in result.
  virtual Status NewCloudReadableFile(
      const std::string& bucket, const std::string& fname,
      std::unique_ptr<CloudStorageReadableFile>* result,
      const EnvOptions& options) = 0;

  // Prepares/Initializes the storage provider for the input cloud environment
  virtual Status Prepare(CloudEnv* env);

  // Verifies the storage provider is correctly initialized and ready to use.
  virtual Status Verify() const = 0;
};
}  // namespace rocksdb
