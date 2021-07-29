//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once

#include <unordered_map>

#include "rocksdb/configurable.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
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

// Generic information of the object in the cloud. Some information might be
// vendor-dependent.
struct CloudObjectInformation {
  uint64_t size;
  uint64_t modification_time;

  // Cloud-vendor dependent. In S3, we will provide ETag of the object.
  std::string content_hash;
  std::unordered_map<std::string, std::string> metadata;
};

// A CloudStorageProvider provides the interface to the cloud object
// store.  Methods can create and empty buckets, as well as other
// standard bucket object operations get/put/list/delete

class CloudStorageProvider : public Configurable {
 public:
  virtual ~CloudStorageProvider();
  static const char* Type() { return "CloudStorageProvider"; }
  // Creates and configures a new CloudStorageProvider from the input options
  // and id.
  static Status CreateFromString(
      const ConfigOptions& config_options, const std::string& id,
      std::shared_ptr<CloudStorageProvider>* provider);

  // Returns name of the cloud storage provider type (e.g., S3)
  virtual const char* Name() const = 0;

  virtual Status CreateBucket(const std::string& bucket_name) = 0;
  virtual Status ExistsBucket(const std::string& bucket_name) = 0;

  // Empties all contents of the associated cloud storage bucket.
  virtual Status EmptyBucket(const std::string& bucket_name,
                             const std::string& object_path) = 0;
  // Delete the specified object from the specified cloud bucket
  virtual Status DeleteCloudObject(const std::string& bucket_name,
                                   const std::string& object_path) = 0;

  // Does the specified object exist in the cloud storage
  // returns all the objects that have the specified path prefix and
  // are stored in a cloud bucket
  virtual Status ListCloudObjects(const std::string& bucket_name,
                                  const std::string& object_path,
                                  std::vector<std::string>* path_names) = 0;

  // Does the specified object exist in the cloud storage
  virtual Status ExistsCloudObject(const std::string& bucket_name,
                                   const std::string& object_path) = 0;

  // Get the size of the object in cloud storage
  virtual Status GetCloudObjectSize(const std::string& bucket_name,
                                    const std::string& object_path,
                                    uint64_t* filesize) = 0;

  // Get the modification time of the object in cloud storage
  virtual Status GetCloudObjectModificationTime(const std::string& bucket_name,
                                                const std::string& object_path,
                                                uint64_t* time) = 0;

  // Get the metadata of the object in cloud storage
  virtual Status GetCloudObjectMetadata(const std::string& bucket_name,
                                        const std::string& object_path,
                                        CloudObjectInformation* info) = 0;

  // Copy the specified cloud object from one location in the cloud
  // storage to another location in cloud storage
  virtual Status CopyCloudObject(const std::string& src_bucket_name,
                                 const std::string& src_object_path,
                                 const std::string& dest_bucket_name,
                                 const std::string& dest_object_path) = 0;

  // Downloads object from the cloud into a local directory
  virtual Status GetCloudObject(const std::string& bucket_name,
                                const std::string& object_path,
                                const std::string& local_path) = 0;

  // Uploads object to the cloud
  virtual Status PutCloudObject(const std::string& local_path,
                                const std::string& bucket_name,
                                const std::string& object_path) = 0;

  // Updates/Sets the metadata of the object in cloud storage
  virtual Status PutCloudObjectMetadata(
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
};
}  // namespace ROCKSDB_NAMESPACE
