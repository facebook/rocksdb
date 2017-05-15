//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once
#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace rocksdb {

enum CloudType : unsigned char {
  kNone = 0x0,       // Not really a cloud env
  kAws = 0x1,        // AWS
  kGoogle = 0x2,     // Google
  kAzure = 0x3,      // Microsoft Azure
  kRackspace = 0x4,  // Rackspace
  kEnd = 0x5,
};

// Credentials needed to access cloud service
class CloudAccessCredentials {
 public:
  std::string access_key_id;
  std::string secret_key;
};

//
// The cloud environment for rocksdb. It allows configuring the rocksdb
// Environent used for the cloud.
//
class CloudEnvOptions {
 public:
  // Specify the type of cloud-service to use.
  CloudType cloud_type;

  // Access credentials
  CloudAccessCredentials credentials;

  //
  // If true,  then sst files are stored locally. They are not uploaded to
  // cloud.
  // If false, then local sst files are created, uploaded to cloud immediately,
  //           and local file is deleted. All reads are satisfied by fetching
  //           data from the cloud.
  // Default:  false
  bool keep_local_sst_files;

  // If true,  then .log and MANIFEST files are stored in a local file system.
  //           they are not uploaded to any cloud logging system.
  // If false, then .log and MANIFEST files are not stored locally, and are
  //           stored in a cloud-logging system like Kafka or Kinesis.
  // Default:  true
  bool keep_local_log_files;

  // The periodicity when the manifest should be made durable by backing it
  // to cloud store. If set to 0, then manifest is not uploaded to S3.
  // This feature is enabled only if keep_local_log_files = true.
  // Default:  1 minute
  uint64_t manifest_durable_periodicity_millis;

  CloudEnvOptions(CloudType _cloud_type = CloudType::kAws,
                  bool _keep_local_sst_files = false,
                  bool _keep_local_log_files = true,
                  uint64_t _manifest_durable_periodicity_millis = 60 * 1000)
      : cloud_type(_cloud_type),
        keep_local_sst_files(_keep_local_sst_files),
        keep_local_log_files(_keep_local_log_files),
        manifest_durable_periodicity_millis(
            _manifest_durable_periodicity_millis) {
    assert(manifest_durable_periodicity_millis == 0 ||
           keep_local_log_files == true);
  }

  // print out all options to the log
  void Dump(Logger* log) const;
};

typedef std::map<std::string, std::string> DbidList;

//
// The Cloud environment
//
class CloudEnv : public Env {
 public:
  // Returns the underlying env
  virtual Env* GetBaseEnv() = 0;
  virtual ~CloudEnv();

  // Empties all contents of the associated cloud storage bucket.
  virtual Status EmptyBucket(const std::string& bucket_prefix) = 0;

  // Reads a file from the cloud
  virtual Status NewSequentialFileCloud(const std::string& bucket_prefix,
                                        const std::string& fname,
                                        unique_ptr<SequentialFile>* result,
                                        const EnvOptions& options) = 0;

  // Saves and retrieves the dbid->dirname mapping in cloud storage
  virtual Status SaveDbid(const std::string& dbid,
                          const std::string& dirname) = 0;
  virtual Status GetPathForDbid(const std::string& bucket_prefix,
                                const std::string& dbid,
                                std::string* dirname) = 0;
  virtual Status GetDbidList(const std::string& bucket_prefix,
                             DbidList* dblist) = 0;
  virtual Status DeleteDbid(const std::string& bucket_prefix,
                            const std::string& dbid) = 0;

  // The SrcBucketPrefix identifies the cloud storage bucket and
  // GetSrcObjectPrefix specifies the path inside that bucket
  // where data files reside. The specified bucket is used in
  // a readonly mode by the associated DBCloud instance.
  virtual const std::string& GetSrcBucketPrefix() = 0;
  virtual const std::string& GetSrcObjectPrefix() = 0;

  // The DestBucketPrefix identifies the cloud storage bucket and
  // GetDestObjectPrefix specifies the path inside that bucket
  // where data files reside. The associated DBCloud instance
  // writes newly created files to this bucket.
  virtual const std::string& GetDestBucketPrefix() = 0;
  virtual const std::string& GetDestObjectPrefix() = 0;

  // Create a new AWS env.
  // src_bucket_name: bucket name suffix where db data is read from
  // src_object_prefix: all db objects in source bucket are prepended with this
  // dest_bucket_name: bucket name suffix where db data is written to
  // dest_object_prefix: all db objects in destination bucket are prepended with
  // this
  //
  // If src_bucket_name is empty, then the associated db does not read any
  // data from cloud storage.
  // If dest_bucket_name is empty, then the associated db does not write any
  // data to cloud storage.
  static Status NewAwsEnv(Env* base_env, const std::string& src_bucket_name,
                          const std::string& src_object_prefix,
                          const std::string& src_bucket_region,
                          const std::string& dest_bucket_name,
                          const std::string& dest_object_prefix,
                          const std::string& dest_bucket_region,
                          const CloudEnvOptions& env_options,
                          std::shared_ptr<Logger> logger, CloudEnv** cenv);
};

}  // namespace
