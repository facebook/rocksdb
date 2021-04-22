//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once
#include <functional>
#include <memory>
#include <unordered_map>

#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace Aws {
namespace Auth {
class AWSCredentialsProvider;
}  // namespace Auth
namespace Client {
struct ClientConfiguration;
}  // namespace Client
}  // namespace Aws

namespace ROCKSDB_NAMESPACE {

class CloudEnv;
class CloudLogController;
class CloudStorageProvider;

enum CloudType : unsigned char {
  kCloudNone = 0x0,       // Not really a cloud env
  kCloudAws = 0x1,        // AWS
  kCloudGoogle = 0x2,     // Google
  kCloudAzure = 0x3,      // Microsoft Azure
  kCloudRackspace = 0x4,  // Rackspace
  kCloudEnd = 0x5,
};

enum LogType : unsigned char {
  kLogNone = 0x0,  // Not really a log env
  // Important: Kinesis integration currently has a known issue and is not
  // supported, see https://github.com/rockset/rocksdb-cloud/issues/35
  kLogKinesis = 0x1,  // Kinesis
  kLogKafka = 0x2,    // Kafka
  kLogEnd = 0x3,
};

// Type of AWS access credentials
enum class AwsAccessType {
  kUndefined,  // Use AWS SDK's default credential chain
  kSimple,
  kInstance,
  kTaskRole,
  kEnvironment,
  kConfig,
  kAnonymous,
};

// Credentials needed to access AWS cloud service
class AwsCloudAccessCredentials {
 public:
  // functions to support AWS credentials
  //
  // Initialize AWS credentials using access_key_id and secret_key
  void InitializeSimple(const std::string& aws_access_key_id,
                        const std::string& aws_secret_key);
  // Initialize AWS credentials using a config file
  void InitializeConfig(const std::string& aws_config_file);
  // Initialize credentials for tests (relies on config vars)
  Status TEST_Initialize();

  // test if valid AWS credentials are present
  Status HasValid() const;
  // Get AWSCredentialsProvider to supply to AWS API calls when required (e.g.
  // to create S3Client)
  Status GetCredentialsProvider(
      std::shared_ptr<Aws::Auth::AWSCredentialsProvider>* result) const;

 private:
  AwsAccessType GetAccessType() const;
  Status CheckCredentials(const AwsAccessType& aws_type) const;

 public:
  std::string access_key_id;
  std::string secret_key;
  std::string config_file;
  AwsAccessType type{AwsAccessType::kUndefined};

  // If non-nullptr, all of the above options are ignored.
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> provider;
};

// Defines parameters required to connect to Kafka
class KafkaLogOptions {
 public:
  // The config parameters for the kafka client. At a bare minimum,
  // there needs to be at least one entry in this map that lists the
  // kafka brokers. That entry is of the type
  //  ("metadata.broker.list", "kafka1.rockset.com,kafka2.rockset.com"
  //
  std::unordered_map<std::string, std::string> client_config_params;
};

enum class CloudRequestOpType {
  kReadOp,
  kWriteOp,
  kListOp,
  kCreateOp,
  kDeleteOp,
  kCopyOp,
  kInfoOp
};
using CloudRequestCallback =
    std::function<void(CloudRequestOpType, uint64_t, uint64_t, bool)>;

class BucketOptions {
 private:
  std::string bucket_;  // The suffix for the bucket name
  std::string
      prefix_;  // The prefix for the bucket name.  Defaults to "rockset."
  std::string object_;  // The object path for the bucket
  std::string region_;  // The region for the bucket
  std::string name_;    // The name of the bucket (prefix_ + bucket_)
 public:
  BucketOptions();
  // Sets the name of the bucket to be the new bucket name.
  // If prefix is specified, the new bucket name will be [prefix][bucket]
  // If no prefix is specified, the bucket name will use the existing prefix
  void SetBucketName(const std::string& bucket, const std::string& prefix = "");
  const std::string& GetBucketName() const { return name_; }
  const std::string& GetObjectPath() const { return object_; }
  void SetObjectPath(const std::string& object) { object_ = object; }
  const std::string& GetRegion() const { return region_; }
  void SetRegion(const std::string& region) { region_ = region; }

  // Initializes the bucket properties for test purposes
  void TEST_Initialize(const std::string& name_prefix,
                       const std::string& object_path,
                       const std::string& region = "");
  bool IsValid() const {
    if (object_.empty() || name_.empty()) {
      return false;
    } else {
      return true;
    }
  }
};

inline bool operator==(const BucketOptions& lhs, const BucketOptions& rhs) {
  if (lhs.IsValid() && rhs.IsValid()) {
    return ((lhs.GetBucketName() == rhs.GetBucketName()) &&
            (lhs.GetObjectPath() == rhs.GetObjectPath()) &&
            (lhs.GetRegion() == rhs.GetRegion()));
  } else {
    return false;
  }
}
inline bool operator!=(const BucketOptions& lhs, const BucketOptions& rhs) {
  return !(lhs == rhs);
}

class AwsCloudOptions {
 public:
  static Status GetClientConfiguration(
      CloudEnv* env, const std::string& region,
      Aws::Client::ClientConfiguration* config);
};

//
// The cloud environment for rocksdb. It allows configuring the rocksdb
// Environent used for the cloud.
//
class CloudEnvOptions {
 private:
 public:
  BucketOptions src_bucket;
  BucketOptions dest_bucket;
  // Specify the type of cloud-service to use.
  CloudType cloud_type;

  // If keep_local_log_files is false, this specifies what service to use
  // for storage of write-ahead log.
  LogType log_type;
  std::shared_ptr<CloudLogController> cloud_log_controller;

  // Specifies the class responsible for writing objects to the cloud
  std::shared_ptr<CloudStorageProvider> storage_provider;

  // Access credentials
  AwsCloudAccessCredentials credentials;

  // Only used if keep_local_log_files is true and log_type is kKafka.
  KafkaLogOptions kafka_log_options;

  //
  // If true,  then sst files are stored locally and uploaded to the cloud in
  // the background. On restart, all files from the cloud that are not present
  // locally are downloaded.
  // If false, then local sst files are created, uploaded to cloud immediately,
  //           and local file is deleted. All reads are satisfied by fetching
  //           data from the cloud.
  // Default:  false
  bool keep_local_sst_files;

  // If true,  then .log and MANIFEST files are stored in a local file system.
  //           they are not uploaded to any cloud logging system.
  // If false, then .log and MANIFEST files are not stored locally, and are
  //           stored in a cloud-logging system like Kinesis or Kafka.
  // Default:  true
  bool keep_local_log_files;

  // This feature is obsolete. We upload MANIFEST to the cloud on every write.
  // uint64_t manifest_durable_periodicity_millis;

  // The time period when the purger checks and deleted obselete files.
  // This is the time when the purger wakes up, scans the cloud bucket
  // for files that are not part of any DB and then deletes them.
  // Default: 10 minutes
  uint64_t purger_periodicity_millis;

  // Validate that locally cached files have the same size as those
  // stored in the cloud.
  // Default: true
  bool validate_filesize;

  // if non-null, will be called *after* every cloud operation with some basic
  // information about the operation. Use this to instrument your calls to the
  // cloud.
  // parameters: (op, size, latency in microseconds, is_success)
  std::shared_ptr<CloudRequestCallback> cloud_request_callback;

  // If true, enables server side encryption. If used with encryption_key_id in
  // S3 mode uses AWS KMS. Otherwise, uses S3 server-side encryption where
  // key is automatically created by Amazon.
  // Default: false
  bool server_side_encryption;

  // If non-empty, uses the key ID for encryption.
  // Default: empty
  std::string encryption_key_id;

  // If false, it will not attempt to create cloud bucket if it doesn't exist.
  // Default: true
  bool create_bucket_if_missing;

  // request timeout for requests from the cloud storage. A value of 0
  // means the default timeout assigned by the underlying cloud storage.
  uint64_t request_timeout_ms;

  // Use this to turn off the purger. You can do this if you don't use the clone
  // feature of RocksDB cloud
  // Default: true
  bool run_purger;

  // An ephemeral clone is a clone that has no destination bucket path. All
  // updates to this clone are stored locally and not uploaded to cloud.
  // It is called ephemeral because locally made updates can get lost if
  // the machines dies.
  // This flag controls whether the ephemeral db needs to be resynced to
  // the source cloud bucket at every db open time.
  // If true,  then the local ephemeral db is re-synced to the src cloud
  //           bucket every time the db is opened. Any previous writes
  //           to this ephemeral db are lost.
  // If false, then the local ephemeral db is initialized from data in the
  //           src cloud bucket only if the local copy does not exist.
  //           If the local copy of the db already exists, then no data
  //           from the src cloud bucket is copied to the local db dir.
  // Default:  false
  bool ephemeral_resync_on_open;

  // If true, we will skip the dbid verification on startup. This is currently
  // only used in tests and is not recommended setting.
  // Default: false
  bool skip_dbid_verification;

  // If true, we will use AWS TransferManager instead of Put/Get operaations to
  // download and upload S3 files.
  // Default: false
  bool use_aws_transfer_manager;

  // The number of object's metadata that are fetched in every iteration when
  // listing the results of a directory Default: 5000
  int number_objects_listed_in_one_iteration;

  // During opening, we get the size of all SST files currently in the
  // folder/bucket for bookkeeping. This operation might be expensive,
  // especially if the bucket is in the cloud. This option allows to use a
  // constant size instead. Non-negative value means use this option.
  //
  // NOTE: If users already passes an SST File Manager through
  // Options.sst_file_manager, constant_sst_file_size_in_sst_file_manager is
  // ignored.
  //
  // Default: -1, means don't use this option.
  int64_t constant_sst_file_size_in_sst_file_manager;

  // Skip listing files in the cloud in GetChildren. That means GetChildren
  // will only return files in local directory. During DB opening, RocksDB
  // makes multiple GetChildren calls, which are very expensive if we list
  // objects in the cloud.
  //
  // This option is used in remote compaction where we open the DB in a
  // temporary folder, and then the folder is deleted after the RPC is done.
  // This requires opening DB to be really fast, and it's unnecessary to cleanup
  // various things, which is what RocksDB calls GetChildren for.
  //
  // Default: false.
  bool skip_cloud_files_in_getchildren;

  CloudEnvOptions(
      CloudType _cloud_type = CloudType::kCloudAws,
      LogType _log_type = LogType::kLogKafka,
      bool _keep_local_sst_files = false, bool _keep_local_log_files = true,
      uint64_t _purger_periodicity_millis = 10 * 60 * 1000,
      bool _validate_filesize = true,
      std::shared_ptr<CloudRequestCallback> _cloud_request_callback = nullptr,
      bool _server_side_encryption = false, std::string _encryption_key_id = "",
      bool _create_bucket_if_missing = true, uint64_t _request_timeout_ms = 0,
      bool _run_purger = false, bool _ephemeral_resync_on_open = false,
      bool _skip_dbid_verification = false,
      bool _use_aws_transfer_manager = false,
      int _number_objects_listed_in_one_iteration = 5000,
      int _constant_sst_file_size_in_sst_file_manager = -1,
      bool _skip_cloud_files_in_getchildren = false)
      : cloud_type(_cloud_type),
        log_type(_log_type),
        keep_local_sst_files(_keep_local_sst_files),
        keep_local_log_files(_keep_local_log_files),
        purger_periodicity_millis(_purger_periodicity_millis),
        validate_filesize(_validate_filesize),
        cloud_request_callback(_cloud_request_callback),
        server_side_encryption(_server_side_encryption),
        encryption_key_id(std::move(_encryption_key_id)),
        create_bucket_if_missing(_create_bucket_if_missing),
        request_timeout_ms(_request_timeout_ms),
        run_purger(_run_purger),
        ephemeral_resync_on_open(_ephemeral_resync_on_open),
        skip_dbid_verification(_skip_dbid_verification),
        use_aws_transfer_manager(_use_aws_transfer_manager),
        number_objects_listed_in_one_iteration(
            _number_objects_listed_in_one_iteration),
        constant_sst_file_size_in_sst_file_manager(
            _constant_sst_file_size_in_sst_file_manager),
        skip_cloud_files_in_getchildren(_skip_cloud_files_in_getchildren) {}

  // print out all options to the log
  void Dump(Logger* log) const;

  // Sets result based on the value of name or alt in the environment
  // Returns true if the name/alt exists in the environment, false otherwise
  static bool GetNameFromEnvironment(const char* name, const char* alt,
                                     std::string* result);
  void TEST_Initialize(const std::string& name_prefix,
                       const std::string& object_path,
                       const std::string& region = "");
};

struct CheckpointToCloudOptions {
  int thread_count = 8;
  bool flush_memtable = false;
};

// A map of dbid to the pathname where the db is stored
typedef std::map<std::string, std::string> DbidList;

//
// The Cloud environment
//
class CloudEnv : public Env {
 protected:
  CloudEnvOptions cloud_env_options;
  Env* base_env_;  // The underlying env

  CloudEnv(const CloudEnvOptions& options, Env* base,
           const std::shared_ptr<Logger>& logger);
 public:
  std::shared_ptr<Logger> info_log_;  // informational messages

  virtual ~CloudEnv();
  // Returns the underlying env
  Env* GetBaseEnv() { return base_env_; }
  virtual const char* Name() const { return "cloud"; }

  virtual Status PreloadCloudManifest(const std::string& local_dbname) = 0;

  // Reads a file from the cloud
  virtual Status NewSequentialFileCloud(const std::string& bucket_prefix,
                                        const std::string& fname,
                                        std::unique_ptr<SequentialFile>* result,
                                        const EnvOptions& options) = 0;

  // Saves and retrieves the dbid->dirname mapping in cloud storage
  virtual Status SaveDbid(const std::string& bucket_name,
                          const std::string& dbid,
                          const std::string& dirname) = 0;
  virtual Status GetPathForDbid(const std::string& bucket_prefix,
                                const std::string& dbid,
                                std::string* dirname) = 0;
  virtual Status GetDbidList(const std::string& bucket_prefix,
                             DbidList* dblist) = 0;
  virtual Status DeleteDbid(const std::string& bucket_prefix,
                            const std::string& dbid) = 0;

  Logger* GetLogger() const { return info_log_.get(); }
  std::shared_ptr<CloudStorageProvider> GetStorageProvider() const {
    return cloud_env_options.storage_provider;
  }
  // The SrcBucketName identifies the cloud storage bucket and
  // GetSrcObjectPath specifies the path inside that bucket
  // where data files reside. The specified bucket is used in
  // a readonly mode by the associated DBCloud instance.
  const std::string& GetSrcBucketName() const {
    return cloud_env_options.src_bucket.GetBucketName();
  }
  const std::string& GetSrcObjectPath() const {
    return cloud_env_options.src_bucket.GetObjectPath();
  }
  bool HasSrcBucket() const { return cloud_env_options.src_bucket.IsValid(); }

  // The DestBucketName identifies the cloud storage bucket and
  // GetDestObjectPath specifies the path inside that bucket
  // where data files reside. The associated DBCloud instance
  // writes newly created files to this bucket.
  const std::string& GetDestBucketName() const {
    return cloud_env_options.dest_bucket.GetBucketName();
  }
  const std::string& GetDestObjectPath() const {
    return cloud_env_options.dest_bucket.GetObjectPath();
  }

  bool HasDestBucket() const { return cloud_env_options.dest_bucket.IsValid(); }
  bool SrcMatchesDest() const {
    if (HasSrcBucket() && HasDestBucket()) {
      return cloud_env_options.src_bucket == cloud_env_options.dest_bucket;
    } else {
      return false;
    }
  }

  // returns the options used to create this env
  const CloudEnvOptions& GetCloudEnvOptions() const {
    return cloud_env_options;
  }

  // Deletes file from a destination bucket.
  virtual Status DeleteCloudFileFromDest(const std::string& fname) = 0;
  // Copies a local file to a destination bucket.
  virtual Status CopyLocalFileToDest(const std::string& local_name,
                                     const std::string& cloud_name) = 0;

  // Transfers the filename from RocksDB's domain to the physical domain, based
  // on information stored in CLOUDMANIFEST.
  // For example, it will map 00010.sst to 00010.sst-[epoch] where [epoch] is
  // an epoch during which that file was created.
  // Files both in S3 and in the local directory have this [epoch] suffix.
  virtual std::string RemapFilename(const std::string& logical_name) const = 0;

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
                          const std::shared_ptr<Logger>& logger,
                          CloudEnv** cenv);
  static Status NewAwsEnv(Env* base_env, const CloudEnvOptions& env_options,
                          const std::shared_ptr<Logger>& logger,
                          CloudEnv** cenv);
};

}  // namespace ROCKSDB_NAMESPACE
