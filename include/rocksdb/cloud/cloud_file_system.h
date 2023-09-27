//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <unordered_map>

#include "rocksdb/cache.h"
#include "rocksdb/configurable.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/status.h"

namespace Aws {
namespace Auth {
class AWSCredentialsProvider;
}  // namespace Auth
namespace Client {
struct ClientConfiguration;
}  // namespace Client
namespace S3 {
class S3Client;
}
}  // namespace Aws

namespace ROCKSDB_NAMESPACE {

class CloudFileSystem;
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
  AwsCloudAccessCredentials();
  // functions to support AWS credentials
  //
  // Initialize AWS credentials using access_key_id and secret_key
  void InitializeSimple(const std::string& aws_access_key_id,
                        const std::string& aws_secret_key);
  // Initialize AWS credentials using a config file
  void InitializeConfig(const std::string& aws_config_file);

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

using S3ClientFactory = std::function<std::shared_ptr<Aws::S3::S3Client>(
    const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>&,
    const Aws::Client::ClientConfiguration&)>;

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
  const std::string& GetBucketPrefix() const { return prefix_; }
  const std::string& GetBucketName(bool full = true) const {
    if (full) {
      return name_;
    } else {
      return bucket_;
    }
  }
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
      CloudFileSystem* fs, const std::string& region,
      Aws::Client::ClientConfiguration* config);
};

//
// The cloud file system for rocksdb. It allows configuring the rocksdb
// FileSystem used for the cloud.
//
class CloudFileSystemOptions {
 private:
 public:
  static const char* kName() { return "CloudFileSystemOptions"; }
  BucketOptions src_bucket;
  BucketOptions dest_bucket;
  // Specify the type of cloud-service to use. Deprecated.
  CloudType cloud_type;

  // If keep_local_log_files is false, this specifies what service to use
  // for storage of write-ahead log.
  LogType log_type;
  std::shared_ptr<CloudLogController> cloud_log_controller;

  // Specifies the class responsible for accessing objects in the cloud.
  // A null value indicates that the default storage provider based on
  // the cloud fs be used.
  // Default:  null
  std::shared_ptr<CloudStorageProvider> storage_provider;

  // Specifies the amount of sst files to be cached in local storage.
  // If non-null, then the local storage would be used as a file cache.
  // The Get or a Scan request on the database generates a random read
  // request on the sst file and such a request causes the sst file to
  // be inserted into the local file cache.
  // A compaction request generates a sequential read request on the sst
  // file and it does not cause the sst file to be inserted into the
  // local file cache.
  // A memtable flush generates a write requst to a new sst file and this
  // sst file is not inserted into the local file cache.
  // Cannot be set if keep_local_log_files is true.
  // Default: null (disabled)
  std::shared_ptr<Cache> sst_file_cache;

  // Access credentials
  AwsCloudAccessCredentials credentials;

  // If present, s3_client_factory will be used to create S3Client instances
  S3ClientFactory s3_client_factory;

  // Only used if keep_local_log_files is true and log_type is kKafka.
  KafkaLogOptions kafka_log_options;

  // If true,  then sst files are stored locally and uploaded to the cloud in
  // the background. On restart, all files from the cloud that are not present
  // locally are downloaded.
  // If false, then local sst files are created, uploaded to cloud immediately,
  //           and local file is deleted. All reads are satisfied by fetching
  //           data from the cloud.
  // Cannot be set if sst_file_cache is enabled.
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

  // If true, we will sync local snapshot(i.e, CLOUDMANIFEST and MANIFEST files)
  // when opening db from cloud.
  // Otherwise, we will use local CLOUDMANIFEST and MANIFEST files directly if
  // present.
  //
  // NOTE: if the files are not present locally, we will always fetch
  // from cloud
  //
  // Default: false
  bool resync_on_open;

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

  // If true, the files from S3 will be downloaded using direct IO. It is
  // recommended to set this to true, the only reason the default is false is to
  // avoid behavior changes with default configuration.
  // The options is ignored if use_aws_transfer_manager = true.
  // Default: false
  bool use_direct_io_for_cloud_download;

  // Experimental option!
  // If false, we don't roll cloud manifest when opening the database.
  // Default: true
  bool roll_cloud_manifest_on_open;

  // If non-empty, cookie will be used as suffix of cloud manifest filename.
  // Cookie can be used to specify the cloud manifest file. Currently, this is
  // only used in leader-follower world, in which we create a new cloud-manifest
  // file during leader election to protect old cloud-manifest file and enable
  // rollback in emergency
  //
  // NOTE: cookie_on_open is only used to determine the CloudManifest file in s3
  // when we open the db. Once db is opened, we should never need to access the
  // CloudManifest file in s3. Also, the in-memory CloudManifest will be updated
  // and it might point to some other CloudManifest file with different cookie
  //
  // Default: "", means there is only one cloud manifest file (named as
  // CLOUDMANIFEST)
  std::string cookie_on_open;

  // Experimental option!
  // The cookie we will switch to when we open the db and roll the new epoch.
  //
  // NOTE: when opening a new db, CLOUDMANIFEST file will only be created with
  // new_cookie_on_open
  //
  // TODO(wei): This is a temporarly option to unblock leader/follower rollout,
  // so that we can support follower taking over as new leader and having two
  // leader running for short period of time. Remove it once leader/follower is
  // ready
  //
  // Default: "", means new cloud manifest file won't have cookie suffix
  std::string new_cookie_on_open;

  // Experimental option!
  // - If true, both cloud and local invisible files (i.e, CLOUDMANIFEST,
  // MANIFEST and SST files which don't belong to current epoch) will be deleted
  // when db is opened.
  // - Otherwise, only local invisible files will be deleted
  //
  // Default: true
  bool delete_cloud_invisible_files_on_open;

  // Experimental option!
  // Delay after files(including both invisible and obsolete files) are
  // scheduled to be deleted.
  // If not set, file deletion won't be scheduled asynchronously but deleted
  // right away. Note this is different from setting the value to 0, in which
  // case the file deletion is still scheduled async.
  //
  // Default: 1 hour
  std::optional<std::chrono::seconds> cloud_file_deletion_delay;

  CloudFileSystemOptions(
      CloudType _cloud_type = CloudType::kCloudAws,
      LogType _log_type = LogType::kLogKafka,
      bool _keep_local_sst_files = false, bool _keep_local_log_files = true,
      uint64_t _purger_periodicity_millis = 10 * 60 * 1000,
      bool _validate_filesize = true,
      std::shared_ptr<CloudRequestCallback> _cloud_request_callback = nullptr,
      bool _server_side_encryption = false, std::string _encryption_key_id = "",
      bool _create_bucket_if_missing = true, uint64_t _request_timeout_ms = 0,
      bool _run_purger = false, bool _resync_on_open = false,
      bool _skip_dbid_verification = false,
      bool _use_aws_transfer_manager = false,
      int _number_objects_listed_in_one_iteration = 5000,
      int _constant_sst_file_size_in_sst_file_manager = -1,
      bool _skip_cloud_files_in_getchildren = false,
      bool _use_direct_io_for_cloud_download = false,
      std::shared_ptr<Cache> _sst_file_cache = nullptr,
      bool _roll_cloud_manifest_on_open = true,
      std::string _cookie_on_open = "", std::string _new_cookie_on_open = "",
      bool _delete_cloud_invisible_files_on_open = true,
      std::chrono::seconds _cloud_file_deletion_delay = std::chrono::hours(1))
      : log_type(_log_type),
        sst_file_cache(_sst_file_cache),
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
        resync_on_open(_resync_on_open),
        skip_dbid_verification(_skip_dbid_verification),
        use_aws_transfer_manager(_use_aws_transfer_manager),
        number_objects_listed_in_one_iteration(
            _number_objects_listed_in_one_iteration),
        constant_sst_file_size_in_sst_file_manager(
            _constant_sst_file_size_in_sst_file_manager),
        skip_cloud_files_in_getchildren(_skip_cloud_files_in_getchildren),
        use_direct_io_for_cloud_download(_use_direct_io_for_cloud_download),
        roll_cloud_manifest_on_open(_roll_cloud_manifest_on_open),
        cookie_on_open(std::move(_cookie_on_open)),
        new_cookie_on_open(_new_cookie_on_open),
        delete_cloud_invisible_files_on_open(
            _delete_cloud_invisible_files_on_open),
        cloud_file_deletion_delay(_cloud_file_deletion_delay) {
    (void)_cloud_type;
  }

  // print out all options to the log
  void Dump(Logger* log) const;

  // Sets result based on the value of name or alt in the environment
  // Returns true if the name/alt exists in the environment, false otherwise
  static bool GetNameFromEnvironment(const char* name, const char* alt,
                                     std::string* result);
  void TEST_Initialize(const std::string& name_prefix,
                       const std::string& object_path,
                       const std::string& region = "");

  Status Configure(const ConfigOptions& config_options,
                   const std::string& opts_str);
  Status Serialize(const ConfigOptions& config_options,
                   std::string* result) const;

  // Is the sst file cache configured?
  bool hasSstFileCache() const {
    return sst_file_cache != nullptr && sst_file_cache->GetCapacity() > 0;
  }
};

struct CheckpointToCloudOptions {
  int thread_count = 8;
  bool flush_memtable = false;
};

// A map of dbid to the pathname where the db is stored
typedef std::map<std::string, std::string> DbidList;

// CloudManifestDelta represents delta changes when rolling cloud manifest.
//
// Whenever we roll cloud manifest, we generate a new MANIFEST-epoch file, all
// files with file number >= file_num are only visible in the new
// MANIFEST-epoch file
struct CloudManifestDelta {
  uint64_t file_num;  // next file number for new epoch
  std::string epoch;  // epoch for the new manifest file
};

//
// The Cloud file system
//
// NOTE: The AWS SDK must be initialized before the CloudFileSystem is
// constructed, and remain active (Aws::ShutdownAPI() not called) as long as any
// CloudFileSystem objects exist.
class CloudFileSystem : public FileSystem {
 protected:
  CloudFileSystemOptions cloud_fs_options;
  std::shared_ptr<FileSystem> base_fs_;  // The underlying file system

  // Creates a new CompositeEnv from "env" and "this".
  // The returned Env must not outlive "this"
  std::unique_ptr<Env> NewCompositeEnvFromThis(Env* env);

  CloudFileSystem(const CloudFileSystemOptions& options,
                  const std::shared_ptr<FileSystem>& base,
                  const std::shared_ptr<Logger>& logger);

 public:
  mutable std::shared_ptr<Logger> info_log_;  // informational messages

  virtual ~CloudFileSystem();

  static void RegisterCloudObjects(const std::string& mode = "");
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& id,
                                 std::unique_ptr<CloudFileSystem>* fs);
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& id,
                                 const CloudFileSystemOptions& cloud_options,
                                 std::unique_ptr<CloudFileSystem>* fs);
  static const char* kCloud() { return "cloud"; }
  static const char* kAws() { return "aws"; }
  virtual const char* Name() const { return "cloud-env"; }
  // Returns the underlying file system
  const std::shared_ptr<FileSystem>& GetBaseFileSystem() const {
    return base_fs_;
  }
  virtual IOStatus PreloadCloudManifest(const std::string& local_dbname) = 0;
  // This method will migrate the database that is using pure RocksDB into
  // RocksDB-Cloud. Call this before opening the database with RocksDB-Cloud.
  virtual IOStatus MigrateFromPureRocksDB(const std::string& local_dbname) = 0;

  // Reads a file from the cloud
  virtual IOStatus NewSequentialFileCloud(
      const std::string& bucket_prefix, const std::string& fname,
      const FileOptions& file_opts, std::unique_ptr<FSSequentialFile>* result,
      IODebugContext* dbg) = 0;

  // Saves and retrieves the dbid->dirname mapping in cloud storage
  virtual IOStatus SaveDbid(const std::string& bucket_name,
                            const std::string& dbid,
                            const std::string& dirname) = 0;
  virtual IOStatus GetPathForDbid(const std::string& bucket_prefix,
                                  const std::string& dbid,
                                  std::string* dirname) = 0;
  virtual IOStatus GetDbidList(const std::string& bucket_prefix,
                               DbidList* dblist) = 0;
  virtual IOStatus DeleteDbid(const std::string& bucket_prefix,
                              const std::string& dbid) = 0;

  Logger* GetLogger() const { return info_log_.get(); }
  const std::shared_ptr<CloudStorageProvider>& GetStorageProvider() const {
    return cloud_fs_options.storage_provider;
  }

  const std::shared_ptr<CloudLogController>& GetLogController() const {
    return cloud_fs_options.cloud_log_controller;
  }

  // The SrcBucketName identifies the cloud storage bucket and
  // GetSrcObjectPath specifies the path inside that bucket
  // where data files reside. The specified bucket is used in
  // a readonly mode by the associated DBCloud instance.
  const std::string& GetSrcBucketName() const {
    return cloud_fs_options.src_bucket.GetBucketName();
  }
  const std::string& GetSrcObjectPath() const {
    return cloud_fs_options.src_bucket.GetObjectPath();
  }
  bool HasSrcBucket() const { return cloud_fs_options.src_bucket.IsValid(); }

  // The DestBucketName identifies the cloud storage bucket and
  // GetDestObjectPath specifies the path inside that bucket
  // where data files reside. The associated DBCloud instance
  // writes newly created files to this bucket.
  const std::string& GetDestBucketName() const {
    return cloud_fs_options.dest_bucket.GetBucketName();
  }
  const std::string& GetDestObjectPath() const {
    return cloud_fs_options.dest_bucket.GetObjectPath();
  }

  bool HasDestBucket() const { return cloud_fs_options.dest_bucket.IsValid(); }
  bool SrcMatchesDest() const {
    if (HasSrcBucket() && HasDestBucket()) {
      return cloud_fs_options.src_bucket == cloud_fs_options.dest_bucket;
    } else {
      return false;
    }
  }

  // returns the options used to create this object
  const CloudFileSystemOptions& GetCloudFileSystemOptions() const {
    return cloud_fs_options;
  }

  // Deletes file from a destination bucket.
  virtual IOStatus DeleteCloudFileFromDest(const std::string& fname) = 0;
  // Copies a local file to a destination bucket.
  virtual IOStatus CopyLocalFileToDest(const std::string& local_name,
                                       const std::string& cloud_name) = 0;

  // Transfers the filename from RocksDB's domain to the physical domain, based
  // on information stored in CLOUDMANIFEST.
  // For example, it will map 00010.sst to 00010.sst-[epoch] where [epoch] is
  // an epoch during which that file was created.
  // Files both in S3 and in the local directory have this [epoch] suffix.
  virtual std::string RemapFilename(const std::string& logical_name) const = 0;

  // Find the list of live files based on CloudManifest and Manifest in local db
  //
  // For the returned filepath in `live_sst_files` and `manifest_file`, we only
  // include the basename of the filepath but not the directory prefix to the
  // file
  virtual IOStatus FindAllLiveFiles(const std::string& local_dbname,
                                    std::vector<std::string>* live_sst_files,
                                    std::string* manifest_file) = 0;

  // Read the given manifest file (either the one used in the database or a
  // copy) and populate live_sst_files with the list of all SST files that it
  //
  // references. Unlike FindAllLiveFiles, this method doesn't resolve the
  // manifest name from the cloud manifest and also doesn't pull the manifest
  // (i.e., it needs to be present locally).
  virtual IOStatus FindLiveFilesFromLocalManifest(
      const std::string& manifest_file,
      std::vector<std::string>* live_sst_files) = 0;

  // Apply cloud manifest delta to in-memory cloud manifest. Does not change the
  // on-disk state.
  //
  // If delta has already been applied in cloud manifest, delta_applied would be
  // `false`
  virtual IOStatus ApplyCloudManifestDelta(const CloudManifestDelta& delta,
                                           bool* delta_applied) = 0;

  // This function does several things:
  // * Writes CLOUDMANIFEST-cookie file based on existing in-memory
  // CLOUDMANIFEST and provided CloudManifestDelta.
  // * Copies MANIFEST-epoch into MANIFEST-delta.epoch
  // * Uploads both CLOUDMANIFEST-cookie and MANIFEST-delta.epoch into cloud
  // storage (if dest bucket is given).
  //
  // Return InvalidArgument status if the delta has been applied in current
  // CloudManifest
  virtual IOStatus RollNewCookie(const std::string& local_dbname,
                                 const std::string& cookie,
                                 const CloudManifestDelta& delta) const = 0;

  virtual IOStatus GetMaxFileNumberFromCurrentManifest(
      const std::string& local_dbname, uint64_t* max_file_number) = 0;

  virtual IOStatus DeleteCloudInvisibleFiles(
      const std::vector<std::string>& active_cookies) = 0;

  // Create a new AWS file system.
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
  static Status NewAwsFileSystem(const std::shared_ptr<FileSystem>& base_fs,
                                 const std::string& src_bucket_name,
                                 const std::string& src_object_prefix,
                                 const std::string& src_bucket_region,
                                 const std::string& dest_bucket_name,
                                 const std::string& dest_object_prefix,
                                 const std::string& dest_bucket_region,
                                 const CloudFileSystemOptions& fs_options,
                                 const std::shared_ptr<Logger>& logger,
                                 CloudFileSystem** cfs);
  static Status NewAwsFileSystem(const std::shared_ptr<FileSystem>& base_fs,
                                 const CloudFileSystemOptions& fs_options,
                                 const std::shared_ptr<Logger>& logger,
                                 CloudFileSystem** cfs);

  // Creates a new Env that delegates all thread/time related
  // calls to env, and all file operations to fs
  static std::unique_ptr<Env> NewCompositeEnv(
      Env* env, const std::shared_ptr<FileSystem>& fs);
};

}  // namespace ROCKSDB_NAMESPACE
