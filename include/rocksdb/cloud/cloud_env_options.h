//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once

namespace rocksdb {

enum CloudType : unsigned char {
  kAws = 0x1,             // AWS
  kGoogle = 0x2,          // Google
  kAzure = 0x3,           // Microsoft Azure
  kRackspace = 0x4,       // Rackspace
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

  // The region where the service is located
  std::string region;

  //
  // If true,  then sst files are stored locally. They are not uploaded to cloud.
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
      manifest_durable_periodicity_millis(_manifest_durable_periodicity_millis) {

        assert(manifest_durable_periodicity_millis == 0 ||
	       keep_local_log_files == true);
  }
};

//
// The Cloud environment
//
class CloudEnv : public Env {
 public:
  // Constructor
  CloudEnv(CloudType type, Env* base_env);

  virtual ~CloudEnv();

  // Returns the cloud_type
  const CloudType& GetCloudType() { return cloud_type_; }

  // Mark the db associated with this env as a clone
  void SetClone() { is_clone_ = true; }
  void ClearClone() { is_clone_ = false; }
  bool IsClone() { return is_clone_; }

  Env* GetBaseEnv() { return base_env_; }

  // Create a new AWS env.
  static Status NewAwsEnv(Env* base_env, const std::string& cloud_storage,
		         const CloudEnvOptions& env_options,
			 std::shared_ptr<Logger> logger,
			 CloudEnv** cenv);

 protected:
  // The type of cloud service aws google azure, etc
  CloudType cloud_type_;

  // Is the db associated with this env a clone?
  bool is_clone_;

  // The underlying env
  Env* base_env_;
};

} // namespace

