//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once

namespace rocksdb {

class KinesisSystem;
class CloudEnvOptions;

//
// The cloud environment for rocksdb. It allows configuring the rocksdb
// Environent used for the cloud.
//
class CloudEnvOptions {
 public:
  //
  // If true,  then sst files are stored locally as well as on the cloud.
  //           Reads are always satisfied from local storage
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

  CloudEnvOptions(bool _keep_local_sst_files = false,
		  bool _keep_local_log_files = true,
		  uint64_t _manifest_durable_periodicity_millis = 60 * 1000)
    : keep_local_sst_files(_keep_local_sst_files),
      keep_local_log_files(_keep_local_log_files),
      manifest_durable_periodicity_millis(_manifest_durable_periodicity_millis) {

        assert(manifest_durable_periodicity_millis == 0 ||
	       keep_local_log_files == true);
  }
};

} // namespace

