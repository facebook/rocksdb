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

  CloudEnvOptions(bool _keep_local_sst_files = false,
		  bool _keep_local_log_files = true)
    : keep_local_sst_files(_keep_local_sst_files),
      keep_local_log_files(_keep_local_log_files) {
  }
};

} // namespace

