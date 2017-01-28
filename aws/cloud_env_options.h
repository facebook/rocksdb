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
  // If true, then sst files are stored locally as well as on the cloud.
  // Reads are always satisfied from local storage
  // If false, then local sst files are created, uploaded to cloud immediately,
  // and the local file is deleted. All reads are satisfied by feting data from
  // the cloud.
  // Default:  false
  bool keep_local_sst_files;

  CloudEnvOptions(bool _keep_local_sst_files = false)
    : keep_local_sst_files(_keep_local_sst_files) {
  }
};

} // namespace

