//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//

#pragma once
#include <stdio.h>
#include <time.h>

#include <algorithm>
#include <iostream>

#include "rocksdb/cloud/cloud_file_system_impl.h"

#ifdef USE_AWS

#include <string.h>

#include <chrono>
#include <list>
#include <unordered_map>

namespace ROCKSDB_NAMESPACE {
//
// The S3 environment for rocksdb. This class overrides all the
// file/dir access methods and delegates all other methods to the
// default posix environment.
//
// When a SST file is written and closed, it is uploaded synchronusly
// to AWS-S3. The local copy of the sst file is either deleted immediately
// or kept depending on a configuration parameter called keep_local_sst_files.
// If the local copy of the sst file is around, then future reads are served
// from the local sst file. If the local copy of the sst file is not found
// locally, then every read request to portions of that file is translated to
// a range-get request from the corresponding AWS-S3 file-object.
//
// When a WAL file or MANIFEST file is written, every write is synchronously
// written to a Kinesis stream.
//
// If you access multiple rocksdb-cloud instances, create a separate instance
// of AwsFileSystem for each of those rocksdb-cloud instances. This is required
// because the cloud-configuration needed to operate on an individual instance
// of rocksdb is associated with a specific instance of AwsFileSystem. All
// AwsFileSystem internally share PosixFileSystem for sharing common file system
// resources.
//
// NOTE: The AWS SDK must be initialized before any AwsFileSystem is
// constructed, and remain active (Aws::ShutdownAPI() not called) as long as any
// AwsFileSystem objects exist.
//
class AwsFileSystem : public CloudFileSystemImpl {
 public:
  // A factory method for creating S3 envs
  static Status NewAwsFileSystem(const std::shared_ptr<FileSystem>& fs,
                                 const CloudFileSystemOptions& cloud_options,
                                 const std::shared_ptr<Logger>& info_log,
                                 CloudFileSystem** cfs);
  static Status NewAwsFileSystem(const std::shared_ptr<FileSystem>& fs,
                                 std::unique_ptr<CloudFileSystem>* cfs);
  virtual ~AwsFileSystem() {}

  static const char* kName() { return kAws(); }
  const char* Name() const override { return kAws(); }

  Status PrepareOptions(const ConfigOptions& options) override;
  // If you do not specify a region, then S3 buckets are created in the
  // standard-region which might not satisfy read-your-own-writes. So,
  // explicitly make the default region be us-west-2.
  static constexpr const char* default_region = "us-west-2";

 private:
  //
  // The AWS credentials are specified to the constructor via
  // access_key_id and secret_key.
  //
  explicit AwsFileSystem(const std::shared_ptr<FileSystem>& underlying_fs,
                         const CloudFileSystemOptions& cloud_options,
                         const std::shared_ptr<Logger>& info_log = nullptr);
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // USE_AWS
