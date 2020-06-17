//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//

#pragma once
#include <stdio.h>
#include <time.h>

#include <algorithm>
#include <iostream>

#include "cloud/cloud_env_impl.h"
#include "port/sys_time.h"
#include "util/random.h"

#ifdef USE_AWS

#include <string.h>

#include <chrono>
#include <list>
#include <unordered_map>

namespace ROCKSDB_NAMESPACE {

class S3ReadableFile;
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
// of AwsEnv for each of those rocksdb-cloud instances. This is required because
// the cloud-configuration needed to operate on an individual instance of
// rocksdb
// is associated with a specific instance of AwsEnv. All AwsEnv internally share
// Env::Posix() for sharing common resources like background threads, etc.
//
class AwsEnv : public CloudEnvImpl {
 public:
  // A factory method for creating S3 envs
  static Status NewAwsEnv(Env* env, const CloudEnvOptions& env_options,
                          const std::shared_ptr<Logger>& info_log,
                          CloudEnv** cenv);

  virtual ~AwsEnv() {}

  const char* Name() const override { return "aws"; }

  // We cannot invoke Aws::ShutdownAPI from the destructor because there could
  // be
  // multiple AwsEnv's ceated by a process and Aws::ShutdownAPI should be called
  // only once by the entire process when all AwsEnvs are destroyed.
  static void Shutdown();

  // If you do not specify a region, then S3 buckets are created in the
  // standard-region which might not satisfy read-your-own-writes. So,
  // explicitly make the default region be us-west-2.
  static constexpr const char* default_region = "us-west-2";

  virtual Status LockFile(const std::string& fname, FileLock** lock) override;

  virtual Status UnlockFile(FileLock* lock) override;

  std::string GetWALCacheDir();

  // Saves and retrieves the dbid->dirname mapping in S3
  Status SaveDbid(const std::string& bucket_name, const std::string& dbid,
                  const std::string& dirname) override;
  Status GetPathForDbid(const std::string& bucket, const std::string& dbid,
                        std::string* dirname) override;
  Status GetDbidList(const std::string& bucket, DbidList* dblist) override;
  Status DeleteDbid(const std::string& bucket,
                    const std::string& dbid) override;

 private:
  //
  // The AWS credentials are specified to the constructor via
  // access_key_id and secret_key.
  //
  explicit AwsEnv(Env* underlying_env, const CloudEnvOptions& cloud_options,
                  const std::shared_ptr<Logger>& info_log = nullptr);

  // The pathname that contains a list of all db's inside a bucket.
  static constexpr const char* dbid_registry_ = "/.rockset/dbid/";

  Random64 rng_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // USE_AWS
