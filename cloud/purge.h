// Copyright (c) 2017 Rockset

#pragma once

#ifndef ROCKSDB_LITE
#include <deque>
#include <string>
#include <vector>

#include "cloud/cloud_env_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"

namespace rocksdb {

//
// Purges all unneeded files in a storage bucket
//
class Purge {
 public:
  // The bucket name is specified when the CloudEnv was created.
  Purge(CloudEnvImpl* env, std::shared_ptr<Logger> info_log);

  virtual ~Purge();

  // Remove any un-needed files from the storage bucket
  virtual Status PurgeObsoleteFiles();

  // Remove any dbids that do not have backing files in S3
  virtual Status PurgeObsoleteDbid();

 private:
  CloudEnvImpl* cenv_;
  std::shared_ptr<Logger> info_log_;
};
}
#endif  // ROCKSDB_LITE
