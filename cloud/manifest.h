// Copyright (c) 2017 Rockset

#pragma once

#ifndef ROCKSDB_LITE
#include <set>
#include <string>
#include <vector>

#include "cloud/cloud_env_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"

namespace rocksdb {

//
// Operates on MANIFEST files stored in the cloud bucket
//
class CloudManifest {
 public:
  CloudManifest(std::shared_ptr<Logger> info_log,
                CloudEnv* cenv,
                const std::string& bucket_prefix
                );

  virtual ~CloudManifest();

  // Retrieve all live files referred to by this MANIFEST
  Status GetLiveFiles(const std::string manifest_path,
                      std::set<uint64_t>* list);

 private:
  std::shared_ptr<Logger> info_log_;
  CloudEnv* cenv_;
  std::string bucket_prefix_;
};
}
#endif  // ROCKSDB_LITE
