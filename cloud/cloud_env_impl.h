//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.

#pragma once
#include <atomic>
#include <thread>
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/cloud/cloud_env_options.h"

namespace rocksdb {

//
// The Cloud environment
//
class CloudEnvImpl : public CloudEnv {
 friend class CloudEnv;
 public:
  // Constructor
  CloudEnvImpl(CloudType type, Env* base_env);

  virtual ~CloudEnvImpl();

  // Ability to read a file directly from cloud storage
  virtual Status NewSequentialFileCloud(const std::string& fname,
		                        unique_ptr<SequentialFile>* result,
					const EnvOptions& options) = 0;
  // Returns the cloud_type
  const CloudType& GetCloudType() { return cloud_type_; }

  // Mark the db associated with this env as a clone
  Status SetClone(const std::string& src_dbid);
  void ClearClone() { is_clone_ = false; }
  bool IsClone() { return is_clone_; }

  // Mark the env so that all requests are satisfied directly and
  // only from cloud storage.
  void SetCloudDirect() { is_cloud_direct_ = true; }
  void ClearCloudDirect() { is_cloud_direct_ = false; }

  // Map a clonepathname to a pathname in the src db
  std::string MapClonePathToSrcPath(const std::string& fname);

  // Returns the underlying env
  Env* GetBaseEnv() { return base_env_; }

 protected:
  // The type of cloud service aws google azure, etc
  CloudType cloud_type_;

  // If set, all requests are satisfied directly from the cloud
  bool is_cloud_direct_;

  // If set, then sst files are fetched from either the dbdir or
  // from src_dbid_.
  bool is_clone_;

  // The dbid of the source database that is cloned
  std::string src_dbid_;

  // The pathname of the source database that is cloned
  std::string src_dbdir_;

  // The underlying env
  Env* base_env_;

  // The purger keep on running till this is set to false.
  std::atomic<bool> purger_is_running_;

  std::shared_ptr<Logger> info_log_;    // informational messages

  std::thread purge_thread_;

  // A background thread that deletes orphaned objects in cloud storage
  void Purger();

  Status PurgeObsoleteFiles();
  Status PurgeObsoleteDbid();
};
}
