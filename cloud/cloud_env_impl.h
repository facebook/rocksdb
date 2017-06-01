//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.

#pragma once
#include <atomic>
#include <thread>
#include "rocksdb/cloud/cloud_env_options.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"

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

  // Returns the cloud_type
  const CloudType& GetCloudType() { return cloud_type_; }

  // Returns the underlying env
  Env* GetBaseEnv() { return base_env_; }

  // The separator used to separate dbids while creating the dbid of a clone
  static constexpr const char* DBID_SEPARATOR =  "rockset";

  // A map from a dbid to the list of all its parent dbids.
  typedef std::map<std::string, std::vector<std::string>> DbidParents;

  Status FindObsoleteFiles(const std::string& bucket_name_prefix,
                           std::vector<std::string>* pathnames);
  Status FindObsoleteDbid(const std::string& bucket_name_prefix,
                          std::vector<std::string>* dbids);
  Status extractParents(const std::string& bucket_name_prefix,
                        const DbidList& dbid_list,
                        DbidParents* parents);

 protected:
  // The type of cloud service aws google azure, etc
  CloudType cloud_type_;

  // The dbid of the source database that is cloned
  std::string src_dbid_;

  // The pathname of the source database that is cloned
  std::string src_dbdir_;

  // The underlying env
  Env* base_env_;

  // The purger keep on running till this is set to false.
  std::atomic<bool> purger_is_running_;

  std::shared_ptr<Logger> info_log_;  // informational messages

  std::thread purge_thread_;

  // A background thread that deletes orphaned objects in cloud storage
  void Purger();
  void StopPurger();

 private:

    // scratch space in local dir
  static constexpr const char* SCRATCH_LOCAL_DIR = "/tmp";
};

}  // namespace rocksdb
