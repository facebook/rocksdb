//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.

#pragma once
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include "cloud/cloud_manifest.h"
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
  CloudEnvImpl(const CloudEnvOptions & options, Env* base_env, const std::shared_ptr<Logger>& logger);

  virtual ~CloudEnvImpl();

  const CloudType& GetCloudType() const { return cloud_env_options.cloud_type; }

  Status SanitizeDirectory(const DBOptions& options,
                           const std::string& clone_name, bool read_only);
  Status LoadCloudManifest(const std::string& local_dbname, bool read_only);
  // The separator used to separate dbids while creating the dbid of a clone
  static constexpr const char* DBID_SEPARATOR = "rockset";

  // A map from a dbid to the list of all its parent dbids.
  typedef std::map<std::string, std::vector<std::string>> DbidParents;

  Status FindObsoleteFiles(const std::string& bucket_name_prefix,
                           std::vector<std::string>* pathnames);
  Status FindObsoleteDbid(const std::string& bucket_name_prefix,
                          std::vector<std::string>* dbids);
  Status extractParents(const std::string& bucket_name_prefix,
                        const DbidList& dbid_list, DbidParents* parents);
  virtual Status PreloadCloudManifest(const std::string& local_dbname) override;

  Status LoadLocalCloudManifest(const std::string& dbname);
  // Transfers the filename from RocksDB's domain to the physical domain, based
  // on information stored in CLOUDMANIFEST.
  // For example, it will map 00010.sst to 00010.sst-[epoch] where [epoch] is
  // an epoch during which that file was created.
  // Files both in S3 and in the local directory have this [epoch] suffix.
  std::string RemapFilename(const std::string& logical_path) const;

  // This will delete all files in dest bucket and locally whose epochs are
  // invalid. For example, if we find 00010.sst-[epochX], but the real mapping
  // for 00010.sst is [epochY], in this function we will delete
  // 00010.sst-[epochX]. Note that local files are deleted immediately, while
  // cloud files are deleted with a delay of one hour (just to prevent issues
  // from two RocksDB databases running on the same bucket for a short time).
  Status DeleteInvisibleFiles(const std::string& dbname);

  EnvOptions OptimizeForLogRead(const EnvOptions& env_options) const override {
    return base_env_->OptimizeForLogRead(env_options);
  }
  EnvOptions OptimizeForManifestRead(
      const EnvOptions& env_options) const override {
    return base_env_->OptimizeForManifestRead(env_options);
  }
  EnvOptions OptimizeForLogWrite(const EnvOptions& env_options,
                                 const DBOptions& db_options) const override {
    return base_env_->OptimizeForLogWrite(env_options, db_options);
  }
  EnvOptions OptimizeForManifestWrite(
      const EnvOptions& env_options) const override {
    return base_env_->OptimizeForManifestWrite(env_options);
  }
  EnvOptions OptimizeForCompactionTableWrite(
      const EnvOptions& env_options,
      const ImmutableDBOptions& immutable_ops) const override {
    return base_env_->OptimizeForCompactionTableWrite(env_options,
                                                      immutable_ops);
  }
  EnvOptions OptimizeForCompactionTableRead(
      const EnvOptions& env_options,
      const ImmutableDBOptions& db_options) const override {
    return base_env_->OptimizeForCompactionTableRead(env_options, db_options);
  }
  Status GetFreeSpace(const std::string& path, uint64_t* diskfree) override {
    return base_env_->GetFreeSpace(path, diskfree);
  }

  CloudManifest* GetCloudManifest() { return cloud_manifest_.get(); }
  void TEST_InitEmptyCloudManifest();
  void TEST_DisableCloudManifest() { test_disable_cloud_manifest_ = true; }

  Status GetThreadList(std::vector<ThreadStatus>* thread_list) override {
    return base_env_->GetThreadList(thread_list);
  }

 protected:
  // Does the dir need to be re-initialized?
  Status NeedsReinitialization(const std::string& clone_dir, bool* do_reinit);

  Status GetCloudDbid(const std::string& local_dir, std::string* src_dbid,
                      std::string* dest_dbid);

  Status ResyncDir(const std::string& local_dir);

  Status CreateNewIdentityFile(const std::string& dbid,
                               const std::string& local_name);

  Status MaybeMigrateManifestFile(const std::string& local_dbname);
  Status FetchCloudManifest(const std::string& local_dbname, bool force);

  Status RollNewEpoch(const std::string& local_dbname);
  // The dbid of the source database that is cloned
  std::string src_dbid_;

  // The pathname of the source database that is cloned
  std::string src_dbdir_;

  // Protects purger_cv_
  std::mutex purger_lock_;
  std::condition_variable purger_cv_;
  // The purger keep on running till this is set to false. (and is notified on
  // purger_cv_);
  bool purger_is_running_;
  std::thread purge_thread_;

  // A background thread that deletes orphaned objects in cloud storage
  void Purger();
  void StopPurger();

 private:
  Status writeCloudManifest(CloudManifest* manifest, const std::string& fname);
  std::string generateNewEpochId();
  std::unique_ptr<CloudManifest> cloud_manifest_;
  // This runs only in tests when we want to disable cloud manifest
  // functionality
  bool test_disable_cloud_manifest_{false};

  // scratch space in local dir
  static constexpr const char* SCRATCH_LOCAL_DIR = "/tmp";
};

}  // namespace rocksdb
