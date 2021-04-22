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

namespace ROCKSDB_NAMESPACE {
class CloudScheduler;
class CloudStorageReadableFile;

//
// The Cloud environment
//
class CloudEnvImpl : public CloudEnv {
  friend class CloudEnv;

 public:
  // Constructor
  CloudEnvImpl(const CloudEnvOptions& options, Env* base_env,
               const std::shared_ptr<Logger>& logger);

  virtual ~CloudEnvImpl();

  const CloudType& GetCloudType() const { return cloud_env_options.cloud_type; }

  Status NewSequentialFile(const std::string& fname,
                           std::unique_ptr<SequentialFile>* result,
                           const EnvOptions& options) override;
  Status NewSequentialFileCloud(const std::string& bucket,
                                const std::string& fname,
                                std::unique_ptr<SequentialFile>* result,
                                const EnvOptions& options) override;

  Status NewRandomAccessFile(const std::string& fname,
                             std::unique_ptr<RandomAccessFile>* result,
                             const EnvOptions& options) override;

  Status NewWritableFile(const std::string& fname,
                         std::unique_ptr<WritableFile>* result,
                         const EnvOptions& options) override;

  Status ReopenWritableFile(const std::string& /*fname*/,
                            std::unique_ptr<WritableFile>* /*result*/,
                            const EnvOptions& /*options*/) override;

  Status RenameFile(const std::string& src, const std::string& target) override;

  Status LinkFile(const std::string& src, const std::string& target) override;

  Status FileExists(const std::string& fname) override;

  Status GetChildren(const std::string& path,
                     std::vector<std::string>* result) override;

  Status GetFileSize(const std::string& fname, uint64_t* size) override;

  Status GetFileModificationTime(const std::string& fname,
                                 uint64_t* file_mtime) override;

  Status NewDirectory(const std::string& name,
                      std::unique_ptr<Directory>* result) override;

  Status CreateDir(const std::string& name) override;

  Status CreateDirIfMissing(const std::string& name) override;

  Status DeleteDir(const std::string& name) override;

  Status DeleteFile(const std::string& fname) override;

  Status NewLogger(const std::string& fname,
                   std::shared_ptr<Logger>* result) override {
    return base_env_->NewLogger(fname, result);
  }

  virtual void Schedule(void (*function)(void* arg), void* arg,
                        Priority pri = LOW, void* tag = nullptr,
                        void (*unschedFunction)(void* arg) = 0) override {
    base_env_->Schedule(function, arg, pri, tag, unschedFunction);
  }

  virtual int UnSchedule(void* tag, Priority pri) override {
    return base_env_->UnSchedule(tag, pri);
  }

  virtual void StartThread(void (*function)(void* arg), void* arg) override {
    base_env_->StartThread(function, arg);
  }

  virtual void WaitForJoin() override { base_env_->WaitForJoin(); }

  virtual unsigned int GetThreadPoolQueueLen(
      Priority pri = LOW) const override {
    return base_env_->GetThreadPoolQueueLen(pri);
  }

  virtual Status GetTestDirectory(std::string* path) override {
    return base_env_->GetTestDirectory(path);
  }

  virtual uint64_t NowMicros() override { return base_env_->NowMicros(); }

  virtual void SleepForMicroseconds(int micros) override {
    base_env_->SleepForMicroseconds(micros);
  }

  virtual Status GetHostName(char* name, uint64_t len) override {
    return base_env_->GetHostName(name, len);
  }

  virtual Status GetCurrentTime(int64_t* unix_time) override {
    return base_env_->GetCurrentTime(unix_time);
  }

  virtual Status GetAbsolutePath(const std::string& db_path,
                                 std::string* output_path) override {
    return base_env_->GetAbsolutePath(db_path, output_path);
  }

  virtual void SetBackgroundThreads(int number, Priority pri = LOW) override {
    base_env_->SetBackgroundThreads(number, pri);
  }
  int GetBackgroundThreads(Priority pri) override {
    return base_env_->GetBackgroundThreads(pri);
  }

  virtual void IncBackgroundThreadsIfNeeded(int number, Priority pri) override {
    base_env_->IncBackgroundThreadsIfNeeded(number, pri);
  }

  virtual std::string TimeToString(uint64_t number) override {
    return base_env_->TimeToString(number);
  }

  virtual uint64_t GetThreadID() const override {
    return base_env_->GetThreadID();
  }

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

  // Load CLOUDMANIFEST if exists in local disk to current env.
  Status LoadLocalCloudManifest(const std::string& dbname);

  // Local CLOUDMANIFEST from `base_env` into `cloud_manifest`.
  static Status LoadLocalCloudManifest(
      const std::string& dbname, Env* base_env,
      std::unique_ptr<CloudManifest>* cloud_manifest);

  // Transfers the filename from RocksDB's domain to the physical domain, based
  // on information stored in CLOUDMANIFEST.
  // For example, it will map 00010.sst to 00010.sst-[epoch] where [epoch] is
  // an epoch during which that file was created.
  // Files both in S3 and in the local directory have this [epoch] suffix.
  std::string RemapFilename(const std::string& logical_path) const override;

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

  Status DeleteCloudFileFromDest(const std::string& fname) override;
  Status CopyLocalFileToDest(const std::string& local_name,
                             const std::string& cloud_name) override;

  void RemoveFileFromDeletionQueue(const std::string& filename);

  void TEST_SetFileDeletionDelay(std::chrono::seconds delay) {
    std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
    file_deletion_delay_ = delay;
  }

 protected:
  // The pathname that contains a list of all db's inside a bucket.
  virtual const char* kDbIdRegistry() const { return "/.rockset/dbid/"; }

  std::string GetDbIdKey(const std::string& dbid) {
    return kDbIdRegistry() + dbid;
  }

  // Checks to see if the input fname exists in the dest or src bucket
  Status ExistsCloudObject(const std::string& fname);

  // Gets the cloud object fname from the dest or src bucket
  Status GetCloudObject(const std::string& fname);

  // Gets the size of the named cloud object from the dest or src bucket
  Status GetCloudObjectSize(const std::string& fname, uint64_t* remote_size);

  // Gets the modification time of the named cloud object from the dest or src
  // bucket
  Status GetCloudObjectModificationTime(const std::string& fname,
                                        uint64_t* time);

  // Returns the list of cloud objects from the src and dest buckets.
  Status ListCloudObjects(const std::string& path,
                          std::vector<std::string>* result);

  // Returns a CloudStorageReadableFile from the dest or src bucket
  Status NewCloudReadableFile(const std::string& fname,
                              std::unique_ptr<CloudStorageReadableFile>* result,
                              const EnvOptions& options);

  // Copy IDENTITY file to cloud storage. Update dbid registry.
  Status SaveIdentityToCloud(const std::string& localfile,
                             const std::string& idfile);

  // Check if options are compatible with the storage system
  virtual Status CheckOption(const EnvOptions& options);

  virtual Status Prepare();
  // Converts a local pathname to an object name in the src bucket
  std::string srcname(const std::string& localname);

  // Converts a local pathname to an object name in the dest bucket
  std::string destname(const std::string& localname);

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

  std::shared_ptr<CloudScheduler> scheduler_;

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
  std::mutex files_to_delete_mutex_;
  std::chrono::seconds file_deletion_delay_ = std::chrono::hours(1);
  std::unordered_map<std::string, int> files_to_delete_;
};

}  // namespace ROCKSDB_NAMESPACE
