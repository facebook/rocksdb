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

#include <chrono>
#include <list>
#include <unordered_map>

namespace rocksdb {

class S3ReadableFile;


namespace detail {
struct JobHandle;
}  // namespace detail

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
  static Status NewAwsEnv(Env* env,
                          const CloudEnvOptions& env_options,
                          const std::shared_ptr<Logger> & info_log, CloudEnv** cenv);

  virtual ~AwsEnv();

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

  virtual Status NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) override;

  virtual Status NewSequentialFileCloud(const std::string& bucket,
                                        const std::string& fname,
                                        std::unique_ptr<SequentialFile>* result,
                                        const EnvOptions& options) override;

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     std::unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& options) override;

  virtual Status NewWritableFile(const std::string& fname,
                                 std::unique_ptr<WritableFile>* result,
                                 const EnvOptions& options) override;

  virtual Status ReopenWritableFile(const std::string& /*fname*/,
                                    std::unique_ptr<WritableFile>* /*result*/,
                                    const EnvOptions& /*options*/) override;

  virtual Status NewDirectory(const std::string& name,
                              std::unique_ptr<Directory>* result) override;

  virtual Status FileExists(const std::string& fname) override;

  virtual Status GetChildren(const std::string& path,
                             std::vector<std::string>* result) override;

  virtual Status DeleteFile(const std::string& fname) override;

  virtual Status CreateDir(const std::string& name) override;

  virtual Status CreateDirIfMissing(const std::string& name) override;

  virtual Status DeleteDir(const std::string& name) override;

  virtual Status GetFileSize(const std::string& fname, uint64_t* size) override;

  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* file_mtime) override;

  virtual Status RenameFile(const std::string& src,
                            const std::string& target) override;

  virtual Status LinkFile(const std::string& src,
                          const std::string& target) override;

  virtual Status LockFile(const std::string& fname, FileLock** lock) override;

  virtual Status UnlockFile(FileLock* lock) override;

  virtual Status NewLogger(const std::string& fname,
                           std::shared_ptr<Logger>* result) override;

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

  static uint64_t gettid() {
    assert(sizeof(pthread_t) <= sizeof(uint64_t));
    return (uint64_t)pthread_self();
  }

  virtual uint64_t GetThreadID() const override { return AwsEnv::gettid(); }

  std::string GetWALCacheDir();

  // Saves and retrieves the dbid->dirname mapping in S3
  Status SaveDbid(const std::string& bucket_name, const std::string& dbid,
                  const std::string& dirname) override;
  Status GetPathForDbid(const std::string& bucket,
                        const std::string& dbid, std::string* dirname) override;
  Status GetDbidList(const std::string& bucket,
                     DbidList* dblist) override;
  Status DeleteDbid(const std::string& bucket,
                    const std::string& dbid) override;
  Status DeleteCloudFileFromDest(const std::string& fname) override;
  Status CopyLocalFileToDest(const std::string& local_name,
                             const std::string& cloud_name) override;

  void RemoveFileFromDeletionQueue(const std::string& filename);

  void TEST_SetFileDeletionDelay(std::chrono::seconds delay) {
    std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
    file_deletion_delay_ = delay;
  }

  Status TEST_DeletePathInS3(const std::string& bucket,
                             const std::string& fname);

 private:
  //
  // The AWS credentials are specified to the constructor via
  // access_key_id and secret_key.
  //
  explicit AwsEnv(Env* underlying_env,
                  const CloudEnvOptions& cloud_options,
                  const std::shared_ptr<Logger> & info_log = nullptr);



  // The pathname that contains a list of all db's inside a bucket.
  static constexpr const char* dbid_registry_ = "/.rockset/dbid/";

  Status create_bucket_status_;

  std::mutex files_to_delete_mutex_;
  std::chrono::seconds file_deletion_delay_ = std::chrono::hours(1);
  std::unordered_map<std::string, std::shared_ptr<detail::JobHandle>>
      files_to_delete_;
  Random64 rng_;

  Status status();

  // Validate options
  Status CheckOption(const EnvOptions& options);


  Status NewS3ReadableFile(const std::string& bucket, const std::string& fname,
                           std::unique_ptr<S3ReadableFile>* result);

  // Save IDENTITY file to S3. Update dbid registry.
  Status SaveIdentitytoS3(const std::string& localfile,
                          const std::string& target_idfile);

  // Converts a local pathname to an object name in the src bucket
  std::string srcname(const std::string& localname);

  // Converts a local pathname to an object name in the dest bucket
  std::string destname(const std::string& localname);
};

}  // namespace rocksdb

#endif  // USE_AWS
