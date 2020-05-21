//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.

#pragma once
#include <atomic>
#include <thread>

#include "cloud/cloud_env_impl.h"
#include "rocksdb/cloud/cloud_env_options.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace rocksdb {

class MockStorageProvider : public CloudStorageProvider {
 public:
  MockStorageProvider() { notsup_ = Status::NotSupported(); }
  virtual const char* Name() const override { return "Mock"; }
  virtual Status CreateBucket(const std::string& /*bucket_name*/) override {
    return notsup_;
  }

  virtual Status ExistsBucket(const std::string& /*bucket_name*/) override {
    return notsup_;
  }

  virtual Status EmptyBucket(const std::string& /*bucket_name*/,
                             const std::string& /*path_prefix*/) override {
    return notsup_;
  }
  Status ListCloudObjects(const std::string& /*bucket_name*/,
                          const std::string& /*object_path*/,
                          std::vector<std::string>* /*result*/) override {
    return notsup_;
  }
  Status DeleteCloudObject(const std::string& /*bucket_name*/,
                           const std::string& /*object_path*/) override {
    return notsup_;
  }
  Status ExistsCloudObject(const std::string& /*bucket_name*/,
                           const std::string& /*object_path*/) override {
    return notsup_;
  }
  Status GetCloudObjectSize(const std::string& /*bucket_name*/,
                            const std::string& /*object_path*/,
                            uint64_t* /*size*/) override {
    return notsup_;
  }
  Status GetCloudObjectModificationTime(const std::string& /*bucket_name*/,
                                        const std::string& /*object_path*/,
                                        uint64_t* /*time*/) override {
    return notsup_;
  }
  Status GetCloudObjectMetadata(const std::string& /*bucket_name*/,
                                const std::string& /*object_path*/,
                                CloudObjectInformation* /* info */) override {
    return notsup_;
  }
  Status CopyCloudObject(const std::string& /*bucket_name_src*/,
                         const std::string& /*object_path_src*/,
                         const std::string& /*bucket_name_dest*/,
                         const std::string& /*object_path_dest*/) override {
    return notsup_;
  }
  Status PutCloudObjectMetadata(
      const std::string& /*bucket_name*/, const std::string& /*object_path*/,
      const std::unordered_map<std::string, std::string>& /*metadata*/)
      override {
    return notsup_;
  }
  Status NewCloudWritableFile(
      const std::string& /*local_path*/, const std::string& /*bucket_name*/,
      const std::string& /*object_path*/,
      std::unique_ptr<CloudStorageWritableFile>* /*result*/,
      const EnvOptions& /*options*/) override {
    return notsup_;
  }

  Status NewCloudReadableFile(
      const std::string& /*bucket*/, const std::string& /*fname*/,
      std::unique_ptr<CloudStorageReadableFile>* /*result*/,
      const EnvOptions& /*options*/) override {
    return notsup_;
  }

  Status GetCloudObject(const std::string& /*bucket_name*/,
                        const std::string& /*object_path*/,
                        const std::string& /*local_path*/) override {
    return notsup_;
  }

  Status PutCloudObject(const std::string& /*local_path*/,
                        const std::string& /*bucket_name*/,
                        const std::string& /*object_path*/) override {
    return notsup_;
  }

 protected:
  Status notsup_;
};
// An implementation of Env that forwards all calls to another Env.
// May be useful to clients who wish to override just part of the
// functionality of another Env.

class MockCloudEnv : public CloudEnv {
 public:
  // Initialize an EnvWrapper that delegates all calls to *t
  explicit MockCloudEnv(const CloudEnvOptions& opts = CloudEnvOptions())
      : CloudEnv(opts, Env::Default(), nullptr) {
    notsup_ = Status::NotSupported();
  }

  virtual ~MockCloudEnv() {}

  const char* Name() const override { return "MockCloudEnv"; }

  Status PreloadCloudManifest(const std::string& /*local_dbname*/) override {
    return notsup_;
  }

  virtual Status NewSequentialFileCloud(
      const std::string& /*bucket_name*/, const std::string& /*fname*/,
      std::unique_ptr<SequentialFile>* /*result*/,
      const EnvOptions& /*options*/) override {
    return notsup_;
  }
  virtual Status SaveDbid(const std::string& /*bucket_name*/,
                          const std::string& /*dbid */,
                          const std::string& /*dirname*/) override {
    return notsup_;
  }
  virtual Status GetPathForDbid(const std::string& /*bucket_name*/,
                                const std::string& /*dbid*/,
                                std::string* /*dirname*/) override {
    return notsup_;
  }
  virtual Status GetDbidList(const std::string& /*bucket_name*/,
                             DbidList* /*dblist*/) override {
    return notsup_;
  }
  virtual Status DeleteDbid(const std::string& /*bucket_name*/,
                            const std::string& /*dbid*/) override {
    return notsup_;
  }

  // Ability to read a file directly from cloud storage
  virtual Status NewSequentialFileCloud(
      const std::string& /*fname*/, std::unique_ptr<SequentialFile>* /*result*/,
      const EnvOptions& /*options*/) {
    return notsup_;
  }

  // The following text is boilerplate that forwards all methods to base_env
  Status NewSequentialFile(const std::string& f,
                           std::unique_ptr<SequentialFile>* r,
                           const EnvOptions& options) override {
    return base_env_->NewSequentialFile(f, r, options);
  }
  Status NewRandomAccessFile(const std::string& f,
                             std::unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& options) override {
    return base_env_->NewRandomAccessFile(f, r, options);
  }
  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& options) override {
    return base_env_->NewWritableFile(f, r, options);
  }
  Status ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           std::unique_ptr<WritableFile>* r,
                           const EnvOptions& options) override {
    return base_env_->ReuseWritableFile(fname, old_fname, r, options);
  }
  Status NewRandomRWFile(const std::string& fname,
                         std::unique_ptr<RandomRWFile>* result,
                         const EnvOptions& options) override {
    return base_env_->NewRandomRWFile(fname, result, options);
  }
  virtual Status NewDirectory(const std::string& name,
                              std::unique_ptr<Directory>* result) override {
    return base_env_->NewDirectory(name, result);
  }
  Status FileExists(const std::string& f) override {
    return base_env_->FileExists(f);
  }
  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* r) override {
    return base_env_->GetChildren(dir, r);
  }
  Status GetChildrenFileAttributes(
      const std::string& dir, std::vector<FileAttributes>* result) override {
    return base_env_->GetChildrenFileAttributes(dir, result);
  }
  Status DeleteFile(const std::string& f) override {
    return base_env_->DeleteFile(f);
  }
  Status CreateDir(const std::string& d) override {
    return base_env_->CreateDir(d);
  }
  Status CreateDirIfMissing(const std::string& d) override {
    return base_env_->CreateDirIfMissing(d);
  }
  Status DeleteDir(const std::string& d) override {
    return base_env_->DeleteDir(d);
  }
  Status GetFileSize(const std::string& f, uint64_t* s) override {
    return base_env_->GetFileSize(f, s);
  }

  Status GetFileModificationTime(const std::string& fname,
                                 uint64_t* file_mtime) override {
    return base_env_->GetFileModificationTime(fname, file_mtime);
  }

  Status RenameFile(const std::string& s, const std::string& t) override {
    return base_env_->RenameFile(s, t);
  }

  Status LinkFile(const std::string& s, const std::string& t) override {
    return base_env_->LinkFile(s, t);
  }

  Status LockFile(const std::string& f, FileLock** l) override {
    return base_env_->LockFile(f, l);
  }

  Status UnlockFile(FileLock* l) override { return base_env_->UnlockFile(l); }

  void Schedule(void (*f)(void* arg), void* a, Priority pri,
                void* tag = nullptr, void (*u)(void* arg) = 0) override {
    return base_env_->Schedule(f, a, pri, tag, u);
  }

  int UnSchedule(void* tag, Priority pri) override {
    return base_env_->UnSchedule(tag, pri);
  }

  void StartThread(void (*f)(void*), void* a) override {
    return base_env_->StartThread(f, a);
  }
  void WaitForJoin() override { return base_env_->WaitForJoin(); }
  virtual unsigned int GetThreadPoolQueueLen(
      Priority pri = LOW) const override {
    return base_env_->GetThreadPoolQueueLen(pri);
  }
  virtual Status GetTestDirectory(std::string* path) override {
    return base_env_->GetTestDirectory(path);
  }
  virtual Status NewLogger(const std::string& fname,
                           std::shared_ptr<Logger>* result) override {
    return base_env_->NewLogger(fname, result);
  }
  uint64_t NowMicros() override { return base_env_->NowMicros(); }
  void SleepForMicroseconds(int micros) override {
    base_env_->SleepForMicroseconds(micros);
  }
  Status GetHostName(char* name, uint64_t len) override {
    return base_env_->GetHostName(name, len);
  }
  Status GetCurrentTime(int64_t* unix_time) override {
    return base_env_->GetCurrentTime(unix_time);
  }
  Status GetAbsolutePath(const std::string& db_path,
                         std::string* output_path) override {
    return base_env_->GetAbsolutePath(db_path, output_path);
  }
  void SetBackgroundThreads(int num, Priority pri) override {
    return base_env_->SetBackgroundThreads(num, pri);
  }
  int GetBackgroundThreads(Priority pri) override {
    return base_env_->GetBackgroundThreads(pri);
  }

  void IncBackgroundThreadsIfNeeded(int num, Priority pri) override {
    return base_env_->IncBackgroundThreadsIfNeeded(num, pri);
  }

  void LowerThreadPoolIOPriority(Priority pool = LOW) override {
    base_env_->LowerThreadPoolIOPriority(pool);
  }

  std::string TimeToString(uint64_t time) override {
    return base_env_->TimeToString(time);
  }

  Status GetThreadList(std::vector<ThreadStatus>* thread_list) override {
    return base_env_->GetThreadList(thread_list);
  }

  ThreadStatusUpdater* GetThreadStatusUpdater() const override {
    return base_env_->GetThreadStatusUpdater();
  }

  uint64_t GetThreadID() const override { return base_env_->GetThreadID(); }

  Status DeleteCloudFileFromDest(const std::string& /*path*/) override {
    return notsup_;
  }

 private:
  Status notsup_;
  std::string empty_;
};
}  // namespace rocksdb
