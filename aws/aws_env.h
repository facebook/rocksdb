//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//

#pragma once
#include <algorithm>
#include <stdio.h>
#include <time.h>
#include <iostream>
#include "port/sys_time.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"

#ifdef USE_AWS

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/utils/Outcome.h>
#include <aws/s3/S3Client.h>
#include <aws/kinesis/KinesisClient.h>

namespace rocksdb {

class KinesisSystem;

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
class AwsEnv : public Env {
 public:
  // A factory method for creating S3 envs
  static  AwsEnv* NewAwsEnv(const std::string& bucket_prefix,
                          const std::string& access_key_id,
                          const std::string& secret_key,
                          std::shared_ptr<Logger> info_log = nullptr);

  virtual ~AwsEnv();

  // We cannot invoke Aws::ShutdownAPI from the destructor because there could be
  // multiple S#Env's ceated by a process and Aws::ShutdownAPI should be called
  // only once by the entire process when all AwsEnvs are destroyed.
  static void Shutdown() {
    Aws::ShutdownAPI(Aws::SDKOptions());
  }

  virtual Status NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options);

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     std::unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& options);

  virtual Status NewWritableFile(const std::string& fname,
                                 std::unique_ptr<WritableFile>* result,
                                 const EnvOptions& options);

  virtual Status NewDirectory(const std::string& name,
                              std::unique_ptr<Directory>* result);

  virtual Status FileExists(const std::string& fname);

  virtual Status GetChildren(const std::string& path,
                             std::vector<std::string>* result);

  virtual Status DeleteFile(const std::string& fname);

  virtual Status CreateDir(const std::string& name);

  virtual Status CreateDirIfMissing(const std::string& name);

  virtual Status DeleteDir(const std::string& name);

  virtual Status GetFileSize(const std::string& fname, uint64_t* size);

  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* file_mtime);

  virtual Status RenameFile(const std::string& src, const std::string& target);

  virtual Status LinkFile(const std::string& src, const std::string& target) {
    return Status::NotSupported(); // not supported
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock);

  virtual Status UnlockFile(FileLock* lock);

  virtual Status NewLogger(const std::string& fname,
                           std::shared_ptr<Logger>* result);

  virtual void Schedule(void (*function)(void* arg), void* arg,
                        Priority pri = LOW, void* tag = nullptr, void (*unschedFunction)(void* arg) = 0) {
    posixEnv_->Schedule(function, arg, pri, tag, unschedFunction);
  }

  virtual int UnSchedule(void* tag, Priority pri) {
    return posixEnv_->UnSchedule(tag, pri);
  }

  virtual void StartThread(void (*function)(void* arg), void* arg) {
    posixEnv_->StartThread(function, arg);
  }

  virtual void WaitForJoin() { posixEnv_->WaitForJoin(); }

  virtual unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const
      override {
    return posixEnv_->GetThreadPoolQueueLen(pri);
  }

  virtual Status GetTestDirectory(std::string* path) {
    return posixEnv_->GetTestDirectory(path);
  }

  virtual uint64_t NowMicros() {
    return posixEnv_->NowMicros();
  }

  virtual void SleepForMicroseconds(int micros) {
    posixEnv_->SleepForMicroseconds(micros);
  }

  virtual Status GetHostName(char* name, uint64_t len) {
    return posixEnv_->GetHostName(name, len);
  }

  virtual Status GetCurrentTime(int64_t* unix_time) {
    return posixEnv_->GetCurrentTime(unix_time);
  }

  virtual Status GetAbsolutePath(const std::string& db_path,
      std::string* output_path) {
    return posixEnv_->GetAbsolutePath(db_path, output_path);
  }

  virtual void SetBackgroundThreads(int number, Priority pri = LOW) {
    posixEnv_->SetBackgroundThreads(number, pri);
  }

  virtual void IncBackgroundThreadsIfNeeded(int number, Priority pri) override {
    posixEnv_->IncBackgroundThreadsIfNeeded(number, pri);
  }

  virtual std::string TimeToString(uint64_t number) {
    return posixEnv_->TimeToString(number);
  }

  static uint64_t gettid() {
    assert(sizeof(pthread_t) <= sizeof(uint64_t));
    return (uint64_t)pthread_self();
  }

  virtual uint64_t GetThreadID() const override {
    return AwsEnv::gettid();
  }

  // get the posix env
  Env* GetPosixEnv() const { return posixEnv_; }

  bool IsRunning() const { return running_; }

  const std::string bucket_prefix_;
  std::shared_ptr<Logger> info_log_;    // informational messages

  // The S3 client
  std::shared_ptr<Aws::S3::S3Client>  s3client_;

  // The Kinesis client
  std::shared_ptr<Aws::Kinesis::KinesisClient> kinesis_client_;

  // If set, then all sst files have a replica on the local storage as
  // well as AWS-S3. Reads are satisfied from the local replica.
  // If reset, then all sst files are stored only on AWS-S3. Reads are
  // satisfied directly from the AWS-S3 files.
  const bool keep_local_sst_files_;

  //
  // Get credentials for running unit tests
  //
  static Status GetTestCredentials(std::string* aws_access_key_id,
		                   std::string* aws_secret_access_key);

 private:
  //
  // The AWS credentials are specified to the constructor via
  // access_key_id and secret_key.
  //  
  explicit AwsEnv(const std::string& bucket_prefix,
		 const std::string& access_key_id,
		 const std::string& secret_key,
		 std::shared_ptr<Logger> info_log = nullptr,
		 const bool keep_local_sst_files = false);

  Env*  posixEnv_;      // This object is derived from Env, but not from
                        // posixEnv_. We have posixnv as an encapsulated
                        // object here so that we can use posix timers,
                        // posix threads, etc.
  Status create_bucket_status_;

  // Background thread to tail stream
  std::thread tid_;
  std::atomic<bool> running_;

  std::unique_ptr<KinesisSystem> tailer_;

  // Create bucket in S3
  Status IsValid();

  // Check if the specified pathname exists
  Status PathExistsInS3(const std::string& fname, bool isfile);

  // Delete the specified path from S3
  Status DeletePathInS3(const std::string& fname);

  // Get size and modtime of file in S3
  Status GetFileInfoInS3(const std::string& fname, uint64_t* size,
		         uint64_t* modtime);

  // Validate options
  Status CheckOption(const EnvOptions& options);

  // Determine type of a file based on filename
  void GetFileType(const std::string& fname,
                   bool* sstFile, bool* logfile);

  // Return the list of children of the specified path
  Status GetChildrenFromS3(const std::string& path,
		           std::vector<std::string>* result);
};

}  // namespace rocksdb

#else // USE_AWS


namespace rocksdb {

static const Status s3_notsup;

class AwsEnv : public Env {

 public:
  explicit AwsEnv(const std::string& fsname) {
    fprintf(stderr, "You have not build rocksdb with S3 support\n");
    fprintf(stderr, "Please see hdfs/README for details\n");
    abort();
  }

  virtual ~AwsEnv() {
  }

  virtual Status NewSequentialFile(const std::string& fname,
                                   unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) override;

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& options) override {
    return s3_notsup;
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 unique_ptr<WritableFile>* result,
                                 const EnvOptions& options) override {
    return s3_notsup;
  }

  virtual Status NewDirectory(const std::string& name,
                              unique_ptr<Directory>* result) override {
    return s3_notsup;
  }

  virtual Status FileExists(const std::string& fname) override {
    return s3_notsup;
  }

  virtual Status GetChildren(const std::string& path,
                             std::vector<std::string>* result) override {
    return s3_notsup;
  }

  virtual Status DeleteFile(const std::string& fname) override {
    return s3_notsup;
  }

  virtual Status CreateDir(const std::string& name) override { return s3_notsup; }

  virtual Status CreateDirIfMissing(const std::string& name) override {
    return s3_notsup;
  }

  virtual Status DeleteDir(const std::string& name) override { return s3_notsup; }

  virtual Status GetFileSize(const std::string& fname,
                             uint64_t* size) override {
    return s3_notsup;
  }

  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* time) override {
    return s3_notsup;
  }

  virtual Status RenameFile(const std::string& src,
                            const std::string& target) override {
    return s3_notsup;
  }

  virtual Status LinkFile(const std::string& src,
                          const std::string& target) override {
    return s3_notsup;
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) override {
    return s3_notsup;
  }

  virtual Status UnlockFile(FileLock* lock) override { return s3_notsup; }

  virtual Status NewLogger(const std::string& fname,
                           shared_ptr<Logger>* result) override {
    return s3_notsup;
  }

  virtual void Schedule(void (*function)(void* arg), void* arg,
                        Priority pri = LOW, void* tag = nullptr,
                        void (*unschedFunction)(void* arg) = 0) override {}

  virtual int UnSchedule(void* tag, Priority pri) override { return 0; }

  virtual void StartThread(void (*function)(void* arg), void* arg) override {}

  virtual void WaitForJoin() override {}

  virtual unsigned int GetThreadPoolQueueLen(
      Priority pri = LOW) const override {
    return 0;
  }

  virtual Status GetTestDirectory(std::string* path) override { return s3_notsup; }

  virtual uint64_t NowMicros() override { return 0; }

  virtual void SleepForMicroseconds(int micros) override {}

  virtual Status GetHostName(char* name, uint64_t len) override {
    return s3_notsup;
  }

  virtual Status GetCurrentTime(int64_t* unix_time) override { return s3_notsup; }

  virtual Status GetAbsolutePath(const std::string& db_path,
                                 std::string* outputpath) override {
    return s3_notsup;
  }

  virtual void SetBackgroundThreads(int number, Priority pri = LOW) override {}
  virtual void IncBackgroundThreadsIfNeeded(int number, Priority pri) override {
  }
  virtual std::string TimeToString(uint64_t number) override { return ""; }

  virtual uint64_t GetThreadID() const override {
    return 0;
  }

  static AwsEnv* NewAwsEnv(const std::string& bucket_prefix,
		         const std::string& access_key_id,
		         const std::string& secret_key,
		         std::shared_ptr<Logger> info_log = nullptr) {
      return nullptr;
  }
  static Status GetTestCredentials(std::string* aws_access_key_id,
		                   std::string* aws_secret_access_key) {
    return s3_notsup;
  }
};
}

#endif // USE_AWS
