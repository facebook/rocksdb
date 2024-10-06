//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
// This file defines an AWS-S3 environment for rocksdb.
// A directory maps to an an zero-size object in an S3 bucket
// A sst file maps to an object in that S3 bucket.
//
#ifndef ROCKSDB_LITE
#ifdef USE_AWS
#include <aws/core/Aws.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/crypto/CryptoStream.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/BucketLocationConstraint.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CopyObjectResult.h>
#include <aws/s3/model/CreateBucketConfiguration.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/CreateBucketResult.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectResult.h>
#include <aws/s3/model/GetBucketVersioningRequest.h>
#include <aws/s3/model/GetBucketVersioningResult.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ListObjectsResult.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/PutObjectResult.h>
#include <aws/s3/model/ServerSideEncryption.h>
#include <aws/transfer/TransferManager.h>
#endif  // USE_AWS

#include <cassert>
#include <cinttypes>
#include <fstream>
#include <iostream>

#include "cloud/aws/aws_file.h"
#include "cloud/aws/aws_file_system.h"
#include "rocksdb/cloud/cloud_storage_provider_impl.h"
#include "cloud/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "port/port.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/convenience.h"
#include "rocksdb/options.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"

#ifdef _WIN32_WINNT
#undef GetMessage
#endif

namespace ROCKSDB_NAMESPACE {
#ifdef USE_AWS
class CloudRequestCallbackGuard {
 public:
  CloudRequestCallbackGuard(CloudRequestCallback* callback,
                            CloudRequestOpType type, uint64_t size = 0)
      : callback_(callback), type_(type), size_(size), start_(now()) {}

  ~CloudRequestCallbackGuard() {
    if (callback_) {
      (*callback_)(type_, size_, now() - start_, success_);
    }
  }

  void SetSize(uint64_t size) { size_ = size; }
  void SetSuccess(bool success) { success_ = success; }

 private:
  uint64_t now() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::system_clock::now() -
               std::chrono::system_clock::from_time_t(0))
        .count();
  }
  CloudRequestCallback* callback_;
  CloudRequestOpType type_;
  uint64_t size_;
  bool success_{false};
  uint64_t start_;
};

template <typename T>
void SetEncryptionParameters(const CloudFileSystemOptions& cloud_fs_options,
                             T& put_request) {
  if (cloud_fs_options.server_side_encryption) {
    if (cloud_fs_options.encryption_key_id.empty()) {
      put_request.SetServerSideEncryption(
          Aws::S3::Model::ServerSideEncryption::AES256);
    } else {
      put_request.SetServerSideEncryption(
          Aws::S3::Model::ServerSideEncryption::aws_kms);
      put_request.SetSSEKMSKeyId(cloud_fs_options.encryption_key_id.c_str());
    }
  }
}

/******************** S3ClientWrapper ******************/

class AwsS3ClientWrapper {
 public:
  AwsS3ClientWrapper(
      const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>& creds,
      const Aws::Client::ClientConfiguration& config,
      const CloudFileSystemOptions& cloud_options)
      : cloud_request_callback_(cloud_options.cloud_request_callback) {
    if (cloud_options.s3_client_factory) {
      client_ = cloud_options.s3_client_factory(creds, config);
    } else if (creds) {
      client_ = std::make_shared<Aws::S3::S3Client>(
          creds, config,
          Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
          true /* useVirtualAddressing */);
    } else {
      client_ = std::make_shared<Aws::S3::S3Client>(config);
    }
    if (cloud_options.use_aws_transfer_manager) {
      Aws::Transfer::TransferManagerConfiguration transferManagerConfig(
          GetAwsTransferManagerExecutor());
      transferManagerConfig.s3Client = client_;
      SetEncryptionParameters(cloud_options,
                              transferManagerConfig.putObjectTemplate);
      SetEncryptionParameters(
          cloud_options, transferManagerConfig.createMultipartUploadTemplate);
      transfer_manager_ =
          Aws::Transfer::TransferManager::Create(transferManagerConfig);
    }
  }

  Aws::S3::Model::ListObjectsOutcome ListCloudObjects(
      const Aws::S3::Model::ListObjectsRequest& request) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kListOp);
    auto outcome = client_->ListObjects(request);
    t.SetSuccess(outcome.IsSuccess());
    return outcome;
  }

  Aws::S3::Model::CreateBucketOutcome CreateBucket(
      const Aws::S3::Model::CreateBucketRequest& request) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kCreateOp);
    auto outcome = client_->CreateBucket(request);
    t.SetSuccess(outcome.IsSuccess());
    return outcome;
  }

  Aws::S3::Model::HeadBucketOutcome HeadBucket(
      const Aws::S3::Model::HeadBucketRequest& request) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kInfoOp);
    auto outcome = client_->HeadBucket(request);
    t.SetSuccess(outcome.IsSuccess());
    return outcome;
  }
  Aws::S3::Model::DeleteObjectOutcome DeleteCloudObject(
      const Aws::S3::Model::DeleteObjectRequest& request) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kDeleteOp);
    auto outcome = client_->DeleteObject(request);
    t.SetSuccess(outcome.IsSuccess());
    return outcome;
  }

  Aws::S3::Model::CopyObjectOutcome CopyCloudObject(
      const Aws::S3::Model::CopyObjectRequest& request) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kCopyOp);
    auto outcome = client_->CopyObject(request);
    t.SetSuccess(outcome.IsSuccess());
    return outcome;
  }

  Aws::S3::Model::GetObjectOutcome GetCloudObject(
      const Aws::S3::Model::GetObjectRequest& request) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kReadOp);
    auto outcome = client_->GetObject(request);
    if (outcome.IsSuccess()) {
      t.SetSize(outcome.GetResult().GetContentLength());
      t.SetSuccess(true);
    }
    return outcome;
  }

  template <class... Args>
  std::shared_ptr<Aws::Transfer::TransferHandle> DownloadFile(Args... args) {
    CloudRequestCallbackGuard guard(cloud_request_callback_.get(),
                                    CloudRequestOpType::kReadOp);
    auto handle = transfer_manager_->DownloadFile(std::forward<Args>(args)...);

    handle->WaitUntilFinished();
    bool success =
        handle->GetStatus() == Aws::Transfer::TransferStatus::COMPLETED;
    guard.SetSuccess(success);
    if (success) {
      guard.SetSize(handle->GetBytesTotalSize());
    }
    return handle;
  }

  Aws::S3::Model::PutObjectOutcome PutCloudObject(
      const Aws::S3::Model::PutObjectRequest& request, uint64_t size_hint = 0) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kWriteOp, size_hint);
    auto outcome = client_->PutObject(request);
    t.SetSuccess(outcome.IsSuccess());
    return outcome;
  }

  std::shared_ptr<Aws::Transfer::TransferHandle> UploadFile(
      const Aws::String& bucket_name, const Aws::String& object_path,
      const Aws::String& destination, uint64_t file_size) {
    CloudRequestCallbackGuard guard(cloud_request_callback_.get(),
                                    CloudRequestOpType::kWriteOp, file_size);

    auto handle = transfer_manager_->UploadFile(
        destination, bucket_name, object_path, Aws::DEFAULT_CONTENT_TYPE,
        Aws::Map<Aws::String, Aws::String>());

    handle->WaitUntilFinished();
    guard.SetSuccess(handle->GetStatus() ==
                     Aws::Transfer::TransferStatus::COMPLETED);
    return handle;
  }

  Aws::S3::Model::HeadObjectOutcome HeadObject(
      const Aws::S3::Model::HeadObjectRequest& request) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kInfoOp);
    auto outcome = client_->HeadObject(request);
    t.SetSuccess(outcome.IsSuccess());
    return outcome;
  }
  CloudRequestCallback* GetRequestCallback() {
    return cloud_request_callback_.get();
  }
  bool HasTransferManager() const { return transfer_manager_.get() != nullptr; }

 private:
  static Aws::Utils::Threading::Executor* GetAwsTransferManagerExecutor() {
    static Aws::Utils::Threading::PooledThreadExecutor executor(8);
    return &executor;
  }

  std::shared_ptr<Aws::S3::S3Client> client_;
  std::shared_ptr<Aws::Transfer::TransferManager> transfer_manager_;
  std::shared_ptr<CloudRequestCallback> cloud_request_callback_;
};

static bool IsNotFound(const Aws::S3::S3Errors& s3err) {
  return (s3err == Aws::S3::S3Errors::NO_SUCH_BUCKET ||
          s3err == Aws::S3::S3Errors::NO_SUCH_KEY ||
          s3err == Aws::S3::S3Errors::RESOURCE_NOT_FOUND);
}

/******************** S3ReadableFile ******************/
class S3ReadableFile : public CloudStorageReadableFileImpl {
 public:
  S3ReadableFile(const std::shared_ptr<AwsS3ClientWrapper>& s3client,
                 Logger* info_log, const std::string& bucket,
                 const std::string& fname, uint64_t size,
                 std::string content_hash)
      : CloudStorageReadableFileImpl(info_log, bucket, fname, size),
        s3client_(s3client),
        content_hash_(std::move(content_hash)) {}

  virtual const char* Type() const { return "s3"; }

  virtual size_t GetUniqueId(char* id, size_t max_size) const override {
    if (content_hash_.empty()) {
      return 0;
    }

    max_size = std::min(content_hash_.size(), max_size);
    memcpy(id, content_hash_.c_str(), max_size);
    return max_size;
  }

  // random access, read data from specified offset in file
  IOStatus DoCloudRead(uint64_t offset, size_t n, const IOOptions& /*options*/,
                       char* scratch, uint64_t* bytes_read,
                       IODebugContext* /*dbg*/) const override {
    // create a range read request
    // Ranges are inclusive, so we can't read 0 bytes; read 1 instead and
    // drop it later.
    size_t rangeLen = (n != 0 ? n : 1);
    char buffer[512];
    int ret = snprintf(buffer, sizeof(buffer), "bytes=%" PRIu64 "-%" PRIu64,
                       offset, offset + rangeLen - 1);
    if (ret < 0) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] S3ReadableFile vsnprintf error %s offset %" PRIu64
          " rangelen %" ROCKSDB_PRIszt "\n",
          fname_.c_str(), offset, rangeLen);
      return IOStatus::IOError("S3ReadableFile vsnprintf ", fname_.c_str());
    }

    // set up S3 request to read this range
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(ToAwsString(bucket_));
    request.SetKey(ToAwsString(fname_));
    request.SetRange(Aws::String(buffer, ret));

    Aws::S3::Model::GetObjectOutcome outcome =
        s3client_->GetCloudObject(request);
    bool isSuccess = outcome.IsSuccess();
    if (!isSuccess) {
      const Aws::Client::AWSError<Aws::S3::S3Errors>& error =
          outcome.GetError();
      std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
      if (IsNotFound(error.GetErrorType()) ||
          errmsg.find("Response code: 404") != std::string::npos) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[s3] S3ReadableFile error in reading not-existent %s %s",
            fname_.c_str(), errmsg.c_str());
        return IOStatus::NotFound(fname_, errmsg.c_str());
      }
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[s3] S3ReadableFile error in reading %s %" PRIu64 " %s %s",
          fname_.c_str(), offset, buffer, error.GetMessage().c_str());
      return IOStatus::IOError(fname_, errmsg.c_str());
    }
    // const Aws::S3::Model::GetObjectResult& res = outcome.GetResult();

    // extract data payload
    Aws::IOStream& body = outcome.GetResult().GetBody();
    *bytes_read = 0;
    if (n != 0) {
      body.read(scratch, n);
      *bytes_read = body.gcount();
      assert(*bytes_read <= n);
    }
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[s3] S3ReadableFile file %s filesize %" PRIu64 " read %" PRIu64
        " bytes",
        fname_.c_str(), file_size_, *bytes_read);
    return IOStatus::OK();
  }

 private:
  std::shared_ptr<AwsS3ClientWrapper> s3client_;
  std::string content_hash_;
};  // End class S3ReadableFile

/******************** Writablefile ******************/

class S3WritableFile : public CloudStorageWritableFileImpl {
 public:
  S3WritableFile(CloudFileSystem* fs, const std::string& local_fname,
                 const std::string& bucket, const std::string& cloud_fname,
                 const FileOptions& options)
      : CloudStorageWritableFileImpl(fs, local_fname, bucket, cloud_fname,
                                     options) {}
  virtual const char* Name() const override {
    return CloudStorageProviderImpl::kS3();
  }
};

/******************** S3StorageProvider ******************/
class S3StorageProvider : public CloudStorageProviderImpl {
 public:
  ~S3StorageProvider() override {}
  virtual const char* Name() const override { return kS3(); }
  IOStatus CreateBucket(const std::string& bucket) override;
  IOStatus ExistsBucket(const std::string& bucket) override;
  IOStatus EmptyBucket(const std::string& bucket_name,
                       const std::string& object_path) override;
  IOStatus DeleteCloudObject(const std::string& bucket_name,
                             const std::string& object_path) override;
  IOStatus ListCloudObjects(const std::string& bucket_name,
                            const std::string& object_path,
                            std::vector<std::string>* result) override;
  IOStatus ExistsCloudObject(const std::string& bucket_name,
                             const std::string& object_path) override;
  IOStatus GetCloudObjectSize(const std::string& bucket_name,
                              const std::string& object_path,
                              uint64_t* filesize) override;
  IOStatus GetCloudObjectModificationTime(const std::string& bucket_name,
                                          const std::string& object_path,
                                          uint64_t* time) override;
  IOStatus GetCloudObjectMetadata(const std::string& bucket_name,
                                  const std::string& object_path,
                                  CloudObjectInformation* info) override;

  IOStatus PutCloudObjectMetadata(
      const std::string& bucket_name, const std::string& object_path,
      const std::unordered_map<std::string, std::string>& metadata) override;
  IOStatus CopyCloudObject(const std::string& bucket_name_src,
                           const std::string& object_path_src,
                           const std::string& bucket_name_dest,
                           const std::string& object_path_dest) override;
  IOStatus DoNewCloudReadableFile(
      const std::string& bucket, const std::string& fname, uint64_t fsize,
      const std::string& content_hash, const FileOptions& options,
      std::unique_ptr<CloudStorageReadableFile>* result,
      IODebugContext* dbg) override;
  IOStatus NewCloudWritableFile(
      const std::string& local_path, const std::string& bucket_name,
      const std::string& object_path, const FileOptions& options,
      std::unique_ptr<CloudStorageWritableFile>* result,
      IODebugContext* dbg) override;
  Status PrepareOptions(const ConfigOptions& options) override;
 protected:
  IOStatus DoGetCloudObject(const std::string& bucket_name,
                            const std::string& object_path,
                            const std::string& destination,
                            uint64_t* remote_size) override;
  IOStatus DoPutCloudObject(const std::string& local_file,
                            const std::string& bucket_name,
                            const std::string& object_path,
                            uint64_t file_size) override;

 private:
  struct HeadObjectResult {
    // If any of the field is non-nullptr, returns requested data
    std::unordered_map<std::string, std::string>* metadata = nullptr;
    uint64_t* size = nullptr;
    uint64_t* modtime = nullptr;
    std::string* etag = nullptr;
  };

  // Retrieves metadata from an object
  IOStatus HeadObject(const std::string& bucket, const std::string& path,
                      HeadObjectResult* result);

  // Retrieves metadata from an object based on a HeadObject request
  // REQUIRES: result != nullptr
  IOStatus HeadObject(const Aws::S3::Model::HeadObjectRequest& request,
                      HeadObjectResult* result);

  // The S3 client
  std::shared_ptr<AwsS3ClientWrapper> s3client_;
};

Status S3StorageProvider::PrepareOptions(const ConfigOptions& options) {
  auto cfs = dynamic_cast<CloudFileSystem*>(options.env->GetFileSystem().get());
  assert(cfs);
  const auto& cloud_opts = cfs->GetCloudFileSystemOptions();
  if (std::string(cfs->Name()) != CloudFileSystemImpl::kAws()) {
    return Status::InvalidArgument("S3 Provider requires AWS Environment");
  }
  // TODO: support buckets being in different regions
  if (!cfs->SrcMatchesDest() && cfs->HasSrcBucket() && cfs->HasDestBucket()) {
    if (cloud_opts.src_bucket.GetRegion() !=
        cloud_opts.dest_bucket.GetRegion()) {
      Log(InfoLogLevel::ERROR_LEVEL, cfs->GetLogger(),
          "[aws] NewAwsFileSystem Buckets %s, %s in two different regions %s, "
          "%s is not supported",
          cloud_opts.src_bucket.GetBucketName().c_str(),
          cloud_opts.dest_bucket.GetBucketName().c_str(),
          cloud_opts.src_bucket.GetRegion().c_str(),
          cloud_opts.dest_bucket.GetRegion().c_str());
      return Status::InvalidArgument("Two different regions not supported");
    }
  }
  Aws::Client::ClientConfiguration config;
  Status status = AwsCloudOptions::GetClientConfiguration(
      cfs, cloud_opts.src_bucket.GetRegion(), &config);
  if (status.ok()) {
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> creds;
    status = cloud_opts.credentials.GetCredentialsProvider(&creds);
    if (!status.ok()) {
      Log(InfoLogLevel::INFO_LEVEL, cfs->GetLogger(),
          "[aws] NewAwsFileSystem - Bad AWS credentials");
    } else {
      Header(cfs->GetLogger(), "S3 connection to endpoint in region: %s",
             config.region.c_str());
      s3client_ =
          std::make_shared<AwsS3ClientWrapper>(creds, config, cloud_opts);
    }
  }
  if (!status.ok()) {
    return status;
  } else {
    return CloudStorageProviderImpl::PrepareOptions(options);
  }
}

//
// Create bucket in S3 if it does not already exist.
//
IOStatus S3StorageProvider::CreateBucket(const std::string& bucket) {
  // specify region for the bucket
  Aws::S3::Model::CreateBucketConfiguration conf;
  // AWS's utility to help out with uploading and downloading S3 file
  Aws::S3::Model::BucketLocationConstraint bucket_location =
      Aws::S3::Model::BucketLocationConstraintMapper::
          GetBucketLocationConstraintForName(ToAwsString(
              cfs_->GetCloudFileSystemOptions().dest_bucket.GetRegion()));
  //
  // If you create a bucket in US-EAST-1, no location constraint should be
  // specified
  //
  // https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
  //
  // By default, the bucket is created in the US East (N. Virginia) Region.
  // You can optionally specify a Region in the request body. You might choose
  // a Region to optimize latency, minimize costs, or address regulatory
  // requirements.
  //
  if ((bucket_location != Aws::S3::Model::BucketLocationConstraint::NOT_SET) &&
      (bucket_location !=
       Aws::S3::Model::BucketLocationConstraint::us_east_1)) {
    conf.SetLocationConstraint(bucket_location);
  }

  // create bucket
  Aws::S3::Model::CreateBucketRequest request;
  request.SetBucket(ToAwsString(bucket));
  request.SetCreateBucketConfiguration(conf);
  Aws::S3::Model::CreateBucketOutcome outcome =
      s3client_->CreateBucket(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error = outcome.GetError();
    std::string errmsg(error.GetMessage().c_str());
    Aws::S3::S3Errors s3err = error.GetErrorType();
    if (s3err != Aws::S3::S3Errors::BUCKET_ALREADY_EXISTS &&
        s3err != Aws::S3::S3Errors::BUCKET_ALREADY_OWNED_BY_YOU) {
      return IOStatus::IOError(bucket.c_str(), errmsg.c_str());
    }
  }
  return IOStatus::OK();
}

IOStatus S3StorageProvider::ExistsBucket(const std::string& bucket) {
  Aws::S3::Model::HeadBucketRequest request;
  request.SetBucket(ToAwsString(bucket));
  Aws::S3::Model::HeadBucketOutcome outcome = s3client_->HeadBucket(request);
  return outcome.IsSuccess() ? IOStatus::OK() : IOStatus::NotFound();
}

//
// Deletes all the objects with the specified path prefix in our bucket
//
IOStatus S3StorageProvider::EmptyBucket(const std::string& bucket_name,
                                        const std::string& object_path) {
  std::vector<std::string> results;

  // Get all the objects in the  bucket
  auto st = ListCloudObjects(bucket_name, object_path, &results);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
        "[s3] EmptyBucket unable to find objects in bucket %s %s",
        bucket_name.c_str(), st.ToString().c_str());
    return st;
  }
  Log(InfoLogLevel::DEBUG_LEVEL, cfs_->GetLogger(),
      "[s3] EmptyBucket going to delete %" ROCKSDB_PRIszt
      " objects in bucket %s",
      results.size(), bucket_name.c_str());

  // Delete all objects from bucket
  for (const auto& path : results) {
    st = DeleteCloudObject(bucket_name, object_path + "/" + path);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
          "[s3] EmptyBucket Unable to delete %s in bucket %s %s", path.c_str(),
          bucket_name.c_str(), st.ToString().c_str());
    }
  }
  return st;
}

IOStatus S3StorageProvider::DeleteCloudObject(const std::string& bucket_name,
                                              const std::string& object_path) {
  IOStatus st;

  // create request
  Aws::S3::Model::DeleteObjectRequest request;
  request.SetBucket(ToAwsString(bucket_name));
  request.SetKey(ToAwsString(
      object_path));  // The filename is the object name in the bucket

  Aws::S3::Model::DeleteObjectOutcome outcome =
      s3client_->DeleteCloudObject(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error = outcome.GetError();
    std::string errmsg(error.GetMessage().c_str());
    if (IsNotFound(error.GetErrorType())) {
      st = IOStatus::NotFound(object_path, errmsg.c_str());
    } else {
      st = IOStatus::IOError(object_path, errmsg.c_str());
    }
  }

  Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
      "[s3] DeleteFromS3 %s/%s, status %s", bucket_name.c_str(),
      object_path.c_str(), st.ToString().c_str());

  return st;
}

//
// Appends the names of all children of the specified path from S3
// into the result set.
//
IOStatus S3StorageProvider::ListCloudObjects(const std::string& bucket_name,
                                             const std::string& object_path,
                                             std::vector<std::string>* result) {
  // S3 paths don't start with '/'
  auto prefix = ltrim_if(object_path, '/');
  // S3 paths better end with '/', otherwise we might also get a list of files
  // in a directory for which our path is a prefix
  prefix = ensure_ends_with_pathsep(std::move(prefix));
  // the starting object marker
  Aws::String marker;
  bool loop = true;

  // get info of bucket+object
  while (loop) {
    Aws::S3::Model::ListObjectsRequest request;
    request.SetBucket(ToAwsString(bucket_name));
    request.SetMaxKeys(cfs_->GetCloudFileSystemOptions()
                           .number_objects_listed_in_one_iteration);

    request.SetPrefix(ToAwsString(prefix));
    request.SetMarker(marker);

    Aws::S3::Model::ListObjectsOutcome outcome =
        s3client_->ListCloudObjects(request);
    bool isSuccess = outcome.IsSuccess();
    if (!isSuccess) {
      const Aws::Client::AWSError<Aws::S3::S3Errors>& error =
          outcome.GetError();
      std::string errmsg(error.GetMessage().c_str());
      if (IsNotFound(error.GetErrorType())) {
        Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
            "[s3] GetChildren dir %s does not exist: %s", object_path.c_str(),
            errmsg.c_str());
        return IOStatus::NotFound(object_path, errmsg.c_str());
      }
      return IOStatus::IOError(object_path, errmsg.c_str());
    }
    const Aws::S3::Model::ListObjectsResult& res = outcome.GetResult();
    const Aws::Vector<Aws::S3::Model::Object>& objs = res.GetContents();
    for (const auto& o : objs) {
      const Aws::String& key = o.GetKey();
      // Our path should be a prefix of the fetched value
      std::string keystr(key.c_str(), key.size());
      assert(keystr.find(prefix) == 0);
      if (keystr.find(prefix) != 0) {
        return IOStatus::IOError("Unexpected result from AWS S3: " + keystr);
      }
      auto fname = keystr.substr(prefix.size());
      result->push_back(std::move(fname));
    }

    // If there are no more entries, then we are done.
    if (!res.GetIsTruncated()) {
      break;
    }
    // The new starting point
    marker = res.GetNextMarker();
    if (marker.empty()) {
      // If response does not include the NextMaker and it is
      // truncated, you can use the value of the last Key in the response
      // as the marker in the subsequent request because all objects
      // are returned in alphabetical order
      marker = objs.back().GetKey();
    }
  }
  return IOStatus::OK();
}

// check existence of the cloud object
IOStatus S3StorageProvider::ExistsCloudObject(const std::string& bucket_name,
                                              const std::string& object_path) {
  HeadObjectResult result;
  return HeadObject(bucket_name, object_path, &result);
}

// Return size of cloud object
IOStatus S3StorageProvider::GetCloudObjectSize(const std::string& bucket_name,
                                               const std::string& object_path,
                                               uint64_t* filesize) {
  HeadObjectResult result;
  result.size = filesize;
  return HeadObject(bucket_name, object_path, &result);
}

IOStatus S3StorageProvider::GetCloudObjectModificationTime(
    const std::string& bucket_name, const std::string& object_path,
    uint64_t* time) {
  HeadObjectResult result;
  result.modtime = time;
  return HeadObject(bucket_name, object_path, &result);
}

IOStatus S3StorageProvider::GetCloudObjectMetadata(
    const std::string& bucket_name, const std::string& object_path,
    CloudObjectInformation* info) {
  assert(info != nullptr);
  HeadObjectResult result;
  result.metadata = &info->metadata;
  result.size = &info->size;
  result.modtime = &info->modification_time;
  result.etag = &info->content_hash;
  return HeadObject(bucket_name, object_path, &result);
}

IOStatus S3StorageProvider::PutCloudObjectMetadata(
    const std::string& bucket_name, const std::string& object_path,
    const std::unordered_map<std::string, std::string>& metadata) {
  Aws::S3::Model::PutObjectRequest request;
  Aws::Map<Aws::String, Aws::String> aws_metadata;
  for (const auto& m : metadata) {
    aws_metadata[ToAwsString(m.first)] = ToAwsString(m.second);
  }
  request.SetBucket(ToAwsString(bucket_name));
  request.SetKey(ToAwsString(object_path));
  request.SetMetadata(aws_metadata);
  SetEncryptionParameters(cfs_->GetCloudFileSystemOptions(), request);

  auto outcome = s3client_->PutCloudObject(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const auto& error = outcome.GetError();
    std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
        "[s3] Bucket %s error in saving metadata %s", bucket_name.c_str(),
        errmsg.c_str());
    return IOStatus::IOError(object_path, errmsg.c_str());
  }
  return IOStatus::OK();
}

IOStatus S3StorageProvider::DoNewCloudReadableFile(
    const std::string& bucket, const std::string& fname, uint64_t fsize,
    const std::string& content_hash, const FileOptions& /*options*/,
    std::unique_ptr<CloudStorageReadableFile>* result,
    IODebugContext* /*dbg*/) {
  result->reset(new S3ReadableFile(s3client_, cfs_->GetLogger(), bucket, fname,
                                   fsize, content_hash));
  return IOStatus::OK();
}

IOStatus S3StorageProvider::NewCloudWritableFile(
    const std::string& local_path, const std::string& bucket_name,
    const std::string& object_path, const FileOptions& file_opts,
    std::unique_ptr<CloudStorageWritableFile>* result,
    IODebugContext* /*dbg*/) {
  result->reset(new S3WritableFile(cfs_, local_path, bucket_name, object_path,
                                   file_opts));
  return (*result)->status();
}

IOStatus S3StorageProvider::HeadObject(const std::string& bucket_name,
                                       const std::string& object_path,
                                       HeadObjectResult* result) {
  Aws::S3::Model::HeadObjectRequest request;
  request.SetBucket(ToAwsString(bucket_name));
  request.SetKey(ToAwsString(object_path));
  return HeadObject(request, result);
}

IOStatus S3StorageProvider::HeadObject(
    const Aws::S3::Model::HeadObjectRequest& request,
    HeadObjectResult* result) {
  assert(result != nullptr);
  auto outcome = s3client_->HeadObject(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const auto& error = outcome.GetError();
    const auto& errMessage = error.GetMessage();
    Slice object_path(request.GetKey().data(), request.GetKey().size());
    if (IsNotFound(error.GetErrorType())) {
      return IOStatus::NotFound(object_path, errMessage.c_str());
    } else {
      return IOStatus::IOError(object_path, errMessage.c_str());
    }
  }
  const auto& res = outcome.GetResult();
  if (result->metadata != nullptr) {
    for (const auto& m : res.GetMetadata()) {
      (*(result->metadata))[m.first.c_str()] = m.second.c_str();
    }
  }
  if (result->size != nullptr) {
    *(result->size) = res.GetContentLength();
  }
  if ((result->modtime) != nullptr) {
    *(result->modtime) = res.GetLastModified().Millis();
  }
  if ((result->etag) != nullptr) {
    *(result->etag) = std::string(res.GetETag().data(), res.GetETag().length());
  }
  return IOStatus::OK();
}

// Copy the specified cloud object from one location in the cloud
// storage to another location in cloud storage
IOStatus S3StorageProvider::CopyCloudObject(
    const std::string& bucket_name_src, const std::string& object_path_src,
    const std::string& bucket_name_dest, const std::string& object_path_dest) {
  Aws::String src_bucket = ToAwsString(bucket_name_src);
  Aws::String dest_bucket = ToAwsString(bucket_name_dest);

  // The filename is the same as the object name in the bucket
  Aws::String src_object = ToAwsString(object_path_src);
  Aws::String dest_object = ToAwsString(object_path_dest);

  Aws::String src_url = src_bucket + src_object;

  // create copy request
  Aws::S3::Model::CopyObjectRequest request;
  request.SetCopySource(src_url);
  request.SetBucket(dest_bucket);
  request.SetKey(dest_object);
  SetEncryptionParameters(cfs_->GetCloudFileSystemOptions(), request);

  // execute request
  Aws::S3::Model::CopyObjectOutcome outcome =
      s3client_->CopyCloudObject(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error = outcome.GetError();
    std::string errmsg(error.GetMessage().c_str());
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
        "[s3] S3WritableFile src path %s error in copying to %s %s",
        src_url.c_str(), dest_object.c_str(), errmsg.c_str());
    return IOStatus::IOError(dest_object.c_str(), errmsg.c_str());
  }
  Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
      "[s3] S3WritableFile src path %s copied to %s OK", src_url.c_str(),
      dest_object.c_str());
  return IOStatus::OK();
}

namespace {
// The AWS SDK for S3 downloads writes the results to a std::iostream, so
// to support O_DIRECT on file download we need to customize iostream. The
// easiest way is to subclass std::streambuf. WritableFileWriter already
// supports direct I/O, including the necessary buffering and alignment
// logic, so rather than implement an O_DIRECT-friendly streambuf we just
// forward streambuf write operations to WritableFileWriter, then wrap the
// forwarder in an iostream.
class WritableFileStreamBuf : public std::streambuf {
 public:
  WritableFileStreamBuf(IOStatus* fileCloseStatus,
			std::unique_ptr<WritableFileWriter>&& fileWriter)
    : fileCloseStatus_(fileCloseStatus), fileWriter_(std::move(fileWriter)) {}

  ~WritableFileStreamBuf() {
    *fileCloseStatus_ = fileWriter_->Close({});
  }

 protected:
  // Appends a block of data to the stream. Must always write n if possible
  std::streamsize xsputn(const char* s, std::streamsize n) override {
    auto st = fileWriter_->Append({}, rocksdb::Slice(s, n));
    if (!st.ok()) {
      return EOF;
    }
    return n;
  }

  // Appends a single character
  int_type overflow(int_type ch) override {
    if (traits_type::eq_int_type(ch, traits_type::eof())) {
      return ch;
    }
    auto c = traits_type::to_char_type(ch);
    auto r = xsputn(&c, 1);
    if (r == EOF) {
      return traits_type::eof();
    }
    // In case of success, the character put is returned
    return ch;
  }

  // Flushes any buffered data
  int sync() override {
    auto st = fileWriter_->Flush({});
    return st.ok() ? 0 : -1;
  }

 private:
  IOStatus *fileCloseStatus_;
  std::unique_ptr<WritableFileWriter> fileWriter_;
};

// std::iostream takes a raw pointer to std::streambuf. This subclass
// takes a unique_ptr to the streambuf, tying the std::streambuf's
// lifetime to the iostream's.
template <class T>
class IOStreamWithOwnedBuf : public std::iostream {
 public:
  IOStreamWithOwnedBuf(std::unique_ptr<T>&& s)
      : std::iostream(s.get()), s_(std::move(s)) {}

 private:
  std::unique_ptr<T> s_;
};

}  // namespace

IOStatus S3StorageProvider::DoGetCloudObject(const std::string& bucket_name,
                                             const std::string& object_path,
                                             const std::string& destination,
                                             uint64_t* remote_size) {
  if (s3client_->HasTransferManager()) {
    // AWS Transfer manager does not work if we provide our stream
    // implementation because of https://github.com/aws/aws-sdk-cpp/issues/1732.
    // The stream is not flushed when WaitUntilFinished() returns.
    // TODO(igor) Fix this once the AWS SDK's bug is fixed.
    auto ioStreamFactory = [=]() -> Aws::IOStream* {
        // fallback to FStream
        return Aws::New<Aws::FStream>(
            Aws::Utils::ARRAY_ALLOCATION_TAG, destination,
            std::ios_base::out | std::ios_base::trunc);

    };

    auto handle = s3client_->DownloadFile(ToAwsString(bucket_name),
                                          ToAwsString(object_path),
                                          std::move(ioStreamFactory));
    bool success =
        handle->GetStatus() == Aws::Transfer::TransferStatus::COMPLETED;
    if (success) {
      *remote_size = handle->GetBytesTotalSize();
    } else {
      const auto& error = handle->GetLastError();
      std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
      Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
          "[s3] DownloadFile %s/%s error %s.", bucket_name.c_str(),
          object_path.c_str(), errmsg.c_str());
      if (IsNotFound(error.GetErrorType())) {
        return IOStatus::NotFound(std::move(errmsg));
      }
      return IOStatus::IOError(std::move(errmsg));
    }
  } else {
    IOStatus fileCloseStatus;
    {
      // Close() will be called in the destructor of the object returned by
      // this factory. Adding an inner scope so that the destructor is called
      // before checking fileCloseStatus.
      auto ioStreamFactory = [this, destination, &fileCloseStatus]() -> Aws::IOStream* {
        FileOptions foptions;
        foptions.use_direct_writes =
            cfs_->GetCloudFileSystemOptions().use_direct_io_for_cloud_download;
        std::unique_ptr<FSWritableFile> file;
        auto st = NewWritableFile(cfs_->GetBaseFileSystem().get(), destination,
                                  &file, foptions);
        if (!st.ok()) {
          // fallback to FStream
          return Aws::New<Aws::FStream>(
              Aws::Utils::ARRAY_ALLOCATION_TAG, destination,
              std::ios_base::out | std::ios_base::trunc);
        }
        return Aws::New<IOStreamWithOwnedBuf<WritableFileStreamBuf>>(
            Aws::Utils::ARRAY_ALLOCATION_TAG,
            std::unique_ptr<WritableFileStreamBuf>(new WritableFileStreamBuf(
                &fileCloseStatus,
                std::unique_ptr<WritableFileWriter>(new WritableFileWriter(
                        std::move(file), destination, foptions)))));
      };

      Aws::S3::Model::GetObjectRequest request;
      request.SetBucket(ToAwsString(bucket_name));
      request.SetKey(ToAwsString(object_path));
      request.SetResponseStreamFactory(std::move(ioStreamFactory));

      auto outcome = s3client_->GetCloudObject(request);
      if (outcome.IsSuccess()) {
        *remote_size = outcome.GetResult().GetContentLength();
      } else {
        const auto& error = outcome.GetError();
        std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
        if (IsNotFound(error.GetErrorType())) {
          Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
              "[s3] GetObject %s/%s error %s.", bucket_name.c_str(),
              object_path.c_str(), errmsg.c_str());
          return IOStatus::NotFound(std::move(errmsg));
        } else {
          Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
              "[s3] GetObject %s/%s error %s.", bucket_name.c_str(),
              object_path.c_str(), errmsg.c_str());
          return IOStatus::IOError(std::move(errmsg));
        }
      }
    }

    if (!fileCloseStatus.ok()) {
      std::string errmsg = fileCloseStatus.ToString();
      Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
          "[s3] GetObject %s/%s error closing file %s.", bucket_name.c_str(),
          object_path.c_str(), errmsg.c_str());
      return IOStatus::IOError(std::move(errmsg));
    }
  }
  return IOStatus::OK();
}

IOStatus S3StorageProvider::DoPutCloudObject(const std::string& local_file,
                                             const std::string& bucket_name,
                                             const std::string& object_path,
                                             uint64_t file_size) {
  if (s3client_->HasTransferManager()) {
    auto handle = s3client_->UploadFile(ToAwsString(bucket_name),
                                        ToAwsString(object_path),
                                        ToAwsString(local_file), file_size);
    if (handle->GetStatus() != Aws::Transfer::TransferStatus::COMPLETED) {
      auto error = handle->GetLastError();
      std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
      Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
          "[s3] UploadFile %s/%s, size %" PRIu64 ", ERROR %s",
          bucket_name.c_str(), object_path.c_str(), file_size, errmsg.c_str());
      return IOStatus::IOError(local_file, errmsg);
    }
  } else {
    auto inputData =
        Aws::MakeShared<Aws::FStream>(object_path.c_str(), local_file.c_str(),
                                      std::ios_base::in | std::ios_base::out);

    Aws::S3::Model::PutObjectRequest putRequest;
    putRequest.SetBucket(ToAwsString(bucket_name));
    putRequest.SetKey(ToAwsString(object_path));
    putRequest.SetBody(inputData);
    SetEncryptionParameters(cfs_->GetCloudFileSystemOptions(), putRequest);

    auto outcome = s3client_->PutCloudObject(putRequest, file_size);
    if (!outcome.IsSuccess()) {
      const auto& error = outcome.GetError();
      std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
      Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
          "[s3] PutCloudObject %s/%s, size %" PRIu64 ", ERROR %s",
          bucket_name.c_str(), object_path.c_str(), file_size, errmsg.c_str());
      return IOStatus::IOError(local_file, errmsg);
    }
  }
  Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
      "[s3] PutCloudObject %s/%s, size %" PRIu64 ", OK", bucket_name.c_str(),
      object_path.c_str(), file_size);
  return IOStatus::OK();
}

#endif /* USE_AWS */

Status CloudStorageProviderImpl::CreateS3Provider(
    std::unique_ptr<CloudStorageProvider>* provider) {
#ifndef USE_AWS
  provider->reset();
  return Status::NotSupported(
      "In order to use S3, make sure you're compiling with USE_AWS=1");
#else
  provider->reset(new S3StorageProvider());
  return Status::OK();
#endif /* USE_AWS */
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
