//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
// This file defines an AWS-S3 environment for rocksdb.
// A directory maps to an an zero-size object in an S3 bucket
// A sst file maps to an object in that S3 bucket.
//
#ifdef USE_AWS

#include <iostream>
#include <fstream>

#include "aws/aws_env.h"
#include "cloud/aws_file.h"
#include "util/string_util.h"
#include "util/stderr_logger.h"

namespace rocksdb {

/******************** Readablefile ******************/

S3ReadableFile::S3ReadableFile(AwsEnv* env, const std::string& fname,
		               bool is_file)
      : env_(env), fname_(fname), offset_(0), file_size_(0),
	last_mod_time_(0), is_file_(is_file) {
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[s3] S3ReadableFile opening file %s",
        fname_.c_str());
    assert(!is_file_ || IsSstFile(fname));
    s3_bucket_ = GetBucket(env_->bucket_prefix_);
    s3_object_ = Aws::String(fname_.c_str(), fname_.size());

    // fetch file size from S3
    status_ = GetFileInfo();
}

S3ReadableFile::~S3ReadableFile() {
  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[s3] S3ReadableFile closed file %s",
      fname_.c_str());
  offset_ = 0;
}

// sequential access, read data at current offset in file
Status S3ReadableFile::Read(size_t n, Slice* result, char* scratch) {
  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[s3] S3ReadableFile reading %s %ld",
      fname_.c_str(), n);
  assert(status_.ok());

  Status s =  Read(offset_, n, result, scratch);

  // If the read successfully returned some data, then update
  // offset_
  if (s.ok()) {
    offset_ += result->size();
  }
  return s;
}

// random access, read data from specified offset in file
Status S3ReadableFile::Read(uint64_t offset, size_t n, Slice* result,
                                    char* scratch) const {
  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[s3] S3ReadableFile reading %s at offset %ld size %ld",
      fname_.c_str(), offset, n);
  assert(status_.ok());
  *result = Slice();

  if (offset > file_size_) {
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[s3] S3ReadableFile reading %s at offset %ld filesize %ld."
	" Nothing to do",
        fname_.c_str(), offset, file_size_);
    return Status::OK();
  }

  // trim size if needed
  if (offset + n > file_size_) {
    n = file_size_ - offset;
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[s3] S3ReadableFile reading %s at offset %ld trimmed size %ld",
        fname_.c_str(), offset, n);
  }

  // create a range read request
  char buffer[512];
  int ret = snprintf(buffer, sizeof(buffer), "bytes=%ld-%ld",
                     offset, offset + n - 1);
  if (ret < 0) {
    Log(InfoLogLevel::ERROR_LEVEL, env_->info_log_,
        "[s3] S3ReadableFile vsnprintf error %s offset %ld size %ld\n",
        fname_.c_str(), offset, n);
    return Status::IOError("S3ReadableFile vsnprintf ", fname_.c_str());
  }
  Aws::String range(buffer);

  // set up S3 request to read this range
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(s3_bucket_);
  request.SetKey(s3_object_);
  request.SetRange(range);

  Aws::S3::Model::GetObjectOutcome outcome =
	    env_->s3client_->GetObject(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error = outcome.GetError();
    std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
    Aws::S3::S3Errors s3err = error.GetErrorType();
    if (s3err == Aws::S3::S3Errors::NO_SUCH_BUCKET ||
        s3err == Aws::S3::S3Errors::NO_SUCH_KEY ||
        s3err == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
      Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
          "[s3] S3ReadableFile error in reading not-existent %s %s",
          fname_.c_str(), errmsg.c_str());
      return Status::NotFound(fname_, errmsg.c_str());
    }
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[s3] S3ReadableFile error in reading %s %ld %s %s",
        fname_.c_str(), offset, buffer, error.GetMessage().c_str());
    return Status::IOError(fname_, errmsg.c_str());
  }
  std::stringstream ss;
  //const Aws::S3::Model::GetObjectResult& res = outcome.GetResult();

  // extract data payload
  Aws::IOStream& body = outcome.GetResult().GetBody();
  ss << body.rdbuf();
  const std::string& str = ss.str();
  uint64_t size = str.size();

  // copy bytes to user buffer
  if (size > n) {
    size = n;
  }
  memcpy(scratch, str.c_str(), size);
  *result = Slice(scratch, size);

  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[s3] S3ReadableFile file %s filesize %ld read %d bytes",
      fname_.c_str(), file_size_, size);
  return Status::OK();
}

Status S3ReadableFile::Skip(uint64_t n) {
  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[s3] S3ReadableFile file %s skip %ld",
      fname_.c_str(), n);
  assert(status_.ok());

  // Update offset_ so that it does not go beyond filesize
  offset_ += n;
  if (offset_ > file_size_) {
    offset_ = file_size_;
  }
  return Status::OK();
}

//
// Retrieves the metadata of file by making a HeadObject call to S3
//
Status S3ReadableFile::GetFileInfo() {
  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[s3] S3GetFileInfo %s",
      fname_.c_str());

  // set up S3 request to read the head
  Aws::S3::Model::HeadObjectRequest request;
  request.SetBucket(s3_bucket_);
  request.SetKey(s3_object_);

  Aws::S3::Model::HeadObjectOutcome outcome =
	    env_->s3client_->HeadObject(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error = outcome.GetError();
    std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
    Aws::S3::S3Errors s3err = error.GetErrorType();
    if (s3err == Aws::S3::S3Errors::NO_SUCH_BUCKET ||
        s3err == Aws::S3::S3Errors::NO_SUCH_KEY ||
        s3err == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
      Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
          "[s3] S3GetFileInfo error not-existent %s %s",
          fname_.c_str(), errmsg.c_str());
      return Status::NotFound(fname_, errmsg.c_str());
    }
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[s3] S3GetFileInfo error %s %s",
        fname_.c_str(), errmsg.c_str());
    return Status::IOError(fname_, errmsg.c_str());
  }
  const Aws::S3::Model::HeadObjectResult& res = outcome.GetResult();

  // extract data payload
  file_size_ = res.GetContentLength();
  last_mod_time_ = res.GetLastModified().Millis();
  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[s3] S3GetFileInfo %s size %ld ok",
      fname_.c_str(), file_size_);
  return Status::OK();
};

/******************** Writablefile ******************/

//
// Create bucket in S3 if it does not already exist.
//
Status S3WritableFile::CreateBucketInS3(std::shared_ptr<Aws::S3::S3Client> client,
		const std::string& bucket_prefix) {

  // create bucket
  Aws::String bucket = GetBucket(bucket_prefix);
  Aws::S3::Model::CreateBucketRequest request;
  request.SetBucket(bucket);
  Aws::S3::Model::CreateBucketOutcome outcome =
      client->CreateBucket(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error = outcome.GetError();
    std::string errmsg(error.GetMessage().c_str());
    Aws::S3::S3Errors s3err = error.GetErrorType();
    if (s3err != Aws::S3::S3Errors::BUCKET_ALREADY_EXISTS) {
      return Status::IOError(bucket.c_str(), errmsg.c_str());
    }
  }
  return Status::OK();
}

S3WritableFile::S3WritableFile(AwsEnv* env,
		 const std::string& fname,
		 const EnvOptions& options)
      : env_(env), fname_(fname) {

    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[s3] S3WritableFile opened file %s",
	fname_.c_str());
    assert(IsSstFile(fname_));

    // Create a temporary file using the posixEnv. This file will be deleted
    // when the file is closed.
    Status s = env_->GetPosixEnv()->NewWritableFile(fname_, &temp_file_, options);
    if (!s.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
          "[s3] NewWritableFile src %s %s",
        fname_.c_str(), s.ToString().c_str());
      status_ = s;
    }
    s3_bucket_ = GetBucket(env_->bucket_prefix_);
    s3_object_ = Aws::String(fname_.c_str(), fname_.size());
}

S3WritableFile::~S3WritableFile() {
}

Status S3WritableFile::Close() {
  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[s3] S3WritableFile closing %s", fname_.c_str());
  assert(status_.ok());

  // close local file
  Status st = temp_file_->Close();
  if (!st.ok()) {
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[s3] S3WritableFile closing error on local %s\n",
        fname_.c_str());
    return st;
  }

  // find file size of local file to be uploaded.
  uint64_t file_size;
  st = env_->GetPosixEnv()->GetFileSize(fname_, &file_size);
  if (!st.ok()) {
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[s3] S3WritableFile error in getting filesize %s %s",
        fname_.c_str(), st.ToString().c_str());
    return st;
  }

  auto input_data = Aws::MakeShared<Aws::FStream>
	                (s3_object_.c_str(), fname_.c_str(),
			 std::ios_base::in | std::ios_base::out);

  // copy local file into S3
  Aws::S3::Model::PutObjectRequest put_request;
  put_request.SetBucket(s3_bucket_);
  put_request.SetKey(s3_object_);
  put_request.SetBody(input_data);

  Aws::S3::Model::PutObjectOutcome put_outcome =
	    env_->s3client_->PutObject(put_request);
  bool isSuccess = put_outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error =
                 put_outcome.GetError();
    std::string errmsg(error.GetMessage().c_str(), error.GetMessage().size());
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[s3] S3WritableFile error in uploading file %s to S3. %s",
        fname_.c_str(), errmsg.c_str());
    return Status::IOError(fname_, errmsg);
  }

  // delete local file
  if (!env_->cloud_env_options.keep_local_sst_files) {
    st = env_->GetPosixEnv()->DeleteFile(fname_);
    if (!st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
          "[s3] S3WritableFile delete failed on local file %s",
          fname_.c_str());
      return st;
    }
  }
  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[s3] S3WritableFile closed file %s size %ld",
      fname_.c_str(), file_size);
  return Status::OK();
}

} // namespace
#endif /* USE_AWS */
