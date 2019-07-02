//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
// This file defines an AWS-Kinesis environment for rocksdb.
// A log file maps to a stream in Kinesis.
//
#ifdef USE_AWS

#include <fstream>
#include <iostream>

#include "cloud/aws/aws_env.h"
#include "cloud/aws/aws_file.h"
#include "cloud/aws/aws_kinesis.h"
#include "rocksdb/status.h"
#include "util/coding.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"

namespace rocksdb {

KinesisWritableFile::KinesisWritableFile(
    AwsEnv* env, const std::string& fname, const EnvOptions& options,
    std::shared_ptr<KinesisClient> kinesis_client)
  : CloudLogWritableFile(env, fname, options),
    kinesis_client_(kinesis_client), current_offset_(0) {

  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[kinesis] WritableFile opened file %s", fname_.c_str());
  std::string bucket = env_->GetSrcBucketName();
  topic_ = ToAwsString(bucket);
}

KinesisWritableFile::~KinesisWritableFile() {}

Status KinesisWritableFile::Append(const Slice& data) {
  assert(status_.ok());

  // create write request
  PutRecordRequest request;
  request.SetStreamName(topic_);
  request.SetPartitionKey(ToAwsString(fname_));

  // serialize write record
  std::string buffer;
  CloudLogController::SerializeLogRecordAppend(fname_, data, current_offset_,
                                              &buffer);
  request.SetData(Aws::Utils::ByteBuffer((const unsigned char*)buffer.c_str(),
                                         buffer.size()));

  // write to stream
  const PutRecordOutcome& outcome = kinesis_client_->PutRecord(request);
  bool isSuccess = outcome.IsSuccess();

  if (!isSuccess) {
    const Aws::Client::AWSError<KinesisErrors>& error = outcome.GetError();
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[kinesis] NewWritableFile src %s Append error %s", fname_.c_str(),
        error.GetMessage().c_str());
    return Status::IOError(fname_, error.GetMessage().c_str());
  }
  current_offset_ += data.size();
  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[kinesis] WritableFile Append file %s %ld %ld", fname_.c_str(),
      data.size(), buffer.size());
  return Status::OK();
}

Status KinesisWritableFile::Close() {
  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[kinesis] S3WritableFile closing %s", fname_.c_str());
  assert(status_.ok());

  // create write request
  PutRecordRequest request;
  request.SetStreamName(topic_);
  request.SetPartitionKey(ToAwsString(fname_));

  // serialize write record
  std::string buffer;
  CloudLogController::SerializeLogRecordClosed(fname_, current_offset_, &buffer);
  request.SetData(Aws::Utils::ByteBuffer((const unsigned char*)buffer.c_str(),
                                         buffer.size()));

  // write to stream
  const PutRecordOutcome& outcome = kinesis_client_->PutRecord(request);
  bool isSuccess = outcome.IsSuccess();

  if (!isSuccess) {
    const Aws::Client::AWSError<KinesisErrors>& error = outcome.GetError();
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[kinesis] NewWritableFile src %s Close error %s", fname_.c_str(),
        error.GetMessage().c_str());
    return Status::IOError(fname_, error.GetMessage().c_str());
  }
  current_offset_ += buffer.size();
  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[kinesis] WritableFile Close file %s %ld", fname_.c_str(),
      buffer.size());
  return Status::OK();
}

//
// Log a delete record to stream
//
Status KinesisWritableFile::LogDelete() {
  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_, "[kinesis] LogDelete %s",
      fname_.c_str());
  assert(status_.ok());

  // create write request
  PutRecordRequest request;
  request.SetStreamName(topic_);
  request.SetPartitionKey(ToAwsString(fname_));

  // serialize write record
  std::string buffer;
  CloudLogController::SerializeLogRecordDelete(fname_, &buffer);
  request.SetData(Aws::Utils::ByteBuffer((const unsigned char*)buffer.c_str(),
                                         buffer.size()));

  // write to stream
  const PutRecordOutcome& outcome = kinesis_client_->PutRecord(request);
  bool isSuccess = outcome.IsSuccess();

  if (!isSuccess) {
    const Aws::Client::AWSError<KinesisErrors>& error = outcome.GetError();
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[kinesis] LogDelete src %s error %s", fname_.c_str(),
        error.GetMessage().c_str());
    return Status::IOError(fname_, error.GetMessage().c_str());
  }
  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[kinesis] LogDelete file %s %ld", fname_.c_str(), buffer.size());
  return Status::OK();
}

KinesisController::KinesisController(
    AwsEnv* env, std::shared_ptr<Logger> info_log,
    std::unique_ptr<Aws::Kinesis::KinesisClient> kinesis_client)
  : CloudLogController(env, info_log),
    kinesis_client_(std::move(kinesis_client)) {

  // Initialize stream name.
  std::string bucket = env_->GetSrcBucketName();
  topic_ = ToAwsString(bucket);

  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[%s] KinesisController opening stream %s using cachedir '%s'",
      GetTypeName().c_str(), topic_.c_str(), cache_dir_.c_str());
}

KinesisController::~KinesisController() {
  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[%s] KinesisController closed", GetTypeName().c_str());
}

Status KinesisController::TailStream() {
  status_ = InitializeShards();

  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[%s] TailStream topic %s %s",
      GetTypeName().c_str(), topic_.c_str(), status_.ToString().c_str());

  Status lastErrorStatus;
  int retryAttempt = 0;
  while (env_->IsRunning()) {
    if (retryAttempt > 10) {
      status_ = lastErrorStatus;
      break;
    }

    // Count the number of records that were read in one iteration
    size_t num_read = 0;

    // Issue a read from Kinesis stream
    GetRecordsRequest request;
    request.SetShardIterator(shards_iterator_[0]);
    GetRecordsOutcome outcome = kinesis_client_->GetRecords(request);
    bool isSuccess = outcome.IsSuccess();
    if (!isSuccess) {
      const Aws::Client::AWSError<KinesisErrors>& error = outcome.GetError();
      Aws::Kinesis::KinesisErrors err = error.GetErrorType();
      if (err == Aws::Kinesis::KinesisErrors::EXPIRED_ITERATOR) {
        Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
            "[%s] expired shard iterator for %s. Reseeking...",
            GetTypeName().c_str(), topic_.c_str(), error.GetMessage().c_str());
        shards_iterator_[0] = "";
        SeekShards();  // read position at last seqno
      } else {
        Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
            "[%s] error reading %s %s",
            GetTypeName().c_str(), topic_.c_str(), error.GetMessage().c_str());
        lastErrorStatus =
            Status::IOError(topic_.c_str(), error.GetMessage().c_str());
        ++retryAttempt;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
      }
      continue;
    }
    retryAttempt = 0;
    GetRecordsResult& res = outcome.GetResult();
    const Aws::Vector<Record>& records = res.GetRecords();

    // skip to the next position in the shard iterator
    const Aws::String& next = res.GetNextShardIterator();
    shards_iterator_[0] = next;

    // read data into buffer
    num_read = records.size();
    for (auto r : records) {
      // extract payload from log record
      const Aws::Utils::ByteBuffer& b = r.GetData();
      const unsigned char* data = b.GetUnderlyingData();
      Slice sl((const char*)data, b.GetLength());

      // apply the payload to local filesystem
      status_ = Apply(sl);
      if (!status_.ok()) {
        Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
            "[%s] error processing message size %ld "
            "extracted from stream %s %s", GetTypeName().c_str(),
            b.GetLength(), topic_.c_str(), status_.ToString().c_str());
      } else {
        Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
            "[%s] successfully processed message size %ld "
            "extracted from stream %s %s", GetTypeName().c_str(),
            b.GetLength(), topic_.c_str(), status_.ToString().c_str());
      }

      // remember last read seqno from stream
      shards_position_[0] = r.GetSequenceNumber();
    }
    // If no records were read in last iteration, then sleep for 50 millis
    if (num_read == 0 && status_.ok()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }
  return status_;
}

Status KinesisController::CreateStream(const std::string& bucket) {
  Aws::String topic = ToAwsString(bucket);

  // create stream
  CreateStreamRequest create_request;
  create_request.SetStreamName(topic);
  create_request.SetShardCount(1);
  CreateStreamOutcome outcome = kinesis_client_->CreateStream(create_request);
  bool isSuccess = outcome.IsSuccess();
  Status st;
  if (!isSuccess) {
    const Aws::Client::AWSError<KinesisErrors>& error = outcome.GetError();
    std::string errmsg(error.GetMessage().c_str());
    if (errmsg.find("already exists") == std::string::npos) {
      st = Status::IOError(topic.c_str(), errmsg);
    }
  }
  if (st.ok()) {
    st = WaitForStreamReady(bucket);
  }
  return st;
}

Status KinesisController::WaitForStreamReady(const std::string& bucket) {
  Aws::String topic = ToAwsString(bucket);

  // Keep looping if the stream is being initialized
  const std::chrono::microseconds start(env_->NowMicros());
  bool isSuccess = false;
  Status st;

  while (!isSuccess) {
    // Find the number of shards for this stream. It should be 1 shard.
    st = Status::OK();
    DescribeStreamRequest request;
    request.SetStreamName(topic);
    DescribeStreamOutcome outcome = kinesis_client_->DescribeStream(request);
    isSuccess = outcome.IsSuccess();
    if (!isSuccess) {
      const Aws::Client::AWSError<KinesisErrors>& error = outcome.GetError();
      st = Status::IOError(topic.c_str(), error.GetMessage().c_str());
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_, " Waiting for stream ready %s",
          st.ToString().c_str());
    } else {
      const DescribeStreamResult& result = outcome.GetResult();
      const StreamDescription& description = result.GetStreamDescription();
      auto& shards = description.GetShards();
      if (shards.size() != 1) {
        isSuccess = false;
        std::string msg = "Kinesis timedout initialize shards " +
                          std::string(topic.c_str(), topic.size());
        st = Status::TimedOut(msg);
      }
    }
    if (!isSuccess) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    if (start + kRetryPeriod < std::chrono::microseconds(env_->NowMicros())) {
      break;
    }
  }
  return st;
}

Status KinesisController::InitializeShards() {
  // Keep looking for about 10 seconds, in case the stream was newly created
  // and is being initialized.
  Status st = WaitForStreamReady(env_->GetSrcBucketName());
  if (!st.ok()) {
    return st;
  }

  // Find the number of shards for this stream. It should be 1 shard.
  DescribeStreamRequest request;
  request.SetStreamName(topic_);
  DescribeStreamOutcome outcome = kinesis_client_->DescribeStream(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<KinesisErrors>& error = outcome.GetError();
    st = Status::IOError(topic_.c_str(), error.GetMessage().c_str()),
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[%s] S3ReadableFile file %s Unable to find shards %s",
        GetTypeName().c_str(), topic_.c_str(), st.ToString().c_str());
  } else {
    const DescribeStreamResult& result = outcome.GetResult();
    const StreamDescription& description = result.GetStreamDescription();

    // append all shards to global list
    auto& shards = description.GetShards();
    assert(shards.size() == 1);
    for (auto s : shards) {
      shards_.push_back(s);
      shards_iterator_.push_back("");
      shards_position_.push_back("");
    }
  }
  if (st.ok()) {
    SeekShards();
  }
  return st;
}

void KinesisController::SeekShards() {
  // Check all shard iterators
  for (size_t i = 0; i < shards_.size(); i++) {
    if (shards_iterator_[i].size() != 0) {
      continue;  // iterator is still valid, nothing to do
    }
    // create new shard iterator at specified seqno
    GetShardIteratorRequest request;
    request.SetStreamName(topic_);
    request.SetShardId(shards_[i].GetShardId());
    if (shards_position_[i].size() == 0) {
      request.SetShardIteratorType(ShardIteratorType::TRIM_HORIZON);
    } else {
      request.SetShardIteratorType(ShardIteratorType::AFTER_SEQUENCE_NUMBER);
      request.SetStartingSequenceNumber(shards_position_[i]);
    }
    GetShardIteratorOutcome outcome =
      kinesis_client_->GetShardIterator(request);
    bool isSuccess = outcome.IsSuccess();
    if (!isSuccess) {
      const Aws::Client::AWSError<KinesisErrors>& error = outcome.GetError();
      status_ = Status::IOError(topic_.c_str(), error.GetMessage().c_str());
      Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
          "[%s] S3ReadableFile file %s Unable to find shards %s",
          GetTypeName().c_str(), topic_.c_str(), status_.ToString().c_str());
    } else {
      const GetShardIteratorResult& result = outcome.GetResult();
      shards_iterator_[i] = result.GetShardIterator();
    }
  }
}

CloudLogWritableFile* KinesisController::CreateWritableFile(
    const std::string& fname, const EnvOptions& options) {
  return dynamic_cast<CloudLogWritableFile*>(
      new KinesisWritableFile(env_, fname, options, kinesis_client_));
}

}  // namespace

#endif /* USE_AWS */
