//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
// This file defines an AWS-Kinesis environment for rocksdb.
// A log file maps to a stream in Kinesis.
//

#include <fstream>
#include <iostream>

#include "cloud/cloud_log_controller_impl.h"
#include "rocksdb/cloud/cloud_env_options.h"
#include "rocksdb/convenience.h"
#include "rocksdb/status.h"
#include "util/coding.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"

#ifdef USE_AWS
#include <aws/core/utils/Outcome.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/KinesisErrors.h>
#include <aws/kinesis/model/CreateStreamRequest.h>
#include <aws/kinesis/model/DescribeStreamRequest.h>
#include <aws/kinesis/model/DescribeStreamResult.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/GetShardIteratorResult.h>
#include <aws/kinesis/model/PutRecordRequest.h>
#include <aws/kinesis/model/PutRecordResult.h>
#include <aws/kinesis/model/PutRecordsRequest.h>
#include <aws/kinesis/model/PutRecordsRequestEntry.h>
#include <aws/kinesis/model/Record.h>
#include <aws/kinesis/model/ShardIteratorType.h>
#include <aws/kinesis/model/StreamDescription.h>
namespace ROCKSDB_NAMESPACE {
namespace cloud {
namespace kinesis {

/***************************************************/
/*              KinesisWritableFile                */
/***************************************************/
class KinesisWritableFile : public CloudLogWritableFile {
 public:
  KinesisWritableFile(
      CloudEnv* env, const std::string& fname, const EnvOptions& options,
      const std::shared_ptr<Aws::Kinesis::KinesisClient>& kinesis_client)
      : CloudLogWritableFile(env, fname, options),
        kinesis_client_(kinesis_client),
        current_offset_(0) {
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[kinesis] WritableFile opened file %s", fname_.c_str());
    std::string bucket = env_->GetSrcBucketName();
    topic_ = Aws::String(bucket.c_str(), bucket.size());
  }
  virtual ~KinesisWritableFile() {}

  virtual Status Append(const Slice& data) override;
  virtual Status Close() override;
  virtual Status LogDelete() override;

 private:
  std::shared_ptr<Aws::Kinesis::KinesisClient> kinesis_client_;
  Aws::String topic_;
  uint64_t current_offset_;
};

Status KinesisWritableFile::Append(const Slice& data) {
  assert(status_.ok());

  // create write request
  Aws::Kinesis::Model::PutRecordRequest request;
  request.SetStreamName(topic_);
  request.SetPartitionKey(Aws::String(fname_.c_str(), fname_.size()));

  // serialize write record
  std::string buffer;
  CloudLogControllerImpl::SerializeLogRecordAppend(fname_, data,
                                                   current_offset_, &buffer);
  request.SetData(Aws::Utils::ByteBuffer((const unsigned char*)buffer.c_str(),
                                         buffer.size()));

  // write to stream
  const auto& outcome = kinesis_client_->PutRecord(request);
  bool isSuccess = outcome.IsSuccess();

  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::Kinesis::KinesisErrors>& error =
        outcome.GetError();
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[kinesis] NewWritableFile src %s Append error %s", fname_.c_str(),
        error.GetMessage().c_str());
    return Status::IOError(fname_, error.GetMessage().c_str());
  }
  current_offset_ += data.size();
  Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
      "[kinesis] WritableFile Append file %s %ld %ld", fname_.c_str(),
      data.size(), buffer.size());
  return Status::OK();
}

Status KinesisWritableFile::Close() {
  Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
      "[kinesis] S3WritableFile closing %s", fname_.c_str());
  assert(status_.ok());

  // create write request
  Aws::Kinesis::Model::PutRecordRequest request;
  request.SetStreamName(topic_);
  request.SetPartitionKey(Aws::String(fname_.c_str(), fname_.size()));

  // serialize write record
  std::string buffer;
  CloudLogControllerImpl::SerializeLogRecordClosed(fname_, current_offset_,
                                                   &buffer);
  request.SetData(Aws::Utils::ByteBuffer((const unsigned char*)buffer.c_str(),
                                         buffer.size()));

  // write to stream
  const auto& outcome = kinesis_client_->PutRecord(request);
  bool isSuccess = outcome.IsSuccess();

  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::Kinesis::KinesisErrors>& error =
        outcome.GetError();
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[kinesis] NewWritableFile src %s Close error %s", fname_.c_str(),
        error.GetMessage().c_str());
    return Status::IOError(fname_, error.GetMessage().c_str());
  }
  current_offset_ += buffer.size();
  Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
      "[kinesis] WritableFile Close file %s %ld", fname_.c_str(),
      buffer.size());
  return Status::OK();
}

//
// Log a delete record to stream
//
Status KinesisWritableFile::LogDelete() {
  Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(), "[kinesis] LogDelete %s",
      fname_.c_str());
  assert(status_.ok());

  // create write request
  Aws::Kinesis::Model::PutRecordRequest request;
  request.SetStreamName(topic_);
  request.SetPartitionKey(Aws::String(fname_.c_str(), fname_.size()));

  // serialize write record
  std::string buffer;
  CloudLogControllerImpl::SerializeLogRecordDelete(fname_, &buffer);
  request.SetData(Aws::Utils::ByteBuffer((const unsigned char*)buffer.c_str(),
                                         buffer.size()));

  // write to stream
  const Aws::Kinesis::Model::PutRecordOutcome& outcome =
      kinesis_client_->PutRecord(request);
  bool isSuccess = outcome.IsSuccess();

  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::Kinesis::KinesisErrors>& error =
        outcome.GetError();
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[kinesis] LogDelete src %s error %s", fname_.c_str(),
        error.GetMessage().c_str());
    return Status::IOError(fname_, error.GetMessage().c_str());
  }
  Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
      "[kinesis] LogDelete file %s %ld", fname_.c_str(), buffer.size());
  return Status::OK();
}

/***************************************************/
/*               KinesisController                 */
/***************************************************/

//
// Intricacies of reading a Kinesis stream
//
class KinesisController : public CloudLogControllerImpl {
 public:
  virtual ~KinesisController() {
    if (env_ != nullptr) {
      Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
          "[%s] KinesisController closed", Name());
    }
  }

  const char* Name() const override { return "kinesis"; }

  Status CreateStream(const std::string& bucket) override;
  Status WaitForStreamReady(const std::string& bucket) override;
  Status TailStream() override;

  CloudLogWritableFile* CreateWritableFile(const std::string& fname,
                                           const EnvOptions& options) override;

  Status PrepareOptions(const ConfigOptions& options) override;

 private:
  std::shared_ptr<Aws::Kinesis::KinesisClient> kinesis_client_;

  Aws::String topic_;

  // list of shards and their positions
  Aws::Vector<Aws::Kinesis::Model::Shard> shards_;
  Aws::Vector<Aws::String> shards_iterator_;
  std::vector<Aws::String> shards_position_;

  Status InitializeShards();

  // Set shard iterator for every shard to position specified by
  // shards_position_.
  void SeekShards();
};

Status KinesisController::PrepareOptions(const ConfigOptions& config_options) {
  CloudEnv* cenv = static_cast<CloudEnv*>(config_options.env);
  Aws::Client::ClientConfiguration config;
  const auto& options = cenv->GetCloudEnvOptions();
  if (std::string(cenv->Name()) != CloudEnv::kAws()) {
    return Status::InvalidArgument("Kinesis Provider requires AWS Environment");
  }
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> provider;
  status_ = options.credentials.GetCredentialsProvider(&provider);
  if (status_.ok()) {
    status_ = AwsCloudOptions::GetClientConfiguration(
        cenv, options.src_bucket.GetRegion(), &config);
  }
  if (status_.ok()) {
    kinesis_client_.reset(
        provider ? new Aws::Kinesis::KinesisClient(provider, config)
                 : new Aws::Kinesis::KinesisClient(config));
  }
  if (status_.ok()) {
    status_ = CloudLogControllerImpl::PrepareOptions(config_options);
  }
  return status_;
}

Status KinesisController::TailStream() {
  status_ = InitializeShards();

  Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
      "[%s] TailStream topic %s %s", Name(), topic_.c_str(),
      status_.ToString().c_str());

  Status lastErrorStatus;
  int retryAttempt = 0;
  while (IsRunning()) {
    if (retryAttempt > 10) {
      status_ = lastErrorStatus;
      break;
    }

    // Count the number of records that were read in one iteration
    size_t num_read = 0;

    // Issue a read from Kinesis stream
    Aws::Kinesis::Model::GetRecordsRequest request;
    request.SetShardIterator(shards_iterator_[0]);
    Aws::Kinesis::Model::GetRecordsOutcome outcome =
        kinesis_client_->GetRecords(request);
    bool isSuccess = outcome.IsSuccess();
    if (!isSuccess) {
      const Aws::Client::AWSError<Aws::Kinesis::KinesisErrors>& error =
          outcome.GetError();
      Aws::Kinesis::KinesisErrors err = error.GetErrorType();
      if (err == Aws::Kinesis::KinesisErrors::EXPIRED_ITERATOR) {
        Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
            "[%s] expired shard iterator for %s. Reseeking...", Name(),
            topic_.c_str());
        shards_iterator_[0] = "";
        SeekShards();  // read position at last seqno
      } else {
        Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
            "[%s] error reading %s %s", Name(), topic_.c_str(),
            error.GetMessage().c_str());
        lastErrorStatus =
            Status::IOError(topic_.c_str(), error.GetMessage().c_str());
        ++retryAttempt;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
      }
      continue;
    }
    retryAttempt = 0;
    Aws::Kinesis::Model::GetRecordsResult& res = outcome.GetResult();
    const Aws::Vector<Aws::Kinesis::Model::Record>& records = res.GetRecords();

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
        Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
            "[%s] error processing message size %ld "
            "extracted from stream %s %s",
            Name(), b.GetLength(), topic_.c_str(), status_.ToString().c_str());
      } else {
        Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
            "[%s] successfully processed message size %ld "
            "extracted from stream %s %s",
            Name(), b.GetLength(), topic_.c_str(), status_.ToString().c_str());
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
  // Initialize stream name.
  topic_ = Aws::String(bucket.c_str(), bucket.size());

  Log(InfoLogLevel::INFO_LEVEL, env_->GetLogger(),
      "[%s] KinesisController opening stream %s using cachedir '%s'", Name(),
      topic_.c_str(), cache_dir_.c_str());

  // create stream
  Aws::Kinesis::Model::CreateStreamRequest create_request;
  create_request.SetStreamName(topic_);
  create_request.SetShardCount(1);
  Aws::Kinesis::Model::CreateStreamOutcome outcome =
      kinesis_client_->CreateStream(create_request);
  bool isSuccess = outcome.IsSuccess();
  Status st;
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::Kinesis::KinesisErrors>& error =
        outcome.GetError();
    std::string errmsg(error.GetMessage().c_str());
    if (errmsg.find("already exists") == std::string::npos) {
      st = Status::IOError(topic_.c_str(), errmsg);
    }
  }
  if (st.ok()) {
    st = WaitForStreamReady(bucket);
  }
  return st;
}

Status KinesisController::WaitForStreamReady(const std::string& bucket) {
  Aws::String topic = Aws::String(bucket.c_str(), bucket.size());

  // Keep looping if the stream is being initialized
  const std::chrono::microseconds start(env_->NowMicros());
  bool isSuccess = false;
  Status st;

  while (!isSuccess) {
    // Find the number of shards for this stream. It should be 1 shard.
    st = Status::OK();
    Aws::Kinesis::Model::DescribeStreamRequest request;
    request.SetStreamName(topic);
    Aws::Kinesis::Model::DescribeStreamOutcome outcome =
        kinesis_client_->DescribeStream(request);
    isSuccess = outcome.IsSuccess();
    if (!isSuccess) {
      const Aws::Client::AWSError<Aws::Kinesis::KinesisErrors>& error =
          outcome.GetError();
      st = Status::IOError(topic.c_str(), error.GetMessage().c_str());
      Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
          " Waiting for stream ready %s", st.ToString().c_str());
    } else {
      const Aws::Kinesis::Model::DescribeStreamResult& result =
          outcome.GetResult();
      const Aws::Kinesis::Model::StreamDescription& description =
          result.GetStreamDescription();
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
  Aws::Kinesis::Model::DescribeStreamRequest request;
  request.SetStreamName(topic_);
  auto outcome = kinesis_client_->DescribeStream(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<Aws::Kinesis::KinesisErrors>& error =
        outcome.GetError();
    st = Status::IOError(topic_.c_str(), error.GetMessage().c_str()),
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[%s] S3ReadableFile file %s Unable to find shards %s", Name(),
        topic_.c_str(), st.ToString().c_str());
  } else {
    const Aws::Kinesis::Model::DescribeStreamResult& result =
        outcome.GetResult();
    const Aws::Kinesis::Model::StreamDescription& description =
        result.GetStreamDescription();

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
    Aws::Kinesis::Model::GetShardIteratorRequest request;
    request.SetStreamName(topic_);
    request.SetShardId(shards_[i].GetShardId());
    if (shards_position_[i].size() == 0) {
      request.SetShardIteratorType(
          Aws::Kinesis::Model::ShardIteratorType::TRIM_HORIZON);
    } else {
      request.SetShardIteratorType(
          Aws::Kinesis::Model::ShardIteratorType::AFTER_SEQUENCE_NUMBER);
      request.SetStartingSequenceNumber(shards_position_[i]);
    }
    Aws::Kinesis::Model::GetShardIteratorOutcome outcome =
        kinesis_client_->GetShardIterator(request);
    bool isSuccess = outcome.IsSuccess();
    if (!isSuccess) {
      const Aws::Client::AWSError<Aws::Kinesis::KinesisErrors>& error =
          outcome.GetError();
      status_ = Status::IOError(topic_.c_str(), error.GetMessage().c_str());
      Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
          "[%s] S3ReadableFile file %s Unable to find shards %s", Name(),
          topic_.c_str(), status_.ToString().c_str());
    } else {
      const Aws::Kinesis::Model::GetShardIteratorResult& result =
          outcome.GetResult();
      shards_iterator_[i] = result.GetShardIterator();
    }
  }
}

CloudLogWritableFile* KinesisController::CreateWritableFile(
    const std::string& fname, const EnvOptions& options) {
  return dynamic_cast<CloudLogWritableFile*>(
      new KinesisWritableFile(env_, fname, options, kinesis_client_));
}

}  // namespace kinesis
}  // namespace cloud
}  // namespace ROCKSDB_NAMESPACE
#endif /* USE_AWS */

namespace ROCKSDB_NAMESPACE {
Status CloudLogControllerImpl::CreateKinesisController(
    std::unique_ptr<CloudLogController>* output) {
#ifndef USE_AWS
  output->reset();
  return Status::NotSupported(
      "In order to use Kinesis, make sure you're compiling with "
      "USE_AWS=1");
#else
  output->reset(new ROCKSDB_NAMESPACE::cloud::kinesis::KinesisController());
  return Status::OK();
#endif /* USE_AWS */
}
}  // namespace ROCKSDB_NAMESPACE
