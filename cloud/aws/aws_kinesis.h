//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once
#ifdef USE_AWS

#include "cloud/aws/aws_log.h"

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

using Aws::Kinesis::Model::PutRecordRequest;
using Aws::Kinesis::Model::PutRecordsRequest;
using Aws::Kinesis::Model::PutRecordsRequestEntry;
using Aws::Kinesis::Model::PutRecordOutcome;
using Aws::Kinesis::Model::PutRecordsOutcome;
using Aws::Kinesis::Model::PutRecordResult;
using Aws::Kinesis::Model::PutRecordsResult;
using Aws::Kinesis::Model::PutRecordsResultEntry;
using Aws::Kinesis::Model::CreateStreamOutcome;
using Aws::Kinesis::Model::CreateStreamRequest;
using Aws::Kinesis::Model::Shard;
using Aws::Kinesis::Model::DescribeStreamRequest;
using Aws::Kinesis::Model::DescribeStreamOutcome;
using Aws::Kinesis::Model::DescribeStreamResult;
using Aws::Kinesis::Model::StreamDescription;
using Aws::Kinesis::Model::GetShardIteratorRequest;
using Aws::Kinesis::Model::GetShardIteratorResult;
using Aws::Kinesis::Model::ShardIteratorType;
using Aws::Kinesis::Model::GetShardIteratorOutcome;
using Aws::Kinesis::Model::GetRecordsRequest;
using Aws::Kinesis::Model::GetRecordsOutcome;
using Aws::Kinesis::Model::GetRecordsResult;
using Aws::Kinesis::Model::Record;
using Aws::Kinesis::KinesisClient;
using Aws::Kinesis::KinesisErrors;

namespace rocksdb {

class KinesisWritableFile : public CloudLogWritableFile {
 public:
  KinesisWritableFile(AwsEnv* env, const std::string& fname,
                      const EnvOptions& options,
                      std::shared_ptr<KinesisClient> kinesis_client);
  virtual ~KinesisWritableFile();

  virtual Status Append(const Slice& data);
  virtual Status Close();
  virtual Status LogDelete();

 private:
  std::shared_ptr<KinesisClient> kinesis_client_;
  Aws::String topic_;
  uint64_t current_offset_;
};

//
// Intricacies of reading a Kinesis stream
//
class KinesisController : public CloudLogController {
 public:
  KinesisController(AwsEnv* env, std::shared_ptr<Logger> info_log,
      std::unique_ptr<Aws::Kinesis::KinesisClient> kinesis_client);
  virtual ~KinesisController();

  virtual const std::string GetTypeName() { return "kinesis"; }

  virtual Status CreateStream(const std::string& bucket_prefix);
  virtual Status WaitForStreamReady(const std::string& bucket_prefix);
  virtual Status TailStream();

  virtual CloudLogWritableFile* CreateWritableFile(const std::string& fname,
                                                   const EnvOptions& options);
 private:
  std::shared_ptr<Aws::Kinesis::KinesisClient> kinesis_client_;

  Aws::String topic_;

  // list of shards and their positions
  Aws::Vector<Shard> shards_;
  Aws::Vector<Aws::String> shards_iterator_;
  std::vector<Aws::String> shards_position_;

  Status InitializeShards();

  // Set shard iterator for every shard to position specified by
  // shards_position_.
  void SeekShards();
};

} // namespace rocksdb

#endif /* USE_AWS */
