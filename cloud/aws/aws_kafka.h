//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once

#ifdef USE_KAFKA

#include <librdkafka/rdkafkacpp.h>
#include "cloud/aws/aws_log.h"

namespace rocksdb {

class KafkaWritableFile : public CloudLogWritableFile {
 public:
  static const std::chrono::microseconds kFlushTimeout;

  KafkaWritableFile(AwsEnv* env, const std::string& fname,
                    const EnvOptions& options,
                    std::shared_ptr<RdKafka::Producer> producer,
                    std::shared_ptr<RdKafka::Topic> topic);
  virtual ~KafkaWritableFile();

  virtual Status Append(const Slice& data);
  virtual Status Close();
  virtual bool IsSyncThreadSafe() const;
  virtual Status Sync();
  virtual Status Flush();
  virtual Status LogDelete();

 private:
  Status ProduceRaw(const std::string& operation_name, const Slice& message);

  std::shared_ptr<RdKafka::Producer> producer_;
  std::shared_ptr<RdKafka::Topic> topic_;

  uint64_t current_offset_;
};

//
// Intricacies of reading a Kafka stream
//
class KafkaController : public CloudLogController {
 public:
  KafkaController(AwsEnv* env, std::shared_ptr<Logger> info_log,
                  std::unique_ptr<RdKafka::Producer> producer,
                  std::unique_ptr<RdKafka::Consumer> consumer);
  virtual ~KafkaController();

  // Utility method for creating a KafkaController without providing
  // producer and consumer instances, but rather cloud_log_options
  // that describes what Kafka instance to connect to.
  static Status create(AwsEnv* env, std::shared_ptr<Logger> info_log,
                       const CloudEnvOptions& cloud_env_options,
                       KafkaController** output);

  virtual const std::string GetTypeName() { return "kafka"; }

  virtual Status CreateStream(const std::string& /* bucket_prefix */) {
    // Kafka client cannot create a topic. Topics are either manually created
    // or implicitly created on first write if auto.create.topics.enable is
    // true.
    return status_;
  }
  virtual Status WaitForStreamReady(const std::string& /* bucket_prefix */) {
    // Kafka topics don't need to be waited on.
    return status_;
  }

  virtual Status TailStream();

  virtual CloudLogWritableFile* CreateWritableFile(const std::string& fname,
                                                   const EnvOptions& options);

 private:
  Status InitializePartitions();

  std::shared_ptr<RdKafka::Producer> producer_;
  std::shared_ptr<RdKafka::Consumer> consumer_;

  std::shared_ptr<RdKafka::Topic> producer_topic_;
  std::shared_ptr<RdKafka::Topic> consumer_topic_;

  std::shared_ptr<RdKafka::Queue> consuming_queue_;

  std::vector<std::shared_ptr<RdKafka::TopicPartition>> partitions_;
};

}  // namespace rocksdb

#endif /* USE_KAFKA */
