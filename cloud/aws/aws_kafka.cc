//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
// This file defines an AWS-Kinesis environment for rocksdb.
// A log file maps to a stream in Kinesis.
//

#include <cinttypes>
#include <fstream>
#include <iostream>

#include "cloud/cloud_log_controller_impl.h"
#include "rocksdb/cloud/cloud_env_options.h"
#include "rocksdb/convenience.h"
#include "rocksdb/status.h"
#include "util/coding.h"
#include "util/string_util.h"

#ifdef USE_KAFKA
#include <librdkafka/rdkafkacpp.h>
namespace ROCKSDB_NAMESPACE {
namespace cloud {
namespace kafka {

/***************************************************/
/*                KafkaWritableFile                */
/***************************************************/
class KafkaWritableFile : public CloudLogWritableFile {
 public:
  static const std::chrono::microseconds kFlushTimeout;

  KafkaWritableFile(CloudEnv* env, const std::string& fname,
                    const EnvOptions& options,
                    std::shared_ptr<RdKafka::Producer> producer,
                    std::shared_ptr<RdKafka::Topic> topic)
      : CloudLogWritableFile(env, fname, options),
        producer_(producer),
        topic_(topic),
        current_offset_(0) {
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[kafka] WritableFile opened file %s", fname_.c_str());
  }

  ~KafkaWritableFile() {}
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
const std::chrono::microseconds KafkaWritableFile::kFlushTimeout =
    std::chrono::seconds(10);

Status KafkaWritableFile::ProduceRaw(const std::string& operation_name,
                                     const Slice& message) {
  if (!status_.ok()) {
    return status_;
  }

  RdKafka::ErrorCode resp;
  resp = producer_->produce(
      topic_.get(), RdKafka::Topic::PARTITION_UA /* UnAssigned */,
      RdKafka::Producer::RK_MSG_COPY /* Copy payload */, (void*)message.data(),
      message.size(), &fname_ /* Partitioning key */, nullptr);

  if (resp == RdKafka::ERR_NO_ERROR) {
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[kafka] WritableFile %s file %s %ld", fname_.c_str(),
        operation_name.c_str(), message.size());
    return Status::OK();
  } else if (resp == RdKafka::ERR__QUEUE_FULL) {
    const std::string formatted_err = RdKafka::err2str(resp);
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[kafka] WritableFile src %s %s error %s", fname_.c_str(),
        operation_name.c_str(), formatted_err.c_str());

    return Status::Busy(topic_->name().c_str(), RdKafka::err2str(resp).c_str());
  } else {
    const std::string formatted_err = RdKafka::err2str(resp);
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[kafka] WritableFile src %s %s error %s", fname_.c_str(),
        operation_name.c_str(), formatted_err.c_str());

    return Status::IOError(topic_->name().c_str(),
                           RdKafka::err2str(resp).c_str());
  }
  current_offset_ += message.size();

  return Status::OK();
}

Status KafkaWritableFile::Append(const Slice& data) {
  std::string serialized_data;
  CloudLogControllerImpl::SerializeLogRecordAppend(
      fname_, data, current_offset_, &serialized_data);

  return ProduceRaw("Append", serialized_data);
}

Status KafkaWritableFile::Close() {
  Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
      "[kafka] S3WritableFile closing %s", fname_.c_str());

  std::string serialized_data;
  CloudLogControllerImpl::SerializeLogRecordClosed(fname_, current_offset_,
                                                   &serialized_data);

  return ProduceRaw("Close", serialized_data);
}

bool KafkaWritableFile::IsSyncThreadSafe() const { return true; }

Status KafkaWritableFile::Sync() { return Flush(); }

Status KafkaWritableFile::Flush() {
  std::chrono::microseconds start(env_->NowMicros());

  bool done = false;
  bool timeout = false;
  while (status_.ok() && !(done = (producer_->outq_len() == 0)) &&
         !(timeout = (std::chrono::microseconds(env_->NowMicros()) - start >
                      kFlushTimeout))) {
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[kafka] WritableFile src %s "
        "Waiting on flush: Output queue length: %d",
        fname_.c_str(), producer_->outq_len());

    producer_->poll(500);
  }

  if (done) {
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[kafka] WritableFile src %s Flushed", fname_.c_str());
  } else if (timeout) {
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[kafka] WritableFile src %s Flushing timed out after %" PRId64 "us",
        fname_.c_str(), kFlushTimeout.count());
    status_ = Status::TimedOut();
  } else {
    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[kafka] WritableFile src %s Flush interrupted", fname_.c_str());
  }

  return status_;
}

Status KafkaWritableFile::LogDelete() {
  Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(), "[kafka] LogDelete %s",
      fname_.c_str());

  std::string serialized_data;
  CloudLogControllerImpl::SerializeLogRecordDelete(fname_, &serialized_data);

  return ProduceRaw("Delete", serialized_data);
}

/***************************************************/
/*                 KafkaController                 */
/***************************************************/

//
// Intricacies of reading a Kafka stream
//
class KafkaController : public CloudLogControllerImpl {
 public:
  ~KafkaController() {
    for (size_t i = 0; i < partitions_.size(); i++) {
      consumer_->stop(consumer_topic_.get(), partitions_[i]->partition());
    }
    if (env_ != nullptr) {
      Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
          "[%s] KafkaController closed.", Name());
    }
  }

  const char* Name() const override { return "kafka"; }

  virtual Status CreateStream(const std::string& /* bucket_prefix */) override {
    // Kafka client cannot create a topic. Topics are either manually created
    // or implicitly created on first write if auto.create.topics.enable is
    // true.
    return status_;
  }
  virtual Status WaitForStreamReady(
      const std::string& /* bucket_prefix */) override {
    // Kafka topics don't need to be waited on.
    return status_;
  }

  virtual Status TailStream() override;

  virtual CloudLogWritableFile* CreateWritableFile(
      const std::string& fname, const EnvOptions& options) override;
  Status PrepareOptions(const ConfigOptions& options) override;

 protected:

 private:
  Status InitializePartitions();

  std::shared_ptr<RdKafka::Producer> producer_;
  std::shared_ptr<RdKafka::Consumer> consumer_;

  std::shared_ptr<RdKafka::Topic> producer_topic_;
  std::shared_ptr<RdKafka::Topic> consumer_topic_;

  std::shared_ptr<RdKafka::Queue> consuming_queue_;

  std::vector<std::shared_ptr<RdKafka::TopicPartition>> partitions_;
};

Status KafkaController::PrepareOptions(const ConfigOptions& options) {
  CloudEnv* env = static_cast<CloudEnv*>(options.env);
  std::string conf_errstr, producer_errstr, consumer_errstr;
  const auto& kconf =
      env->GetCloudEnvOptions().kafka_log_options.client_config_params;
  if (kconf.empty()) {
    return Status::InvalidArgument("No configs specified to kafka client");
  }

  std::unique_ptr<RdKafka::Conf> conf(
      RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

  for (auto const& item : kconf) {
    if (conf->set(item.first, item.second, conf_errstr) !=
        RdKafka::Conf::CONF_OK) {
      Status s = Status::InvalidArgument(
          "Failed adding specified conf to Kafka conf", conf_errstr.c_str());

      Log(InfoLogLevel::ERROR_LEVEL, env->GetLogger(),
          "Kafka conf set error: %s", s.ToString().c_str());
      return s;
    }
  }

  producer_.reset(RdKafka::Producer::create(conf.get(), producer_errstr));
  consumer_.reset(RdKafka::Consumer::create(conf.get(), consumer_errstr));

  Status s;
  if (!producer_) {
    s = Status::InvalidArgument("Failed creating Kafka producer",
                                producer_errstr.c_str());

    Log(InfoLogLevel::ERROR_LEVEL, env->GetLogger(),
        "[%s] Kafka producer error: %s", Name(), s.ToString().c_str());
  } else if (!consumer_) {
    s = Status::InvalidArgument("Failed creating Kafka consumer",
                                consumer_errstr.c_str());

    Log(InfoLogLevel::ERROR_LEVEL, env->GetLogger(),
        "[%s] Kafka consumer error: %s", Name(), s.ToString().c_str());
  } else {
    const std::string topic_name = env->GetSrcBucketName();

    Log(InfoLogLevel::DEBUG_LEVEL, env->GetLogger(),
        "[%s] KafkaController opening stream %s using cachedir '%s'", Name(),
        topic_name.c_str(), cache_dir_.c_str());

    std::string pt_errstr, ct_errstr;

    // Initialize stream name.
    consuming_queue_.reset(RdKafka::Queue::create(consumer_.get()));
    producer_topic_.reset(
        RdKafka::Topic::create(producer_.get(), topic_name, NULL, pt_errstr));
    consumer_topic_.reset(
        RdKafka::Topic::create(consumer_.get(), topic_name, NULL, ct_errstr));

    assert(producer_topic_ != nullptr);
    assert(consumer_topic_ != nullptr);
    assert(consuming_queue_ != nullptr);
  }
  if (s.ok()) {
    s = CloudLogControllerImpl::PrepareOptions(options);
  }
  return s;
}

Status KafkaController::TailStream() {
  InitializePartitions();

  if (!status_.ok()) {
    return status_;
  }

  Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
      "[%s] TailStream topic %s %s", Name(), consumer_topic_->name().c_str(),
      status_.ToString().c_str());

  Status lastErrorStatus;
  int retryAttempt = 0;
  while (IsRunning()) {
    if (retryAttempt > 10) {
      status_ = lastErrorStatus;
      break;
    }

    std::unique_ptr<RdKafka::Message> message(
        consumer_->consume(consuming_queue_.get(), 1000));

    switch (message->err()) {
      case RdKafka::ERR_NO_ERROR: {
        /* Real message */
        Slice sl(static_cast<const char*>(message->payload()),
                 static_cast<size_t>(message->len()));

        // Apply the payload to local filesystem
        status_ = Apply(sl);
        if (!status_.ok()) {
          Log(InfoLogLevel::ERROR_LEVEL, env_->GetLogger(),
              "[%s] error processing message size %ld "
              "extracted from stream %s %s",
              Name(), message->len(), consumer_topic_->name().c_str(),
              status_.ToString().c_str());
        } else {
          Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
              "[%s] successfully processed message size %ld "
              "extracted from stream %s %s",
              Name(), message->len(), consumer_topic_->name().c_str(),
              status_.ToString().c_str());
        }

        // Remember last read offset from topic (currently unused).
        partitions_[message->partition()]->set_offset(message->offset());
        break;
      }
      case RdKafka::ERR__PARTITION_EOF: {
        // There are no new messages.
        consumer_->poll(50);
        break;
      }
      default: {
        lastErrorStatus =
            Status::IOError(consumer_topic_->name().c_str(),
                            RdKafka::err2str(message->err()).c_str());

        Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
            "[%s] error reading %s %s", Name(), consumer_topic_->name().c_str(),
            RdKafka::err2str(message->err()).c_str());

        ++retryAttempt;
        break;
      }
    }
  }
  Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
      "[%s] TailStream topic %s finished: %s", Name(),
      consumer_topic_->name().c_str(), status_.ToString().c_str());

  return status_;
}

Status KafkaController::InitializePartitions() {
  if (!status_.ok()) {
    return status_;
  }

  RdKafka::Metadata* result;
  RdKafka::ErrorCode err =
      consumer_->metadata(false, consumer_topic_.get(), &result, 5000);

  std::unique_ptr<RdKafka::Metadata> metadata(result);

  if (err != RdKafka::ERR_NO_ERROR) {
    status_ = Status::IOError(consumer_topic_->name().c_str(),
                              RdKafka::err2str(err).c_str());

    Log(InfoLogLevel::DEBUG_LEVEL, env_->GetLogger(),
        "[%s] S3ReadableFile file %s Unable to find shards %s", Name(),
        consumer_topic_->name().c_str(), status_.ToString().c_str());

    return status_;
  }

  assert(metadata->topics()->size() == 1);

  const RdKafka::TopicMetadata* topic_metadata = metadata->topics()->at(0);
  if (topic_metadata->partitions()->size() == 0) {
    // Topic's currently empty. As soon as writing starts, there'll be a
    // partition.
    partitions_.push_back(std::shared_ptr<RdKafka::TopicPartition>(
        RdKafka::TopicPartition::create(topic_metadata->topic(), 0)));
    partitions_.back()->set_offset(0);
  } else {
    assert(topic_metadata->partitions()->size() == 1);

    for (auto partition_metadata : *(topic_metadata->partitions())) {
      partitions_.push_back(std::shared_ptr<RdKafka::TopicPartition>(
          RdKafka::TopicPartition::create(topic_metadata->topic(),
                                          partition_metadata->id())));
      partitions_.back()->set_offset(0);
    }
  }

  for (size_t i = 0; i < partitions_.size(); i++) {
    if (partitions_[i]->offset() > 0) {
      continue;
    }

    consumer_->start(consumer_topic_.get(), partitions_[i]->partition(),
                     partitions_[i]->offset(), consuming_queue_.get());
  }

  return status_;
}

CloudLogWritableFile* KafkaController::CreateWritableFile(
    const std::string& fname, const EnvOptions& options) {
  return dynamic_cast<CloudLogWritableFile*>(
      new KafkaWritableFile(env_, fname, options, producer_, producer_topic_));
}

}  // namespace kafka
}  // namespace cloud
}  // namespace ROCKSDB_NAMESPACE

#endif /* USE_KAFKA */

namespace ROCKSDB_NAMESPACE {
Status CloudLogControllerImpl::CreateKafkaController(
    std::unique_ptr<CloudLogController>* output) {
#ifndef USE_KAFKA
  output->reset();
  return Status::NotSupported(
      "In order to use Kafka, make sure you're compiling with "
      "USE_KAFKA=1");
#else
  output->reset(new ROCKSDB_NAMESPACE::cloud::kafka::KafkaController());
  return Status::OK();
#endif  // USE_KAFKA
}
}  // namespace ROCKSDB_NAMESPACE
