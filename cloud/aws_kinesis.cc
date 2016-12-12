//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
// This file defines an AWS-Kinesis environment for rocksdb.
// A log file maps to a stream in Kinesis.
//
#ifdef USE_AWS

#include <iostream>
#include <fstream>

#include "aws/aws_env.h"
#include "cloud/aws_file.h"
#include "rocksdb/status.h"
#include "util/string_util.h"
#include "util/stderr_logger.h"
#include "util/coding.h"

namespace rocksdb {

KinesisSystem::KinesisSystem(AwsEnv* env, std::shared_ptr<Logger> info_log)
	 :env_(env), info_log_(info_log) {

  // create a random number for the cache directory
  std::string  random = env_->GetPosixEnv()->GenerateUniqueId();
  random = trim(random);

  // temporary directory for cache
  cache_dir_ = "/tmp/ROCKSET" + pathsep + env_->bucket_prefix_ +
	       pathsep + random + pathsep;

  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[kinesis] KinesisReadableSystem opening stream %s using cachedir '%s'",
      topic_.c_str(), cache_dir_.c_str());

  // initialize stream name
  topic_ = GetStreamName(env_->bucket_prefix_);

  // create /tmp/ROCKSET/bucket_prefix/xxxx
  status_ = env_->GetPosixEnv()->CreateDirIfMissing("/tmp/ROCKSET");
  if (status_.ok()) {
    status_ = env_->GetPosixEnv()->CreateDirIfMissing("/tmp/ROCKSET/" +
		            env_->bucket_prefix_);
  }
  if (status_.ok()) {
    status_ = env_->GetPosixEnv()->CreateDirIfMissing("/tmp/ROCKSET/" +
		            env_->bucket_prefix_ + pathsep +  random +
			    pathsep);
  }
}

KinesisSystem::~KinesisSystem() {
  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[kinesis] KinesisSystem closed ");
}

//
// Tail data from stream and insert into local file system
//
Status KinesisSystem::TailStream() {

  status_ = InitializeShards();

  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[kinesis] TailStream topic %s %s",
      topic_.c_str(), status_.ToString().c_str());

  while (env_->IsRunning() && status_.ok()) {

    // Count the number of records that were read in one iteration
    int num_read = 0;

    // Issue a read from Kinesis stream
    GetRecordsRequest request;
    request.SetShardIterator(shards_iterator_[0]);
    GetRecordsOutcome outcome = env_->kinesis_client_->GetRecords(request);
    bool isSuccess = outcome.IsSuccess();
    if (!isSuccess) {
      const Aws::Client::AWSError<KinesisErrors>& error = outcome.GetError();
      Aws::Kinesis::KinesisErrors err = error.GetErrorType();
      if (err == Aws::Kinesis::KinesisErrors::EXPIRED_ITERATOR) {
        Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
            "[kinesis] expired shard iterator for %s. "
	    "Reseeking...",
            topic_.c_str(), error.GetMessage().c_str());
	shards_iterator_[0] = "";
	SeekShards();            // read position at last seqno
      } else {
        Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
            "[kinesis] error reading %s %s",
            topic_.c_str(), error.GetMessage().c_str());
	status_ = Status::IOError(topic_.c_str(), error.GetMessage().c_str());
      }
      continue;
    }
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
      Slice sl((const char *)data, b.GetLength());

      // apply the payload to local filesystem
      status_ = Apply(sl);
      if (!status_.ok()) {
        Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
            "[kinesis] error processing message size %ld "
	    "extracted from stream %s %s",
            b.GetLength(), topic_.c_str(), status_.ToString().c_str());
      } else {
        Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
            "[kinesis] successfully processed message size %ld "
	    "extracted from stream %s %s",
            b.GetLength(), topic_.c_str(), status_.ToString().c_str());
      }

      // remember last read seqno from stream
      shards_position_[0] = r.GetSequenceNumber();
    }
    // If no records were read in last iteration, then sleep for 1 second
    if (num_read == 0 && status_.ok()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
  }
  return status_;
}

// Convert a pathname to a path in the cache directory
std::string KinesisSystem::GetCachePath(const std::string& cache_dir,
		const Slice& original_pathname) {
  return (cache_dir + pathsep +
	  basename(original_pathname.ToString()));
}
		                       
//
// Process incoming message from log
//
Status KinesisSystem::Apply(const Slice& in) {
  uint32_t  operation;
  uint64_t offset_in_file;
  uint64_t file_size;
  Slice original_pathname;
  Slice payload;
  Status st;
  bool ret = ExtractLogRecord(in, &operation, &original_pathname,
		              &offset_in_file,
		              &file_size, &payload);
  if (!ret) {
    return Status::IOError("Unable to parse payload from stream");
  }
  // find pathname of file in cache directory
  std::string pathname = GetCachePath(cache_dir_, original_pathname);

  // Apply operation on cache file
  if (operation == Append) {
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[kinesis] Tailer Appending %ld bytes to %s at offset %ld",
        payload.size(), pathname.c_str(), offset_in_file);
    auto iter = cache_fds_.find(pathname);

    // If this file is not yet open, then open it and store it in cache
    if (iter == cache_fds_.end()) {
      unique_ptr<RandomRWFile> result;
      st = env_->GetPosixEnv()->NewRandomRWFile(pathname, &result, EnvOptions());
      if (st.ok()) {
        cache_fds_[pathname] = std::move(result); 
        Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
            "[kinesis] Tailer Successfully opened file %s and cached",
	    pathname.c_str());
      }
    }
    RandomRWFile* fd = cache_fds_[pathname].get();
    st = fd->Write(offset_in_file, payload);
    if (!st.ok()) {
        Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
            "[kinesis] Tailer Error writing to cached file %s",
	    pathname.c_str(), st.ToString().c_str());
    }
  } else if (operation == Delete) {
    // Delete file from cache dir
    auto iter = cache_fds_.find(pathname);
    assert(iter == cache_fds_.end());
    st = env_->GetPosixEnv()->DeleteFile(pathname);
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[kinesis] Tailer Delete file %s %s",
        pathname.c_str(), st.ToString().c_str());
    if (st.IsNotFound()) {
      st = Status::OK();
    }
  } else if (operation == Closed) {
    auto iter = cache_fds_.find(pathname);
    if (iter != cache_fds_.end()) {
      RandomRWFile* fd = iter->second.get();
      st = fd->Close();
      cache_fds_.erase(iter);
    }
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[kinesis] Tailer Closed file %s %s",
        pathname.c_str(), st.ToString().c_str());
  } else {
    st = Status::IOError("Unknown operation");
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[kinesis] Tailer file %s unknown operation %s",
        pathname.c_str(), st.ToString().c_str());
  }
  return st;
}

//
// Create stream if it does not already exists
//
Status KinesisSystem::CreateStream(
		AwsEnv* env,
		std::shared_ptr<Logger> info_log,
		std::shared_ptr<Aws::Kinesis::KinesisClient> client,
		const std::string& bucket_prefix) {

  Aws::String topic = GetStreamName(bucket_prefix);

  // create stream
  CreateStreamRequest create_request;
  create_request.SetStreamName(topic);
  create_request.SetShardCount(1);
  CreateStreamOutcome outcome = client->CreateStream(create_request);
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
    st = WaitForStreamReady(env, info_log, client, bucket_prefix);
  }
  return st;
}

//
// Wait for stream to be ready
//
Status KinesisSystem::WaitForStreamReady(
		AwsEnv* env,
		std::shared_ptr<Logger> info_log,
		std::shared_ptr<Aws::Kinesis::KinesisClient> client,
		const std::string& bucket_prefix) {
  Aws::String topic = GetStreamName(bucket_prefix);

  // Keep looping if the stream is being initialized
  uint64_t start = env->NowMicros();
  bool isSuccess = false;
  Status st;

  while (!isSuccess) {
    // Find the number of shards for this stream. It should be 1 shard.
    st = Status::OK();
    DescribeStreamRequest request;
    request.SetStreamName(topic);
    DescribeStreamOutcome outcome = client->DescribeStream(request);
    isSuccess = outcome.IsSuccess();
    if (!isSuccess) {
      const Aws::Client::AWSError<KinesisErrors>& error = outcome.GetError();
      st = Status::IOError(topic.c_str(), error.GetMessage().c_str());
      Log(InfoLogLevel::DEBUG_LEVEL, info_log,
	  " Waiting for stream ready %s", st.ToString().c_str());
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
    if (start + retry_period_micros < env->NowMicros()) {
      break;
    }
  }
  return st;
}

Status KinesisSystem::InitializeShards() {
  // Keep looking for about 10 seconds, in case the stream was newly created
  // and is being initialized.
  Status st = WaitForStreamReady(env_, info_log_, env_->kinesis_client_,
		                 env_->bucket_prefix_);
  if (!st.ok()) {
    return st;
  }

  // Find the number of shards for this stream. It should be 1 shard.
  DescribeStreamRequest request;
  request.SetStreamName(topic_);
  DescribeStreamOutcome outcome = env_->kinesis_client_->DescribeStream(request);
  bool isSuccess = outcome.IsSuccess();
  if (!isSuccess) {
    const Aws::Client::AWSError<KinesisErrors>& error = outcome.GetError();
    st = Status::IOError(topic_.c_str(), error.GetMessage().c_str()),
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[kinesis] S3ReadableFile file %s Unable to find shards %s",
        topic_.c_str(), st.ToString().c_str());
  } else {
    const DescribeStreamResult& result = outcome.GetResult();
    const StreamDescription& description = result.GetStreamDescription();

    // append all shards to global list
    auto& shards = description.GetShards();
    assert (shards.size() == 1);
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

//
// Set shard iterator for every shard to position specified by
// shards_position_
//
void KinesisSystem::SeekShards() {
    // Check all shard iterators
    for (size_t i = 0; i < shards_.size(); i++) {
      if (shards_iterator_[i].size() != 0) {
        continue;          // iterator is still valid, nothing to do
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
      GetShardIteratorOutcome outcome = env_->kinesis_client_->GetShardIterator(request);
      bool isSuccess = outcome.IsSuccess();
      if (!isSuccess) {
        const Aws::Client::AWSError<KinesisErrors>& error = outcome.GetError();
        status_ = Status::IOError(topic_.c_str(), error.GetMessage().c_str());
        Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
            "[kinesis] S3ReadableFile file %s Unable to find shards %s",
  	  topic_.c_str(), status_.ToString().c_str());
      } else {
        const GetShardIteratorResult& result = outcome.GetResult();
	shards_iterator_[i] = result.GetShardIterator();
      }
    }
}

//
// Serialize a log-file-append to a data record.
//
void KinesisSystem::SerializeLogRecordAppend(const Slice& filename,
		const Slice& data, uint64_t offset, std::string* out) {
  // write the operation type
  PutVarint32(out, Append);

  // write out the offset in file where the data needs to be written
  PutFixed64(out, offset);

  // write out the filename
  PutLengthPrefixedSlice(out, filename);

  // write out the data
  PutLengthPrefixedSlice(out, data);
}

//
// Serialize a log-file-close to a data record.
//
void KinesisSystem::SerializeLogRecordClosed(
		const Slice& filename,
		uint64_t file_size,
		std::string* out) {
  // write the operation type
  PutVarint32(out, Closed);

  // write out the file size
  PutFixed64(out, file_size);

  // write out the filename
  PutLengthPrefixedSlice(out, filename);
}

//
// Serialize a log-file-delete to a data record.
//
void KinesisSystem::SerializeLogRecordDelete(
		const std::string& filename,
		std::string* out) {
  // write the operation type
  PutVarint32(out, Delete);

  // write out the filename
  PutLengthPrefixedSlice(out, filename);
}

//
// Extract a log record
//
bool KinesisSystem::ExtractLogRecord(const Slice& input,
       uint32_t* operation,
       Slice* filename,
       uint64_t* offset_in_file,
       uint64_t* file_size,
       Slice* data) {
  Slice in = input;
  if (in.size() < 1) {
    return false;
  }
  // extract operation
  if (!GetVarint32(&in, operation)) {
    return false;
  }
  if (*operation == Append) {
    *file_size = 0;
    if (!GetFixed64(&in, offset_in_file) || // extract offset in file
        !GetLengthPrefixedSlice(&in, filename) || // extract filename
        !GetLengthPrefixedSlice(&in, data)) {  // extract file contents
      return false;
    }
  } else if (*operation == Delete) {
    *file_size = 0;
    *offset_in_file = 0;
    if (!GetLengthPrefixedSlice(&in, filename)) { // extract filename
      return false;
    }
  } else if (*operation == Closed) {
    *offset_in_file = 0;
    if (!GetFixed64(&in, file_size) || // extract filesize
        !GetLengthPrefixedSlice(&in, filename)) { // extract filename
      return false;
    }
  } else {
    return false;
  }
  return true;
}

KinesisWritableFile::KinesisWritableFile(AwsEnv* env,
		      const std::string& fname,
		      const EnvOptions& options)
      : env_(env), fname_(fname), current_offset_(0) {

    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[kinesis] WritableFile opened file %s",
	fname_.c_str());
    topic_ = GetStreamName(env_->bucket_prefix_);
}

KinesisWritableFile::~KinesisWritableFile() {
}

Status KinesisWritableFile::Append(const Slice& data) {
    assert(status_.ok());

    // create write request
    PutRecordRequest request;
    request.SetStreamName(topic_);
    request.SetPartitionKey(Aws::String(fname_.c_str(), fname_.size()));

    // serialize write record
    std::string buffer;
    KinesisSystem::SerializeLogRecordAppend(fname_, data, current_offset_, &buffer);
    request.SetData(Aws::Utils::ByteBuffer(
                    (const unsigned char*)buffer.c_str(), buffer.size()));

    // write to stream
    const PutRecordOutcome& outcome = env_->kinesis_client_->PutRecord(request);
    bool isSuccess = outcome.IsSuccess();

    if (!isSuccess) {
      const Aws::Client::AWSError<KinesisErrors>& error = outcome.GetError();
      Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
          "[kinesis] NewWritableFile src %s Append error %s",
	  fname_.c_str(), error.GetMessage().c_str());
      return Status::IOError(fname_, error.GetMessage().c_str());
    }
    current_offset_ += data.size();
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[kinesis] WritableFile Append file %s %ld %ld",
	fname_.c_str(), data.size(), buffer.size());
    return Status::OK();
}

Status KinesisWritableFile::Close() {
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[kinesis] S3WritableFile closing %s", fname_.c_str());
    assert(status_.ok());

    // create write request
    PutRecordRequest request;
    request.SetStreamName(topic_);
    request.SetPartitionKey(Aws::String(fname_.c_str(), fname_.size()));

    // serialize write record
    std::string buffer;
    KinesisSystem::SerializeLogRecordClosed(fname_, current_offset_, &buffer);
    request.SetData(Aws::Utils::ByteBuffer(
                    (const unsigned char*)buffer.c_str(), buffer.size()));

    // write to stream
    const PutRecordOutcome& outcome = env_->kinesis_client_->PutRecord(request);
    bool isSuccess = outcome.IsSuccess();

    if (!isSuccess) {
      const Aws::Client::AWSError<KinesisErrors>& error = outcome.GetError();
      Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
          "[kinesis] NewWritableFile src %s Close error %s",
	  fname_.c_str(), error.GetMessage().c_str());
      return Status::IOError(fname_, error.GetMessage().c_str());
    }
    current_offset_ += buffer.size();
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[kinesis] WritableFile Close file %s %ld",
	fname_.c_str(), buffer.size());
    return Status::OK();
}

//
// Log a delete record to stream
//
Status KinesisWritableFile::LogDelete() {
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[kinesis] LogDelete %s", fname_.c_str());
    assert(status_.ok());

    // create write request
    PutRecordRequest request;
    request.SetStreamName(topic_);
    request.SetPartitionKey(Aws::String(fname_.c_str(), fname_.size()));

    // serialize write record
    std::string buffer;
    KinesisSystem::SerializeLogRecordDelete(fname_, &buffer);
    request.SetData(Aws::Utils::ByteBuffer(
                    (const unsigned char*)buffer.c_str(), buffer.size()));

    // write to stream
    const PutRecordOutcome& outcome = env_->kinesis_client_->PutRecord(request);
    bool isSuccess = outcome.IsSuccess();

    if (!isSuccess) {
      const Aws::Client::AWSError<KinesisErrors>& error = outcome.GetError();
      Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
          "[kinesis] LogDelete src %s error %s",
	  fname_.c_str(), error.GetMessage().c_str());
      return Status::IOError(fname_, error.GetMessage().c_str());
    }
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[kinesis] LogDelete file %s %ld",
	fname_.c_str(), buffer.size());
    return Status::OK();
}

}  // namespace

#endif /* USE_AWS */

