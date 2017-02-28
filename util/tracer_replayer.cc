// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <inttypes.h>
#include <chrono>
#include <thread>
#include "db/db_impl.h"
#include "util/read_batch.h"
#include "util/sync_point.h"
#include "util/tracer_replayer.h"

namespace rocksdb {

Tracer::Tracer(Env* env, std::unique_ptr<TraceWriter>&& writer)
    : env_(env), stop_(false), num_ops_(0) {
  assert(writer != nullptr);
  trace_writer_ = std::move(writer);
  Start();
  bg_thread_.reset(new port::Thread(&Tracer::BackgroundTrace, this));
}

Tracer::~Tracer() {
  {
    Finish();
    std::lock_guard<std::mutex> lock(mutex_);
    stop_ = true;
    cv_.notify_all();
  }
  if (bg_thread_) {
    bg_thread_->join();
  }
  TEST_SYNC_POINT_CALLBACK("Tracer::~Tracer", &num_ops_);
  fprintf(stdout, "[Trace] Finish tracing: %lu ops\n", num_ops_);
  trace_writer_->Close();
}

void Tracer::Start() {
  AppendTrace(env_->NowMicros(), kTraceStart, "");
}

void Tracer::Finish() {
  std::string ops(8, '\0');
  EncodeFixed64(&ops[0], num_ops_);
  AppendTrace(env_->NowMicros(), kTraceEnd, std::move(ops));
}

void Tracer::Write(WriteBatch* batch) {
  std::string payload(batch->Data());
  AppendTrace(env_->NowMicros(), kTraceWrite, std::move(payload));
}

void Tracer::Get(ColumnFamilyHandle* column_family,
                 const Slice& key) {
  ReadBatch batch;
  batch.Get(column_family->GetID(), key);
  std::string payload(batch.Data());
  AppendTrace(env_->NowMicros(), kTraceRead, std::move(payload));
}

void Tracer::AppendTrace(uint64_t timestamp, TraceType type,
                         std::string&& value) {
  if (type != kTraceStart && type != kTraceEnd) {
    num_ops_++;
  }
  std::string key = EncodeKey(timestamp, type);
  std::lock_guard<std::mutex> lock(mutex_);
  user_queue_.emplace(key, value);
  cv_.notify_all();
}

void Tracer::BackgroundTrace() {
  std::unique_lock<std::mutex> lock(mutex_, std::defer_lock);
  while (true) {
    lock.lock();
    while (user_queue_.empty() && !stop_) {
      cv_.wait(lock);
    }
    if (!user_queue_.empty()) {
      // Record the trace in the queue
      swap(user_queue_, log_queue_);
      lock.unlock();
      while (!log_queue_.empty()) {
        trace_writer_->AddRecord(log_queue_.front().first,
                                 log_queue_.front().second);
        log_queue_.pop();
      }
    }
    if (stop_) {
      break;
    }
  }
}

std::string Tracer::EncodeKey(uint64_t timestamp, TraceType type) {
  std::string key(9, '\0');
  EncodeFixed64(&key[0], timestamp);
  key[kKeySize - 1] = static_cast<char>(type);
  return key;
}

void Replayer::DecodeKey(const std::string& key, uint64_t* timestamp,
                                TraceType* type) {
  assert(key.length() == Tracer::kKeySize);
  *timestamp = DecodeFixed64(key.data());
  *type = static_cast<TraceType>(key[Tracer::kKeySize - 1]);
}

Replayer::Replayer(DBImpl* db, std::vector<ColumnFamilyHandle*>& handles,
                   std::unique_ptr<TraceReader>&& reader)
    : db_(db), stop_(false) {
  for (ColumnFamilyHandle* cfh: handles) {
    handle_map_[cfh->GetID()] = cfh;
  }
  trace_reader_ = std::move(reader);
  bg_thread_.reset(new port::Thread(&Replayer::BackgroundReplay, this));
}

Replayer::~Replayer() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_ = true;
    cv_.notify_all();
  }
  if (bg_thread_) {
    bg_thread_->join();
    bg_thread_.reset();
  }
}

Status Replayer::WaitForReplay() {
  Status s;
  if (bg_thread_ != nullptr) {
    bg_thread_->join();
    bg_thread_.reset();
  }
  return s;
}

void Replayer::BackgroundReplay() {
  std::string key, value;
  uint64_t trace_epoch, timestamp;
  TraceType type;
  WriteOptions write_options;
  uint64_t num_ops = 0;
  uint64_t ops = 0;
  // Read the last entry for num of ops
  Status s = trace_reader_->ReadLastRecord(&key, &value);
  if (s.ok()) {
    DecodeKey(key, &timestamp, &type);
    assert(type == kTraceEnd);
    assert(value.size() == 8);
    num_ops = DecodeFixed64(value.data());
  }

  s = trace_reader_->ReadRecord(&key, &value);
  DecodeKey(key, &timestamp, &type);
  assert(type == kTraceStart);
  trace_epoch = timestamp;
  std::chrono::system_clock::time_point replay_epoch =
      std::chrono::system_clock::now();
  do {
    s = trace_reader_->ReadRecord(&key, &value);
    DecodeKey(key, &timestamp, &type);
    if (s.ok() && type != kTraceEnd) {
      ops++;
      std::this_thread::sleep_until(
          replay_epoch + std::chrono::microseconds(timestamp - trace_epoch));
      switch (type) {
        case kTraceWrite: {
          WriteBatch write_batch(value);
          db_->Write(write_options, &write_batch);
          break;
        }
        case kTraceRead: {
          ReadBatch read_batch(value);
          read_batch.Execute(db_, handle_map_);
          break;
        }
        case kTraceIterator:
        default:
          break;
      }
      if (num_ops != 0) {
        fprintf(stderr, "... finished %3.2f%%\r", ops * 1.0f / num_ops * 100);
      }
    } else {
      break;
    }
  } while (!stop_);
  assert(ops == num_ops);
  TEST_SYNC_POINT_CALLBACK("Replay::BackgroundReplay", &ops);
  fprintf(stderr, "[Replay] Finished %" PRIu64 " ops\n", ops);
}

}  // namespace rocksdb
