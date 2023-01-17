//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "logging/event_logger.h"

#include <cassert>
#include <cinttypes>
#include <sstream>
#include <string>

#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

EventLoggerStream::EventLoggerStream(Logger* logger)
    : logger_(logger),
      log_buffer_(nullptr),
      max_log_size_(0),
      json_writer_(nullptr) {}

EventLoggerStream::EventLoggerStream(LogBuffer* log_buffer,
                                     const size_t max_log_size)
    : logger_(nullptr),
      log_buffer_(log_buffer),
      max_log_size_(max_log_size),
      json_writer_(nullptr) {}

EventLoggerStream::~EventLoggerStream() {
  if (json_writer_) {
    json_writer_->EndObject();
#ifdef ROCKSDB_PRINT_EVENTS_TO_STDOUT
    printf("%s\n", json_writer_->Get().c_str());
#else
    if (logger_) {
      EventLogger::Log(logger_, *json_writer_);
    } else if (log_buffer_) {
      assert(max_log_size_);
      EventLogger::LogToBuffer(log_buffer_, *json_writer_, max_log_size_);
    }
#endif
    delete json_writer_;
  }
}

void EventLogger::Log(const JSONWriter& jwriter) { Log(logger_, jwriter); }

void EventLogger::Log(Logger* logger, const JSONWriter& jwriter) {
#ifdef ROCKSDB_PRINT_EVENTS_TO_STDOUT
  printf("%s\n", jwriter.Get().c_str());
#else
  ROCKSDB_NAMESPACE::Log(logger, "%s %s", Prefix(), jwriter.Get().c_str());
#endif
}

void EventLogger::LogToBuffer(LogBuffer* log_buffer, const JSONWriter& jwriter,
                              const size_t max_log_size) {
#ifdef ROCKSDB_PRINT_EVENTS_TO_STDOUT
  printf("%s\n", jwriter.Get().c_str());
#else
  assert(log_buffer);
  ROCKSDB_NAMESPACE::LogToBuffer(log_buffer, max_log_size, "%s %s", Prefix(),
                                 jwriter.Get().c_str());
#endif
}

}  // namespace ROCKSDB_NAMESPACE
